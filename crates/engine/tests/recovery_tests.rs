//! Primitive Recovery Tests
//!
//! Tests verifying that ALL primitives survive crash + WAL replay.
//! The recovery contract ensures:
//! - Sequence numbers: Preserved
//! - Secondary indices: Replayed, not rebuilt
//! - Derived keys (hashes): Stored, not recomputed

use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::database::OpenSpec;
use strata_engine::SearchSubsystem;
use strata_engine::{Database, SearchRequest};
use strata_engine::{EventLog, KVStore};
use tempfile::TempDir;

/// Helper to create an object payload with a string value
fn string_payload(s: &str) -> Value {
    Value::object(HashMap::from([(
        "data".to_string(),
        Value::String(s.into()),
    )]))
}

/// Helper to create an object payload with an integer value
fn int_payload(v: i64) -> Value {
    Value::object(HashMap::from([("value".to_string(), Value::Int(v))]))
}

fn setup() -> (Arc<Database>, TempDir, BranchId) {
    let temp_dir = TempDir::new().unwrap();
    let db =
        Database::open_runtime(OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    let branch_id = BranchId::new();
    (db, temp_dir, branch_id)
}

/// Helper to get the path from TempDir
fn get_path(temp_dir: &TempDir) -> PathBuf {
    temp_dir.path().to_path_buf()
}

/// Test KV data survives recovery
#[test]
fn test_kv_survives_recovery() {
    let (db, temp_dir, branch_id) = setup();
    let path = get_path(&temp_dir);

    // Write KV data
    let kv = KVStore::new(db.clone());
    kv.put(
        &branch_id,
        "default",
        "key1",
        Value::String("value1".into()),
    )
    .unwrap();
    kv.put(&branch_id, "default", "key2", Value::Int(42))
        .unwrap();
    kv.put(&branch_id, "default", "nested/path/key", Value::Bool(true))
        .unwrap();

    // Verify before crash
    assert_eq!(
        kv.get(&branch_id, "default", "key1").unwrap(),
        Some(Value::String("value1".into()))
    );

    // Simulate crash
    drop(kv);
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let kv = KVStore::new(db.clone());

    // Data survived
    assert_eq!(
        kv.get(&branch_id, "default", "key1").unwrap(),
        Some(Value::String("value1".into()))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "key2").unwrap(),
        Some(Value::Int(42))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "nested/path/key").unwrap(),
        Some(Value::Bool(true))
    );

    // Can still write after recovery
    kv.put(
        &branch_id,
        "default",
        "key3",
        Value::String("after_recovery".into()),
    )
    .unwrap();
    assert_eq!(
        kv.get(&branch_id, "default", "key3").unwrap(),
        Some(Value::String("after_recovery".into()))
    );
}

/// Test KV list survives recovery
#[test]
fn test_kv_list_survives_recovery() {
    let (db, temp_dir, branch_id) = setup();
    let path = get_path(&temp_dir);

    let kv = KVStore::new(db.clone());

    // Create multiple keys with prefix
    kv.put(&branch_id, "default", "config/a", Value::Int(1))
        .unwrap();
    kv.put(&branch_id, "default", "config/b", Value::Int(2))
        .unwrap();
    kv.put(&branch_id, "default", "config/c", Value::Int(3))
        .unwrap();
    kv.put(&branch_id, "default", "other/x", Value::Int(99))
        .unwrap();

    // Verify list before crash
    let config_keys = kv.list(&branch_id, "default", Some("config/")).unwrap();
    assert_eq!(config_keys.len(), 3);

    // Simulate crash
    drop(kv);
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let kv = KVStore::new(db.clone());

    // List still works
    let config_keys = kv.list(&branch_id, "default", Some("config/")).unwrap();
    assert_eq!(config_keys.len(), 3);
    assert!(config_keys.contains(&"config/a".to_string()));
    assert!(config_keys.contains(&"config/b".to_string()));
    assert!(config_keys.contains(&"config/c".to_string()));
}

/// Test EventLog chain survives recovery and sequences continue correctly
#[test]
fn test_event_log_chain_survives_recovery() {
    let (db, temp_dir, branch_id) = setup();
    let path = get_path(&temp_dir);

    use strata_core::contract::Version;

    let event_log = EventLog::new(db.clone());

    // Append multiple events (sequences are 0-based)
    event_log
        .append(&branch_id, "default", "event1", string_payload("payload1"))
        .unwrap();
    event_log
        .append(&branch_id, "default", "event2", string_payload("payload2"))
        .unwrap();
    event_log
        .append(&branch_id, "default", "event3", string_payload("payload3"))
        .unwrap();

    // Read to get hashes before crash
    let pre_event0 = event_log.get(&branch_id, "default", 0).unwrap().unwrap();
    let pre_event1 = event_log.get(&branch_id, "default", 1).unwrap().unwrap();
    let pre_event2 = event_log.get(&branch_id, "default", 2).unwrap().unwrap();

    let hash0 = pre_event0.value.hash;
    let hash1 = pre_event1.value.hash;
    let hash2 = pre_event2.value.hash;

    // Verify event count before crash (verify_chain removed in MVP)
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 3);

    // Simulate crash
    drop(event_log);
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let event_log = EventLog::new(db.clone());

    // Data is intact (verify_chain removed in MVP)
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 3);

    // Events readable with correct hashes
    let event0 = event_log.get(&branch_id, "default", 0).unwrap().unwrap();
    assert_eq!(event0.value.event_type, "event1");
    assert_eq!(event0.value.payload, string_payload("payload1"));
    assert_eq!(event0.value.hash, hash0);

    let event2 = event_log.get(&branch_id, "default", 2).unwrap().unwrap();
    assert_eq!(event2.value.hash, hash2);

    // Hash chaining preserved - event1 prev_hash points to event0's hash
    let event1 = event_log.get(&branch_id, "default", 1).unwrap().unwrap();
    assert_eq!(event1.value.prev_hash, hash0);
    assert_eq!(event1.value.hash, hash1);

    // Sequence continues correctly (not restarted)
    let v3 = event_log
        .append(&branch_id, "default", "event4", string_payload("payload4"))
        .unwrap();
    assert!(matches!(v3, Version::Sequence(3))); // Not 0 (would be restart)

    // Data still valid after new append (verify_chain removed in MVP)
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 4);
}

/// Test EventLog multiple events survive recovery
#[test]
fn test_event_log_multiple_events_survives_recovery() {
    let (db, temp_dir, branch_id) = setup();
    let path = get_path(&temp_dir);

    let event_log = EventLog::new(db.clone());

    // Append 5 events
    for i in 0..5 {
        event_log
            .append(&branch_id, "default", "numbered", int_payload(i))
            .unwrap();
    }

    // Simulate crash
    drop(event_log);
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let event_log = EventLog::new(db.clone());

    // Individual reads work (read_range removed in MVP)
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 5);
    assert_eq!(
        event_log
            .get(&branch_id, "default", 1)
            .unwrap()
            .unwrap()
            .value
            .sequence,
        1
    );
    assert_eq!(
        event_log
            .get(&branch_id, "default", 2)
            .unwrap()
            .unwrap()
            .value
            .sequence,
        2
    );
    assert_eq!(
        event_log
            .get(&branch_id, "default", 3)
            .unwrap()
            .unwrap()
            .value
            .sequence,
        3
    );
}

/// Test branch metadata survives recovery.
#[test]
fn test_branch_index_survives_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();

    let branches = db.branches();

    // Create branch with metadata
    let branch_meta = branches.create("test-branch").unwrap();
    let branch_name = branch_meta.name.clone();

    // Note: update_status and add_tags removed in MVP simplification

    // Verify before crash
    let branch = branches.info_versioned(&branch_name).unwrap().unwrap();
    assert_eq!(branch.value.name, "test-branch");

    // Simulate crash
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let branches = db.branches();

    // Branch preserved
    let recovered = branches.info_versioned(&branch_name).unwrap().unwrap();
    assert_eq!(recovered.value.name, "test-branch");
}

/// Test branch listing survives recovery.
#[test]
fn test_branch_index_list_survives_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();

    let branches = db.branches();

    // Create multiple branches
    branches.create("branch1").unwrap();
    branches.create("branch2").unwrap();
    branches.create("branch3").unwrap();

    // Note: update_status and query_by_status removed in MVP simplification

    // Simulate crash
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let branches = db.branches();

    // List all branches works (includes _system_ from init_system_branch)
    let branch_names = branches.list().unwrap();
    let user_branches: Vec<_> = branch_names
        .iter()
        .filter(|b| !b.starts_with("_system"))
        .collect();
    assert_eq!(user_branches.len(), 3);
    assert!(branch_names.contains(&"branch1".to_string()));
    assert!(branch_names.contains(&"branch2".to_string()));
    assert!(branch_names.contains(&"branch3".to_string()));
}

/// Test branch delete survives recovery.
#[test]
fn test_branch_delete_survives_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();

    let branches = db.branches();
    let kv = KVStore::new(db.clone());

    // Create two branches
    let meta1 = branches.create("branch1").unwrap();
    let meta2 = branches.create("branch2").unwrap();
    let branch1 = BranchId::from_string(&meta1.branch_id).unwrap();
    let branch2 = BranchId::from_string(&meta2.branch_id).unwrap();

    // Write data to both
    kv.put(&branch1, "default", "key", Value::Int(1)).unwrap();
    kv.put(&branch2, "default", "key", Value::Int(2)).unwrap();

    // Delete branch1
    branches.delete("branch1").unwrap();

    // Simulate crash
    drop(kv);
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let branches = db.branches();
    let kv = KVStore::new(db.clone());

    // branch1 is still deleted
    assert!(branches.info("branch1").unwrap().is_none());
    assert!(kv.get(&branch1, "default", "key").unwrap().is_none());

    // branch2 data preserved
    assert!(branches.info("branch2").unwrap().is_some());
    assert_eq!(
        kv.get(&branch2, "default", "key").unwrap(),
        Some(Value::Int(2))
    );
}

/// Test cross-primitive transaction survives recovery
#[test]
fn test_cross_primitive_transaction_survives_recovery() {
    use strata_engine::{EventLogExt, KVStoreExt};

    let (db, temp_dir, branch_id) = setup();
    let path = get_path(&temp_dir);

    // Perform atomic transaction
    let result = db.transaction(branch_id, |txn| {
        txn.kv_put("txn_key", Value::String("txn_value".into()))?;
        txn.event_append("txn_event", int_payload(100))?;
        Ok(())
    });
    assert!(result.is_ok());

    // Simulate crash
    drop(db);

    // Recovery
    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    // All operations survived
    assert_eq!(
        kv.get(&branch_id, "default", "txn_key").unwrap(),
        Some(Value::String("txn_value".into()))
    );
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 1);
}

/// Test multiple sequential recoveries
#[test]
fn test_multiple_recovery_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let branch_id = BranchId::new();

    // Cycle 1: Create and populate
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());
        kv.put(&branch_id, "default", "cycle1", Value::Int(1))
            .unwrap();
    }

    // Cycle 2: Add more data
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        // Verify cycle 1 data
        assert_eq!(
            kv.get(&branch_id, "default", "cycle1").unwrap(),
            Some(Value::Int(1))
        );

        // Add cycle 2 data
        kv.put(&branch_id, "default", "cycle2", Value::Int(2))
            .unwrap();
    }

    // Cycle 3: Add more data
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        // Verify all previous data
        assert_eq!(
            kv.get(&branch_id, "default", "cycle1").unwrap(),
            Some(Value::Int(1))
        );
        assert_eq!(
            kv.get(&branch_id, "default", "cycle2").unwrap(),
            Some(Value::Int(2))
        );

        // Add cycle 3 data
        kv.put(&branch_id, "default", "cycle3", Value::Int(3))
            .unwrap();
    }

    // Final verification
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        assert_eq!(
            kv.get(&branch_id, "default", "cycle1").unwrap(),
            Some(Value::Int(1))
        );
        assert_eq!(
            kv.get(&branch_id, "default", "cycle2").unwrap(),
            Some(Value::Int(2))
        );
        assert_eq!(
            kv.get(&branch_id, "default", "cycle3").unwrap(),
            Some(Value::Int(3))
        );
    }
}

/// Test that all primitives recover together correctly
#[test]
fn test_all_primitives_recover_together() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);

    // Phase 1: Create data for all primitives
    let branch_id: BranchId;
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let branches = db.branches();
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());

        // Create branch
        let branch_meta = branches.create("full-test").unwrap();
        branch_id = BranchId::from_string(&branch_meta.branch_id).unwrap();

        // Populate all primitives
        kv.put(
            &branch_id,
            "default",
            "full_key",
            Value::String("full_value".into()),
        )
        .unwrap();

        event_log
            .append(&branch_id, "default", "full_event", int_payload(999))
            .unwrap();
    }

    // Phase 2: Verify all recovered
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let branches = db.branches();
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());

        // Branch metadata
        let branch = branches.info_versioned("full-test").unwrap().unwrap();
        assert_eq!(branch.value.name, "full-test");

        // KV
        assert_eq!(
            kv.get(&branch_id, "default", "full_key").unwrap(),
            Some(Value::String("full_value".into()))
        );

        // EventLog
        assert_eq!(event_log.len(&branch_id, "default").unwrap(), 1);
        let event = event_log.get(&branch_id, "default", 0).unwrap().unwrap();
        assert_eq!(event.value.payload, int_payload(999));
    }
}

/// Test BM25 search index survives recovery (#1486)
#[test]
fn test_search_index_survives_recovery() {
    use strata_engine::search::Searchable;

    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let branch_id = BranchId::new();

    // Session 1: index documents and verify search works
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("breast cancer cholesterol statin drugs".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "doc2",
            Value::String("cardiovascular disease heart attack prevention".into()),
        )
        .unwrap();
        kv.put(
            &branch_id,
            "default",
            "doc3",
            Value::String("machine learning neural network deep learning".into()),
        )
        .unwrap();

        // Search works in same session
        let req = SearchRequest::new(branch_id, "cholesterol");
        let response = kv.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Search should return results in same session"
        );
    }

    // Session 2: reopen and verify search still works
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        // KV data survived
        assert_eq!(
            kv.get(&branch_id, "default", "doc1").unwrap(),
            Some(Value::String(
                "breast cancer cholesterol statin drugs".into()
            ))
        );

        // Search index also survived
        let req = SearchRequest::new(branch_id, "cholesterol");
        let response = kv.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Search should return results after reopen (index must survive recovery)"
        );
    }
}

/// Test search index survives multiple recovery cycles (#1486)
#[test]
fn test_search_index_survives_multiple_recoveries() {
    use strata_engine::search::Searchable;

    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let branch_id = BranchId::new();

    // Cycle 1: Create initial data
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());
        kv.put(
            &branch_id,
            "default",
            "doc1",
            Value::String("database indexing search retrieval".into()),
        )
        .unwrap();
    }

    // Cycle 2: Add more data, verify previous data searchable
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        let req = SearchRequest::new(branch_id, "indexing");
        let response = kv.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Cycle 2: doc1 should be searchable"
        );

        kv.put(
            &branch_id,
            "default",
            "doc2",
            Value::String("information retrieval ranking algorithms".into()),
        )
        .unwrap();
    }

    // Cycle 3: Verify both documents searchable
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        let req = SearchRequest::new(branch_id, "retrieval");
        let response = kv.search(&req).unwrap();
        assert!(
            response.hits.len() >= 2,
            "Cycle 3: both docs should be searchable for 'retrieval', got {}",
            response.hits.len()
        );
    }
}

/// Issue #1710: checkpoint watermark must not include in-flight versions.
///
/// Writes data, checkpoints, writes more data, checkpoints again, then
/// simulates a crash and verifies ALL data survives recovery. This is an
/// end-to-end correctness test for the checkpoint + WAL replay path.
#[test]
fn test_issue_1710_checkpoint_recovery_preserves_all_data() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    let branch_id = BranchId::new();

    // Phase 1: Write initial data and checkpoint
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        for i in 0..10 {
            kv.put(
                &branch_id,
                "default",
                &format!("batch1_key_{}", i),
                Value::Int(i),
            )
            .unwrap();
        }

        db.flush().unwrap();
        db.checkpoint().unwrap();

        // Phase 2: Write more data after checkpoint
        for i in 0..10 {
            kv.put(
                &branch_id,
                "default",
                &format!("batch2_key_{}", i),
                Value::Int(100 + i),
            )
            .unwrap();
        }

        // Second checkpoint covers both batches
        db.flush().unwrap();
        db.checkpoint().unwrap();

        // Simulate crash
        drop(kv);
        drop(db);
    }

    // Phase 3: Recovery — all 20 keys must be present
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        for i in 0..10 {
            let val = kv
                .get(&branch_id, "default", &format!("batch1_key_{}", i))
                .unwrap();
            assert_eq!(
                val,
                Some(Value::Int(i)),
                "batch1_key_{} missing after checkpoint recovery",
                i
            );
        }

        for i in 0..10 {
            let val = kv
                .get(&branch_id, "default", &format!("batch2_key_{}", i))
                .unwrap();
            assert_eq!(
                val,
                Some(Value::Int(100 + i)),
                "batch2_key_{} missing after checkpoint recovery",
                i
            );
        }
    }
}

/// Issue #1710 concurrent variant: writes happening on multiple threads
/// while checkpoint runs, followed by crash + recovery.
#[test]
fn test_issue_1710_checkpoint_concurrent_writes_recovery() {
    use std::sync::{Arc, Barrier};

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    let branch_id = BranchId::new();
    let num_writers = 4;
    let keys_per_writer = 25;

    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        // Write initial data
        kv.put(&branch_id, "default", "sentinel", Value::Int(0))
            .unwrap();
        db.flush().unwrap();
        db.checkpoint().unwrap();

        // Concurrent writes on different threads (same branch)
        let barrier = Arc::new(Barrier::new(num_writers));
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let db_clone = db.clone();
            let kv_clone = KVStore::new(db_clone);
            let barrier_clone = Arc::clone(&barrier);

            handles.push(std::thread::spawn(move || {
                barrier_clone.wait();
                for k in 0..keys_per_writer {
                    kv_clone
                        .put(
                            &branch_id,
                            "default",
                            &format!("w{}_k{}", writer_id, k),
                            Value::Int((writer_id * 1000 + k) as i64),
                        )
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Checkpoint after concurrent writes
        db.flush().unwrap();
        db.checkpoint().unwrap();

        // Simulate crash
        drop(kv);
        drop(db);
    }

    // Recovery: all keys must be present
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        assert_eq!(
            kv.get(&branch_id, "default", "sentinel").unwrap(),
            Some(Value::Int(0)),
            "sentinel key missing"
        );

        for writer_id in 0..num_writers {
            for k in 0..keys_per_writer {
                let key_name = format!("w{}_k{}", writer_id, k);
                let val = kv.get(&branch_id, "default", &key_name).unwrap();
                assert_eq!(
                    val,
                    Some(Value::Int((writer_id * 1000 + k) as i64)),
                    "key {} missing after concurrent checkpoint recovery",
                    key_name
                );
            }
        }
    }
}

/// Issue #1908: Events committed to KV but missing from search index after crash recovery.
///
/// Simulates a crash where the search manifest is stale (written before the latest
/// events were committed). After recovery, the fast-path mmap load must reconcile
/// against KV to re-index any missing entries.
#[test]
fn test_issue_1908_search_index_reconciles_after_crash() {
    use strata_engine::search::Searchable;

    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let branch_id = BranchId::new();
    let search_manifest = path.join("search").join("search.manifest");

    // Session 1: Write initial data and close cleanly (creates search manifest)
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        kv.put(
            &branch_id,
            "default",
            "doc_initial",
            Value::String("established baseline document for search".into()),
        )
        .unwrap();

        // Also test events
        let event_log = EventLog::new(db.clone());
        event_log
            .append(
                &branch_id,
                "default",
                "sensor_reading",
                string_payload("temperature humidity pressure"),
            )
            .unwrap();
    }

    // Save the search manifest (represents state before crash)
    assert!(
        search_manifest.exists(),
        "search manifest should exist after clean close"
    );
    let stale_manifest = std::fs::read(&search_manifest).unwrap();

    // Session 2: Add more data, close cleanly (manifest updated with new data)
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());

        kv.put(
            &branch_id,
            "default",
            "doc_post_crash",
            Value::String("quantum computing algorithm optimization".into()),
        )
        .unwrap();

        let event_log = EventLog::new(db.clone());
        event_log
            .append(
                &branch_id,
                "default",
                "alert",
                string_payload("critical threshold exceeded anomaly"),
            )
            .unwrap();
    }

    // Simulate crash: revert search manifest to pre-session-2 state.
    // KV still has session 2 data (WAL replay restores it), but the
    // search index manifest is stale — missing session 2 entries.
    std::fs::write(&search_manifest, &stale_manifest).unwrap();

    // Session 3: Reopen — fast path loads stale manifest.
    // Without the fix, session 2 data is in KV but invisible to search.
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());

        // KV data from session 2 must exist (WAL replay)
        assert_eq!(
            kv.get(&branch_id, "default", "doc_post_crash").unwrap(),
            Some(Value::String(
                "quantum computing algorithm optimization".into()
            )),
            "KV data from session 2 should survive via WAL replay"
        );

        // Search for session 1 data — should always work
        let req = SearchRequest::new(branch_id, "baseline");
        let response = kv.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Session 1 KV data should be searchable"
        );

        // Search for session 2 KV data — fails without the fix
        let req = SearchRequest::new(branch_id, "quantum");
        let response = kv.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Session 2 KV data should be searchable after crash recovery reconciliation"
        );

        // Search for session 2 event data — fails without the fix.
        // Use EventLog::search (not KVStore::search) because each
        // primitive's search() filters to its own EntityRef variant.
        // KVStore::search used to return all variants from the shared
        // inverted index, but that pre-existing inconsistency was
        // fixed alongside the v0.3 Gap A+C work — every primitive now
        // returns only its own type, mirroring how JSON/Event/Graph
        // already worked.
        let req = SearchRequest::new(branch_id, "anomaly");
        let response = event_log.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Session 2 event data should be searchable after crash recovery reconciliation"
        );
    }
}

// =============================================================================
// Phase 1 — Search space-correctness tests
// =============================================================================

/// Phase 1 Part 1: BM25 search must filter by `space`, not just `branch_id`.
///
/// Two KV writes share the same key in two different spaces. Searching
/// from each space must return only that space's row.
#[test]
fn test_bm25_search_isolates_by_space_kv() {
    use strata_engine::search::Searchable;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let kv = KVStore::new(db.clone());

    // Same key, different spaces, identical text → identical BM25 candidates
    kv.put(
        &branch_id,
        "tenant_a",
        "shared_key",
        Value::String("alpha bravo charlie delta".into()),
    )
    .unwrap();
    kv.put(
        &branch_id,
        "tenant_b",
        "shared_key",
        Value::String("alpha bravo charlie delta".into()),
    )
    .unwrap();

    // Search from tenant_a
    let req_a = SearchRequest::new(branch_id, "bravo").with_space("tenant_a");
    let resp_a = kv.search(&req_a).unwrap();
    assert_eq!(
        resp_a.hits.len(),
        1,
        "tenant_a search should return exactly its own row, got {}",
        resp_a.hits.len()
    );
    if let strata_engine::search::EntityRef::Kv { space, .. } = &resp_a.hits[0].doc_ref {
        assert_eq!(space, "tenant_a", "hit must belong to tenant_a");
    } else {
        panic!("expected EntityRef::Kv");
    }

    // Search from tenant_b
    let req_b = SearchRequest::new(branch_id, "bravo").with_space("tenant_b");
    let resp_b = kv.search(&req_b).unwrap();
    assert_eq!(resp_b.hits.len(), 1);
    if let strata_engine::search::EntityRef::Kv { space, .. } = &resp_b.hits[0].doc_ref {
        assert_eq!(space, "tenant_b");
    } else {
        panic!("expected EntityRef::Kv");
    }
}

/// Phase 1 Part 1: same as above for `EventLog`.
#[test]
fn test_bm25_search_isolates_by_space_event() {
    use strata_engine::search::Searchable;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    let db =
        Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem)).unwrap();
    let event_log = EventLog::new(db.clone());

    event_log
        .append(
            &branch_id,
            "tenant_a",
            "alert",
            string_payload("anomaly threshold breach"),
        )
        .unwrap();
    event_log
        .append(
            &branch_id,
            "tenant_b",
            "alert",
            string_payload("anomaly threshold breach"),
        )
        .unwrap();

    let req_a = SearchRequest::new(branch_id, "anomaly").with_space("tenant_a");
    let resp_a = event_log.search(&req_a).unwrap();
    assert_eq!(resp_a.hits.len(), 1, "tenant_a should see only its event");
    if let strata_engine::search::EntityRef::Event { space, .. } = &resp_a.hits[0].doc_ref {
        assert_eq!(space, "tenant_a");
    } else {
        panic!("expected EntityRef::Event");
    }

    let req_b = SearchRequest::new(branch_id, "anomaly").with_space("tenant_b");
    let resp_b = event_log.search(&req_b).unwrap();
    assert_eq!(resp_b.hits.len(), 1);
    if let strata_engine::search::EntityRef::Event { space, .. } = &resp_b.hits[0].doc_ref {
        assert_eq!(space, "tenant_b");
    } else {
        panic!("expected EntityRef::Event");
    }
}

/// Phase 1 Part 3: JSON documents must survive a slow-path BM25 rebuild.
///
/// Writes a JSON doc, then deletes the search manifest to force a slow-path
/// rebuild on the next open. Without Part 3 the JSON doc would silently
/// vanish from BM25.
#[test]
fn test_json_survives_bm25_slow_path_rebuild() {
    use strata_core::JsonValue;
    use strata_engine::search::Searchable;
    use strata_engine::JsonStore;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Session 1: write a JSON doc
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let json = JsonStore::new(db.clone());

        let doc = JsonValue::from_value(serde_json::json!({
            "title": "breakthrough quantum supremacy result"
        }));
        json.create(&branch_id, "tenant_a", "doc1", doc).unwrap();

        // Sanity: searchable in same session
        let req = SearchRequest::new(branch_id, "supremacy").with_space("tenant_a");
        let resp = json.search(&req).unwrap();
        assert!(!resp.hits.is_empty(), "doc must be searchable in session 1");
    }

    // Wipe the search index manifest to force a slow-path rebuild on reopen.
    let search_dir = path.join("search");
    if search_dir.exists() {
        std::fs::remove_dir_all(&search_dir).unwrap();
    }

    // Session 2: reopen — slow path must re-index the JSON doc
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let json = JsonStore::new(db.clone());

        let req = SearchRequest::new(branch_id, "supremacy").with_space("tenant_a");
        let resp = json.search(&req).unwrap();
        assert_eq!(
            resp.hits.len(),
            1,
            "JSON doc must survive slow-path rebuild — got {} hits",
            resp.hits.len()
        );

        if let strata_engine::search::EntityRef::Json { space, doc_id, .. } = &resp.hits[0].doc_ref
        {
            assert_eq!(space, "tenant_a", "rebuilt ref must carry the real space");
            assert_eq!(doc_id, "doc1");
        } else {
            panic!("expected EntityRef::Json");
        }
    }
}

/// Phase 1 Part 3 — regression: JSON entries from internal `_idx/` and
/// `_idx_meta/` spaces must NOT be indexed by the slow-path BM25 rebuild.
///
/// This test exercises the filter directly by writing a real, deserializable
/// `JsonDoc` into a `_idx_meta/...` space (bypassing the executor's
/// space-name validator). Without the explicit space-prefix skip in
/// `recovery.rs`, this entry would be re-indexed during the slow path
/// because its bytes deserialize cleanly — it's only protected today by
/// the explicit `is_json_internal_space()` filter, not by accidents in the
/// deserialization path.
#[test]
fn test_json_slow_path_skips_secondary_index_storage() {
    use strata_core::JsonValue;
    use strata_engine::primitives::json::index::IndexType;
    use strata_engine::search::Searchable;
    use strata_engine::JsonStore;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Session 1: write a real user doc, create a secondary index, AND
    // smuggle a real JsonDoc into `_idx_meta/tenant_a` so we test the
    // filter against a doc that *would* deserialize successfully if the
    // filter weren't there.
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let json = JsonStore::new(db.clone());

        let doc = JsonValue::from_value(serde_json::json!({
            "title": "alpha bravo charlie",
            "category": "books"
        }));
        json.create(&branch_id, "tenant_a", "doc1", doc).unwrap();

        // Create a secondary index — writes JSON-encoded IndexDef metadata
        // at `_idx_meta/tenant_a` (TypeTag::Json). This is the natural
        // shape of internal storage.
        json.create_index(&branch_id, "tenant_a", "by_cat", "category", IndexType::Tag)
            .unwrap();

        // Force a real deserializable JsonDoc into `_idx_meta/tenant_a`
        // by calling `JsonStore::create` directly. The executor would
        // reject this space name, but the engine doesn't validate.
        // Without the filter, this would be re-indexed by the slow path.
        let smuggled = JsonValue::from_value(serde_json::json!({
            "title": "smuggled stowaway document"
        }));
        json.create(&branch_id, "_idx_meta/tenant_a", "spy", smuggled)
            .unwrap();
    }

    // Wipe the search dir to force a slow-path rebuild.
    let search_dir = path.join("search");
    if search_dir.exists() {
        std::fs::remove_dir_all(&search_dir).unwrap();
    }

    // Session 2: reopen — slow path should index doc1 but skip both the
    // serde-encoded IndexDef metadata AND the smuggled JsonDoc.
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        let json = JsonStore::new(db.clone());

        // doc1 is searchable in tenant_a
        let req = SearchRequest::new(branch_id, "alpha").with_space("tenant_a");
        let resp = json.search(&req).unwrap();
        assert_eq!(resp.hits.len(), 1, "doc1 must survive rebuild");

        // The smuggled `_idx_meta/tenant_a` doc is a real JsonDoc whose
        // deserialization would succeed — the only thing keeping it out
        // of BM25 is the space-prefix filter. This is the actual
        // regression check.
        let req_smug = SearchRequest::new(branch_id, "stowaway").with_space("_idx_meta/tenant_a");
        let resp_smug = json.search(&req_smug).unwrap();
        assert_eq!(
            resp_smug.hits.len(),
            0,
            "smuggled JSON in internal `_idx_meta/` space must be skipped by slow-path rebuild"
        );

        // Belt-and-braces: secondary-index metadata also must not surface
        // (this part is protected by both the filter AND the
        // deserialization fallback).
        let req_meta = SearchRequest::new(branch_id, "by_cat").with_space("_idx_meta/tenant_a");
        let resp_meta = json.search(&req_meta).unwrap();
        assert_eq!(resp_meta.hits.len(), 0);
    }
}

// =============================================================================
// Phase 3 — Lower-layer registration, discovery, repair, startup wiring
// =============================================================================
//
// These tests pin every part of Phase 3 to a single revert-checkable test:
//
//   Part 1 (engine-layer registration)
//     test_kv_put_registers_space_at_engine_layer
//     test_json_create_registers_space_at_engine_layer
//     (vector/graph tests moved to tests/integration/recovery_cross_crate.rs)
//
//   Part 3 (discovery + repair + union list/exists + startup wiring)
//     test_discover_used_spaces_finds_data_in_unregistered_space
//     test_discover_used_spaces_excludes_system_space
//     test_discover_used_spaces_filters_tombstones
//     test_list_returns_union_of_metadata_and_discovered
//     test_exists_returns_true_for_unregistered_space_with_data
//     test_repair_space_metadata_is_idempotent
//     test_startup_repair_registers_orphan_data_spaces
//
// Empirical revert verification: revert each fix in turn and confirm the
// matching test fails.

/// Storage-layer bypass: write a KV entry to `(branch, space)` without
/// going through any primitive. Used by Phase 3 discovery / repair tests
/// to construct the exact "orphan data" state that must be repaired.
fn bypass_kv_put(
    db: &Arc<Database>,
    branch_id: BranchId,
    space: &str,
    user_key: &[u8],
    value: Value,
) {
    use std::sync::Arc as StdArc;
    use strata_storage::{Key, Namespace, TypeTag};

    let ns = StdArc::new(Namespace::for_branch_space(branch_id, space));
    let key = Key::new(ns, TypeTag::KV, user_key.to_vec());
    db.transaction(branch_id, |txn| txn.put(key, value))
        .unwrap();
}

/// Metadata-only check: does the space have an entry in `SpaceIndex`'s
/// metadata key namespace? This bypasses the public `list/exists` union and
/// is the only way to verify that Part 1's lower-layer registration helper
/// actually fired — the union path would otherwise mask a missing
/// registration via the data scan.
fn space_metadata_exists(db: &Arc<Database>, branch_id: BranchId, space: &str) -> bool {
    use strata_storage::Key;

    db.transaction(branch_id, |txn| {
        let key = Key::new_space(branch_id, space);
        Ok(txn.get(&key)?.is_some())
    })
    .unwrap()
}

// --- Part 1 — engine-layer registration ----------------------------------

#[test]
fn test_kv_put_registers_space_at_engine_layer() {
    let (db, _temp, branch_id) = setup();
    let kv = KVStore::new(db.clone());

    // Write directly via the engine primitive — bypass the executor's
    // pre-registration. Only Part 1's `ensure_space_registered_in_txn`
    // call inside `KVStore::put` makes the metadata key exist.
    //
    // We check the metadata key directly (not via `SpaceIndex::list`)
    // because the union behaviour of `list` would otherwise mask a
    // missing Part 1 registration with a data-scan hit.
    kv.put(&branch_id, "tenant_kv", "k1", Value::Int(7))
        .unwrap();

    assert!(
        space_metadata_exists(&db, branch_id, "tenant_kv"),
        "KVStore::put must persist a SpaceIndex metadata key for its space"
    );
}

#[test]
fn test_json_create_registers_space_at_engine_layer() {
    use strata_core::JsonValue;
    use strata_engine::JsonStore;

    let (db, _temp, branch_id) = setup();
    let json = JsonStore::new(db.clone());

    let doc = JsonValue::from_value(serde_json::json!({"hello": "world"}));
    json.create(&branch_id, "tenant_json", "doc1", doc).unwrap();

    assert!(
        space_metadata_exists(&db, branch_id, "tenant_json"),
        "JsonStore::create must persist a SpaceIndex metadata key for its space"
    );
}

// Vector/graph registration tests moved to tests/integration/recovery_cross_crate.rs

// --- Part 3 — discovery / repair / union / startup -----------------------

#[test]
fn test_discover_used_spaces_finds_data_in_unregistered_space() {
    use strata_engine::SpaceIndex;

    let (db, _temp, branch_id) = setup();

    // Write data into "ghost" via the storage layer — bypassing every
    // primitive AND every metadata-write call site. Only `discover_used_spaces`
    // can resurface it.
    bypass_kv_put(&db, branch_id, "ghost", b"k1", Value::String("v".into()));

    let space_index = SpaceIndex::new(db.clone());
    let discovered = space_index.discover_used_spaces(branch_id).unwrap();
    assert!(
        discovered.contains(&"ghost".to_string()),
        "discover_used_spaces must find data in unregistered space; got {discovered:?}"
    );
}

#[test]
fn test_discover_used_spaces_excludes_system_space() {
    use strata_engine::SpaceIndex;

    let (db, _temp, branch_id) = setup();

    // Write to the reserved system space directly. Discovery must filter it.
    bypass_kv_put(
        &db,
        branch_id,
        "_system_",
        b"shadow",
        Value::String("internal".into()),
    );

    let space_index = SpaceIndex::new(db.clone());
    let discovered = space_index.discover_used_spaces(branch_id).unwrap();
    assert!(
        !discovered.contains(&"_system_".to_string()),
        "_system_ must be excluded from discover_used_spaces; got {discovered:?}"
    );
}

#[test]
fn test_discover_used_spaces_filters_tombstones() {
    use std::sync::Arc as StdArc;
    use strata_engine::SpaceIndex;
    use strata_storage::{Key, Namespace, TypeTag};

    let (db, _temp, branch_id) = setup();

    // Write one KV in "tombstone-only", then delete it. After the delete
    // the only visible MVCC entry for that key is a tombstone — discovery
    // must skip it, otherwise a fully-deleted space would phantom-register.
    let ns = StdArc::new(Namespace::for_branch_space(branch_id, "tombstone-only"));
    let key = Key::new(ns, TypeTag::KV, b"k1".to_vec());
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("v".into()))
    })
    .unwrap();
    db.transaction(branch_id, |txn| txn.delete(key)).unwrap();

    let space_index = SpaceIndex::new(db.clone());
    let discovered = space_index.discover_used_spaces(branch_id).unwrap();
    assert!(
        !discovered.contains(&"tombstone-only".to_string()),
        "tombstoned-only space must NOT appear in discover_used_spaces; got {discovered:?}"
    );
}

#[test]
fn test_list_returns_union_of_metadata_and_discovered() {
    use strata_engine::SpaceIndex;

    let (db, _temp, branch_id) = setup();
    let space_index = SpaceIndex::new(db.clone());

    // `a` is registered the conventional way; `b` only has bypass-written
    // data — no metadata. The post-Phase-3 union `list` must return both.
    space_index.register(branch_id, "a").unwrap();
    bypass_kv_put(&db, branch_id, "b", b"k1", Value::String("v".into()));

    let spaces = space_index.list(branch_id).unwrap();
    assert!(
        spaces.contains(&"a".to_string()),
        "metadata-registered space `a` must be in union list; got {spaces:?}"
    );
    assert!(
        spaces.contains(&"b".to_string()),
        "data-only space `b` must be in union list; got {spaces:?}"
    );
}

#[test]
fn test_exists_returns_true_for_unregistered_space_with_data() {
    use strata_engine::SpaceIndex;

    let (db, _temp, branch_id) = setup();

    bypass_kv_put(&db, branch_id, "data-only", b"k", Value::Int(1));

    let space_index = SpaceIndex::new(db.clone());
    assert!(
        space_index.exists(branch_id, "data-only").unwrap(),
        "exists() must return true for an unregistered space that has data"
    );
}

#[test]
fn test_repair_space_metadata_is_idempotent() {
    use strata_engine::SpaceIndex;

    let (db, _temp, branch_id) = setup();

    // One orphan space — repair should register exactly one new entry,
    // and the second call should find nothing left to do.
    bypass_kv_put(&db, branch_id, "orphan", b"k", Value::Int(1));

    let space_index = SpaceIndex::new(db.clone());
    let repaired_first = space_index.repair_space_metadata(branch_id).unwrap();
    assert_eq!(repaired_first, 1, "first repair should register the orphan");

    let repaired_second = space_index.repair_space_metadata(branch_id).unwrap();
    assert_eq!(
        repaired_second, 0,
        "second repair must be a no-op (idempotent)"
    );
}

#[test]
fn test_startup_repair_registers_orphan_data_spaces() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    let branch_id = BranchId::new();

    // Session 1: write orphan data via storage bypass and drop the DB.
    // No metadata is ever written for this space — the bypass helper
    // skips every primitive's registration call site.
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        bypass_kv_put(&db, branch_id, "orphan_startup", b"k", Value::Int(1));
        drop(db);
    }

    // Session 2: reopen. `repair_space_metadata_on_open` must fire BEFORE
    // we get our handle, persisting a SpaceIndex metadata key for
    // "orphan_startup". We verify this with a metadata-only check via
    // `space_metadata_exists` — bypassing the union `list/exists` so the
    // assertion fails when startup repair is reverted instead of being
    // masked by the data-scan fallback.
    {
        let db = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
            .unwrap();
        assert!(
            space_metadata_exists(&db, branch_id, "orphan_startup"),
            "startup repair must persist a SpaceIndex metadata key for orphan data"
        );
        drop(db);
    }
}

// ============================================================================
// Audit-follow-up regression for stratalab/strata-core#2354 Finding 1:
// concurrent open must not observe a half-recovered Database. See the race
// trace in the PR that introduced this test.
// ============================================================================

/// Test-only `Subsystem` that flips a flag on entry to `recover()`, sleeps
/// for a configurable delay, then flips a completion flag. Used to force an
/// observable window between "Database struct exists" and "subsystem
/// recovery complete" so a concurrent opener can try to race into it.
struct SlowRecoverySubsystem {
    started: Arc<std::sync::atomic::AtomicBool>,
    done: Arc<std::sync::atomic::AtomicBool>,
    delay: std::time::Duration,
}

impl strata_engine::Subsystem for SlowRecoverySubsystem {
    fn name(&self) -> &'static str {
        "slow-recovery-regression-test"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
        self.started
            .store(true, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.done.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// Marker subsystem with same name as `SlowRecoverySubsystem` for signature
/// compatibility. Used by concurrent openers that need to match the first
/// opener's subsystem list for hard reuse rejection.
struct SlowRecoveryMarker;

impl strata_engine::Subsystem for SlowRecoveryMarker {
    fn name(&self) -> &'static str {
        "slow-recovery-regression-test"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
        Ok(())
    }
}

/// Audit-follow-up regression for stratalab/strata-core#2354 Finding 1.
///
/// Before the fix, `acquire_primary_db` inserted the fresh `Arc<Database>`
/// into the process-global `OPEN_DATABASES` registry **before** running
/// the `subsystem.recover()` loop. A concurrent opener for the same path
/// that came in during the recovery window would upgrade the weak ref,
/// return `(db, false)`, and hand the caller a reference to a `Database`
/// whose in-memory subsystem state was still being rebuilt.
///
/// This test pins a `SlowRecoverySubsystem` on thread A and waits for
/// thread A to enter `recover()` (via the `started` flag). While thread A
/// is deterministically inside the sleep, thread B calls
/// `Database::open_runtime(OpenSpec::primary(path))` for the same path.
/// With the fix, thread B blocks on the `OPEN_DATABASES` mutex until
/// thread A finishes recovery and inserts. When thread B's call returns,
/// the `done` flag must be `true` — proving thread B saw the Arc only
/// after recovery fully completed.
#[test]
#[serial(open_databases)]
fn test_concurrent_open_blocks_until_recovery_completes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    let temp = TempDir::new().unwrap();
    let path = Arc::new(temp.path().to_path_buf());

    let started = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));

    // Thread A: opens with SlowRecoverySubsystem that sleeps 500ms inside
    // recover(). This is deterministically longer than the window the
    // main thread spends waiting for `started` + spawning thread B, so
    // thread A is guaranteed to still be in recover() when thread B tries
    // to acquire the OPEN_DATABASES mutex.
    let path_a = Arc::clone(&path);
    let started_a = Arc::clone(&started);
    let done_a = Arc::clone(&done);
    let handle_a = std::thread::spawn(move || {
        Database::open_runtime(
            OpenSpec::primary(&*path_a).with_subsystem(SlowRecoverySubsystem {
                started: started_a,
                done: done_a,
                delay: Duration::from_millis(500),
            }),
        )
        .unwrap()
    });

    // Spin until thread A has definitely entered recover(). This removes
    // any "did thread A actually start?" flakiness.
    while !started.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(5));
    }

    // Thread B: concurrent open of the same path with matching subsystem.
    // With the fix, this blocks on OPEN_DATABASES until thread A finishes
    // recovery and publishes the Arc. Without the fix, thread B returns
    // immediately and the `done` flag load below is `false`.
    // Note: Must use SlowRecoveryMarker (same name) to pass hard reuse rejection.
    let path_b = Arc::clone(&path);
    let done_b = Arc::clone(&done);
    let handle_b = std::thread::spawn(move || {
        let db =
            Database::open_runtime(OpenSpec::primary(&*path_b).with_subsystem(SlowRecoveryMarker))
                .unwrap();
        let observed_done = done_b.load(Ordering::SeqCst);
        (db, observed_done)
    });

    let db_a = handle_a.join().unwrap();
    let (db_b, b_observed_recovery_done) = handle_b.join().unwrap();

    assert!(
        b_observed_recovery_done,
        "concurrent opener returned a Database before the first opener's \
         subsystem recovery completed — OPEN_DATABASES registry published \
         the Arc too early (Finding 1 regressed). See audit follow-up to #2354."
    );

    // Thread B should have taken the registry fast path and returned the
    // same `Arc<Database>` as thread A (singleton-per-path invariant).
    // Without this assertion, the test could pass spuriously if thread B
    // fell through to its own independent open — in which case thread B's
    // `done` flag would still be `true` (thread A already set it), but
    // we would have two distinct `Database` instances for the same path,
    // violating the singleton contract.
    assert!(
        Arc::ptr_eq(&db_a, &db_b),
        "concurrent openers for the same path must be deduped through \
         OPEN_DATABASES — thread B got a different Arc than thread A, \
         meaning thread B ran its own parallel open instead of taking \
         the registry fast path."
    );
}

/// Test-only `Subsystem` whose `recover()` panics. Used to verify that
/// a recovery panic does not deadlock against the OPEN_DATABASES mutex
/// (see `test_recovery_panic_does_not_deadlock`). The `Drop for Database`
/// impl must use `try_lock` on the registry so unwinding through the
/// `Arc` in `acquire_primary_db`'s locked region does not re-enter the
/// same non-reentrant `parking_lot::Mutex`.
struct PanickingSubsystem;

impl strata_engine::Subsystem for PanickingSubsystem {
    fn name(&self) -> &'static str {
        "panicking-regression-test"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
        panic!("test subsystem intentionally panics during recover");
    }
}

/// Test-only `Subsystem` whose `recover()` always returns `Err`. Used to
/// verify that a recovery failure does not deadlock against the
/// OPEN_DATABASES mutex (see `test_recovery_failure_does_not_deadlock`).
struct AlwaysFailingSubsystem;

impl strata_engine::Subsystem for AlwaysFailingSubsystem {
    fn name(&self) -> &'static str {
        "always-failing-regression-test"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
        Err(strata_core::StrataError::internal(
            "test subsystem intentionally fails recovery",
        ))
    }
}

/// Recovery-failure must not deadlock on `OPEN_DATABASES`.
///
/// With the Finding 1 fix, `acquire_primary_db` holds the
/// `OPEN_DATABASES` mutex across `subsystem.recover()`. If a subsystem
/// returns `Err`, Rust unwinds the function and drops locals in reverse
/// declaration order: the `Arc<Database>` drops **before** the
/// `MutexGuard`. `Drop for Database` then tries to reacquire
/// `OPEN_DATABASES` to remove itself from the registry, and because
/// `parking_lot::Mutex` is non-reentrant, the same thread deadlocks
/// against its own still-held guard.
///
/// The fix must drop the guard before `return Err(...)` on the recovery
/// error path. This test exercises that path by passing an
/// `AlwaysFailingSubsystem` through `Database::open_runtime(OpenSpec)`.
/// A failing version of this fix will deadlock and never return — the
/// test is wrapped in a joined thread with a hard deadline so a deadlock
/// surfaces as a visible timeout rather than a hung test binary.
#[test]
#[serial(open_databases)]
fn test_recovery_failure_does_not_deadlock() {
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::Duration;

    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Run the open on a worker thread and signal completion via an
    // mpsc channel. If the main thread times out waiting for the
    // signal, we know the worker deadlocked.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result =
            Database::open_runtime(OpenSpec::primary(&path).with_subsystem(AlwaysFailingSubsystem));
        let _ = tx.send(result.is_err());
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(true) => {
            // Correct: recovery failed and the error propagated without
            // deadlocking. The Arc dropped cleanly, Drop for Database
            // removed any stale registry entry, and the function
            // returned Err.
        }
        Ok(false) => panic!(
            "AlwaysFailingSubsystem unexpectedly succeeded — test is \
             not exercising the failure path it was designed for"
        ),
        Err(RecvTimeoutError::Timeout) => panic!(
            "acquire_primary_db deadlocked on recovery failure. \
             Drop for Database tries to reacquire OPEN_DATABASES, but \
             acquire_primary_db was still holding the guard when the \
             Arc unwound through it. The error path must drop the \
             registry guard BEFORE the Arc drops. See audit follow-up \
             to #2354 Finding 1 review."
        ),
        Err(RecvTimeoutError::Disconnected) => panic!(
            "worker thread exited without sending a result — likely \
             panicked unexpectedly. This is NOT the deadlock this test \
             is designed to catch; rerun with RUST_BACKTRACE=1 to see \
             the underlying panic."
        ),
    }
}

/// Recovery-panic must not deadlock on `OPEN_DATABASES`.
///
/// This is the panic-unwinding companion to
/// `test_recovery_failure_does_not_deadlock`. A subsystem's `recover()`
/// can panic (not just return `Err`) — e.g. a bug, a failed assertion,
/// or an arithmetic overflow in the rebuild path. Rust unwinds out of
/// `acquire_primary_db` and drops the `Arc<Database>` before the
/// `OPEN_DATABASES` `MutexGuard` it was holding, because the Arc was
/// declared later in the function. `Drop for Database` then tries to
/// remove the database's entry from `OPEN_DATABASES`, which must use
/// `try_lock` to avoid self-deadlocking against the still-held guard.
///
/// The fix is in `Drop for Database` (see `crates/engine/src/database/mod.rs`):
/// the registry-remove path uses `OPEN_DATABASES.try_lock()` and silently
/// skips the remove on contention. Stale weak refs self-heal — the next
/// call to `acquire_primary_db` for this path will find the dead weak
/// ref, fail to upgrade, fall through to a fresh open, and overwrite
/// the entry.
///
/// Like the Err-path test, this runs on a worker thread with a bounded
/// deadline so a deadlock surfaces as a visible timeout.
#[test]
#[serial(open_databases)]
fn test_recovery_panic_does_not_deadlock() {
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::Duration;

    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        // catch_unwind the panic so it doesn't abort the test process.
        // The worker thread reports back whether it reached here at all
        // (i.e. whether it did not deadlock).
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Database::open_runtime(OpenSpec::primary(&path).with_subsystem(PanickingSubsystem))
        }));
        let _ = tx.send(caught.is_err());
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(true) => {
            // Correct: the panic unwound through acquire_primary_db,
            // Drop for Database's try_lock skipped the registry remove
            // (guard was still held), and catch_unwind captured the
            // panic. No deadlock.
        }
        Ok(false) => panic!(
            "PanickingSubsystem::recover() unexpectedly did not panic \
             — test is not exercising the panic path"
        ),
        Err(RecvTimeoutError::Timeout) => panic!(
            "acquire_primary_db deadlocked when subsystem recovery \
             panicked. Drop for Database must use try_lock on \
             OPEN_DATABASES to avoid self-deadlock against the \
             still-held guard from acquire_primary_db. See audit \
             follow-up to #2354 Finding 1 review."
        ),
        Err(RecvTimeoutError::Disconnected) => panic!(
            "worker thread exited without sending a result — \
             catch_unwind may have failed. This is NOT the deadlock \
             this test is designed to catch."
        ),
    }
}

// ============================================================================
// Hard reuse rejection: mixing openers with different subsystem lists
// must return an error, not silently drop the later caller's subsystems.
// ============================================================================

/// Trivial test-only `Subsystem` whose `recover()` is a no-op. Used by
/// `test_mixed_opener_rejects_subsystem_mismatch` to construct a second
/// caller's subsystem list that is intentionally distinct from
/// `Database::open`'s hardcoded `[SearchSubsystem]`.
struct NoopRegressionSubsystem;

impl strata_engine::Subsystem for NoopRegressionSubsystem {
    fn name(&self) -> &'static str {
        "noop-mixed-opener-regression"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
        Ok(())
    }
}

/// T2-E3 hard reuse rejection: subsystem mismatches are errors.
///
/// When two callers open the same canonical path through
/// `acquire_primary_db` with different subsystem lists, the second
/// caller receives `StrataError::IncompatibleReuse`. This prevents
/// silent subsystem dropping when mixing openers (e.g., `Database::open`
/// and `Strata::open` on the same path).
///
/// Lives in the integration test binary (not `crates/engine/src/database/tests.rs`)
/// because several lib tests call `OPEN_DATABASES.lock().clear()` for
/// their own isolation; in parallel test runs they would wipe this
/// test's fresh registry entry out from under it, causing the second
/// `open()` to fall through to a file-lock collision. Integration
/// tests run in a separate process so the race cannot fire.
#[test]
fn test_mixed_opener_rejects_subsystem_mismatch() {
    let temp = TempDir::new().unwrap();

    // Caller A: `open_runtime` with SearchSubsystem → `[SearchSubsystem]`.
    let db_a =
        Database::open_runtime(OpenSpec::primary(temp.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert_eq!(
        db_a.installed_subsystem_names(),
        vec!["search"],
        "open_runtime with SearchSubsystem should install only SearchSubsystem"
    );

    // Caller B: `open_runtime` with a different subsystem list.
    // This must fail with IncompatibleReuse, not silently return db_a.
    let result = Database::open_runtime(
        OpenSpec::primary(temp.path())
            .with_subsystem(strata_engine::SearchSubsystem)
            .with_subsystem(NoopRegressionSubsystem),
    );

    assert!(
        matches!(
            &result,
            Err(strata_core::StrataError::IncompatibleReuse { .. })
        ),
        "mixed-opener with different subsystems must return IncompatibleReuse, got {}",
        if result.is_ok() {
            "Ok(_)"
        } else {
            "different error"
        }
    );

    // db_a is still valid and has the original subsystem list
    assert_eq!(db_a.installed_subsystem_names(), vec!["search"]);
}

/// T3-E6: explicit `shutdown()` is the authoritative close barrier — data
/// committed before shutdown must be readable after reopen, without relying
/// on Drop for final flush/freeze.
#[test]
#[serial(open_databases)]
fn shutdown_is_ordered_and_deterministic() {
    let (db, temp_dir, branch_id) = setup();
    let path = get_path(&temp_dir);

    {
        let kv = KVStore::new(db.clone());
        kv.put(
            &branch_id,
            "default",
            "ordered_key",
            Value::String("ordered_value".into()),
        )
        .unwrap();
    }

    db.shutdown()
        .expect("explicit shutdown must succeed on an idle database");

    // T3-E6 contract: fresh reopen on the same path must succeed
    // immediately after `shutdown()` — WITHOUT dropping the old
    // `Arc<Database>`. `shutdown()` releases both the `OPEN_DATABASES`
    // slot and the on-disk `.lock` flock so a new open can acquire them.
    let db2 = Database::open_runtime(OpenSpec::primary(&path).with_subsystem(SearchSubsystem))
        .expect("reopen after shutdown must succeed while old Arc is still alive");
    let kv2 = KVStore::new(db2.clone());
    assert_eq!(
        kv2.get(&branch_id, "default", "ordered_key").unwrap(),
        Some(Value::String("ordered_value".into())),
        "data committed before shutdown() must be readable after reopen"
    );
    drop(db);
}
