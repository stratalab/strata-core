//! Primitive Recovery Tests
//!
//! Tests verifying that ALL primitives survive crash + WAL replay.
//! The recovery contract ensures:
//! - Sequence numbers: Preserved
//! - Secondary indices: Replayed, not rebuilt
//! - Derived keys (hashes): Stored, not recomputed

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::{BranchIndex, EventLog, KVStore};
use strata_engine::{Database, SearchRequest};
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
    let db = Database::open(temp_dir.path()).unwrap();
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
    let db = Database::open(&path).unwrap();
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
    let db = Database::open(&path).unwrap();
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
    let db = Database::open(&path).unwrap();
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
    let db = Database::open(&path).unwrap();
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

/// Test BranchIndex survives recovery
#[test]
fn test_branch_index_survives_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let db = Database::open(&path).unwrap();

    let branch_index = BranchIndex::new(db.clone());

    // Create branch with metadata
    let branch_meta = branch_index.create_branch("test-branch").unwrap();
    let branch_name = branch_meta.value.name.clone();

    // Note: update_status and add_tags removed in MVP simplification

    // Verify before crash
    let branch = branch_index.get_branch(&branch_name).unwrap().unwrap();
    assert_eq!(branch.value.name, "test-branch");

    // Simulate crash
    drop(branch_index);
    drop(db);

    // Recovery
    let db = Database::open(&path).unwrap();
    let branch_index = BranchIndex::new(db.clone());

    // Branch preserved
    let recovered = branch_index.get_branch(&branch_name).unwrap().unwrap();
    assert_eq!(recovered.value.name, "test-branch");
}

/// Test BranchIndex list survives recovery
#[test]
fn test_branch_index_list_survives_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let db = Database::open(&path).unwrap();

    let branch_index = BranchIndex::new(db.clone());

    // Create multiple branches
    branch_index.create_branch("branch1").unwrap();
    branch_index.create_branch("branch2").unwrap();
    branch_index.create_branch("branch3").unwrap();

    // Note: update_status and query_by_status removed in MVP simplification

    // Simulate crash
    drop(branch_index);
    drop(db);

    // Recovery
    let db = Database::open(&path).unwrap();
    let branch_index = BranchIndex::new(db.clone());

    // List all branches works (includes _system_ from init_system_branch)
    let branches = branch_index.list_branches().unwrap();
    let user_branches: Vec<_> = branches
        .iter()
        .filter(|b| !b.starts_with("_system"))
        .collect();
    assert_eq!(user_branches.len(), 3);
    assert!(branches.contains(&"branch1".to_string()));
    assert!(branches.contains(&"branch2".to_string()));
    assert!(branches.contains(&"branch3".to_string()));
}

/// Test BranchIndex cascading delete survives recovery
#[test]
fn test_branch_delete_survives_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let path = get_path(&temp_dir);
    let db = Database::open(&path).unwrap();

    let branch_index = BranchIndex::new(db.clone());
    let kv = KVStore::new(db.clone());

    // Create two branches
    let meta1 = branch_index.create_branch("branch1").unwrap();
    let meta2 = branch_index.create_branch("branch2").unwrap();
    let branch1 = BranchId::from_string(&meta1.value.branch_id).unwrap();
    let branch2 = BranchId::from_string(&meta2.value.branch_id).unwrap();

    // Write data to both
    kv.put(&branch1, "default", "key", Value::Int(1)).unwrap();
    kv.put(&branch2, "default", "key", Value::Int(2)).unwrap();

    // Delete branch1
    branch_index.delete_branch("branch1").unwrap();

    // Simulate crash
    drop(branch_index);
    drop(kv);
    drop(db);

    // Recovery
    let db = Database::open(&path).unwrap();
    let branch_index = BranchIndex::new(db.clone());
    let kv = KVStore::new(db.clone());

    // branch1 is still deleted
    assert!(branch_index.get_branch("branch1").unwrap().is_none());
    assert!(kv.get(&branch1, "default", "key").unwrap().is_none());

    // branch2 data preserved
    assert!(branch_index.get_branch("branch2").unwrap().is_some());
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
    let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
        let kv = KVStore::new(db.clone());
        kv.put(&branch_id, "default", "cycle1", Value::Int(1))
            .unwrap();
    }

    // Cycle 2: Add more data
    {
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
        let branch_index = BranchIndex::new(db.clone());
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());

        // Create branch
        let branch_meta = branch_index.create_branch("full-test").unwrap();
        branch_id = BranchId::from_string(&branch_meta.value.branch_id).unwrap();

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
        let db = Database::open(&path).unwrap();
        let branch_index = BranchIndex::new(db.clone());
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());

        // BranchIndex
        let branch = branch_index.get_branch("full-test").unwrap().unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
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
        let db = Database::open(&path).unwrap();
        let kv = KVStore::new(db.clone());

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

        // Search for session 2 event data — fails without the fix
        let req = SearchRequest::new(branch_id, "anomaly");
        let response = kv.search(&req).unwrap();
        assert!(
            !response.hits.is_empty(),
            "Session 2 event data should be searchable after crash recovery reconciliation"
        );
    }
}
