//! Branch Isolation Integration Tests
//!
//! Tests verifying that different branches are completely isolated from each other.
//! Each branch has its own namespace and cannot see or affect other branches' data.

use std::collections::HashMap;
use std::sync::Arc;
use strata_core::BranchId;
use strata_core::Value;
use strata_engine::database::config::StorageConfig;
use strata_engine::database::OpenSpec;
use strata_engine::Database;
use strata_engine::SearchSubsystem;
use strata_engine::StrataConfig;
use strata_engine::{EventLog, KVStore};
use tempfile::TempDir;

/// Helper to create an empty object payload for EventLog
fn empty_payload() -> Value {
    Value::object(HashMap::new())
}

/// Helper to create an object payload with an integer value
fn int_payload(v: i64) -> Value {
    Value::object(HashMap::from([("value".to_string(), Value::Int(v))]))
}

/// Helper to create an object payload with a string value
fn string_payload(s: &str) -> Value {
    Value::object(HashMap::from([(
        "data".to_string(),
        Value::String(s.into()),
    )]))
}

fn setup() -> (Arc<Database>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db =
        Database::open_runtime(OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    (db, temp_dir)
}

/// Test KV isolation - same key in different branches are independent
#[test]
fn test_kv_isolation() {
    let (db, _temp) = setup();
    let kv = KVStore::new(db.clone());

    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Same key, different branches
    kv.put(&branch1, "default", "key", Value::Int(1)).unwrap();
    kv.put(&branch2, "default", "key", Value::Int(2)).unwrap();

    // Each branch sees ONLY its own data
    assert_eq!(
        kv.get(&branch1, "default", "key").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&branch2, "default", "key").unwrap(),
        Some(Value::Int(2))
    );

    // List shows only own keys
    let branch1_keys = kv.list(&branch1, "default", None).unwrap();
    let branch2_keys = kv.list(&branch2, "default", None).unwrap();

    assert_eq!(branch1_keys.len(), 1);
    assert_eq!(branch2_keys.len(), 1);
    assert!(branch1_keys.contains(&"key".to_string()));
    assert!(branch2_keys.contains(&"key".to_string()));
}

/// Test EventLog isolation - each branch has independent sequence numbers
#[test]
fn test_event_log_isolation() {
    let (db, _temp) = setup();
    let event_log = EventLog::new(db.clone());

    use strata_core::contract::Version;

    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Both branches start at sequence 0
    let v1 = event_log
        .append(&branch1, "default", "event", string_payload("branch1"))
        .unwrap();
    let v2 = event_log
        .append(&branch2, "default", "event", string_payload("branch2"))
        .unwrap();

    // Independent sequences - both start at 0
    assert!(matches!(v1, Version::Sequence(0)));
    assert!(matches!(v2, Version::Sequence(0)));

    // Each has length 1
    assert_eq!(event_log.len(&branch1, "default").unwrap(), 1);
    assert_eq!(event_log.len(&branch2, "default").unwrap(), 1);

    // Appending more to branch1 doesn't affect branch2
    event_log
        .append(&branch1, "default", "event2", string_payload("branch1-2"))
        .unwrap();
    event_log
        .append(&branch1, "default", "event3", string_payload("branch1-3"))
        .unwrap();

    assert_eq!(event_log.len(&branch1, "default").unwrap(), 3);
    assert_eq!(event_log.len(&branch2, "default").unwrap(), 1); // Still 1
}

/// Test that queries in one branch context NEVER return data from another branch
#[test]
fn test_cross_branch_query_isolation() {
    let (db, _temp) = setup();

    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Create extensive data in both branches
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    // Branch1 data
    for i in 0..10 {
        kv.put(&branch1, "default", &format!("key{}", i), Value::Int(i))
            .unwrap();
        event_log
            .append(&branch1, "default", "event", int_payload(i))
            .unwrap();
    }

    // Branch2 data
    for i in 0..5 {
        kv.put(
            &branch2,
            "default",
            &format!("key{}", i),
            Value::Int(i + 100),
        )
        .unwrap();
        event_log
            .append(&branch2, "default", "event", int_payload(i + 100))
            .unwrap();
    }

    // Verify counts are isolated
    assert_eq!(kv.list(&branch1, "default", None).unwrap().len(), 10);
    assert_eq!(kv.list(&branch2, "default", None).unwrap().len(), 5);

    assert_eq!(event_log.len(&branch1, "default").unwrap(), 10);
    assert_eq!(event_log.len(&branch2, "default").unwrap(), 5);

    // Verify values are isolated
    assert_eq!(
        kv.get(&branch1, "default", "key0").unwrap(),
        Some(Value::Int(0))
    );
    assert_eq!(
        kv.get(&branch2, "default", "key0").unwrap(),
        Some(Value::Int(100))
    );
}

/// Test that deleting a branch only affects that branch's data
#[test]
fn test_branch_delete_isolation() {
    let (db, _temp) = setup();

    let branches = db.branches();
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    // Create two branches via the canonical branch service.
    let meta1 = branches.create("branch1").unwrap();
    let meta2 = branches.create("branch2").unwrap();

    let branch1 = BranchId::from_string(&meta1.branch_id).unwrap();
    let branch2 = BranchId::from_string(&meta2.branch_id).unwrap();

    // Write data to both branches
    kv.put(&branch1, "default", "key", Value::Int(1)).unwrap();
    kv.put(&branch2, "default", "key", Value::Int(2)).unwrap();

    event_log
        .append(&branch1, "default", "event", empty_payload())
        .unwrap();
    event_log
        .append(&branch2, "default", "event", empty_payload())
        .unwrap();

    // Verify both branches have data
    assert!(kv.get(&branch1, "default", "key").unwrap().is_some());
    assert!(kv.get(&branch2, "default", "key").unwrap().is_some());

    // Delete branch1 (cascading delete)
    branches.delete("branch1").unwrap();

    // branch1 data is GONE
    assert!(kv.get(&branch1, "default", "key").unwrap().is_none());
    assert_eq!(event_log.len(&branch1, "default").unwrap(), 0);

    // branch2 data is UNTOUCHED
    assert_eq!(
        kv.get(&branch2, "default", "key").unwrap(),
        Some(Value::Int(2))
    );
    assert_eq!(event_log.len(&branch2, "default").unwrap(), 1);
}

/// Test that many concurrent branches remain isolated
#[test]
fn test_many_branches_isolation() {
    let (db, _temp) = setup();
    let kv = KVStore::new(db.clone());

    // Create 100 branches
    let branches: Vec<BranchId> = (0..100).map(|_| BranchId::new()).collect();

    // Each branch writes its own data
    for (i, branch_id) in branches.iter().enumerate() {
        kv.put(branch_id, "default", "value", Value::Int(i as i64))
            .unwrap();
        kv.put(branch_id, "default", "branch_index", Value::Int(i as i64))
            .unwrap();
    }

    // Verify each branch sees only its data
    for (i, branch_id) in branches.iter().enumerate() {
        let value = kv.get(branch_id, "default", "value").unwrap().unwrap();
        assert_eq!(value, Value::Int(i as i64));

        let keys = kv.list(branch_id, "default", None).unwrap();
        assert_eq!(keys.len(), 2);
    }
}

/// Test EventLog chain isolation - chains are independent per branch
#[test]
fn test_event_log_chain_isolation() {
    let (db, _temp) = setup();
    let event_log = EventLog::new(db.clone());

    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Build chain in branch1
    event_log
        .append(&branch1, "default", "e1", int_payload(0))
        .unwrap();
    event_log
        .append(&branch1, "default", "e2", int_payload(1))
        .unwrap();
    event_log
        .append(&branch1, "default", "e3", int_payload(2))
        .unwrap();

    // Build different chain in branch2
    event_log
        .append(&branch2, "default", "x1", int_payload(100))
        .unwrap();
    event_log
        .append(&branch2, "default", "x2", int_payload(101))
        .unwrap();

    // Read events to get hashes
    let event1_0 = event_log.get(&branch1, "default", 0).unwrap().unwrap();
    let event2_0 = event_log.get(&branch2, "default", 0).unwrap().unwrap();

    // Chains have different hashes (different content)
    assert_ne!(event1_0.value.hash, event2_0.value.hash);

    // Read event from branch1 - prev_hash links within branch1 only
    let event1_1 = event_log.get(&branch1, "default", 1).unwrap().unwrap();
    assert_eq!(event1_1.value.prev_hash, event1_0.value.hash);

    // Each branch maintains its own independent chain length.
    assert_eq!(event_log.len(&branch1, "default").unwrap(), 3);
    assert_eq!(event_log.len(&branch2, "default").unwrap(), 2);
}

/// #1702: branch deletion must clean up storage-layer segment files.
///
/// When a branch has been flushed to disk (producing .sst files), deleting the
/// branch via the engine-level API must remove those files. Without the fix,
/// only logical KV entries are removed — .sst files, manifests, and the branch
/// directory leak on disk forever.
#[test]
fn test_issue_1702_delete_branch_cleans_up_segment_files() {
    let temp_dir = TempDir::new().unwrap();

    // Use a tiny write_buffer_size to force memtable rotation (and thus segment
    // creation) on every write transaction commit.
    let cfg = StrataConfig {
        storage: StorageConfig {
            write_buffer_size: 1, // 1 byte → rotates immediately
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path())
            .with_config(cfg)
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let branches = db.branches();
    let kv = KVStore::new(db.clone());

    // Create a branch and write data.
    branches.create("doomed").unwrap();
    let branch_id = strata_engine::primitives::branch::resolve_branch_name("doomed");

    // Each write triggers rotation → flush → .sst file on disk.
    for i in 0..5 {
        kv.put(&branch_id, "default", &format!("key{i}"), Value::Int(i))
            .unwrap();
    }

    // Wait for background flush to complete (flush is async since #2262).
    db.scheduler().drain();

    // Compute the branch's on-disk directory.
    let segments_dir = temp_dir.path().join("segments");
    let branch_hex = {
        let bytes = branch_id.as_bytes();
        let mut s = String::with_capacity(32);
        for &b in bytes.iter() {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", b);
        }
        s
    };
    let branch_dir = segments_dir.join(&branch_hex);

    // Collect .sst files for this branch before deletion.
    let sst_count_before = std::fs::read_dir(&branch_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .count();
    assert!(
        sst_count_before > 0,
        "Expected .sst files in {:?} before delete, found none",
        branch_dir,
    );

    // Drain background tasks (compaction may be running due to
    // compaction-on-open and tiny write_buffer_size) before deleting.
    // Without this, in-flight compaction may hold segment references or
    // create output files that clear_branch misses.
    db.scheduler().drain();

    // Delete the branch through the engine-level API.
    branches.delete("doomed").unwrap();

    // Logical data should be gone.
    assert!(kv.get(&branch_id, "default", "key0").unwrap().is_none());

    // B5.2 two-phase delete: the logical delete commits and the branch's own
    // segments are quarantined via Stage-3 rename into `__quarantine__/`, but
    // Stage-5 final purge is left to explicit GC / reopen reconciliation.
    // The top-level branch directory therefore has no `.sst` files
    // immediately after `delete()` even though `__quarantine__/` still holds
    // the inventory + renamed files as retention debt.
    let sst_after_delete: Vec<_> = std::fs::read_dir(&branch_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let path = e.path();
            path.is_file() && path.extension().is_some_and(|ext| ext == "sst")
        })
        .collect();
    assert!(
        sst_after_delete.is_empty(),
        "Expected no top-level `.sst` files after branch delete (Stage-3 moved them into __quarantine__), \
         found {} files: {:?}",
        sst_after_delete.len(),
        sst_after_delete.iter().map(|e| e.path()).collect::<Vec<_>>(),
    );

    // Drive Stage-5 final purge explicitly. GC drains the inventory but
    // leaves the empty branch directory as post-commit debt until a retry
    // converges it — per the B5.2 two-phase delete design.
    db.storage().gc_orphan_segments().unwrap();

    if branch_dir.exists() {
        let recursive_ssts: Vec<_> = walk_dir_ssts(&branch_dir).collect();
        assert!(
            recursive_ssts.is_empty(),
            "Stage-5 purge should drain every `.sst` under {:?}, still have {:?}",
            branch_dir,
            recursive_ssts,
        );
    }

    // Retry via the storage-level clear_branch — this is the idempotent
    // convergence step that removes the leftover empty dir.
    db.storage().clear_branch(&branch_id).unwrap();

    assert!(
        !branch_dir.exists(),
        "Branch directory {:?} should be removed after Stage-5 purge + clear_branch retry",
        branch_dir,
    );
}

fn walk_dir_ssts(root: &std::path::Path) -> impl Iterator<Item = std::path::PathBuf> {
    let mut stack: Vec<std::path::PathBuf> = vec![root.to_path_buf()];
    std::iter::from_fn(move || {
        while let Some(dir) = stack.pop() {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if path.extension().is_some_and(|ext| ext == "sst") {
                        return Some(path);
                    }
                }
            }
        }
        None
    })
}
