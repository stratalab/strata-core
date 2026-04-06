//! Command Dispatch Tests
//!
//! Tests that the Executor correctly dispatches all Command variants
//! and returns the appropriate Output types.

use crate::common::*;
use strata_core::Value;
use strata_executor::{BranchId, Command, DistanceMetric, Error, Output};

// ============================================================================
// Database Commands
// ============================================================================

#[test]
fn ping_returns_version_string() {
    let executor = create_executor();

    let output = executor.execute(Command::Ping).unwrap();

    match output {
        Output::Pong { version } => {
            assert!(!version.is_empty());
        }
        _ => panic!("Expected Pong output"),
    }
}

#[test]
fn info_returns_database_info() {
    let executor = create_executor();

    let output = executor.execute(Command::Info).unwrap();

    match output {
        Output::DatabaseInfo(info) => {
            assert!(!info.version.is_empty());
            // Uptime should be 0 or very small for a just-opened database
            assert!(info.uptime_secs <= 1);
            // Cache database has system branch entries in storage
            assert!(info.total_keys > 0);
            // branch_count is user-visible branches (default branch is lazy-created)
        }
        _ => panic!("Expected DatabaseInfo output"),
    }
}

#[test]
fn health_returns_all_subsystems() {
    let executor = create_executor();

    let output = executor.execute(Command::Health).unwrap();

    match output {
        Output::Health(report) => {
            assert_eq!(
                report.status,
                strata_engine::SubsystemStatus::Healthy,
                "ephemeral DB should be fully healthy"
            );
            assert!(report.uptime_secs <= 1);
            // All 6 subsystems present
            assert_eq!(report.subsystems.len(), 6);
            let names: Vec<&str> = report.subsystems.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(
                names,
                &[
                    "storage",
                    "wal",
                    "flush_thread",
                    "disk",
                    "coordinator",
                    "scheduler"
                ]
            );
            // Every subsystem should be healthy on a fresh ephemeral DB
            for sub in &report.subsystems {
                assert_eq!(
                    sub.status,
                    strata_engine::SubsystemStatus::Healthy,
                    "subsystem '{}' should be healthy, got: {:?}",
                    sub.name,
                    sub.message
                );
            }
        }
        _ => panic!("Expected Health output"),
    }
}

#[test]
fn metrics_returns_all_subsystem_data() {
    let executor = create_executor();

    let output = executor.execute(Command::Metrics).unwrap();

    match output {
        Output::Metrics(m) => {
            assert!(m.uptime_secs <= 1);
            // Ephemeral DB has no WAL
            assert!(m.wal_counters.is_none());
            assert!(m.wal_disk_usage.is_none());
            assert!(m.available_disk_bytes.is_none());
            // Storage should have branches (system branch at minimum)
            assert!(m.storage.total_branches >= 1);
            // Scheduler should have workers
            assert!(m.scheduler.worker_count >= 1);
            // Cache hit ratio should be 0.0 on fresh DB
            assert_eq!(m.cache.hit_ratio, 0.0);
        }
        _ => panic!("Expected Metrics output"),
    }
}

#[test]
fn flush_returns_unit() {
    let executor = create_executor();

    let output = executor.execute(Command::Flush).unwrap();
    assert!(matches!(output, Output::Unit));
}

#[test]
fn compact_succeeds_on_ephemeral() {
    let executor = create_executor();

    // Compact on an ephemeral database is a no-op
    let result = executor.execute(Command::Compact);
    assert!(result.is_ok());
}

// ============================================================================
// KV Commands
// ============================================================================

#[test]
fn kv_put_returns_version() {
    let executor = create_executor();

    let output = executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "test_key".into(),
            value: Value::String("test_value".into()),
        })
        .unwrap();

    match output {
        Output::WriteResult { key, version } => {
            assert_eq!(key, "test_key", "WriteResult should echo back the key");
            assert!(version > 0);
        }
        _ => panic!("Expected WriteResult output, got {:?}", output),
    }
}

#[test]
fn kv_get_returns_maybe_versioned() {
    let executor = create_executor();

    // Put first
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "k".into(),
            value: Value::Int(42),
        })
        .unwrap();

    // Get
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "k".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::Int(42));
        }
        _ => panic!("Expected Maybe(Some) output"),
    }
}

#[test]
fn kv_get_missing_returns_none() {
    let executor = create_executor();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "nonexistent".into(),
            as_of: None,
        })
        .unwrap();

    assert!(matches!(
        output,
        Output::MaybeVersioned(None) | Output::Maybe(None)
    ));
}

#[test]
fn kv_delete_returns_bool() {
    let executor = create_executor();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "k".into(),
            value: Value::Int(1),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: "k".into(),
        })
        .unwrap();

    match output {
        Output::DeleteResult { key, deleted } => {
            assert_eq!(key, "k", "DeleteResult should echo back the key");
            assert!(deleted, "Existing key should report deleted=true");
        }
        _ => panic!("Expected DeleteResult output, got {:?}", output),
    }

    // Delete again - should return false
    let output = executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: "k".into(),
        })
        .unwrap();

    match output {
        Output::DeleteResult { key, deleted } => {
            assert_eq!(key, "k", "DeleteResult should echo back the key");
            assert!(!deleted, "Missing key should report deleted=false");
        }
        _ => panic!("Expected DeleteResult output, got {:?}", output),
    }
}

// ============================================================================
// Event Commands
// ============================================================================

#[test]
fn event_append_returns_version() {
    let executor = create_executor();

    let output = executor
        .execute(Command::EventAppend {
            branch: None,
            space: None,
            event_type: "test_stream".into(),
            payload: event_payload("data", Value::String("event1".into())),
        })
        .unwrap();

    match output {
        Output::EventAppendResult {
            sequence,
            event_type,
        } => {
            // Sequence is 0-based (first event gets sequence 0)
            assert!(sequence < u64::MAX, "Event sequence should be valid");
            assert_eq!(
                event_type, "test_stream",
                "EventAppendResult should echo event_type"
            );
        }
        _ => panic!("Expected EventAppendResult output, got {:?}", output),
    }
}

#[test]
fn event_len_returns_count() {
    let executor = create_executor();

    for i in 0..5 {
        executor
            .execute(Command::EventAppend {
                branch: None,
                space: None,
                event_type: "counting".into(),
                payload: event_payload("i", Value::Int(i)),
            })
            .unwrap();
    }

    let output = executor
        .execute(Command::EventLen {
            branch: None,
            space: None,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::Uint(count) => assert_eq!(count, 5),
        _ => panic!("Expected Uint output"),
    }
}

// ============================================================================
// Vector Commands
// ============================================================================

#[test]
fn vector_create_collection_and_upsert() {
    let executor = create_executor();

    // Create collection
    let output = executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    assert!(matches!(output, Output::Version(_)));

    // Upsert vector
    let output = executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            key: "v1".into(),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();

    match output {
        Output::VectorWriteResult {
            collection,
            key,
            version,
        } => {
            assert_eq!(collection, "embeddings", "Should echo collection");
            assert_eq!(key, "v1", "Should echo key");
            assert!(version > 0);
        }
        _ => panic!("Expected VectorWriteResult output, got {:?}", output),
    }
}

#[test]
fn vector_query_returns_matches() {
    let executor = create_executor();

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "search_test".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "search_test".into(),
            key: "v1".into(),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "search_test".into(),
            key: "v2".into(),
            vector: vec![0.0, 1.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: "search_test".into(),
            query: vec![1.0, 0.0, 0.0, 0.0],
            k: 10,
            filter: None,
            metric: None,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::VectorMatches(matches) => {
            assert_eq!(matches.len(), 2);
            assert_eq!(matches[0].key, "v1"); // Exact match should be first
        }
        _ => panic!("Expected VectorMatches output"),
    }
}

#[test]
fn vector_list_collections() {
    let executor = create_executor();

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "coll_a".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "coll_b".into(),
            dimension: 8,
            metric: DistanceMetric::Euclidean,
        })
        .unwrap();

    let output = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .unwrap();

    match output {
        Output::VectorCollectionList(infos) => {
            assert_eq!(infos.len(), 2);
        }
        _ => panic!("Expected VectorCollectionList output"),
    }
}

// ============================================================================
// Branch Commands
// ============================================================================

#[test]
fn branch_create_and_get() {
    let executor = create_executor();

    // Users can name branches like git branches - no UUID required
    let output = executor
        .execute(Command::BranchCreate {
            branch_id: Some("main".into()),
            metadata: None,
        })
        .unwrap();

    let branch_id = match output {
        Output::BranchWithVersion { info, .. } => {
            assert_eq!(info.id.as_str(), "main");
            info.id
        }
        _ => panic!("Expected BranchCreated output"),
    };

    let output = executor
        .execute(Command::BranchGet { branch: branch_id })
        .unwrap();

    match output {
        Output::MaybeBranchInfo(Some(versioned)) => {
            assert_eq!(versioned.info.id.as_str(), "main");
        }
        _ => panic!("Expected MaybeBranchInfo(Some(...)) output"),
    }
}

#[test]
fn branch_names_can_be_human_readable() {
    let executor = create_executor();

    // Test various human-readable branch names (like git branches)
    let names = ["experiment-1", "feature/new-model", "v2.0", "test_branch"];

    for name in names {
        let output = executor
            .execute(Command::BranchCreate {
                branch_id: Some(name.into()),
                metadata: None,
            })
            .unwrap();

        match output {
            Output::BranchWithVersion { info, .. } => {
                assert_eq!(info.id.as_str(), name, "Branch name should be preserved");
            }
            _ => panic!("Expected BranchWithVersion output"),
        }
    }
}

#[test]
fn branch_list_returns_branches() {
    let executor = create_executor();

    executor
        .execute(Command::BranchCreate {
            branch_id: Some("production".into()),
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::BranchCreate {
            branch_id: Some("staging".into()),
            metadata: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::BranchList {
            state: None,
            limit: Some(100),
            offset: None,
        })
        .unwrap();

    match output {
        Output::BranchInfoList(branches) => {
            // At least the default branch plus our two created branches
            assert!(
                branches.len() >= 2,
                "Expected >= 2 branches (production + staging), got {}",
                branches.len()
            );
        }
        _ => panic!("Expected BranchInfoList output"),
    }
}

#[test]
fn branch_delete_removes_branch() {
    let executor = create_executor();

    let branch_id = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("deletable-branch".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    // Verify it exists
    let output = executor
        .execute(Command::BranchExists {
            branch: branch_id.clone(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));

    // Delete it
    executor
        .execute(Command::BranchDelete {
            branch: branch_id.clone(),
        })
        .unwrap();

    // Verify it's gone
    let output = executor
        .execute(Command::BranchExists { branch: branch_id })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));
}

#[test]
fn branch_exists_returns_bool() {
    let executor = create_executor();

    // Non-existent branch
    let output = executor
        .execute(Command::BranchExists {
            branch: BranchId::from("non-existent-branch"),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    // Create a branch
    executor
        .execute(Command::BranchCreate {
            branch_id: Some("exists-test".into()),
            metadata: None,
        })
        .unwrap();

    // Now it exists
    let output = executor
        .execute(Command::BranchExists {
            branch: BranchId::from("exists-test"),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));
}

// ============================================================================
// Default Branch Resolution
// ============================================================================

#[test]
fn commands_with_none_branch_use_default() {
    let executor = create_executor();

    // Put with branch: None
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "default_test".into(),
            value: Value::String("value".into()),
        })
        .unwrap();

    // Get with explicit default branch
    let output = executor
        .execute(Command::KvGet {
            branch: Some(BranchId::default()),
            space: None,
            key: "default_test".into(),
            as_of: None,
        })
        .unwrap();

    // Should find the value
    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("value".into()));
        }
        _ => panic!("Expected to find value in default branch"),
    }
}

#[test]
fn different_branches_are_isolated() {
    let executor = create_executor();

    // Create two branches with human-readable names
    let branch_a = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("agent-alpha".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchCreated"),
    };

    let branch_b = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("agent-beta".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchCreated"),
    };

    // Put in branch A
    executor
        .execute(Command::KvPut {
            branch: Some(branch_a.clone()),
            space: None,
            key: "shared_key".into(),
            value: Value::String("branch_a_value".into()),
        })
        .unwrap();

    // Put in branch B
    executor
        .execute(Command::KvPut {
            branch: Some(branch_b.clone()),
            space: None,
            key: "shared_key".into(),
            value: Value::String("branch_b_value".into()),
        })
        .unwrap();

    // Get from branch A
    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_a),
            space: None,
            key: "shared_key".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("branch_a_value".into()));
        }
        _ => panic!("Expected branch A value"),
    }

    // Get from branch B
    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_b),
            space: None,
            key: "shared_key".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("branch_b_value".into()));
        }
        _ => panic!("Expected branch B value"),
    }
}

// ============================================================================
// Write Metadata Tests (#1443)
// ============================================================================

#[test]
fn vector_delete_returns_delete_result() {
    let executor = create_executor();

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "vdel".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "vdel".into(),
            key: "v1".into(),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::VectorDelete {
            branch: None,
            space: None,
            collection: "vdel".into(),
            key: "v1".into(),
        })
        .unwrap();

    match output {
        Output::VectorDeleteResult {
            collection,
            key,
            deleted,
        } => {
            assert_eq!(collection, "vdel", "Should echo collection");
            assert_eq!(key, "v1", "Should echo key");
            assert!(deleted, "Existing vector should report deleted=true");
        }
        _ => panic!("Expected VectorDeleteResult, got {:?}", output),
    }
}

#[test]
fn json_set_and_delete_return_structured_results() {
    let executor = create_executor();

    let output = executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "doc1".into(),
            path: "$".into(),
            value: Value::object(
                [("name".to_string(), Value::String("Alice".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();

    match output {
        Output::WriteResult { key, version } => {
            assert_eq!(key, "doc1", "WriteResult should echo document key");
            assert!(version > 0);
        }
        _ => panic!("Expected WriteResult, got {:?}", output),
    }

    let output = executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "doc1".into(),
            path: "$".into(),
        })
        .unwrap();

    match output {
        Output::DeleteResult { key, deleted } => {
            assert_eq!(key, "doc1", "DeleteResult should echo document key");
            assert!(deleted);
        }
        _ => panic!("Expected DeleteResult, got {:?}", output),
    }
}

// ============================================================================
// Pagination Tests (#1444)
// ============================================================================

#[test]
fn kv_list_with_limit_returns_keys_page() {
    let executor = create_executor();

    // Insert 5 keys
    for i in 0..5 {
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: format!("page:{:02}", i),
                value: Value::Int(i),
            })
            .unwrap();
    }

    // List with limit=3
    let output = executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: Some("page:".into()),
            cursor: None,
            limit: Some(3),
            as_of: None,
        })
        .unwrap();

    match &output {
        Output::KeysPage {
            keys,
            has_more,
            cursor,
        } => {
            assert_eq!(keys.len(), 3, "Should return exactly 3 keys");
            assert!(*has_more, "Should indicate more results exist");
            assert!(cursor.is_some(), "Should provide cursor for next page");
        }
        _ => panic!("Expected KeysPage, got {:?}", output),
    }

    // Fetch second page using cursor
    let cursor = match output {
        Output::KeysPage { cursor, .. } => cursor,
        _ => unreachable!(),
    };
    let output = executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: Some("page:".into()),
            cursor,
            limit: Some(3),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::KeysPage {
            keys,
            has_more,
            cursor,
        } => {
            assert_eq!(keys.len(), 2, "Second page should have remaining 2 keys");
            assert!(!has_more, "No more results after this page");
            assert!(cursor.is_none(), "No cursor on last page");
        }
        _ => panic!("Expected KeysPage, got {:?}", output),
    }
}

#[test]
fn kv_list_without_limit_returns_all_keys() {
    let executor = create_executor();

    for i in 0..3 {
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: format!("all:{}", i),
                value: Value::Int(i),
            })
            .unwrap();
    }

    let output = executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: Some("all:".into()),
            cursor: None,
            limit: None,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::Keys(keys) => {
            assert_eq!(keys.len(), 3, "Should return all keys without pagination");
        }
        _ => panic!(
            "Expected Keys (not KeysPage) without limit, got {:?}",
            output
        ),
    }
}

#[test]
fn kv_list_exact_limit_returns_no_more() {
    let executor = create_executor();

    for i in 0..3 {
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: format!("exact:{}", i),
                value: Value::Int(i),
            })
            .unwrap();
    }

    // limit exactly matches count
    let output = executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: Some("exact:".into()),
            cursor: None,
            limit: Some(3),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::KeysPage {
            keys,
            has_more,
            cursor,
        } => {
            assert_eq!(keys.len(), 3);
            assert!(!has_more, "Exact match should not indicate more");
            assert!(cursor.is_none(), "No cursor when nothing remains");
        }
        _ => panic!("Expected KeysPage, got {:?}", output),
    }
}

// ============================================================================
// Error Hint Tests (#1442)
// ============================================================================

#[test]
fn access_denied_includes_read_only_hint() {
    use strata_executor::{AccessMode, OpenOptions, Strata};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.strata");

    // Create a database, then reopen read-only
    {
        let _db = Strata::open(path.to_str().unwrap()).unwrap();
    }

    let db = Strata::open_with(
        path.to_str().unwrap(),
        OpenOptions::new().access_mode(AccessMode::ReadOnly),
    )
    .unwrap();

    let result = db.kv_put("test", "value");
    match result {
        Err(Error::AccessDenied { hint, .. }) => {
            let hint_text = hint.expect("AccessDenied should include hint");
            assert!(
                hint_text.contains("read-only"),
                "Hint should mention read-only mode, got: {}",
                hint_text
            );
        }
        Err(e) => panic!("Expected AccessDenied, got: {:?}", e),
        Ok(_) => panic!("Expected error on write to read-only db"),
    }
}

#[test]
fn kv_list_empty_prefix_with_limit() {
    let executor = create_executor();

    // No keys exist, list with limit
    let output = executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: Some("nonexistent:".into()),
            cursor: None,
            limit: Some(10),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::KeysPage {
            keys,
            has_more,
            cursor,
        } => {
            assert!(keys.is_empty(), "Should return empty list");
            assert!(!has_more, "Empty result should not have more");
            assert!(cursor.is_none(), "Empty result should not have cursor");
        }
        _ => panic!("Expected KeysPage, got {:?}", output),
    }
}

// ============================================================================
// Error Quality Tests (#1550)
// ============================================================================

#[test]
fn config_set_rejects_zero_embed_batch_size() {
    let executor = create_executor();

    let result = executor.execute(Command::ConfigureSet {
        key: "embed_batch_size".into(),
        value: "0".into(),
    });
    match result {
        Err(Error::InvalidInput { reason, .. }) => {
            assert!(reason.contains("greater than 0"), "reason: {}", reason);
        }
        other => panic!("Expected InvalidInput for 0, got {:?}", other),
    }
}

#[test]
fn version_conflict_includes_retry_hint() {
    use strata_core::{EntityRef, StrataError, Version};

    let err = StrataError::version_conflict(
        EntityRef::kv(strata_core::types::BranchId::from_bytes([0; 16]), "k"),
        Version::Txn(1),
        Version::Txn(2),
    );
    let converted: Error = err.into();
    match converted {
        Error::VersionConflict { hint, .. } => {
            assert!(hint.is_some(), "VersionConflict should have retry hint");
            assert!(hint.unwrap().contains("retry"), "hint should mention retry");
        }
        _ => panic!("Expected VersionConflict"),
    }
}

#[test]
fn internal_error_includes_bug_report_hint() {
    use strata_core::StrataError;

    let err = StrataError::internal("test invariant");
    let converted: Error = err.into();
    match converted {
        Error::Internal { hint, .. } => {
            assert!(hint.is_some(), "Internal should have bug report hint");
            assert!(
                hint.unwrap().contains("report"),
                "hint should mention reporting"
            );
        }
        _ => panic!("Expected Internal"),
    }
}

#[test]
fn error_severity_classification() {
    use strata_executor::ErrorSeverity;

    // User errors
    let user_err = Error::KeyNotFound {
        key: "x".into(),
        hint: None,
    };
    assert_eq!(user_err.severity(), ErrorSeverity::UserError);

    // System errors
    let io_err = Error::Io {
        reason: "disk".into(),
        hint: None,
    };
    assert_eq!(io_err.severity(), ErrorSeverity::SystemFailure);

    let ser_err = Error::Serialization {
        reason: "bad".into(),
    };
    assert_eq!(ser_err.severity(), ErrorSeverity::SystemFailure);

    // Internal bugs
    let bug = Error::Internal {
        reason: "oops".into(),
        hint: None,
    };
    assert_eq!(bug.severity(), ErrorSeverity::InternalBug);
}
