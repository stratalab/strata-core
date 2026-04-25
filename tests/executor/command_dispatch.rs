//! Command Dispatch Tests
//!
//! Tests that the Executor correctly dispatches all Command variants
//! and returns the appropriate Output types.

use crate::common::*;
use strata_core::Value;
use strata_executor::{BranchId, Command, DistanceMetric, Error, Executor, Output, ScanDirection};

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
            // Cache hit ratio is a process-wide global counter (see
            // `strata_storage::block_cache::global_cache()` in
            // `database/mod.rs`), so it can be polluted by any other
            // parallel test running in the same process. Assert only the
            // sanity bound that the ratio is a valid probability — testing
            // == 0.0 was inherently flaky and only passed when this test
            // happened to run before any other cache activity.
            assert!(
                (0.0..=1.0).contains(&m.cache.hit_ratio),
                "hit_ratio should be in [0.0, 1.0], got {}",
                m.cache.hit_ratio
            );
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

#[test]
fn kv_batch_scan_count_and_sample_return_structured_results() {
    let executor = create_executor();

    let output = executor
        .execute(Command::KvBatchPut {
            branch: None,
            space: None,
            entries: vec![
                strata_executor::BatchKvEntry {
                    key: "scan:01".into(),
                    value: Value::Int(1),
                },
                strata_executor::BatchKvEntry {
                    key: "scan:02".into(),
                    value: Value::Int(2),
                },
                strata_executor::BatchKvEntry {
                    key: "scan:03".into(),
                    value: Value::Int(3),
                },
            ],
        })
        .unwrap();

    match output {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 3);
            assert!(results.iter().all(|result| result.version.is_some()));
            assert!(results.iter().all(|result| result.error.is_none()));
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }

    let output = executor
        .execute(Command::KvBatchGet {
            branch: None,
            space: None,
            keys: vec!["scan:01".into(), "scan:03".into(), "scan:missing".into()],
        })
        .unwrap();

    match output {
        Output::BatchGetResults(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].value, Some(Value::Int(1)));
            assert_eq!(results[1].value, Some(Value::Int(3)));
            assert_eq!(results[2].value, None);
            assert!(results.iter().all(|result| result.error.is_none()));
        }
        other => panic!("Expected BatchGetResults, got {:?}", other),
    }

    let output = executor
        .execute(Command::KvScan {
            branch: None,
            space: None,
            start: Some("scan:02".into()),
            limit: Some(2),
        })
        .unwrap();

    match output {
        Output::KvScanResult(entries) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0], ("scan:02".into(), Value::Int(2)));
            assert_eq!(entries[1], ("scan:03".into(), Value::Int(3)));
        }
        other => panic!("Expected KvScanResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::KvCount {
            branch: None,
            space: None,
            prefix: Some("scan:".into()),
        })
        .unwrap();
    assert!(matches!(output, Output::Uint(3)));

    let output = executor
        .execute(Command::KvSample {
            branch: None,
            space: None,
            prefix: Some("scan:".into()),
            count: Some(2),
        })
        .unwrap();

    match output {
        Output::SampleResult { total_count, items } => {
            assert_eq!(total_count, 3);
            assert_eq!(items.len(), 2);
            assert!(items.iter().all(|item| item.key.starts_with("scan:")));
        }
        other => panic!("Expected SampleResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::KvBatchDelete {
            branch: None,
            space: None,
            keys: vec!["scan:01".into(), "scan:missing".into()],
        })
        .unwrap();

    match output {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2);
            assert!(results[0].version.is_some());
            assert!(results[0].error.is_none());
            assert!(results[1].error.is_none());
        }
        other => panic!("Expected BatchResults, got {:?}", other),
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

#[test]
fn event_range_and_type_listing_return_structured_results() {
    let executor = create_executor();

    for (event_type, value) in [("alpha", 1), ("beta", 2), ("alpha", 3)] {
        executor
            .execute(Command::EventAppend {
                branch: None,
                space: None,
                event_type: event_type.into(),
                payload: event_payload("value", Value::Int(value)),
            })
            .unwrap();
    }

    let output = executor
        .execute(Command::EventRange {
            branch: None,
            space: None,
            start_seq: 0,
            end_seq: None,
            limit: Some(1),
            direction: ScanDirection::Forward,
            event_type: Some("alpha".into()),
        })
        .unwrap();

    match output {
        Output::EventRangeResult {
            events,
            has_more,
            next_cursor,
        } => {
            assert_eq!(events.len(), 1);
            assert!(has_more);
            assert!(next_cursor.is_some());
        }
        other => panic!("Expected EventRangeResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::EventRangeByTime {
            branch: None,
            space: None,
            start_ts: 0,
            end_ts: Some(u64::MAX),
            limit: Some(10),
            direction: ScanDirection::Forward,
            event_type: Some("alpha".into()),
        })
        .unwrap();

    match output {
        Output::EventRangeResult {
            events, has_more, ..
        } => {
            assert_eq!(events.len(), 2);
            assert!(!has_more);
        }
        other => panic!("Expected EventRangeResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::EventListTypes {
            branch: None,
            space: None,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::Keys(types) => {
            assert_eq!(types.len(), 2);
            assert!(types.contains(&"alpha".to_string()));
            assert!(types.contains(&"beta".to_string()));
        }
        other => panic!("Expected Keys output, got {:?}", other),
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

#[test]
fn vector_writes_register_space_and_require_existing_branch() {
    let executor = create_executor();

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "embeddings".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: Some("embeddings".into()),
            collection: "docs".into(),
            dimension: 3,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "embeddings".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));

    let error = executor
        .execute(Command::VectorUpsert {
            branch: Some(BranchId::from("missing-branch")),
            space: Some("embeddings".into()),
            collection: "docs".into(),
            key: "doc1".into(),
            vector: vec![1.0, 2.0, 3.0],
            metadata: None,
        })
        .expect_err("vector writes on missing branches must fail");

    assert!(matches!(error, Error::BranchNotFound { .. }));
}

#[test]
fn vector_query_sees_latest_overwrite_on_disk_backed_executor() {
    use strata_engine::database::OpenSpec;
    use strata_engine::{Database, SearchSubsystem};
    use strata_vector::VectorSubsystem;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vector_overwrite_executor.strata");
    let db = Database::open_runtime(
        OpenSpec::primary(&path)
            .with_subsystem(VectorSubsystem)
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let executor = Executor::new(db);

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".into(),
            dimension: 3,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".into(),
            key: "doc1".into(),
            vector: vec![1.0, 2.0, 3.0],
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".into(),
            key: "doc1".into(),
            vector: vec![3.0, 4.0, 5.0],
            metadata: None,
        })
        .unwrap();

    let fetched = executor
        .execute(Command::VectorGet {
            branch: None,
            space: None,
            collection: "docs".into(),
            key: "doc1".into(),
            as_of: None,
        })
        .unwrap();
    match fetched {
        Output::VectorData(Some(data)) => {
            assert_eq!(data.data.embedding, vec![3.0, 4.0, 5.0]);
        }
        _ => panic!(
            "Expected VectorData(Some(..)) after overwrite, got {:?}",
            fetched
        ),
    }

    let output = executor
        .execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".into(),
            query: vec![3.0, 4.0, 5.0],
            k: 10,
            filter: None,
            metric: None,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::VectorMatches(matches) => {
            assert!(
                !matches.is_empty(),
                "overwrite query should still return the updated vector"
            );
            assert_eq!(
                matches[0].key, "doc1",
                "exact query against overwritten vector should return doc1 first"
            );
        }
        _ => panic!("Expected VectorMatches output"),
    }
}

#[test]
fn vector_query_keeps_overwritten_key_visible_after_batch_upsert() {
    use strata_engine::database::OpenSpec;
    use strata_engine::{Database, SearchSubsystem};
    use strata_vector::VectorSubsystem;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vector_overwrite_batch_executor.strata");
    let db = Database::open_runtime(
        OpenSpec::primary(&path)
            .with_subsystem(VectorSubsystem)
            .with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let executor = Executor::new(db);

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".into(),
            dimension: 3,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".into(),
            key: "doc1".into(),
            vector: vec![1.0, 2.0, 3.0],
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".into(),
            key: "doc1".into(),
            vector: vec![3.0, 4.0, 5.0],
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "docs".into(),
            entries: vec![
                strata_executor::BatchVectorEntry {
                    key: "doc2".into(),
                    vector: vec![5.0, 6.0, 7.0],
                    metadata: None,
                },
                strata_executor::BatchVectorEntry {
                    key: "doc3".into(),
                    vector: vec![7.0, 8.0, 9.0],
                    metadata: None,
                },
            ],
        })
        .unwrap();

    let output = executor
        .execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".into(),
            query: vec![3.0, 4.0, 5.0],
            k: 10,
            filter: None,
            metric: None,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::VectorMatches(matches) => {
            assert!(
                matches.iter().any(|m| m.key == "doc1"),
                "batch-upsert after overwrite must not hide doc1 from search; got {:?}",
                matches
            );
        }
        _ => panic!("Expected VectorMatches output"),
    }
}

#[test]
fn vector_advanced_commands_expose_direct_results() {
    let executor = create_executor();

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            dimension: 3,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            key: "v1".into(),
            vector: vec![1.0, 0.0, 0.0],
            metadata: Some(Value::object(
                [("kind".to_string(), Value::String("seed".into()))]
                    .into_iter()
                    .collect(),
            )),
        })
        .unwrap();

    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            key: "v1".into(),
            vector: vec![0.0, 1.0, 0.0],
            metadata: Some(Value::object(
                [("kind".to_string(), Value::String("updated".into()))]
                    .into_iter()
                    .collect(),
            )),
        })
        .unwrap();

    executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            entries: vec![
                strata_executor::BatchVectorEntry {
                    key: "v2".into(),
                    vector: vec![0.0, 0.0, 1.0],
                    metadata: None,
                },
                strata_executor::BatchVectorEntry {
                    key: "v3".into(),
                    vector: vec![1.0, 1.0, 0.0],
                    metadata: None,
                },
            ],
        })
        .unwrap();

    let output = executor
        .execute(Command::VectorGetv {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            key: "v1".into(),
        })
        .unwrap();
    match output {
        Output::VectorVersionHistory(Some(history)) => {
            assert!(
                history.len() >= 2,
                "updated vector should expose multiple versions"
            );
        }
        other => panic!("Expected VectorVersionHistory, got {:?}", other),
    }

    let output = executor
        .execute(Command::VectorCollectionStats {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
        })
        .unwrap();
    match output {
        Output::VectorCollectionList(collections) => {
            assert_eq!(collections.len(), 1);
            assert_eq!(collections[0].name, "advanced");
            assert_eq!(collections[0].count, 3);
        }
        other => panic!("Expected VectorCollectionList, got {:?}", other),
    }

    let output = executor
        .execute(Command::VectorBatchGet {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            keys: vec!["v1".into(), "v2".into(), "missing".into()],
        })
        .unwrap();
    match output {
        Output::BatchVectorGetResults(values) => {
            assert_eq!(values.len(), 3);
            assert!(values[0].is_some());
            assert!(values[1].is_some());
            assert!(values[2].is_none());
        }
        other => panic!("Expected BatchVectorGetResults, got {:?}", other),
    }

    let output = executor
        .execute(Command::VectorSample {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            count: Some(2),
        })
        .unwrap();
    match output {
        Output::SampleResult { total_count, items } => {
            assert_eq!(total_count, 3);
            assert_eq!(items.len(), 2);
        }
        other => panic!("Expected SampleResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::VectorBatchDelete {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
            keys: vec!["v2".into(), "missing".into()],
        })
        .unwrap();
    match output {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].version, Some(0));
            assert_eq!(results[1].version, None);
            assert!(results.iter().all(|result| result.error.is_none()));
        }
        other => panic!("Expected BatchResults, got {:?}", other),
    }

    let output = executor
        .execute(Command::VectorDeleteCollection {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));

    let output = executor
        .execute(Command::VectorDeleteCollection {
            branch: None,
            space: Some("vectors".into()),
            collection: "advanced".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));
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
fn branch_list_excludes_system_branch() {
    let db = create_db();
    strata_graph::branch_dag::init_system_branch(&db);
    let executor = Executor::new(db);

    executor
        .execute(Command::BranchCreate {
            branch_id: Some("visible-branch".into()),
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
            assert!(branches
                .iter()
                .any(|branch| branch.info.id.as_str() == "visible-branch"));
            assert!(branches
                .iter()
                .all(|branch| !branch.info.id.as_str().starts_with("_system")));
        }
        other => panic!("Expected BranchInfoList output, got {:?}", other),
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

#[test]
fn branch_power_commands_round_trip_and_bundle_portability() {
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("branch_power_source.strata");
    let source_db = strata_engine::Database::open_runtime(
        strata_engine::database::OpenSpec::primary(&source_path)
            .with_subsystem(strata_graph::GraphSubsystem)
            .with_subsystem(strata_vector::VectorSubsystem)
            .with_subsystem(strata_engine::SearchSubsystem),
    )
    .unwrap();
    let executor = Executor::new(source_db);

    let bundle_dir = tempfile::tempdir().unwrap();
    let bundle_path = bundle_dir.path().join("feature.branchbundle.tar.zst");

    let imported_dir = tempfile::tempdir().unwrap();
    let imported_path = imported_dir.path().join("branch_power_imported.strata");
    let imported_db = strata_engine::Database::open_runtime(
        strata_engine::database::OpenSpec::primary(&imported_path)
            .with_subsystem(strata_graph::GraphSubsystem)
            .with_subsystem(strata_vector::VectorSubsystem)
            .with_subsystem(strata_engine::SearchSubsystem),
    )
    .unwrap();
    let imported_executor = Executor::new(imported_db);

    executor
        .execute(Command::BranchCreate {
            branch_id: Some("mainline".into()),
            metadata: None,
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("mainline")),
            space: None,
            key: "shared".into(),
            value: Value::Int(1),
        })
        .unwrap();

    let output = executor
        .execute(Command::BranchFork {
            source: "mainline".into(),
            destination: "feature".into(),
            message: None,
            creator: None,
        })
        .unwrap();
    match output {
        Output::BranchForked(info) => {
            assert_eq!(info.source, "mainline");
            assert_eq!(info.destination, "feature");
        }
        other => panic!("Expected BranchForked, got {:?}", other),
    }

    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("feature")),
            space: None,
            key: "shared".into(),
            value: Value::Int(2),
        })
        .unwrap();
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("feature")),
            space: None,
            key: "only_feature".into(),
            value: Value::Int(9),
        })
        .unwrap();

    let output = executor
        .execute(Command::BranchDiff {
            branch_a: "mainline".into(),
            branch_b: "feature".into(),
            filter_primitives: None,
            filter_spaces: None,
            as_of: None,
        })
        .unwrap();
    match output {
        Output::BranchDiff(diff) => {
            assert_eq!(diff.summary.total_added, 1);
            assert_eq!(diff.summary.total_modified, 1);
        }
        other => panic!("Expected BranchDiff, got {:?}", other),
    }

    let output = executor
        .execute(Command::BranchDiffThreeWay {
            branch_a: "mainline".into(),
            branch_b: "feature".into(),
        })
        .unwrap();
    match output {
        Output::ThreeWayDiff(diff) => {
            assert_eq!(diff.source, "mainline");
            assert_eq!(diff.target, "feature");
            assert!(!diff.entries.is_empty());
        }
        other => panic!("Expected ThreeWayDiff, got {:?}", other),
    }

    let output = executor
        .execute(Command::BranchMergeBase {
            branch_a: "mainline".into(),
            branch_b: "feature".into(),
        })
        .unwrap();
    match output {
        Output::MergeBaseInfo(Some(info)) => {
            assert_eq!(info.branch, "mainline");
        }
        other => panic!("Expected MergeBaseInfo(Some(..)), got {:?}", other),
    }

    let merge_version = match executor
        .execute(Command::BranchMerge {
            source: "feature".into(),
            target: "mainline".into(),
            strategy: strata_engine::MergeStrategy::LastWriterWins,
            message: None,
            creator: None,
        })
        .unwrap()
    {
        Output::BranchMerged(info) => {
            assert_eq!(info.source, "feature");
            assert_eq!(info.target, "mainline");
            assert_eq!(info.keys_applied, 2);
            info.merge_version
                .expect("merge should return a merge version")
        }
        other => panic!("Expected BranchMerged, got {:?}", other),
    };

    let output = executor
        .execute(Command::KvGet {
            branch: Some(BranchId::from("mainline")),
            space: None,
            key: "shared".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(vv)) => assert_eq!(vv.value, Value::Int(2)),
        other => panic!("Expected merged shared value, got {:?}", other),
    }

    let output = executor
        .execute(Command::BranchRevert {
            branch: "mainline".into(),
            from_version: merge_version,
            to_version: merge_version,
        })
        .unwrap();
    match output {
        Output::BranchReverted(info) => {
            assert!(info.keys_reverted >= 1);
        }
        other => panic!("Expected BranchReverted, got {:?}", other),
    }

    let output = executor
        .execute(Command::KvGet {
            branch: Some(BranchId::from("mainline")),
            space: None,
            key: "shared".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(vv)) => assert_eq!(vv.value, Value::Int(1)),
        other => panic!("Expected reverted shared value, got {:?}", other),
    }

    let output = executor
        .execute(Command::BranchCherryPick {
            source: "feature".into(),
            target: "mainline".into(),
            keys: Some(vec![("default".into(), "shared".into())]),
            filter_spaces: None,
            filter_keys: None,
            filter_primitives: None,
        })
        .unwrap();
    match output {
        Output::BranchCherryPicked(info) => {
            assert_eq!(info.keys_applied, 1);
        }
        other => panic!("Expected BranchCherryPicked, got {:?}", other),
    }

    let output = executor
        .execute(Command::BranchExport {
            branch_id: "feature".into(),
            path: bundle_path.to_string_lossy().into_owned(),
        })
        .unwrap();
    match output {
        Output::BranchExported(result) => {
            assert_eq!(result.branch_id, "feature");
            assert_eq!(result.path, bundle_path.to_string_lossy());
        }
        other => panic!("Expected BranchExported, got {:?}", other),
    }
    assert!(
        bundle_path.exists(),
        "branch export should create a bundle file"
    );

    let output = executor
        .execute(Command::BranchBundleValidate {
            path: bundle_path.to_string_lossy().into_owned(),
        })
        .unwrap();
    match output {
        Output::BundleValidated(result) => {
            assert_eq!(result.branch_id, "feature");
            assert!(result.checksums_valid);
        }
        other => panic!("Expected BundleValidated, got {:?}", other),
    }

    let output = imported_executor
        .execute(Command::BranchImport {
            path: bundle_path.to_string_lossy().into_owned(),
        })
        .unwrap();
    match output {
        Output::BranchImported(result) => {
            assert_eq!(result.branch_id, "feature");
            assert!(result.keys_written >= 1);
        }
        other => panic!("Expected BranchImported, got {:?}", other),
    }

    let output = imported_executor
        .execute(Command::KvGet {
            branch: Some(BranchId::from("feature")),
            space: None,
            key: "shared".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(vv)) => assert_eq!(vv.value, Value::Int(2)),
        other => panic!("Expected imported feature branch value, got {:?}", other),
    }
}

#[test]
fn reserved_system_branch_is_rejected_by_public_executor() {
    let db = create_db();
    strata_graph::branch_dag::init_system_branch(&db);
    let executor = Executor::new(db);

    let error = executor
        .execute(Command::KvGet {
            branch: Some(BranchId::from("_system")),
            space: None,
            key: "secret".into(),
            as_of: None,
        })
        .expect_err("system branch access should be rejected");

    assert!(matches!(error, Error::InvalidInput { .. }));
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

// ============================================================================
// Graph Commands
// ============================================================================

#[test]
fn graph_commands_register_space_and_expose_direct_results() {
    let executor = create_executor();

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "graphs".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    let output = executor
        .execute(Command::GraphCreate {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            cascade_policy: None,
        })
        .unwrap();
    assert!(matches!(output, Output::Unit));

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "graphs".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));

    let output = executor
        .execute(Command::GraphList {
            branch: None,
            space: Some("graphs".into()),
        })
        .unwrap();
    match output {
        Output::Keys(graphs) => assert!(graphs.contains(&"demo".to_string())),
        other => panic!("Expected graph key list, got {:?}", other),
    }

    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            node_id: "alice".into(),
            entity_ref: None,
            properties: Some(Value::object(
                [("name".to_string(), Value::String("Alice".into()))]
                    .into_iter()
                    .collect(),
            )),
            object_type: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            node_id: "acme".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            src: "alice".into(),
            dst: "acme".into(),
            edge_type: "WORKS_AT".into(),
            weight: None,
            properties: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphGetNode {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            node_id: "alice".into(),
            as_of: None,
        })
        .unwrap();
    assert!(is_some_value(&output));

    let output = executor
        .execute(Command::GraphListNodes {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::Keys(nodes) => {
            assert!(nodes.contains(&"alice".to_string()));
            assert!(nodes.contains(&"acme".to_string()));
        }
        other => panic!("Expected node list, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphNeighbors {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            node_id: "alice".into(),
            direction: None,
            edge_type: None,
            as_of: None,
        })
        .unwrap();
    match output {
        Output::GraphNeighbors(neighbors) => {
            assert_eq!(neighbors.len(), 1);
            assert_eq!(neighbors[0].node_id, "acme");
        }
        other => panic!("Expected graph neighbors, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphBfs {
            branch: None,
            space: Some("graphs".into()),
            graph: "demo".into(),
            start: "alice".into(),
            max_depth: 8,
            max_nodes: None,
            edge_types: None,
            direction: None,
        })
        .unwrap();
    match output {
        Output::GraphBfs(result) => {
            assert_eq!(result.visited.first().map(String::as_str), Some("alice"));
            assert!(result.visited.iter().any(|node| node == "acme"));
        }
        other => panic!("Expected GraphBfs, got {:?}", other),
    }
}

#[test]
fn graph_writes_require_existing_branch() {
    let executor = create_executor();

    let error = executor
        .execute(Command::GraphCreate {
            branch: Some(BranchId::from("missing-branch")),
            space: Some("graphs".into()),
            graph: "demo".into(),
            cascade_policy: None,
        })
        .expect_err("graph writes on missing branches must fail");

    assert!(matches!(error, Error::BranchNotFound { .. }));
}

#[test]
fn graph_advanced_commands_expose_direct_results() {
    let executor = create_executor();
    let space = Some("graphs-advanced".to_string());
    let graph = "ontology-demo".to_string();

    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            cascade_policy: Some("detach".into()),
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphGetMeta {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();
    assert!(is_some_value(&output));

    executor
        .execute(Command::GraphDefineObjectType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            definition: Value::object(
                [
                    ("name".to_string(), Value::String("Person".into())),
                    (
                        "properties".to_string(),
                        Value::object(std::collections::HashMap::new()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::GraphDefineObjectType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            definition: Value::object(
                [
                    ("name".to_string(), Value::String("Temp".into())),
                    (
                        "properties".to_string(),
                        Value::object(std::collections::HashMap::new()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphGetObjectType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            name: "Person".into(),
        })
        .unwrap();
    assert!(is_some_value(&output));

    let output = executor
        .execute(Command::GraphListObjectTypes {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();
    match output {
        Output::Keys(types) => {
            assert!(types.contains(&"Person".to_string()));
            assert!(types.contains(&"Temp".to_string()));
        }
        other => panic!("Expected Keys, got {:?}", other),
    }

    executor
        .execute(Command::GraphDefineLinkType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            definition: Value::object(
                [
                    ("name".to_string(), Value::String("KNOWS".into())),
                    ("source".to_string(), Value::String("Person".into())),
                    ("target".to_string(), Value::String("Person".into())),
                    (
                        "properties".to_string(),
                        Value::object(std::collections::HashMap::new()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::GraphDefineLinkType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            definition: Value::object(
                [
                    ("name".to_string(), Value::String("TEMP_LINK".into())),
                    ("source".to_string(), Value::String("Person".into())),
                    ("target".to_string(), Value::String("Person".into())),
                    (
                        "properties".to_string(),
                        Value::object(std::collections::HashMap::new()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphGetLinkType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            name: "KNOWS".into(),
        })
        .unwrap();
    assert!(is_some_value(&output));

    let output = executor
        .execute(Command::GraphListLinkTypes {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();
    match output {
        Output::Keys(types) => {
            assert!(types.contains(&"KNOWS".to_string()));
            assert!(types.contains(&"TEMP_LINK".to_string()));
        }
        other => panic!("Expected Keys, got {:?}", other),
    }

    executor
        .execute(Command::GraphDeleteLinkType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            name: "TEMP_LINK".into(),
        })
        .unwrap();
    executor
        .execute(Command::GraphDeleteObjectType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            name: "Temp".into(),
        })
        .unwrap();

    executor
        .execute(Command::GraphBulkInsert {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            nodes: vec![
                strata_executor::BulkGraphNode {
                    node_id: "alice".into(),
                    entity_ref: None,
                    properties: None,
                    object_type: Some("Person".into()),
                },
                strata_executor::BulkGraphNode {
                    node_id: "bob".into(),
                    entity_ref: None,
                    properties: None,
                    object_type: Some("Person".into()),
                },
                strata_executor::BulkGraphNode {
                    node_id: "carol".into(),
                    entity_ref: None,
                    properties: None,
                    object_type: Some("Person".into()),
                },
            ],
            edges: vec![
                strata_executor::BulkGraphEdge {
                    src: "alice".into(),
                    dst: "bob".into(),
                    edge_type: "KNOWS".into(),
                    weight: None,
                    properties: None,
                },
                strata_executor::BulkGraphEdge {
                    src: "bob".into(),
                    dst: "carol".into(),
                    edge_type: "KNOWS".into(),
                    weight: None,
                    properties: None,
                },
            ],
            chunk_size: Some(2),
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphBulkInsert {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            nodes: vec![strata_executor::BulkGraphNode {
                node_id: "dave".into(),
                entity_ref: None,
                properties: None,
                object_type: Some("Person".into()),
            }],
            edges: vec![],
            chunk_size: Some(1),
        })
        .unwrap();
    match output {
        Output::GraphBulkInsertResult {
            nodes_inserted,
            edges_inserted,
        } => {
            assert_eq!(nodes_inserted, 1);
            assert_eq!(edges_inserted, 0);
        }
        other => panic!("Expected GraphBulkInsertResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphListNodesPaginated {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            limit: 2,
            cursor: None,
        })
        .unwrap();
    let next_cursor = match output {
        Output::GraphPage { items, next_cursor } => {
            assert_eq!(items.len(), 2);
            assert!(next_cursor.is_some());
            next_cursor
        }
        other => panic!("Expected GraphPage, got {:?}", other),
    };

    let output = executor
        .execute(Command::GraphListNodesPaginated {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            limit: 2,
            cursor: next_cursor,
        })
        .unwrap();
    match output {
        Output::GraphPage { items, .. } => {
            assert!(!items.is_empty());
        }
        other => panic!("Expected GraphPage, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphNodesByType {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            object_type: "Person".into(),
        })
        .unwrap();
    match output {
        Output::Keys(nodes) => {
            assert!(nodes.contains(&"alice".to_string()));
            assert!(nodes.contains(&"dave".to_string()));
        }
        other => panic!("Expected Keys, got {:?}", other),
    }

    executor
        .execute(Command::GraphFreezeOntology {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphOntologyStatus {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();
    assert!(is_some_value(&output));

    let output = executor
        .execute(Command::GraphOntologySummary {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();
    assert!(is_some_value(&output));

    let output = executor
        .execute(Command::GraphListOntologyTypes {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();
    match output {
        Output::Keys(types) => {
            assert!(types.contains(&"Person".to_string()));
            assert!(types.contains(&"KNOWS".to_string()));
        }
        other => panic!("Expected Keys, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphWcc {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            top_n: Some(3),
            include_all: Some(true),
        })
        .unwrap();
    match output {
        Output::GraphGroupSummary(summary) => {
            assert_eq!(summary.algorithm, "wcc");
            assert_eq!(summary.node_count, 4);
            assert!(summary.group_count >= 2);
            assert!(summary.all.is_some());
        }
        other => panic!("Expected GraphGroupSummary, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphCdlp {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            max_iterations: 8,
            direction: Some("both".into()),
            top_n: Some(3),
            include_all: Some(true),
        })
        .unwrap();
    match output {
        Output::GraphGroupSummary(summary) => {
            assert_eq!(summary.algorithm, "cdlp");
            assert_eq!(summary.node_count, 4);
            assert!(summary.all.is_some());
        }
        other => panic!("Expected GraphGroupSummary, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphPagerank {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            damping: None,
            max_iterations: Some(16),
            tolerance: None,
            top_n: Some(3),
            include_all: Some(true),
        })
        .unwrap();
    match output {
        Output::GraphScoreSummary(summary) => {
            assert_eq!(summary.algorithm, "pagerank");
            assert_eq!(summary.node_count, 4);
            assert!(!summary.top_nodes.is_empty());
            assert!(summary.all.is_some());
        }
        other => panic!("Expected GraphScoreSummary, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphLcc {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            top_n: Some(3),
            include_all: Some(true),
        })
        .unwrap();
    match output {
        Output::GraphScoreSummary(summary) => {
            assert_eq!(summary.algorithm, "lcc");
            assert_eq!(summary.node_count, 4);
            assert!(summary.global_clustering_coefficient.is_some());
            assert!(summary.zero_count.is_some());
        }
        other => panic!("Expected GraphScoreSummary, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphSssp {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            source: "alice".into(),
            direction: Some("both".into()),
            top_n: Some(3),
            include_all: Some(true),
        })
        .unwrap();
    match output {
        Output::GraphScoreSummary(summary) => {
            assert_eq!(summary.algorithm, "sssp");
            assert_eq!(summary.source.as_deref(), Some("alice"));
            assert!(summary.farthest.is_some());
            assert!(summary.all.is_some());
        }
        other => panic!("Expected GraphScoreSummary, got {:?}", other),
    }

    executor
        .execute(Command::GraphRemoveEdge {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            src: "alice".into(),
            dst: "bob".into(),
            edge_type: "KNOWS".into(),
        })
        .unwrap();
    executor
        .execute(Command::GraphRemoveNode {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
            node_id: "dave".into(),
        })
        .unwrap();

    executor
        .execute(Command::GraphDelete {
            branch: None,
            space: space.clone(),
            graph: graph.clone(),
        })
        .unwrap();

    let output = executor
        .execute(Command::GraphList {
            branch: None,
            space,
        })
        .unwrap();
    match output {
        Output::Keys(graphs) => assert!(!graphs.contains(&graph)),
        other => panic!("Expected Keys, got {:?}", other),
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

#[test]
fn json_list_count_sample_and_index_commands_return_expected_shapes() {
    let executor = create_executor();

    for (key, value) in [("doc:01", "alice"), ("doc:02", "bob"), ("doc:03", "carol")] {
        executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: key.into(),
                path: "$".into(),
                value: Value::object(
                    [("name".to_string(), Value::String(value.into()))]
                        .into_iter()
                        .collect(),
                ),
            })
            .unwrap();
    }

    let output = executor
        .execute(Command::JsonList {
            branch: None,
            space: None,
            prefix: Some("doc:".into()),
            cursor: None,
            limit: 2,
            as_of: None,
        })
        .unwrap();

    match output {
        Output::JsonListResult {
            keys,
            has_more,
            cursor,
        } => {
            assert_eq!(keys, vec!["doc:01".to_string(), "doc:02".to_string()]);
            assert!(has_more);
            assert!(cursor.is_some());
        }
        other => panic!("Expected JsonListResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::JsonCount {
            branch: None,
            space: None,
            prefix: Some("doc:".into()),
        })
        .unwrap();
    assert!(matches!(output, Output::Uint(3)));

    let output = executor
        .execute(Command::JsonSample {
            branch: None,
            space: None,
            prefix: Some("doc:".into()),
            count: Some(2),
        })
        .unwrap();

    match output {
        Output::SampleResult { total_count, items } => {
            assert_eq!(total_count, 3);
            assert_eq!(items.len(), 2);
            assert!(items.iter().all(|item| item.key.starts_with("doc:")));
        }
        other => panic!("Expected SampleResult, got {:?}", other),
    }

    let output = executor
        .execute(Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "people_name".into(),
            field_path: "$.name".into(),
            index_type: "tag".into(),
        })
        .unwrap();

    match output {
        Output::Maybe(Some(Value::String(json))) => {
            let value: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert_eq!(value["name"], "people_name");
        }
        other => panic!("Expected serialized index definition, got {:?}", other),
    }

    let output = executor
        .execute(Command::JsonListIndexes {
            branch: None,
            space: None,
        })
        .unwrap();

    match output {
        Output::Maybe(Some(Value::String(json))) => {
            let value: serde_json::Value = serde_json::from_str(&json).unwrap();
            let entries = value.as_array().expect("index list should be an array");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0]["name"], "people_name");
        }
        other => panic!("Expected serialized index list, got {:?}", other),
    }

    let output = executor
        .execute(Command::JsonDropIndex {
            branch: None,
            space: None,
            name: "people_name".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));
}

#[test]
fn space_create_exists_and_list_round_trip() {
    let executor = create_executor();

    let output = executor
        .execute(Command::SpaceCreate {
            branch: None,
            space: "analytics".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Unit));

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "analytics".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(true)));

    let output = executor
        .execute(Command::SpaceList { branch: None })
        .unwrap();
    match output {
        Output::SpaceList(spaces) => {
            assert!(spaces.contains(&"analytics".to_string()));
        }
        other => panic!("Expected SpaceList, got {:?}", other),
    }
}

#[test]
fn space_delete_force_removes_space_data_and_metadata() {
    let executor = create_executor();

    executor
        .execute(Command::SpaceCreate {
            branch: None,
            space: "analytics".into(),
        })
        .unwrap();
    executor
        .execute(Command::KvPut {
            branch: None,
            space: Some("analytics".into()),
            key: "user:1".into(),
            value: Value::String("Alice".into()),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: Some("analytics".into()),
            key: "doc:1".into(),
            path: "$".into(),
            value: Value::object(
                [("name".to_string(), Value::String("Alice".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::EventAppend {
            branch: None,
            space: Some("analytics".into()),
            event_type: "user.created".into(),
            payload: Value::object([("id".to_string(), Value::Int(1))].into_iter().collect()),
        })
        .unwrap();
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: Some("analytics".into()),
            collection: "embeddings".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: Some("analytics".into()),
            collection: "embeddings".into(),
            key: "user:1".into(),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: Some("analytics".into()),
            graph: "people".into(),
            cascade_policy: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: Some("analytics".into()),
            graph: "people".into(),
            node_id: "alice".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();

    let error = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "analytics".into(),
            force: false,
        })
        .expect_err("non-empty spaces should require force");
    assert!(matches!(error, Error::ConstraintViolation { .. }));

    let output = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "analytics".into(),
            force: true,
        })
        .unwrap();
    assert!(matches!(output, Output::Unit));

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "analytics".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: Some("analytics".into()),
            key: "user:1".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::Maybe(None) | Output::MaybeVersioned(None)
    ));

    let output = executor
        .execute(Command::JsonGet {
            branch: None,
            space: Some("analytics".into()),
            key: "doc:1".into(),
            path: "$".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::Maybe(None) | Output::MaybeVersioned(None)
    ));

    let output = executor
        .execute(Command::EventLen {
            branch: None,
            space: Some("analytics".into()),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(output, Output::Uint(0)));

    let output = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: Some("analytics".into()),
        })
        .unwrap();
    match output {
        Output::VectorCollectionList(collections) => assert!(collections.is_empty()),
        other => panic!("Expected empty vector collection list, got {:?}", other),
    }

    let output = executor
        .execute(Command::GraphList {
            branch: None,
            space: Some("analytics".into()),
        })
        .unwrap();
    match output {
        Output::Keys(graphs) => assert!(graphs.is_empty()),
        other => panic!("Expected empty graph list, got {:?}", other),
    }

    let error = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "default".into(),
            force: true,
        })
        .expect_err("default space should be protected");
    assert!(matches!(error, Error::ConstraintViolation { .. }));
}

#[test]
fn describe_reports_default_space_counts_and_available_spaces() {
    let executor = create_executor();

    executor
        .execute(Command::SpaceCreate {
            branch: None,
            space: "analytics".into(),
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "default:key".into(),
            value: Value::Int(1),
        })
        .unwrap();
    executor
        .execute(Command::KvPut {
            branch: None,
            space: Some("analytics".into()),
            key: "analytics:key".into(),
            value: Value::Int(2),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "doc:1".into(),
            path: "$".into(),
            value: Value::object(
                [("kind".to_string(), Value::String("default".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: Some("analytics".into()),
            key: "doc:2".into(),
            path: "$".into(),
            value: Value::object(
                [("kind".to_string(), Value::String("analytics".into()))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();
    executor
        .execute(Command::EventAppend {
            branch: None,
            space: None,
            event_type: "default.created".into(),
            payload: Value::object([("id".to_string(), Value::Int(1))].into_iter().collect()),
        })
        .unwrap();
    executor
        .execute(Command::EventAppend {
            branch: None,
            space: Some("analytics".into()),
            event_type: "analytics.created".into(),
            payload: Value::object([("id".to_string(), Value::Int(2))].into_iter().collect()),
        })
        .unwrap();
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "embeddings".into(),
            key: "v1".into(),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            metadata: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "demo".into(),
            cascade_policy: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddNode {
            branch: None,
            space: None,
            graph: "demo".into(),
            node_id: "alice".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::Describe { branch: None })
        .unwrap();

    match output {
        Output::Described(description) => {
            assert_eq!(description.branch, "default");
            assert!(description.branches.contains(&"default".to_string()));
            assert!(description.spaces.contains(&"default".to_string()));
            assert!(description.spaces.contains(&"analytics".to_string()));
            assert_eq!(description.primitives.kv.count, 1);
            assert_eq!(description.primitives.json.count, 1);
            assert_eq!(description.primitives.events.count, 1);
            assert_eq!(description.primitives.vector.collections.len(), 1);
            assert_eq!(
                description.primitives.vector.collections[0].name,
                "embeddings"
            );
            assert_eq!(description.primitives.graph.graphs.len(), 1);
            assert_eq!(description.primitives.graph.graphs[0].name, "demo");
            assert!(description.capabilities.vector_query);
        }
        other => panic!("Expected DescribeResult, got {:?}", other),
    }
}

#[test]
fn json_index_commands_do_not_register_space_as_a_side_effect() {
    let db = create_db();
    let executor = Executor::new(db);

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "ghost".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    let error = executor
        .execute(Command::JsonCreateIndex {
            branch: None,
            space: Some("ghost".into()),
            name: "ghost_index".into(),
            field_path: "$.name".into(),
            index_type: "bogus".into(),
        })
        .expect_err("invalid index types should still fail");
    assert!(matches!(error, Error::InvalidInput { .. }));

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "ghost".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    let output = executor
        .execute(Command::JsonDropIndex {
            branch: None,
            space: Some("ghost".into()),
            name: "ghost_index".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    let output = executor
        .execute(Command::SpaceExists {
            branch: None,
            space: "ghost".into(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));
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
        EntityRef::kv(
            strata_core::types::BranchId::from_bytes([0; 16]),
            "default",
            "k",
        ),
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
