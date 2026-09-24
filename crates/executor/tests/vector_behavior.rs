//! Executor vector command behavior tests.

use serde_json::json;
use strata_engine::{CacheOpenOptions, Database};
use strata_executor::{
    BatchItemStatus, BatchStatus, BatchVectorEntry, Command, Executor, ExecutorErrorClass,
    MutationEffect, Output, VectorDistanceMetric, VectorFilterCondition, VectorFilterOp,
    VectorMetadataFilter, VectorScalar, VectorVersionedData, DEFAULT_BRANCH,
};
use tempfile::TempDir;

#[test]
fn cache_executor_runs_complete_vector_command_suite() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    run_vector_command_suite(&mut executor);
}

#[test]
fn vector_upsert_rejects_a_subnormal_embedding_instead_of_storing_zeros() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 3,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("collection create succeeds");

    // The SDK sends JSON numbers (f64). A finite but subnormal-magnitude embedding
    // must be rejected, not silently narrowed to the zero vector and reported as
    // applied. Deserialize through the wire command so the engine sees the f64
    // precision (a `Vec<f32>` built in Rust would already have underflowed).
    let subnormal: Command = serde_json::from_str(
        r#"{"type":"vector_upsert","collection":"docs","key":"z","vector":[1e-308,1e-308,1e-308]}"#,
    )
    .expect("command deserializes");
    let error = executor
        .execute(subnormal)
        .expect_err("a subnormal embedding is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.vector_embedding");

    // The opposite extreme is (and stays) rejected with the same code.
    let overflow: Command = serde_json::from_str(
        r#"{"type":"vector_upsert","collection":"docs","key":"big","vector":[1e308,0.0,0.0]}"#,
    )
    .expect("command deserializes");
    assert_eq!(
        executor
            .execute(overflow)
            .expect_err("an overflowing embedding is rejected")
            .code(),
        "invalid_argument.engine.vector_embedding"
    );

    // Neither rejected upsert stored anything.
    let Output::Uint(count) = executor
        .execute(Command::VectorCount {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("count succeeds")
    else {
        panic!("unexpected vector count output");
    };
    assert_eq!(count, 0);
}

#[test]
fn vector_upsert_rejects_non_object_metadata_instead_of_storing_it_unfilterable() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("collection create succeeds");

    // Metadata that is not a JSON object (list, scalar, number) can never match
    // a filter, so the engine must reject it at ingest rather than store an
    // unfilterable row and report the upsert as applied.
    for body in [
        r#"{"type":"vector_upsert","collection":"docs","key":"a","vector":[1.0,0.0],"metadata":["tag"]}"#,
        r#"{"type":"vector_upsert","collection":"docs","key":"a","vector":[1.0,0.0],"metadata":"scalar"}"#,
        r#"{"type":"vector_upsert","collection":"docs","key":"a","vector":[1.0,0.0],"metadata":7}"#,
    ] {
        let command: Command = serde_json::from_str(body).expect("command deserializes");
        let error = executor
            .execute(command)
            .expect_err("non-object metadata is rejected");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.vector_metadata");
    }

    // Object metadata still upserts, and nothing from the rejected calls stuck.
    let ok: Command = serde_json::from_str(
        r#"{"type":"vector_upsert","collection":"docs","key":"a","vector":[1.0,0.0],"metadata":{"kind":"doc"}}"#,
    )
    .expect("command deserializes");
    executor.execute(ok).expect("object metadata upserts");
    let Output::Uint(count) = executor
        .execute(Command::VectorCount {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("count succeeds")
    else {
        panic!("unexpected vector count output");
    };
    assert_eq!(count, 1);
}

#[test]
fn vector_query_rejects_a_subnormal_query_vector_instead_of_searching_zeros() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 3,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("collection create succeeds");
    let seed: Command = serde_json::from_str(
        r#"{"type":"vector_upsert","collection":"docs","key":"a","vector":[1.0,0.0,0.0]}"#,
    )
    .expect("command deserializes");
    executor.execute(seed).expect("seed upsert succeeds");

    // A query embedding whose components underflow f32 must be rejected, not
    // silently narrowed to the zero vector and matched against every row.
    // Deserialize through the wire command so the engine sees f64 precision.
    let subnormal: Command = serde_json::from_str(
        r#"{"type":"vector_query","collection":"docs","query":[1e-308,1e-308,1e-308],"k":5}"#,
    )
    .expect("command deserializes");
    let error = executor
        .execute(subnormal)
        .expect_err("a subnormal query vector is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.vector_embedding");

    // The index-planner query shares the guard, and the overflow extreme stays
    // rejected with the same code.
    let subnormal_index: Command = serde_json::from_str(
        r#"{"type":"vector_index_query","collection":"docs","query":[1e-308,1e-308,1e-308],"k":5}"#,
    )
    .expect("command deserializes");
    assert_eq!(
        executor
            .execute(subnormal_index)
            .expect_err("a subnormal index query vector is rejected")
            .code(),
        "invalid_argument.engine.vector_embedding"
    );

    let overflow: Command = serde_json::from_str(
        r#"{"type":"vector_query","collection":"docs","query":[1e308,0.0,0.0],"k":5}"#,
    )
    .expect("command deserializes");
    assert_eq!(
        executor
            .execute(overflow)
            .expect_err("an overflowing query vector is rejected")
            .code(),
        "invalid_argument.engine.vector_embedding"
    );
}

#[test]
fn vector_batch_get_reports_missing_keys_as_misses() {
    // A missing key in a batch read is a positional miss, not a success —
    // matching kv batch-get. The outer batch is then partial.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("create collection");
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "present".to_owned(),
            vector: vec![1.0, 0.0],
            text: None,
            metadata: None,
        })
        .expect("upsert present vector");

    let Output::VectorBatchGetResults(results) = executor
        .execute(Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["present".to_owned(), "absent".to_owned()],
        })
        .expect("batch get succeeds")
    else {
        panic!("unexpected batch get output");
    };
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status(), BatchItemStatus::Ok);
    assert!(results[0].error_status().is_none());
    assert_eq!(
        results[1].status(),
        BatchItemStatus::Miss,
        "a missing vector is a miss, not an ok"
    );
    assert!(results[1].error_status().is_none());
    assert_eq!(
        results.status(),
        BatchStatus::Partial,
        "a batch with a miss is partial, not ok"
    );
}

#[test]
fn vector_batch_exists_reports_present_and_absent_keys() {
    // exists=false for an absent key is a definitive answer, so it is an ok
    // item and the outer batch is ok (never a miss) — matching kv batch-exists.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("create collection");
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "present".to_owned(),
            vector: vec![1.0, 0.0],
            text: None,
            metadata: None,
        })
        .expect("upsert present vector");

    let Output::VectorBatchExistsResults(results) = executor
        .execute(Command::VectorBatchExists {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["present".to_owned(), "absent".to_owned()],
        })
        .expect("batch exists succeeds")
    else {
        panic!("unexpected batch exists output");
    };
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status(), BatchItemStatus::Ok);
    assert!(results[0].exists(), "present key exists");
    assert_eq!(results[1].status(), BatchItemStatus::Ok);
    assert!(!results[1].exists(), "absent key does not exist");
    assert_eq!(
        results.status(),
        BatchStatus::Ok,
        "a definitive exists=false answer is ok, not a miss"
    );
}

#[test]
fn durable_executor_reopens_vector_collections_rows_and_history() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");

    {
        let mut executor = Executor::open_durable_local(&path).expect("durable executor opens");
        run_vector_command_suite(&mut executor);
        executor.close().expect("durable executor closes");
    }

    let mut reopened = Executor::open_durable_local(&path).expect("durable executor reopens");
    assert_eq!(vector_count(&mut reopened, "docs"), 1);
    let Output::VectorData(value) = reopened
        .execute(Command::VectorGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("vector get succeeds")
    else {
        panic!("unexpected vector get output");
    };
    let value = value.into_option().expect("vector value present");
    assert_eq!(value.data().embedding(), &[0.0, 1.0]);
    assert_eq!(value.vector_revision(), 3);
    assert_vector_history_has_tombstone(&mut reopened, "docs", "doc-b");
}

#[test]
fn vector_index_query_returns_matches_and_planner_diagnostics() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_collection(&mut executor, "docs", VectorDistanceMetric::Cosine);
    upsert_vector(
        &mut executor,
        "docs",
        "doc-a",
        vec![1.0, 0.0],
        json!({"kind": "doc"}),
    );
    upsert_vector(
        &mut executor,
        "docs",
        "note-b",
        vec![0.0, 1.0],
        json!({"kind": "note"}),
    );

    let Output::VectorIndexQuery(result) = executor
        .execute(Command::VectorIndexQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: vec![1.0, 0.0],
            k: 10,
            filter: Some(kind_filter("doc")),
            as_of: None,
            as_of_time: None,
        })
        .expect("vector index query succeeds")
    else {
        panic!("unexpected vector index query output");
    };

    assert_eq!(result.matches().len(), 1);
    assert_eq!(result.matches()[0].key(), "doc-a");
    assert_eq!(result.diagnostics().collection(), "docs");
    assert_eq!(result.diagnostics().manifest_status(), "missing");
    assert!(!result.diagnostics().last_query_used_index());
    assert_eq!(result.diagnostics().indexed_source_count(), 0);
    assert_eq!(result.diagnostics().exact_fallback_count(), 0);
    assert_eq!(result.diagnostics().hnsw_graph_builds(), 0);
    assert!(result.diagnostics().artifact_sources().is_empty());
}

#[test]
fn vector_branch_and_space_defaults_are_isolated() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_collection_in(
        &mut executor,
        None,
        None,
        "shared",
        VectorDistanceMetric::Cosine,
    );
    executor
        .branch_fork_current(DEFAULT_BRANCH, "feature")
        .expect("branch creates");

    upsert_vector_in(
        &mut executor,
        None,
        None,
        "shared",
        "doc",
        vec![1.0, 0.0],
        json!({"where": "default"}),
    );
    upsert_vector_in(
        &mut executor,
        Some("feature"),
        None,
        "shared",
        "doc",
        vec![0.0, 1.0],
        json!({"where": "feature"}),
    );
    create_collection_in(
        &mut executor,
        None,
        Some("tenant-a"),
        "shared",
        VectorDistanceMetric::Euclidean,
    );
    upsert_vector_in(
        &mut executor,
        None,
        Some("tenant-a"),
        "shared",
        "doc",
        vec![0.5, 0.5],
        json!({"where": "space"}),
    );

    assert_eq!(
        get_vector_where(&mut executor, None, None, "shared", "doc"),
        Some("default".to_owned())
    );
    assert_eq!(
        get_vector_where(&mut executor, Some("feature"), None, "shared", "doc"),
        Some("feature".to_owned())
    );
    assert_eq!(
        get_vector_where(&mut executor, None, Some("tenant-a"), "shared", "doc"),
        Some("space".to_owned())
    );
    assert_eq!(vector_count_in(&mut executor, None, None, "shared"), 1);
    assert_eq!(
        vector_count_in(&mut executor, Some("feature"), None, "shared"),
        1
    );
    assert_eq!(
        vector_count_in(&mut executor, None, Some("tenant-a"), "shared"),
        1
    );
}

#[test]
fn vector_executor_inherits_configured_database_default_branch() {
    let options = CacheOpenOptions::new()
        .with_default_branch("main")
        .expect("valid branch");
    let database = Database::open_cache(options)
        .expect("cache database opens")
        .into_database();
    let mut executor = Executor::from_database(database);

    assert_eq!(executor.default_branch(), "main");
    create_collection(&mut executor, "docs", VectorDistanceMetric::Cosine);
    upsert_vector(
        &mut executor,
        "docs",
        "doc",
        vec![1.0, 0.0],
        json!({"where": "main"}),
    );
    assert_eq!(
        get_vector_where(&mut executor, None, None, "docs", "doc"),
        Some("main".to_owned())
    );

    let error = executor
        .execute(Command::VectorGet {
            branch: Some(DEFAULT_BRANCH.to_owned()),
            space: None,
            collection: "docs".to_owned(),
            key: "doc".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("literal default branch is absent");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
}

#[test]
fn vector_error_contract_runs_in_cache_mode() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_collection(&mut executor, "docs", VectorDistanceMetric::Cosine);

    assert_invalid_input_vector_commands(&mut executor);
    assert_not_found_vector_commands(&mut executor);
    assert_closed_handle_rejects_vector_commands(executor);
}

#[test]
fn vector_collection_and_empty_batch_edges_run_in_cache_and_durable_modes() {
    run_vector_modes(assert_vector_collection_and_empty_batch_edges);
}

#[test]
fn vector_mutation_patch_and_delete_edges_run_in_cache_and_durable_modes() {
    run_vector_modes(assert_vector_mutation_patch_and_delete_edges);
}

#[test]
fn vector_batch_filter_and_delete_all_edges_run_in_cache_and_durable_modes() {
    run_vector_modes(assert_vector_batch_filter_and_delete_all_edges);
}

#[test]
fn vector_query_metric_ordering_runs_in_cache_and_durable_modes() {
    run_vector_modes(assert_vector_query_metric_ordering);
}

#[test]
fn vector_as_of_query_and_history_run_in_cache_and_durable_modes() {
    run_vector_modes(assert_vector_as_of_query_and_history);
}

#[test]
fn vector_branch_and_space_mutation_edges_run_in_cache_and_durable_modes() {
    run_vector_modes(assert_vector_branch_and_space_mutation_edges);
}

#[test]
fn vector_command_to_output_mapping_is_explicit_for_every_variant() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_collection(&mut executor, "map", VectorDistanceMetric::Cosine);
    upsert_vector(
        &mut executor,
        "map",
        "map-a",
        vec![1.0, 0.0],
        json!({"kind": "doc"}),
    );

    let outputs = vector_mapping_commands()
        .into_iter()
        .map(|command| executor.execute(command).expect("command succeeds"))
        .collect::<Vec<_>>();

    assert_vector_mapping_outputs(&outputs);
}

fn vector_mapping_commands() -> Vec<Command> {
    let mut commands = vector_mapping_collection_commands();
    commands.extend(vector_mapping_row_commands());
    commands.extend(vector_mapping_bulk_commands());
    commands
}

fn vector_mapping_collection_commands() -> Vec<Command> {
    vec![
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "other".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::DotProduct,
            embedding_model: None,
        },
        Command::VectorDeleteCollection {
            branch: None,
            space: None,
            collection: "other".to_owned(),
            force: true,
        },
        Command::VectorListCollections {
            branch: None,
            space: None,
        },
        Command::VectorCollectionStats {
            branch: None,
            space: None,
            collection: "map".to_owned(),
        },
        Command::VectorCount {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            as_of: None,
            as_of_time: None,
        },
    ]
}

fn vector_mapping_row_commands() -> Vec<Command> {
    vec![
        Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            key: "map-b".to_owned(),
            vector: vec![0.0, 1.0],
            text: None,
            metadata: Some(json!({"kind": "doc"})),
        },
        Command::VectorGet {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            key: "map-a".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorHistory {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            key: "map-a".to_owned(),
        },
        Command::VectorExists {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            key: "map-a".to_owned(),
        },
        Command::VectorBatchExists {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            keys: vec!["map-a".to_owned(), "missing".to_owned()],
        },
        Command::VectorListKeys {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            prefix: Some("map-".to_owned()),
            cursor: None,
            limit: Some(2),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            key: "map-a".to_owned(),
            patch: json!({"rank": 2}),
        },
        Command::VectorDelete {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            key: "missing".to_owned(),
        },
    ]
}

fn vector_mapping_bulk_commands() -> Vec<Command> {
    vec![
        Command::VectorDeleteByFilter {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            filter: kind_filter("none"),
        },
        Command::VectorDeleteAll {
            branch: None,
            space: None,
            collection: "map".to_owned(),
        },
        Command::VectorQuery {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            query: vec![1.0, 0.0],
            text: None,
            k: 10,
            filter: None,
            as_of: None,
            as_of_time: None,
        },
        Command::VectorIndexQuery {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            query: vec![1.0, 0.0],
            k: 10,
            filter: None,
            as_of: None,
            as_of_time: None,
        },
        Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            entries: vec![BatchVectorEntry::new(
                "map-c",
                vec![1.0, 0.0],
                Some(json!({"kind": "doc"})),
            )],
        },
        Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            keys: vec!["map-c".to_owned(), "missing".to_owned()],
        },
        Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: "map".to_owned(),
            keys: vec!["map-c".to_owned(), "missing".to_owned()],
        },
    ]
}

fn assert_vector_mapping_outputs(outputs: &[Output]) {
    assert_eq!(outputs.len(), 20);
    assert!(matches!(outputs[0], Output::VectorCollectionList { .. }));
    assert!(matches!(outputs[1], Output::Bool(_)));
    assert!(matches!(outputs[2], Output::VectorCollectionList { .. }));
    assert!(matches!(outputs[3], Output::VectorCollectionList { .. }));
    assert!(matches!(outputs[4], Output::Uint(_)));
    assert!(matches!(outputs[5], Output::VectorWriteResult { .. }));
    assert!(matches!(outputs[6], Output::VectorData(_)));
    assert!(matches!(outputs[7], Output::VectorVersionHistory(_)));
    assert!(matches!(outputs[8], Output::Bool(_)));
    assert!(matches!(outputs[9], Output::VectorBatchExistsResults(_)));
    assert!(matches!(outputs[10], Output::VectorKeyPage { .. }));
    assert!(matches!(
        outputs[11],
        Output::VectorMetadataUpdateResult { .. }
    ));
    assert!(matches!(outputs[12], Output::VectorDeleteResult { .. }));
    assert!(matches!(outputs[13], Output::VectorBulkDeleteResult { .. }));
    assert!(matches!(outputs[14], Output::VectorBulkDeleteResult { .. }));
    assert!(matches!(outputs[15], Output::VectorMatches(_)));
    assert!(matches!(outputs[16], Output::VectorIndexQuery(_)));
    assert!(matches!(outputs[17], Output::VectorBatchUpsertResults(_)));
    assert!(matches!(outputs[18], Output::VectorBatchGetResults(_)));
    assert!(matches!(outputs[19], Output::VectorBatchDeleteResults(_)));
}

fn assert_invalid_input_vector_commands(executor: &mut Executor) {
    for command in invalid_input_vector_commands() {
        let error = executor.execute(command).expect_err("command fails");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert!(
            error.code().contains(".engine.") || error.code().contains(".executor."),
            "unexpected public error code: {}",
            error.code()
        );
    }
}

fn invalid_input_vector_commands() -> Vec<Command> {
    vec![
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "bad/name".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        },
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "zero".to_owned(),
            dimension: 0,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        },
        Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "empty-vector".to_owned(),
            vector: Vec::new(),
            text: None,
            metadata: None,
        },
        Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "nan-vector".to_owned(),
            vector: vec![f64::NAN, 0.0],
            text: None,
            metadata: None,
        },
        Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc".to_owned(),
            patch: json!([]),
        },
        Command::VectorDeleteByFilter {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            filter: VectorMetadataFilter::new(Vec::new()),
        },
        Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: Vec::new(),
            text: None,
            k: 10,
            filter: None,
            as_of: None,
            as_of_time: None,
        },
        Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: vec![1.0, 0.0],
            text: None,
            k: 10,
            filter: Some(VectorMetadataFilter::new(vec![VectorFilterCondition::eq(
                "nested.path",
                "doc",
            )])),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorIndexQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: Vec::new(),
            k: 10,
            filter: None,
            as_of: None,
            as_of_time: None,
        },
    ]
}

fn assert_not_found_vector_commands(executor: &mut Executor) {
    for command in not_found_vector_commands() {
        let error = executor.execute(command).expect_err("command fails");
        assert_eq!(error.class(), ExecutorErrorClass::NotFound);
        assert!(
            error.code().contains(".engine.") || error.code().contains(".executor."),
            "unexpected public error code: {}",
            error.code()
        );
    }
}

fn not_found_vector_commands() -> Vec<Command> {
    vec![
        Command::VectorCollectionStats {
            branch: None,
            space: None,
            collection: "missing".to_owned(),
        },
        Command::VectorCount {
            branch: None,
            space: None,
            collection: "missing".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorGet {
            branch: Some("missing".to_owned()),
            space: None,
            collection: "docs".to_owned(),
            key: "doc".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::VectorBatchUpsert {
            branch: Some("missing".to_owned()),
            space: None,
            collection: "docs".to_owned(),
            entries: Vec::new(),
        },
    ]
}

fn assert_closed_handle_rejects_vector_commands(mut executor: Executor) {
    executor.close().expect("close succeeds");
    for command in closed_handle_vector_commands() {
        let error = executor.execute(command).expect_err("closed command fails");
        assert_eq!(error.class(), ExecutorErrorClass::ClosedHandle);
    }
}

fn closed_handle_vector_commands() -> Vec<Command> {
    vec![
        Command::VectorListCollections {
            branch: None,
            space: None,
        },
        Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc".to_owned(),
            vector: vec![1.0, 0.0],
            text: None,
            metadata: None,
        },
        Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: vec![1.0, 0.0],
            text: None,
            k: 1,
            filter: None,
            as_of: None,
            as_of_time: None,
        },
    ]
}

fn run_vector_modes(mut assert_case: impl FnMut(&mut Executor)) {
    let mut cache = Executor::open_cache().expect("cache executor opens");
    assert_case(&mut cache);

    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");
    let mut durable = Executor::open_durable_local(&path).expect("durable executor opens");
    assert_case(&mut durable);
}

fn assert_vector_collection_and_empty_batch_edges(executor: &mut Executor) {
    create_collection(executor, "cosine", VectorDistanceMetric::Cosine);
    create_collection(executor, "euclidean", VectorDistanceMetric::Euclidean);
    create_collection(executor, "dot", VectorDistanceMetric::DotProduct);

    assert_eq!(
        collection_names(executor),
        vec![
            "cosine".to_owned(),
            "dot".to_owned(),
            "euclidean".to_owned()
        ]
    );
    assert_eq!(
        collection_metric(executor, "dot"),
        VectorDistanceMetric::DotProduct
    );
    assert_eq!(vector_count(executor, "cosine"), 0);
    assert_eq!(
        query_vector_keys(executor, "cosine", vec![1.0, 0.0], 0, None),
        Vec::<String>::new()
    );
    assert_eq!(
        query_vector_keys(executor, "cosine", vec![1.0, 0.0], 10, None),
        Vec::<String>::new()
    );

    let duplicate = executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "cosine".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect_err("duplicate collection fails");
    assert_eq!(duplicate.class(), ExecutorErrorClass::Conflict);

    assert_vector_batch_outputs_empty(executor, "cosine");
    assert!(delete_collection(executor, "dot"));
    assert!(!delete_collection(executor, "missing"));
}

fn assert_vector_mutation_patch_and_delete_edges(executor: &mut Executor) {
    create_collection(executor, "docs", VectorDistanceMetric::Cosine);
    upsert_without_metadata(executor, "docs", "doc", vec![1.0, 0.0]);
    let no_metadata =
        get_vector_value(executor, None, None, "docs", "doc", None).expect("vector exists");
    assert!(no_metadata.data().metadata().is_none());

    upsert_vector(
        executor,
        "docs",
        "doc",
        vec![0.0, 1.0],
        json!({"kind": "doc", "rank": 1}),
    );
    let patched_revision = patch_vector_metadata(executor, "docs", "doc", json!({"rank": 2}));
    let patched =
        get_vector_value(executor, None, None, "docs", "doc", None).expect("patched vector exists");
    assert_eq!(patched.data().embedding(), &[0.0, 1.0]);
    assert_eq!(patched.vector_revision(), patched_revision);
    let metadata = patched.data().metadata().expect("metadata exists");
    assert_eq!(metadata["kind"], "doc");
    assert_eq!(metadata["rank"], 2);

    let missing_patch = executor
        .execute(Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "missing".to_owned(),
            patch: json!({"rank": 3}),
        })
        .expect("missing patch succeeds");
    assert!(matches!(
        missing_patch,
        Output::VectorMetadataUpdateResult { effect, .. } if !effect.applied()
    ));

    let bad_patch = executor
        .execute(Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc".to_owned(),
            patch: json!([]),
        })
        .expect_err("bad patch fails");
    assert_eq!(bad_patch.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        get_vector_value(executor, None, None, "docs", "doc", None)
            .expect("vector still exists")
            .data()
            .metadata()
            .expect("metadata exists")["rank"],
        2
    );

    assert!(delete_vector(executor, "docs", "doc"));
    assert!(!vector_exists(executor, "docs", "doc"));
    assert!(get_vector_value(executor, None, None, "docs", "doc", None).is_none());
    assert_eq!(
        list_vector_keys(executor, None, None, "docs", None, None, Some(10))
            .0
            .len(),
        0
    );
    assert_eq!(vector_count(executor, "docs"), 0);
    assert!(query_vector_keys(executor, "docs", vec![0.0, 1.0], 10, None).is_empty());
    assert!(!delete_vector(executor, "docs", "doc"));
}

fn assert_vector_batch_filter_and_delete_all_edges(executor: &mut Executor) {
    create_collection(executor, "docs", VectorDistanceMetric::Cosine);
    assert_vector_batch_outputs_empty(executor, "docs");
    assert_mixed_validity_batch_returns_positional_errors(executor);
    assert_duplicate_batch_upsert_and_batch_reads(executor);
    assert_batch_delete_edges(executor);
    assert_filtered_delete_edges(executor);
    assert_delete_all_edges(executor);
}

fn assert_mixed_validity_batch_returns_positional_errors(executor: &mut Executor) {
    let Output::VectorBatchUpsertResults(results) = executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            entries: vec![
                BatchVectorEntry::new("valid", vec![1.0, 0.0], Some(json!({"group": "doc"}))),
                BatchVectorEntry::new("invalid", vec![1.0], Some(json!({"group": "doc"}))),
            ],
        })
        .expect("mixed-validity batch succeeds")
    else {
        panic!("unexpected vector batch output");
    };
    assert_eq!(results.status(), BatchStatus::Partial);
    assert_eq!(results.len(), 2);
    assert!(results[0].applied());
    assert!(results[0].commit().is_some());
    assert_eq!(results[0].index(), 0);
    assert!(!results[1].applied());
    assert_eq!(results[1].index(), 1);
    assert_eq!(
        results[1]
            .error_status()
            .expect("dimension item error")
            .code(),
        "invalid_argument.executor.vector_dimension"
    );
    assert_eq!(vector_count(executor, "docs"), 1);
    assert!(delete_vector(executor, "docs", "valid"));
    assert_eq!(vector_count(executor, "docs"), 0);
}

fn assert_duplicate_batch_upsert_and_batch_reads(executor: &mut Executor) {
    // A duplicate key in a mutation batch rejects the whole batch (KV's rule)
    // and leaves state untouched, rather than silently applying last-wins.
    let error = executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            entries: vec![
                BatchVectorEntry::new("a", vec![1.0, 0.0], Some(json!({"group": "batch"}))),
                BatchVectorEntry::new("a", vec![0.5, 0.5], Some(json!({"group": "batch"}))),
            ],
        })
        .expect_err("duplicate-key batch upsert is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.executor.vector_batch_duplicate_key"
    );
    assert_eq!(vector_count(executor, "docs"), 0);

    // A batch with unique keys applies and establishes "a" and "b".
    let Output::VectorBatchUpsertResults(results) = executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            entries: vec![
                BatchVectorEntry::new("b", vec![0.0, 1.0], Some(json!({"group": "batch"}))),
                BatchVectorEntry::new("a", vec![1.0, 0.0], Some(json!({"group": "batch"}))),
            ],
        })
        .expect("unique-key batch upsert succeeds")
    else {
        panic!("unexpected batch upsert output");
    };
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(strata_executor::BatchItem::applied));
    assert_eq!(results[0].vector_revision(), Some(1));
    assert_eq!(results[1].vector_revision(), Some(1));
    assert_eq!(vector_count(executor, "docs"), 2);

    let Output::VectorBatchGetResults(values) = executor
        .execute(Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec![
                "a".to_owned(),
                "missing".to_owned(),
                "a".to_owned(),
                "b".to_owned(),
            ],
        })
        .expect("batch get succeeds")
    else {
        panic!("unexpected batch get output");
    };
    assert_eq!(values.len(), 4);
    assert_eq!(values[0].value().expect("a exists").key(), "a");
    assert!(values[1].value().is_none());
    assert_eq!(values[2].value().expect("a exists").key(), "a");
    assert_eq!(values[3].value().expect("b exists").key(), "b");

    let Output::VectorBatchGetResults(invalid_values) = executor
        .execute(Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["bad\0key".to_owned(), "a".to_owned()],
        })
        .expect("mixed-validity batch get succeeds")
    else {
        panic!("unexpected batch get output");
    };
    assert_eq!(invalid_values.status(), BatchStatus::Partial);
    assert_eq!(
        invalid_values[0]
            .error_status()
            .expect("invalid key item error")
            .code(),
        "invalid_argument.engine.vector_key"
    );
    assert_eq!(invalid_values[0].index(), 0);
    assert_eq!(invalid_values[1].value().expect("a exists").key(), "a");
}

fn assert_batch_delete_edges(executor: &mut Executor) {
    // A duplicate key in a delete batch rejects the whole batch and changes
    // nothing (KV's rule).
    let error = executor
        .execute(Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["a".to_owned(), "a".to_owned()],
        })
        .expect_err("duplicate-key batch delete is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.executor.vector_batch_duplicate_key"
    );
    assert!(get_vector_value(executor, None, None, "docs", "a", None).is_some());

    // Unique keys apply; a missing key is a positional no-op.
    let Output::VectorBatchDeleteResults(results) = executor
        .execute(Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["a".to_owned(), "missing".to_owned()],
        })
        .expect("batch delete succeeds")
    else {
        panic!("unexpected batch delete output");
    };
    assert_eq!(
        results
            .iter()
            .map(strata_executor::BatchItem::applied)
            .collect::<Vec<_>>(),
        vec![true, false]
    );
    assert!(get_vector_value(executor, None, None, "docs", "a", None).is_none());
    assert_eq!(
        list_vector_keys(executor, None, None, "docs", None, None, Some(10)).0,
        vec!["b"]
    );

    let Output::VectorBatchDeleteResults(invalid_delete) = executor
        .execute(Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["bad\0key".to_owned(), "missing".to_owned()],
        })
        .expect("mixed-validity batch delete succeeds")
    else {
        panic!("unexpected batch delete output");
    };
    assert_eq!(invalid_delete.status(), BatchStatus::Partial);
    assert_eq!(
        invalid_delete[0]
            .error_status()
            .expect("invalid key item error")
            .code(),
        "invalid_argument.engine.vector_key"
    );
    assert_eq!(invalid_delete[0].index(), 0);
    assert!(!invalid_delete[1].applied());
    assert_eq!(invalid_delete[1].index(), 1);
}

fn assert_filtered_delete_edges(executor: &mut Executor) {
    upsert_vector(
        executor,
        "docs",
        "doc-1",
        vec![1.0, 0.0],
        json!({"kind": "doc"}),
    );
    upsert_vector(
        executor,
        "docs",
        "doc-2",
        vec![1.0, 0.0],
        json!({"kind": "doc"}),
    );
    upsert_vector(
        executor,
        "docs",
        "note-1",
        vec![0.0, 1.0],
        json!({"kind": "note"}),
    );

    let before_invalid = vector_count(executor, "docs");
    let invalid = executor
        .execute(Command::VectorDeleteByFilter {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            filter: VectorMetadataFilter::new(vec![VectorFilterCondition::eq(
                "nested.path",
                "doc",
            )]),
        })
        .expect_err("invalid filter fails");
    assert_eq!(invalid.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(vector_count(executor, "docs"), before_invalid);

    assert_eq!(delete_by_filter(executor, "docs", "doc"), 2);
    assert_eq!(delete_by_filter(executor, "docs", "doc"), 0);
    assert!(get_vector_value(executor, None, None, "docs", "doc-1", None).is_none());
    assert_eq!(
        list_vector_keys(executor, None, None, "docs", None, None, Some(10)).0,
        vec!["b", "note-1"]
    );
    assert_eq!(
        query_vector_keys(executor, "docs", vec![1.0, 0.0], 10, None),
        vec!["b".to_owned(), "note-1".to_owned()]
    );
}

fn assert_delete_all_edges(executor: &mut Executor) {
    let previous_count = vector_count(executor, "docs");
    assert!(previous_count > 0);
    assert_eq!(delete_all_vectors(executor, "docs"), previous_count);
    assert_eq!(
        collection_metric(executor, "docs"),
        VectorDistanceMetric::Cosine
    );
    assert_eq!(vector_count(executor, "docs"), 0);
    assert!(
        list_vector_keys(executor, None, None, "docs", None, None, Some(10))
            .0
            .is_empty()
    );
    assert!(query_vector_keys(executor, "docs", vec![1.0, 0.0], 10, None).is_empty());
    assert_eq!(delete_all_vectors(executor, "docs"), 0);
}

fn assert_vector_query_metric_ordering(executor: &mut Executor) {
    create_collection(executor, "cosine", VectorDistanceMetric::Cosine);
    create_collection(executor, "euclidean", VectorDistanceMetric::Euclidean);
    create_collection(executor, "dot", VectorDistanceMetric::DotProduct);
    seed_metric_vectors(executor);

    assert_eq!(
        query_vector_keys(executor, "cosine", vec![1.0, 0.0], 10, None),
        vec!["a".to_owned(), "b".to_owned(), "z".to_owned()]
    );
    assert_eq!(
        query_vector_keys(executor, "euclidean", vec![1.0, 0.0], 10, None),
        vec!["same".to_owned(), "near".to_owned(), "far".to_owned()]
    );
    assert_eq!(
        query_vector_keys(executor, "dot", vec![1.0, 0.0], 10, None),
        vec![
            "high".to_owned(),
            "same".to_owned(),
            "orthogonal".to_owned()
        ]
    );
    assert_eq!(
        query_vector_keys(
            executor,
            "cosine",
            vec![1.0, 0.0],
            10,
            Some(kind_filter("match"))
        ),
        vec!["a".to_owned(), "b".to_owned()]
    );
}

fn seed_metric_vectors(executor: &mut Executor) {
    upsert_vector(
        executor,
        "cosine",
        "b",
        vec![2.0, 0.0],
        json!({"kind": "match"}),
    );
    upsert_vector(
        executor,
        "cosine",
        "a",
        vec![1.0, 0.0],
        json!({"kind": "match"}),
    );
    upsert_vector(
        executor,
        "cosine",
        "z",
        vec![0.0, 1.0],
        json!({"kind": "other"}),
    );
    upsert_vector(
        executor,
        "euclidean",
        "far",
        vec![0.0, 1.0],
        json!({"kind": "metric"}),
    );
    upsert_vector(
        executor,
        "euclidean",
        "near",
        vec![0.5, 0.0],
        json!({"kind": "metric"}),
    );
    upsert_vector(
        executor,
        "euclidean",
        "same",
        vec![1.0, 0.0],
        json!({"kind": "metric"}),
    );
    upsert_vector(
        executor,
        "dot",
        "orthogonal",
        vec![0.0, 1.0],
        json!({"kind": "metric"}),
    );
    upsert_vector(
        executor,
        "dot",
        "same",
        vec![1.0, 0.0],
        json!({"kind": "metric"}),
    );
    upsert_vector(
        executor,
        "dot",
        "high",
        vec![2.0, 0.0],
        json!({"kind": "metric"}),
    );
}

fn assert_vector_as_of_query_and_history(executor: &mut Executor) {
    create_collection(executor, "docs", VectorDistanceMetric::Cosine);
    let first = upsert_vector(
        executor,
        "docs",
        "doc-a",
        vec![1.0, 0.0],
        json!({"stage": "first"}),
    );
    let second = upsert_vector(
        executor,
        "docs",
        "doc-b",
        vec![0.0, 1.0],
        json!({"stage": "second"}),
    );
    let third = upsert_vector(
        executor,
        "docs",
        "doc-a",
        vec![0.0, 1.0],
        json!({"stage": "third"}),
    );
    assert!(delete_vector(executor, "docs", "doc-a"));

    let first_value = get_vector_value(executor, None, None, "docs", "doc-a", Some(first))
        .expect("historical value exists");
    assert_eq!(first_value.data().embedding(), &[1.0, 0.0]);
    assert_eq!(
        first_value.data().metadata().expect("metadata")["stage"],
        "first"
    );
    assert_eq!(
        query_vector_keys_as_of(executor, "docs", vec![1.0, 0.0], 10, first),
        vec!["doc-a".to_owned()]
    );
    assert_eq!(
        query_vector_keys_as_of(executor, "docs", vec![1.0, 0.0], 10, second),
        vec!["doc-a".to_owned(), "doc-b".to_owned()]
    );
    assert_eq!(
        query_vector_keys_as_of(executor, "docs", vec![0.0, 1.0], 10, third),
        vec!["doc-a".to_owned(), "doc-b".to_owned()]
    );
    assert_eq!(
        query_vector_keys(executor, "docs", vec![0.0, 1.0], 10, None),
        vec!["doc-b"]
    );
    assert_vector_history_shape(executor, "docs", "doc-a");
}

fn assert_vector_history_shape(executor: &mut Executor, collection: &str, key: &str) {
    let Output::VectorVersionHistory(Some(history)) = executor
        .execute(Command::VectorHistory {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
        })
        .expect("history succeeds")
    else {
        panic!("unexpected history output");
    };
    let items = history.items();
    assert_eq!(history.count(), 3);
    assert_eq!(items.len(), 3);
    assert!(items[0].is_tombstone());
    assert_eq!(items[0].vector_revision(), None);
    assert_eq!(items[1].vector_revision(), Some(2));
    assert_eq!(items[2].vector_revision(), Some(1));
    assert!(items
        .windows(2)
        .all(|window| window[0].version() > window[1].version()));
}

fn assert_vector_branch_and_space_mutation_edges(executor: &mut Executor) {
    assert_vector_branch_mutation_edges(executor);
    assert_vector_space_mutation_edges(executor);
}

fn assert_vector_branch_mutation_edges(executor: &mut Executor) {
    create_collection_in(executor, None, None, "shared", VectorDistanceMetric::Cosine);
    executor
        .branch_fork_current(DEFAULT_BRANCH, "feature")
        .expect("branch creates");
    upsert_vector_in(
        executor,
        None,
        None,
        "shared",
        "doc",
        vec![1.0, 0.0],
        json!({"where": "default"}),
    );
    upsert_vector_in(
        executor,
        Some("feature"),
        None,
        "shared",
        "doc",
        vec![0.0, 1.0],
        json!({"where": "feature", "kind": "feature"}),
    );
    patch_vector_metadata_in(
        executor,
        Some("feature"),
        None,
        "shared",
        "doc",
        json!({"rank": 2}),
    );
    assert_eq!(
        delete_by_filter_in(executor, Some("feature"), None, "shared", "feature"),
        1
    );
    assert_eq!(
        get_vector_where(executor, None, None, "shared", "doc"),
        Some("default".to_owned())
    );
    assert_eq!(
        get_vector_where(executor, Some("feature"), None, "shared", "doc"),
        None
    );
    assert_eq!(
        query_vector_keys_in(executor, None, None, "shared", vec![1.0, 0.0], 10),
        vec!["doc"]
    );
    assert!(query_vector_keys_in(
        executor,
        Some("feature"),
        None,
        "shared",
        vec![0.0, 1.0],
        10
    )
    .is_empty());
}

fn assert_vector_space_mutation_edges(executor: &mut Executor) {
    create_collection_in(
        executor,
        None,
        Some("tenant-a"),
        "shared",
        VectorDistanceMetric::Cosine,
    );
    create_collection_in(
        executor,
        None,
        Some("tenant-b"),
        "shared",
        VectorDistanceMetric::Cosine,
    );
    upsert_vector_in(
        executor,
        None,
        Some("tenant-a"),
        "shared",
        "doc",
        vec![1.0, 0.0],
        json!({"where": "tenant-a"}),
    );
    upsert_vector_in(
        executor,
        None,
        Some("tenant-b"),
        "shared",
        "doc",
        vec![0.0, 1.0],
        json!({"where": "tenant-b"}),
    );
    assert_eq!(
        delete_all_vectors_in(executor, None, Some("tenant-a"), "shared"),
        1
    );
    assert_eq!(
        get_vector_where(executor, None, Some("tenant-a"), "shared", "doc"),
        None
    );
    assert_eq!(
        get_vector_where(executor, None, Some("tenant-b"), "shared", "doc"),
        Some("tenant-b".to_owned())
    );
    assert_eq!(vector_count_in(executor, None, None, "shared"), 1);
    assert_eq!(
        vector_count_in(executor, Some("feature"), None, "shared"),
        0
    );
    assert_eq!(
        vector_count_in(executor, None, Some("tenant-a"), "shared"),
        0
    );
    assert_eq!(
        vector_count_in(executor, None, Some("tenant-b"), "shared"),
        1
    );
}

fn run_vector_command_suite(executor: &mut Executor) {
    create_vector_fixture_collections(executor);
    assert_vector_collection_list_contains(executor, "docs");
    let first_timestamp = seed_vector_versions(executor);
    assert_vector_current_and_historical_reads(executor, first_timestamp);
    assert_vector_listing_metadata_and_query(executor);
    assert_vector_batch_delete_and_bulk_deletes(executor);
}

fn create_vector_fixture_collections(executor: &mut Executor) {
    create_collection(executor, "docs", VectorDistanceMetric::Cosine);
    create_collection(executor, "euclidean", VectorDistanceMetric::Euclidean);
    create_collection(executor, "dot", VectorDistanceMetric::DotProduct);
}

fn assert_vector_collection_list_contains(executor: &mut Executor, collection: &str) {
    let Output::VectorCollectionList {
        items: collections, ..
    } = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .expect("list collections succeeds")
    else {
        panic!("unexpected collection list output");
    };
    assert!(collections.iter().any(|info| info.name() == collection));
}

fn seed_vector_versions(executor: &mut Executor) -> u64 {
    let first_timestamp = upsert_vector(
        executor,
        "docs",
        "doc-a",
        vec![1.0, 0.0],
        json!({"kind": "doc", "rank": 1}),
    );
    let second = executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            vector: vec![0.0, 1.0],
            text: None,
            metadata: Some(json!({"kind": "doc", "rank": 2})),
        })
        .expect("second upsert succeeds");
    let Output::VectorWriteResult {
        effect,
        commit,
        vector_revision,
        ..
    } = second
    else {
        panic!("unexpected upsert output");
    };
    assert_eq!(effect, MutationEffect::updated());
    assert!(commit.version() > 0);
    assert_eq!(vector_revision, 2);

    let Output::VectorBatchUpsertResults(batch) = executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            entries: vec![
                BatchVectorEntry::new("doc-b", vec![1.0, 0.0], Some(json!({"kind": "doc"}))),
                BatchVectorEntry::new("note-c", vec![0.0, 1.0], Some(json!({"kind": "note"}))),
                BatchVectorEntry::new(
                    "doc-a",
                    vec![0.0, 1.0],
                    Some(json!({"kind": "doc", "rank": 3})),
                ),
            ],
        })
        .expect("batch upsert succeeds")
    else {
        panic!("unexpected batch upsert output");
    };
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].effect(), Some(&MutationEffect::created()));
    assert!(batch[0].commit().is_some());
    assert_eq!(batch[1].effect(), Some(&MutationEffect::created()));
    assert_eq!(batch[2].effect(), Some(&MutationEffect::updated()));
    assert_eq!(batch[2].vector_revision(), Some(3));

    assert_eq!(vector_count(executor, "docs"), 3);
    assert!(vector_exists(executor, "docs", "doc-a"));
    assert!(!vector_exists(executor, "docs", "missing"));
    first_timestamp
}

fn assert_vector_current_and_historical_reads(executor: &mut Executor, first_timestamp: u64) {
    let Output::VectorData(current) = executor
        .execute(Command::VectorGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("get succeeds")
    else {
        panic!("unexpected vector get output");
    };
    let current = current.into_option().expect("vector value present");
    assert_eq!(current.data().embedding(), &[0.0, 1.0]);
    assert_eq!(current.vector_revision(), 3);

    let Output::VectorData(historical) = executor
        .execute(Command::VectorGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            as_of: Some(first_timestamp),
            as_of_time: None,
        })
        .expect("historical get succeeds")
    else {
        panic!("unexpected historical get output");
    };
    let historical = historical.into_option().expect("vector value present");
    assert_eq!(historical.data().embedding(), &[1.0, 0.0]);
}

fn assert_vector_listing_metadata_and_query(executor: &mut Executor) {
    let Output::VectorKeyPage { items: keys, page } = executor
        .execute(Command::VectorListKeys {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            prefix: Some("doc".to_owned()),
            cursor: None,
            limit: Some(1),
            as_of: None,
            as_of_time: None,
        })
        .expect("key list succeeds")
    else {
        panic!("unexpected key page output");
    };
    assert_eq!(keys, vec!["doc-a"]);
    assert!(page.has_more());
    assert_eq!(page.cursor(), Some(&"doc-a".to_owned()));

    let Output::VectorMetadataUpdateResult {
        effect,
        commit,
        vector_revision,
        ..
    } = executor
        .execute(Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "doc-b".to_owned(),
            patch: json!({"rank": 9}),
        })
        .expect("metadata update succeeds")
    else {
        panic!("unexpected metadata update output");
    };
    assert!(effect.applied());
    assert_eq!(effect, MutationEffect::updated());
    assert!(commit.is_some());
    assert_eq!(vector_revision, Some(2));

    let Output::VectorMatches(matches) = executor
        .execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: vec![1.0, 0.0],
            text: None,
            k: 10,
            filter: Some(kind_filter("doc")),
            as_of: None,
            as_of_time: None,
        })
        .expect("query succeeds")
    else {
        panic!("unexpected query output");
    };
    assert_eq!(matches[0].key(), "doc-b");
}

fn assert_vector_batch_delete_and_bulk_deletes(executor: &mut Executor) {
    let Output::VectorBatchGetResults(values) = executor
        .execute(Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["doc-a".to_owned(), "missing".to_owned()],
        })
        .expect("batch get succeeds")
    else {
        panic!("unexpected batch get output");
    };
    assert!(values[0].value().is_some());
    assert!(values[1].value().is_none());

    let Output::VectorBatchDeleteResults(deleted) = executor
        .execute(Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            keys: vec!["doc-b".to_owned(), "missing".to_owned()],
        })
        .expect("batch delete succeeds")
    else {
        panic!("unexpected batch delete output");
    };
    assert_eq!(
        deleted
            .iter()
            .map(strata_executor::BatchItem::applied)
            .collect::<Vec<_>>(),
        vec![true, false]
    );
    assert_eq!(deleted[0].effect(), Some(&MutationEffect::deleted()));
    assert!(deleted[0].commit().is_some());
    assert_eq!(deleted[1].effect(), Some(&MutationEffect::not_found()));
    assert_eq!(deleted[1].commit(), None);

    let Output::VectorBulkDeleteResult { effect, commit, .. } = executor
        .execute(Command::VectorDeleteByFilter {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            filter: kind_filter("note"),
        })
        .expect("filtered delete succeeds")
    else {
        panic!("unexpected filtered delete output");
    };
    assert_eq!(effect.affected_count(), 1);
    assert!(effect.applied());
    assert!(commit.is_some());

    assert_vector_history_has_tombstone(executor, "docs", "doc-b");

    let Output::VectorDeleteResult { effect, commit, .. } = executor
        .execute(Command::VectorDelete {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "missing".to_owned(),
        })
        .expect("single delete succeeds")
    else {
        panic!("unexpected delete output");
    };
    assert!(!effect.applied());
    assert_eq!(effect, MutationEffect::not_found());
    assert_eq!(commit, None);

    let Output::VectorBulkDeleteResult { effect, commit, .. } = executor
        .execute(Command::VectorDeleteAll {
            branch: None,
            space: None,
            collection: "euclidean".to_owned(),
        })
        .expect("delete all succeeds")
    else {
        panic!("unexpected delete-all output");
    };
    assert_eq!(effect.affected_count(), 0);
    assert_eq!(effect, MutationEffect::not_found());
    assert_eq!(commit, None);
}

fn collection_names(executor: &mut Executor) -> Vec<String> {
    let Output::VectorCollectionList {
        items: collections, ..
    } = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .expect("list collections succeeds")
    else {
        panic!("unexpected collection list output");
    };
    collections
        .into_iter()
        .map(|collection| collection.name().to_owned())
        .collect()
}

fn collection_metric(executor: &mut Executor, collection: &str) -> VectorDistanceMetric {
    let Output::VectorCollectionList {
        items: collections, ..
    } = executor
        .execute(Command::VectorCollectionStats {
            branch: None,
            space: None,
            collection: collection.to_owned(),
        })
        .expect("collection stats succeeds")
    else {
        panic!("unexpected collection stats output");
    };
    collections
        .first()
        .expect("collection stats contains one item")
        .metric()
}

fn assert_vector_batch_outputs_empty(executor: &mut Executor, collection: &str) {
    let Output::VectorBatchUpsertResults(upserts) = executor
        .execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            entries: Vec::new(),
        })
        .expect("empty batch upsert succeeds")
    else {
        panic!("unexpected empty batch upsert output");
    };
    assert!(upserts.is_empty());

    let Output::VectorBatchGetResults(gets) = executor
        .execute(Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            keys: Vec::new(),
        })
        .expect("empty batch get succeeds")
    else {
        panic!("unexpected empty batch get output");
    };
    assert!(gets.is_empty());

    let Output::VectorBatchDeleteResults(deletes) = executor
        .execute(Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            keys: Vec::new(),
        })
        .expect("empty batch delete succeeds")
    else {
        panic!("unexpected empty batch delete output");
    };
    assert!(deletes.is_empty());
}

fn delete_collection(executor: &mut Executor, collection: &str) -> bool {
    let Output::Bool(deleted) = executor
        .execute(Command::VectorDeleteCollection {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            force: true,
        })
        .expect("delete collection succeeds")
    else {
        panic!("unexpected delete collection output");
    };
    deleted
}

fn create_collection(executor: &mut Executor, collection: &str, metric: VectorDistanceMetric) {
    create_collection_in(executor, None, None, collection, metric);
}

fn create_collection_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    metric: VectorDistanceMetric,
) {
    executor
        .execute(Command::VectorCreateCollection {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            dimension: 2,
            metric,
            embedding_model: None,
        })
        .expect("collection create succeeds");
}

fn upsert_without_metadata(
    executor: &mut Executor,
    collection: &str,
    key: &str,
    vector: Vec<f64>,
) -> u64 {
    let Output::VectorWriteResult { commit, .. } = executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
            vector,
            text: None,
            metadata: None,
        })
        .expect("upsert without metadata succeeds")
    else {
        panic!("unexpected upsert output");
    };
    commit.timestamp()
}

fn upsert_vector(
    executor: &mut Executor,
    collection: &str,
    key: &str,
    vector: Vec<f64>,
    metadata: serde_json::Value,
) -> u64 {
    let Output::VectorWriteResult { commit, .. } = executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
            vector,
            text: None,
            metadata: Some(metadata),
        })
        .expect("upsert succeeds")
    else {
        panic!("unexpected upsert output");
    };
    commit.timestamp()
}

fn upsert_vector_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    key: &str,
    vector: Vec<f64>,
    metadata: serde_json::Value,
) {
    executor
        .execute(Command::VectorUpsert {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            key: key.to_owned(),
            vector,
            text: None,
            metadata: Some(metadata),
        })
        .expect("upsert succeeds");
}

fn get_vector_value(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    key: &str,
    as_of: Option<u64>,
) -> Option<VectorVersionedData> {
    let Output::VectorData(value) = executor
        .execute(Command::VectorGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            key: key.to_owned(),
            as_of,
            as_of_time: None,
        })
        .expect("get succeeds")
    else {
        panic!("unexpected vector get output");
    };
    value.into_option()
}

fn patch_vector_metadata(
    executor: &mut Executor,
    collection: &str,
    key: &str,
    patch: serde_json::Value,
) -> u64 {
    patch_vector_metadata_in(executor, None, None, collection, key, patch).expect("patch applied")
}

fn patch_vector_metadata_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    key: &str,
    patch: serde_json::Value,
) -> Option<u64> {
    let Output::VectorMetadataUpdateResult {
        vector_revision, ..
    } = executor
        .execute(Command::VectorUpdateMetadata {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            key: key.to_owned(),
            patch,
        })
        .expect("metadata patch succeeds")
    else {
        panic!("unexpected metadata update output");
    };
    vector_revision
}

fn delete_vector(executor: &mut Executor, collection: &str, key: &str) -> bool {
    let Output::VectorDeleteResult { effect, .. } = executor
        .execute(Command::VectorDelete {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
        })
        .expect("delete succeeds")
    else {
        panic!("unexpected delete output");
    };
    effect.applied()
}

#[test]
fn vector_sample_returns_total_and_deterministic_vectors() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_collection(&mut executor, "docs", VectorDistanceMetric::Cosine);
    for index in 0..10 {
        upsert_vector(
            &mut executor,
            "docs",
            &format!("k{index:02}"),
            vec![1.0, 0.0],
            json!({}),
        );
    }

    let sample = |executor: &mut Executor| {
        let Output::VectorSampleResult {
            total_count, items, ..
        } = executor
            .execute(Command::VectorSample {
                branch: None,
                space: None,
                collection: "docs".to_owned(),
                count: Some(3),
            })
            .expect("vector sample succeeds")
        else {
            panic!("unexpected vector sample output");
        };
        (
            total_count,
            items
                .iter()
                .map(|entry| entry.key().to_owned())
                .collect::<Vec<_>>(),
        )
    };

    let (total_count, keys) = sample(&mut executor);
    assert_eq!(total_count, 10, "total is the full live vector count");
    assert_eq!(keys.len(), 3);
    // The stride sample is deterministic across identical reads.
    assert_eq!(sample(&mut executor).1, keys);
}

fn vector_count(executor: &mut Executor, collection: &str) -> u64 {
    vector_count_in(executor, None, None, collection)
}

fn vector_count_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
) -> u64 {
    let Output::Uint(count) = executor
        .execute(Command::VectorCount {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("count succeeds")
    else {
        panic!("unexpected count output");
    };
    count
}

fn vector_count_as_of(executor: &mut Executor, collection: &str, as_of: u64) -> u64 {
    let Output::Uint(count) = executor
        .execute(Command::VectorCount {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("count as_of succeeds")
    else {
        panic!("unexpected count output");
    };
    count
}

fn list_vector_keys_as_of(executor: &mut Executor, collection: &str, as_of: u64) -> Vec<String> {
    let Output::VectorKeyPage { items: keys, .. } = executor
        .execute(Command::VectorListKeys {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            prefix: None,
            cursor: None,
            limit: Some(100),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("list keys as_of succeeds")
    else {
        panic!("unexpected list keys output");
    };
    keys
}

#[test]
fn vector_count_and_list_keys_as_of_read_the_historical_snapshot() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_collection(&mut executor, "docs", VectorDistanceMetric::Cosine);
    let first = upsert_vector(&mut executor, "docs", "a", vec![1.0, 0.0], json!({}));
    upsert_vector(&mut executor, "docs", "b", vec![0.0, 1.0], json!({}));
    upsert_vector(&mut executor, "docs", "c", vec![1.0, 1.0], json!({}));

    assert_eq!(vector_count(&mut executor, "docs"), 3);
    assert_eq!(
        vector_count_as_of(&mut executor, "docs", first),
        1,
        "count as_of the first upsert sees only the first key"
    );
    assert_eq!(
        list_vector_keys_as_of(&mut executor, "docs", first),
        vec!["a".to_owned()],
        "list keys as_of the first upsert sees only the first key"
    );
}

fn vector_exists(executor: &mut Executor, collection: &str, key: &str) -> bool {
    let Output::Bool(exists) = executor
        .execute(Command::VectorExists {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
        })
        .expect("exists succeeds")
    else {
        panic!("unexpected exists output");
    };
    exists
}

fn list_vector_keys(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    prefix: Option<&str>,
    cursor: Option<&str>,
    limit: Option<u64>,
) -> (Vec<String>, bool, Option<String>) {
    let Output::VectorKeyPage { items: keys, page } = executor
        .execute(Command::VectorListKeys {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            prefix: prefix.map(str::to_owned),
            cursor: cursor.map(str::to_owned),
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("list keys succeeds")
    else {
        panic!("unexpected key page output");
    };
    let has_more = page.has_more();
    let cursor = page.cursor().cloned();
    (keys, has_more, cursor)
}

fn query_vector_keys(
    executor: &mut Executor,
    collection: &str,
    query: Vec<f64>,
    k: u64,
    filter: Option<VectorMetadataFilter>,
) -> Vec<String> {
    query_vector_keys_with_options(executor, None, None, collection, query, k, filter, None)
}

fn query_vector_keys_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    query: Vec<f64>,
    k: u64,
) -> Vec<String> {
    query_vector_keys_with_options(executor, branch, space, collection, query, k, None, None)
}

fn query_vector_keys_as_of(
    executor: &mut Executor,
    collection: &str,
    query: Vec<f64>,
    k: u64,
    as_of: u64,
) -> Vec<String> {
    query_vector_keys_with_options(
        executor,
        None,
        None,
        collection,
        query,
        k,
        None,
        Some(as_of),
    )
}

#[allow(clippy::too_many_arguments)]
fn query_vector_keys_with_options(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    query: Vec<f64>,
    k: u64,
    filter: Option<VectorMetadataFilter>,
    as_of: Option<u64>,
) -> Vec<String> {
    let Output::VectorMatches(matches) = executor
        .execute(Command::VectorQuery {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            query,
            text: None,
            k,
            filter,
            as_of,
            as_of_time: None,
        })
        .expect("query succeeds")
    else {
        panic!("unexpected query output");
    };
    matches
        .into_iter()
        .map(|value| value.key().to_owned())
        .collect()
}

fn delete_by_filter(executor: &mut Executor, collection: &str, kind: &str) -> u64 {
    delete_by_filter_in(executor, None, None, collection, kind)
}

fn delete_by_filter_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    kind: &str,
) -> u64 {
    let Output::VectorBulkDeleteResult { effect, .. } = executor
        .execute(Command::VectorDeleteByFilter {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            filter: kind_filter(kind),
        })
        .expect("filtered delete succeeds")
    else {
        panic!("unexpected filtered delete output");
    };
    effect.affected_count()
}

fn delete_all_vectors(executor: &mut Executor, collection: &str) -> u64 {
    delete_all_vectors_in(executor, None, None, collection)
}

fn delete_all_vectors_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
) -> u64 {
    let Output::VectorBulkDeleteResult { effect, .. } = executor
        .execute(Command::VectorDeleteAll {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
        })
        .expect("delete all succeeds")
    else {
        panic!("unexpected delete-all output");
    };
    effect.affected_count()
}

fn get_vector_where(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    collection: &str,
    key: &str,
) -> Option<String> {
    let Output::VectorData(value) = executor
        .execute(Command::VectorGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            collection: collection.to_owned(),
            key: key.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("get succeeds")
    else {
        panic!("unexpected get output");
    };
    value
        .into_option()
        .and_then(|value| value.data().metadata().cloned())
        .and_then(|metadata| metadata.get("where").cloned())
        .and_then(|where_value| where_value.as_str().map(str::to_owned))
}

fn assert_vector_history_has_tombstone(executor: &mut Executor, collection: &str, key: &str) {
    let Output::VectorVersionHistory(Some(history)) = executor
        .execute(Command::VectorHistory {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
        })
        .expect("history succeeds")
    else {
        panic!("unexpected history output");
    };
    assert!(history
        .items()
        .iter()
        .any(strata_executor::VectorHistoryItem::is_tombstone));
}

fn kind_filter(kind: &str) -> VectorMetadataFilter {
    VectorMetadataFilter::new(vec![VectorFilterCondition::new(
        "kind",
        VectorFilterOp::Eq,
        VectorScalar::from(kind),
    )])
}

/// Pins the `VectorScan` contract: ordered key + value rows and honest cursor
/// pagination — a limit below the row count reports `has_more` with the first
/// unreturned key, and resuming from that inclusive cursor returns the rest with
/// no gap or overlap before a terminal page. Mirrors the KV scan contract.
#[test]
fn vector_scan_returns_ordered_rows_and_paginates_honestly() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("create collection succeeds");
    for i in 0u8..6 {
        executor
            .execute(Command::VectorUpsert {
                branch: None,
                space: None,
                collection: "docs".to_owned(),
                key: format!("doc-{i}"),
                vector: vec![f64::from(i) + 1.0, 1.0],
                text: None,
                metadata: Some(json!({ "n": i })),
            })
            .expect("upsert succeeds");
    }

    // A page below the row count returns ordered key+value rows and a cursor at
    // the first unreturned key.
    let (rows, cursor) = vector_scan_page(&mut executor, None, 2);
    assert_eq!(
        rows,
        vec![
            ("doc-0".to_owned(), vec![1.0, 1.0]),
            ("doc-1".to_owned(), vec![2.0, 1.0]),
        ]
    );
    assert_eq!(cursor.as_deref(), Some("doc-2"));

    // Resume from the cursor until a terminal page. The union of pages is every
    // vector, in ascending order, with no overlap or gap.
    let mut seen = rows;
    let mut start = cursor;
    let mut pages = 1;
    while let Some(next) = start {
        let (rows, cursor) = vector_scan_page(&mut executor, Some(next), 2);
        assert!(rows.len() <= 2);
        seen.extend(rows);
        pages += 1;
        assert!(pages <= 6, "pagination did not terminate");
        start = cursor;
    }
    assert_eq!(pages, 3);
    let keys: Vec<String> = seen.iter().map(|(key, _)| key.clone()).collect();
    let mut deduped = keys.clone();
    deduped.dedup();
    assert_eq!(deduped.len(), keys.len(), "no duplicate keys across pages");
    let expected: Vec<(String, Vec<f32>)> = (0u8..6)
        .map(|i| (format!("doc-{i}"), vec![f32::from(i) + 1.0, 1.0]))
        .collect();
    assert_eq!(seen, expected);
}

fn vector_scan_page(
    executor: &mut Executor,
    start: Option<String>,
    limit: u64,
) -> (Vec<(String, Vec<f32>)>, Option<String>) {
    let output = executor
        .execute(Command::VectorScan {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            start,
            limit: Some(limit),
        })
        .expect("vector scan succeeds");
    let Output::VectorScanResult { items, page } = output else {
        panic!("unexpected vector scan output: {output:?}");
    };
    (
        items
            .iter()
            .map(|item| (item.key().to_owned(), item.data().embedding().to_vec()))
            .collect(),
        page.cursor().cloned(),
    )
}

/// The recorded embedding model reaches the wire (D9).
///
/// The mutation gate found `VectorCollectionInfo::embedding_model` untested:
/// returning `None`, an empty string, or a wrong name all passed. That accessor
/// is what puts the field on the wire, so `None` would make provenance
/// invisible to every consumer while the engine still stored it, and a wrong
/// name would misreport which model wrote the collection.
#[test]
fn a_collection_reports_the_embedding_model_it_was_created_with() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let created = executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: 4,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: Some("miniLM".to_owned()),
        })
        .expect("collection creates");
    let Output::VectorCollectionList { items, .. } = created else {
        panic!("expected a collection list");
    };
    assert_eq!(
        items[0].embedding_model(),
        Some("miniLM"),
        "the create acknowledgement carries the model"
    );

    // And a fresh read, not just the creation echo — this is the path a
    // caller actually uses to discover what wrote a collection.
    let listed = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .expect("collections list");
    let Output::VectorCollectionList { items, .. } = listed else {
        panic!("expected a collection list");
    };
    let docs = items
        .iter()
        .find(|item| item.name() == "docs")
        .expect("the collection is listed");
    assert_eq!(docs.embedding_model(), Some("miniLM"));

    // A collection created without one reports absence, not an empty string:
    // "no model recorded" and "recorded as nothing" are different states, and
    // only the first legitimately accepts vectors from any model.
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "raw".to_owned(),
            dimension: 4,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("collection creates");
    let listed = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
        .expect("collections list");
    let Output::VectorCollectionList { items, .. } = listed else {
        panic!("expected a collection list");
    };
    let raw = items
        .iter()
        .find(|item| item.name() == "raw")
        .expect("the collection is listed");
    assert_eq!(raw.embedding_model(), None);
}
