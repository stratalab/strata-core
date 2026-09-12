//! Executor JSON command behavior tests.

use serde_json::{json, Value};
use strata_engine::{CacheOpenOptions, Database};
use strata_executor::{
    BatchItemStatus, BatchJsonDeleteEntry, BatchJsonEntry, BatchJsonGetEntry, BatchStatus, Bytes,
    Command, Executor, ExecutorErrorClass, JsonIndexType, MutationEffectKind, Output,
    DEFAULT_BRANCH,
};
use tempfile::TempDir;

#[test]
#[allow(clippy::result_large_err)] // ExecutorResult mirrors the wire error type
fn wire_command_guard_rejects_integers_beyond_i64_u64_range() {
    use strata_executor::guard_json_integers;

    // The guard scans a raw wire command for integer literals serde_json would
    // silently coerce to a lossy f64, rejecting them before parse. Bindings call
    // it before `from_str::<Command>`; one scan covers every user-JSON field in
    // every command. Representable extremes (u64::MAX, 2^63, i64::MIN, and any
    // float) pass and round-trip exactly.
    let json_set = |value: &str| {
        guard_json_integers(&format!(
            r#"{{"type":"json_set","key":"k","path":"$","value":{value}}}"#
        ))
    };
    let error = json_set(r#"{"i":18446744073709551616}"#).expect_err("2^64 is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.json_number");
    assert!(json_set(r#"{"i":-9223372036854775809}"#).is_err()); // -(2^63) - 1
    assert!(json_set("18446744073709551617").is_err());
    // The same guard covers user JSON in any other command.
    assert!(
        guard_json_integers(r#"{"type":"event_append","payload":{"n":18446744073709551616}}"#)
            .is_err()
    );

    json_set(r#"{"max":18446744073709551615,"mid":9223372036854775808}"#)
        .expect("in-range integers pass the guard");
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let command: Command = serde_json::from_str(
        r#"{"type":"json_set","key":"k","path":"$","value":{"max":18446744073709551615,"mid":9223372036854775808,"min":-9223372036854775808,"f":1.5}}"#,
    )
    .expect("in-range command parses");
    executor
        .execute(command)
        .expect("in-range numbers are stored");
    let stored = match executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "k".to_owned(),
            path: "$".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("get succeeds")
    {
        Output::JsonVersionedValue(value) => value.value().map(|value| value.value().clone()),
        other => panic!("unexpected get output: {other:?}"),
    };
    assert_eq!(
        stored,
        Some(json!({
            "max": 18_446_744_073_709_551_615_u64,
            "mid": 9_223_372_036_854_775_808_u64,
            "min": -9_223_372_036_854_775_808_i64,
            "f": 1.5,
        }))
    );
}

#[test]
fn json_get_as_of_answers_with_the_same_versioned_envelope_as_a_live_read() {
    // #3334: the as-of branch used to answer with a bare `json_value`, so one
    // command had two wire shapes and an as-of read lost the very facts it
    // asked about. Both branches now return `json_versioned_value`, and the
    // as-of answer reports the commit that was visible then.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let set = |executor: &mut Executor, value: Value| {
        executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: "as-of-doc".to_owned(),
                path: "$".to_owned(),
                value,
            })
            .expect("set succeeds")
    };
    let first = match set(&mut executor, json!({"name": "Ada"})) {
        Output::JsonWriteResult { commit, .. } => commit,
        other => panic!("unexpected set output: {other:?}"),
    };
    set(&mut executor, json!({"name": "Grace"}));

    let get = |executor: &mut Executor, as_of: Option<u64>| match executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "as-of-doc".to_owned(),
            path: "$".to_owned(),
            as_of,
            as_of_time: None,
        })
        .expect("get succeeds")
    {
        Output::JsonVersionedValue(value) => value,
        other => panic!("unexpected get output: {other:?}"),
    };

    let live = get(&mut executor, None);
    let live = live.value().expect("the document is present");
    assert_eq!(live.value(), &json!({"name": "Grace"}));

    let historical = get(&mut executor, Some(first.timestamp()));
    let historical = historical.value().expect("the earlier document is present");
    assert_eq!(historical.value(), &json!({"name": "Ada"}));
    assert_eq!(historical.version(), first.version());
    assert_eq!(historical.timestamp(), first.timestamp());
    assert_ne!(historical.version(), live.version());

    // A miss keeps the same envelope, so absence reads the same either way.
    let absent = match executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "never-written".to_owned(),
            path: "$".to_owned(),
            as_of: Some(first.timestamp()),
            as_of_time: None,
        })
        .expect("get succeeds")
    {
        Output::JsonVersionedValue(value) => value,
        other => panic!("unexpected get output: {other:?}"),
    };
    assert!(!absent.is_found());
    assert!(absent.value().is_none());
}

#[test]
fn cache_executor_runs_complete_json_command_suite() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    run_json_command_suite(&mut executor);
}

#[test]
fn json_write_outputs_report_commit_receipts_and_effects() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let created = executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "effect-doc".to_owned(),
            path: "$".to_owned(),
            value: json!({"name": "Ada"}),
        })
        .expect("create succeeds");
    let Output::JsonWriteResult {
        key,
        effect,
        commit,
    } = created
    else {
        panic!("unexpected create output: {created:?}");
    };
    assert_eq!(key, "effect-doc");
    assert_eq!(effect.kind(), MutationEffectKind::Created);
    assert!(effect.applied());
    assert!(!effect.matched());
    assert_eq!(commit.put_count(), 1);
    assert_eq!(commit.delete_count(), 0);

    let updated = executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "effect-doc".to_owned(),
            path: "$.name".to_owned(),
            value: json!("Grace"),
        })
        .expect("update succeeds");
    let Output::JsonWriteResult {
        key,
        effect,
        commit,
    } = updated
    else {
        panic!("unexpected update output: {updated:?}");
    };
    assert_eq!(key, "effect-doc");
    assert_eq!(effect.kind(), MutationEffectKind::Updated);
    assert!(effect.applied());
    assert!(effect.matched());
    assert_eq!(commit.put_count(), 1);
    assert_eq!(commit.delete_count(), 0);

    let deleted = executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "effect-doc".to_owned(),
            path: "$".to_owned(),
        })
        .expect("delete succeeds");
    let Output::JsonDeleteResult {
        key,
        effect,
        commit,
    } = deleted
    else {
        panic!("unexpected delete output: {deleted:?}");
    };
    assert_eq!(key, "effect-doc");
    assert_eq!(effect.kind(), MutationEffectKind::Deleted);
    assert!(effect.applied());
    assert!(effect.matched());
    assert!(commit.is_some());

    let missing = executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "effect-doc".to_owned(),
            path: "$".to_owned(),
        })
        .expect("missing delete succeeds");
    let Output::JsonDeleteResult {
        key,
        effect,
        commit,
    } = missing
    else {
        panic!("unexpected missing delete output: {missing:?}");
    };
    assert_eq!(key, "effect-doc");
    assert_eq!(effect.kind(), MutationEffectKind::NotFound);
    assert!(!effect.applied());
    assert!(!effect.matched());
    assert_eq!(effect.affected_count(), 0);
    assert!(commit.is_none());
}

#[test]
fn json_batch_write_outputs_report_per_item_commit_receipts_and_effects() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    write_json(
        &mut executor,
        None,
        None,
        "batch-effect-existing",
        "$",
        json!({"name": "Ada"}),
    );

    let output = executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("batch-effect-created", "$", json!({"name": "Grace"})),
                BatchJsonEntry::new("batch-effect-created", "$.lang", json!("rust")),
                BatchJsonEntry::new("batch-effect-existing", "$.name", json!("Katherine")),
            ],
        })
        .expect("JSON batch set succeeds");
    let Output::JsonBatchResults(results) = output else {
        panic!("unexpected JSON batch set output: {output:?}");
    };
    assert_eq!(results.len(), 3);
    let created = results[0].effect().expect("created effect");
    assert_eq!(created.kind(), MutationEffectKind::Created);
    assert!(created.applied());
    assert!(!created.matched());
    assert_eq!(results[0].document_version(), Some(1));
    let repeated_update = results[1].effect().expect("repeated update effect");
    assert_eq!(repeated_update.kind(), MutationEffectKind::Updated);
    assert!(repeated_update.applied());
    assert!(repeated_update.matched());
    assert_eq!(results[1].document_version(), Some(2));
    let existing_update = results[2].effect().expect("existing update effect");
    assert_eq!(existing_update.kind(), MutationEffectKind::Updated);
    assert!(existing_update.applied());
    assert!(existing_update.matched());
    assert_eq!(results[2].document_version(), Some(2));
    let batch_commit = results[0].commit().expect("created commit");
    assert_eq!(batch_commit.put_count(), 2);
    assert_eq!(batch_commit.delete_count(), 0);
    assert_eq!(
        results[1]
            .commit()
            .expect("repeated update commit")
            .version(),
        batch_commit.version()
    );
    assert_eq!(
        results[2]
            .commit()
            .expect("existing update commit")
            .version(),
        batch_commit.version()
    );

    let output = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry::new("batch-effect-created", "$"),
                BatchJsonDeleteEntry::new("batch-effect-missing", "$"),
            ],
        })
        .expect("JSON batch delete succeeds");
    let Output::JsonBatchResults(results) = output else {
        panic!("unexpected JSON batch delete output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    let deleted = results[0].effect().expect("deleted effect");
    assert_eq!(deleted.kind(), MutationEffectKind::Deleted);
    assert!(deleted.applied());
    assert!(deleted.matched());
    let missing = results[1].effect().expect("missing effect");
    assert_eq!(missing.kind(), MutationEffectKind::NotFound);
    assert!(!missing.applied());
    assert!(!missing.matched());
    assert!(results[0].commit().is_some());
    assert!(results[1].commit().is_none());
}

#[test]
fn durable_executor_reopens_json_documents_history_and_indexes() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");

    {
        let mut executor = Executor::open_durable_local(&path).expect("durable executor opens");
        run_json_command_suite(&mut executor);
        executor.close().expect("durable executor closes");
    }

    let mut reopened = Executor::open_durable_local(&path).expect("durable executor reopens");
    assert_eq!(
        execute_json_get_value(&mut reopened, "doc-alpha", "$.name"),
        Some(json!("Ada Lovelace"))
    );
    assert_eq!(execute_json_count(&mut reopened, Some("doc-")), 4);
    assert_json_history_has_tombstone(&mut reopened, "doc-delete");

    let Output::JsonIndexList { items: indexes, .. } = reopened
        .execute(Command::JsonListIndexes {
            branch: None,
            space: None,
        })
        .expect("list indexes succeeds")
    else {
        panic!("unexpected index list output");
    };
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name(), "by-name");
}

#[test]
fn json_null_documents_are_listed_as_present_documents() {
    run_executor_modes(|executor| {
        let null_write = write_json(executor, None, None, "doc-null", "$", Value::Null);
        write_json(
            executor,
            None,
            None,
            "doc-object",
            "$",
            json!({"name": "Ada"}),
        );

        assert_eq!(
            execute_json_get_value(executor, "doc-null", "$"),
            Some(Value::Null)
        );
        assert_eq!(execute_json_get_value(executor, "doc-missing", "$"), None);
        assert_eq!(
            execute_json_list(executor, Some("doc-"), None, 10),
            (vec!["doc-null".to_owned(), "doc-object".to_owned()], false)
        );
        assert_eq!(execute_json_count(executor, Some("doc-")), 2);
        assert_eq!(
            execute_json_list_as_of(executor, Some("doc-"), null_write.timestamp),
            vec!["doc-null".to_owned()]
        );
    });
}

#[test]
fn json_branch_and_space_defaults_are_isolated() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .branch_fork_current(DEFAULT_BRANCH, "feature")
        .expect("branch creates");

    write_json(
        &mut executor,
        None,
        None,
        "shared",
        "$",
        json!({"where": "default"}),
    );
    write_json(
        &mut executor,
        Some("feature"),
        None,
        "shared",
        "$",
        json!({"where": "feature"}),
    );
    write_json(
        &mut executor,
        None,
        Some("tenant-a"),
        "shared",
        "$",
        json!({"where": "space"}),
    );

    assert_eq!(
        execute_json_get_value(&mut executor, "shared", "$.where"),
        Some(json!("default"))
    );
    assert_eq!(
        execute_json_get_value_in(&mut executor, Some("feature"), None, "shared", "$.where"),
        Some(json!("feature"))
    );
    assert_eq!(
        execute_json_get_value_in(&mut executor, None, Some("tenant-a"), "shared", "$.where"),
        Some(json!("space"))
    );
    assert_eq!(
        execute_json_count_in(&mut executor, None, None, Some("shared")),
        1
    );
    assert_eq!(
        execute_json_count_in(&mut executor, Some("feature"), None, Some("shared")),
        1
    );
    assert_eq!(
        execute_json_count_in(&mut executor, None, Some("tenant-a"), Some("shared")),
        1
    );
    assert_eq!(
        execute_json_sample_in(&mut executor, Some("feature"), None, Some("shared"), 1).0,
        1
    );

    create_json_index_in(
        &mut executor,
        None,
        Some("tenant-a"),
        "by-where",
        "$.where",
        JsonIndexType::Tag,
    );
    assert!(list_json_indexes_in(&mut executor, None, None).is_empty());
    assert_eq!(
        list_json_indexes_in(&mut executor, None, Some("tenant-a")).len(),
        1
    );
}

#[test]
fn json_executor_inherits_configured_database_default_branch() {
    let options = CacheOpenOptions::new()
        .with_default_branch("main")
        .expect("valid branch");
    let database = Database::open_cache(options)
        .expect("cache database opens")
        .into_database();
    let mut executor = Executor::from_database(database);

    assert_eq!(executor.default_branch(), "main");
    write_json(
        &mut executor,
        None,
        None,
        "shared",
        "$",
        json!({"where": "main"}),
    );
    assert_eq!(
        execute_json_get_value(&mut executor, "shared", "$.where"),
        Some(json!("main"))
    );

    let error = executor
        .execute(Command::JsonGet {
            branch: Some(DEFAULT_BRANCH.to_owned()),
            space: None,
            key: "shared".to_owned(),
            path: "$".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("literal default branch is absent");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
}

#[test]
fn json_edge_contract_runs_in_cache_and_durable_modes() {
    run_executor_modes(run_json_edge_contract);
}

#[test]
fn json_error_contract_runs_in_cache_and_durable_modes() {
    run_executor_modes(run_json_error_contract);
}

#[test]
fn json_large_batch_smoke_uses_batch_commands() {
    const ROWS: usize = 512;
    const BATCH: usize = 64;

    let mut executor = Executor::open_cache().expect("cache executor opens");
    for start in (0..ROWS).step_by(BATCH) {
        let end = start.saturating_add(BATCH).min(ROWS);
        let entries = (start..end)
            .map(|index| {
                BatchJsonEntry::new(
                    format!("bulk-{index:04}"),
                    "$",
                    json!({"index": index, "group": index % 8}),
                )
            })
            .collect::<Vec<_>>();
        let Output::JsonBatchResults(results) = executor
            .execute(Command::JsonBatchSet {
                branch: None,
                space: None,
                entries,
            })
            .expect("batch set succeeds")
        else {
            panic!("unexpected batch output");
        };
        assert_eq!(results.len(), end - start);
        assert!(results.iter().all(|result| result
            .commit()
            .map(strata_executor::CommitReceipt::version)
            .is_some()));
    }

    assert_eq!(
        execute_json_count(&mut executor, Some("bulk-")),
        ROWS as u64
    );
}

#[test]
fn json_command_to_output_mapping_is_explicit_for_every_variant() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    seed_json_mapping_documents(&mut executor);

    let outputs = json_mapping_commands()
        .into_iter()
        .map(|command| executor.execute(command).expect("command succeeds"))
        .collect::<Vec<_>>();

    assert_json_mapping_outputs(&outputs);
}

fn seed_json_mapping_documents(executor: &mut Executor) {
    write_json(
        executor,
        None,
        None,
        "map-a",
        "$",
        json!({"name": "Ada", "active": true}),
    );
    write_json(
        executor,
        None,
        None,
        "map-b",
        "$",
        json!({"name": "Grace", "active": true}),
    );
}

fn json_mapping_commands() -> Vec<Command> {
    vec![
        Command::JsonSet {
            branch: None,
            space: None,
            key: "map-put".to_owned(),
            path: "$".to_owned(),
            value: json!({"name": "Lin"}),
        },
        Command::JsonGet {
            branch: None,
            space: None,
            key: "map-a".to_owned(),
            path: "$.name".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonDelete {
            branch: None,
            space: None,
            key: "map-delete-missing".to_owned(),
            path: "$".to_owned(),
        },
        Command::JsonHistory {
            branch: None,
            space: None,
            key: "map-a".to_owned(),
        },
        Command::JsonExists {
            branch: None,
            space: None,
            key: "map-a".to_owned(),
        },
        Command::JsonBatchExists {
            branch: None,
            space: None,
            keys: vec!["map-a".to_owned(), "missing".to_owned()],
        },
        Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![BatchJsonEntry::new(
                "map-c",
                "$",
                json!({"name": "Katherine"}),
            )],
        },
        Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry::new("map-a", "$.name"),
                BatchJsonGetEntry::new("missing", "$"),
            ],
        },
        Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![BatchJsonDeleteEntry::new("map-c", "$")],
        },
        Command::JsonList {
            branch: None,
            space: None,
            prefix: Some("map-".to_owned()),
            cursor: None,
            limit: Some(2),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonCount {
            branch: None,
            space: None,
            prefix: Some("map-".to_owned()),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonSample {
            branch: None,
            space: None,
            prefix: Some("map-".to_owned()),
            count: Some(1),
        },
        Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
            field_path: "$.name".to_owned(),
            index_type: JsonIndexType::Text,
        },
        Command::JsonDropIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
        },
        Command::JsonListIndexes {
            branch: None,
            space: None,
        },
    ]
}

fn assert_json_mapping_outputs(outputs: &[Output]) {
    assert!(matches!(outputs[0], Output::JsonWriteResult { .. }));
    assert!(matches!(outputs[1], Output::JsonVersionedValue(_)));
    assert!(matches!(outputs[2], Output::JsonDeleteResult { .. }));
    assert!(matches!(outputs[3], Output::JsonVersionHistory(_)));
    assert!(matches!(outputs[4], Output::Bool(_)));
    assert!(matches!(outputs[5], Output::JsonBatchExistsResults(_)));
    assert!(matches!(outputs[6], Output::JsonBatchResults(_)));
    assert!(matches!(outputs[7], Output::JsonBatchGetResults(_)));
    assert!(matches!(outputs[8], Output::JsonBatchResults(_)));
    assert!(matches!(outputs[9], Output::JsonListResult { .. }));
    assert!(matches!(outputs[10], Output::Uint(_)));
    assert!(matches!(outputs[11], Output::JsonSampleResult { .. }));
    assert!(matches!(outputs[12], Output::JsonIndexDefinition(_)));
    assert!(matches!(outputs[13], Output::Bool(_)));
    assert!(matches!(outputs[14], Output::JsonIndexList { .. }));
}

#[test]
fn json_batch_get_reports_missing_documents_as_misses() {
    // A missing document in a batch read is a positional miss, not a
    // success — matching kv batch-get. The outer batch is then partial.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "present".to_owned(),
            path: "$".to_owned(),
            value: json!({"name": "Ada"}),
        })
        .expect("set present document");

    let output = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry::new("present", "$"),
                BatchJsonGetEntry::new("absent", "$"),
            ],
        })
        .expect("batch get succeeds");
    let Output::JsonBatchGetResults(results) = output else {
        panic!("unexpected batch get output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status(), BatchItemStatus::Ok);
    assert!(results[0].error().is_none());
    assert_eq!(
        results[1].status(),
        BatchItemStatus::Miss,
        "a missing document is a miss, not an ok"
    );
    assert!(results[1].error().is_none());
    assert_eq!(
        results.status(),
        BatchStatus::Partial,
        "a batch with a miss is partial, not ok"
    );
}

#[test]
fn json_batch_exists_reports_present_and_absent_documents() {
    // exists=false for an absent document is a definitive answer, so it is an
    // ok item and the outer batch is ok (never a miss) — matching kv
    // batch-exists.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::JsonSet {
            branch: None,
            space: None,
            key: "present".to_owned(),
            path: "$".to_owned(),
            value: json!({"name": "Ada"}),
        })
        .expect("set present document");

    let output = executor
        .execute(Command::JsonBatchExists {
            branch: None,
            space: None,
            keys: vec!["present".to_owned(), "absent".to_owned()],
        })
        .expect("batch exists succeeds");
    let Output::JsonBatchExistsResults(results) = output else {
        panic!("unexpected batch exists output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status(), BatchItemStatus::Ok);
    assert!(results[0].exists(), "present document exists");
    assert_eq!(results[1].status(), BatchItemStatus::Ok);
    assert!(!results[1].exists(), "absent document does not exist");
    assert_eq!(
        results.status(),
        BatchStatus::Ok,
        "a definitive exists=false answer is ok, not a miss"
    );
}

#[test]
fn json_invalid_batch_items_are_positional_errors() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let output = executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("", "$", json!({"bad": true})),
                BatchJsonEntry::new("valid", "$.name", json!("Ada")),
            ],
        })
        .expect("batch set returns positional errors");
    let Output::JsonBatchResults(results) = output else {
        panic!("unexpected batch set output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].error().is_some());
    assert_eq!(
        results[0].error_status().expect("item error status").code(),
        "invalid_argument.engine.json_document_id"
    );
    assert!(results[1]
        .commit()
        .map(strata_executor::CommitReceipt::version)
        .is_some());
    assert_eq!(
        execute_json_get_value(&mut executor, "valid", "$.name"),
        Some(json!("Ada"))
    );

    let output = executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonGetEntry::new("", "$"),
                BatchJsonGetEntry::new("valid", "$.name"),
            ],
        })
        .expect("batch get returns positional errors");
    let Output::JsonBatchGetResults(results) = output else {
        panic!("unexpected batch get output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].error().is_some());
    assert_eq!(
        results[0].error_status().expect("item error status").code(),
        "invalid_argument.engine.json_document_id"
    );
    assert_eq!(results[1].value(), Some(&json!("Ada")));

    let output = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry::new("", "$"),
                BatchJsonDeleteEntry::new("valid", "$.name"),
            ],
        })
        .expect("batch delete returns positional errors");
    let Output::JsonBatchResults(results) = output else {
        panic!("unexpected batch delete output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].error().is_some());
    assert_eq!(
        results[0].error_status().expect("item error status").code(),
        "invalid_argument.engine.json_document_id"
    );
    assert!(results[1]
        .commit()
        .map(strata_executor::CommitReceipt::version)
        .is_some());
    assert_eq!(
        execute_json_get_value(&mut executor, "valid", "$.name"),
        None
    );
}

#[test]
fn json_batch_commands_validate_branch_before_item_results() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    for command in [
        Command::JsonBatchSet {
            branch: Some("missing".to_owned()),
            space: None,
            entries: Vec::new(),
        },
        Command::JsonBatchSet {
            branch: Some("missing".to_owned()),
            space: None,
            entries: vec![BatchJsonEntry::new("", "$", json!("bad"))],
        },
        Command::JsonBatchGet {
            branch: Some("missing".to_owned()),
            space: None,
            entries: Vec::new(),
        },
        Command::JsonBatchDelete {
            branch: Some("missing".to_owned()),
            space: None,
            entries: Vec::new(),
        },
    ] {
        let error = executor.execute(command).expect_err("missing branch fails");
        assert_eq!(error.class(), ExecutorErrorClass::NotFound);
    }
}

fn run_json_edge_contract(executor: &mut Executor) {
    assert_single_set_get_path_delete_and_exists(executor);
    assert_json_batch_edges(executor);
    assert_json_list_count_sample_edges(executor);
    assert_json_history_edges(executor);
}

fn assert_single_set_get_path_delete_and_exists(executor: &mut Executor) {
    let created = write_json(
        executor,
        None,
        None,
        "edge-doc",
        "$",
        json!({"profile": {"name": "Ada"}, "tags": ["math", "logic"]}),
    );
    let versioned = execute_json_get_versioned(executor, "edge-doc", "$").expect("document exists");
    assert_eq!(
        versioned.value,
        json!({"profile": {"name": "Ada"}, "tags": ["math", "logic"]})
    );
    assert_eq!(versioned.version, created.version);
    assert_eq!(versioned.timestamp, created.timestamp);
    assert_eq!(versioned.document_version, 1);

    write_json(
        executor,
        None,
        None,
        "edge-doc",
        "$.profile.city",
        json!("London"),
    );
    write_json(
        executor,
        None,
        None,
        "edge-doc",
        "$.tags[0]",
        json!("analysis"),
    );
    assert_eq!(
        execute_json_get_value(executor, "edge-doc", "$.profile.city"),
        Some(json!("London"))
    );
    assert_eq!(
        execute_json_get_value(executor, "edge-doc", "$.tags[0]"),
        Some(json!("analysis"))
    );
    assert_eq!(
        execute_json_get_value(executor, "edge-doc", "$"),
        Some(json!({
            "profile": {"name": "Ada", "city": "London"},
            "tags": ["analysis", "logic"]
        }))
    );

    assert!(execute_json_delete(executor, "edge-doc", "$.profile.city"));
    assert_eq!(
        execute_json_get_value(executor, "edge-doc", "$.profile.city"),
        None
    );
    assert!(execute_json_exists(executor, "edge-doc"));
    assert_eq!(
        execute_json_get_value(executor, "edge-doc", "$"),
        Some(json!({
            "profile": {"name": "Ada"},
            "tags": ["analysis", "logic"]
        }))
    );

    assert!(execute_json_delete(executor, "edge-doc", "$"));
    assert!(!execute_json_exists(executor, "edge-doc"));
    assert_eq!(execute_json_get_value(executor, "edge-doc", "$"), None);
    assert!(!execute_json_delete(executor, "edge-doc", "$"));
}

fn assert_json_batch_edges(executor: &mut Executor) {
    assert_empty_json_batches(executor);

    let Output::JsonBatchResults(results) = executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("batch-dupe", "$", json!({"count": 1})),
                BatchJsonEntry::new("batch-dupe", "$.count", json!(2)),
                BatchJsonEntry::new("batch-other", "$", json!({"count": 3})),
            ],
        })
        .expect("duplicate-document batch set succeeds")
    else {
        panic!("unexpected batch set output");
    };
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|result| result
        .commit()
        .map(strata_executor::CommitReceipt::version)
        .is_some()));
    assert_eq!(results[0].document_version(), Some(1));
    assert_eq!(results[1].document_version(), Some(2));
    assert_eq!(
        execute_json_get_value(executor, "batch-dupe", "$.count"),
        Some(json!(2))
    );

    let values = execute_json_batch_get(
        executor,
        vec![
            BatchJsonGetEntry::new("batch-dupe", "$.count"),
            BatchJsonGetEntry::new("batch-dupe", "$.count"),
            BatchJsonGetEntry::new("batch-dupe", "$.missing"),
            BatchJsonGetEntry::new("missing", "$"),
        ],
    );
    assert_eq!(values, vec![Some(json!(2)), Some(json!(2)), None, None]);

    write_json(
        executor,
        None,
        None,
        "batch-delete",
        "$",
        json!({"a": 1, "b": 2}),
    );
    let deleted = execute_json_batch_delete(
        executor,
        vec![
            BatchJsonDeleteEntry::new("batch-delete", "$.a"),
            BatchJsonDeleteEntry::new("batch-delete", "$.missing"),
            BatchJsonDeleteEntry::new("missing", "$"),
            BatchJsonDeleteEntry::new("batch-delete", "$"),
        ],
    );
    assert_eq!(deleted, vec![true, false, false, true]);
    assert_eq!(execute_json_get_value(executor, "batch-delete", "$"), None);
}

#[test]
fn json_batch_rejects_duplicate_targets() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    // Two items writing the same (document, path) reject the whole batch,
    // matching KV's rule instead of silently applying last-wins.
    let error = executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("doc", "$.count", json!(1)),
                BatchJsonEntry::new("doc", "$.count", json!(2)),
            ],
        })
        .expect_err("duplicate-target batch set is rejected");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.executor.json_batch_duplicate_key"
    );
    // The whole batch was rejected before any write.
    assert_eq!(execute_json_get_value(&mut executor, "doc", "$"), None);

    // Two distinct paths of one document are separate targets, not duplicates.
    executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("doc", "$", json!({"count": 1})),
                BatchJsonEntry::new("doc", "$.label", json!("x")),
            ],
        })
        .expect("distinct-path batch set succeeds");
    assert_eq!(
        execute_json_get_value(&mut executor, "doc", "$.label"),
        Some(json!("x"))
    );

    // Delete batches reject duplicate targets the same way.
    let error = executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonDeleteEntry::new("doc", "$.count"),
                BatchJsonDeleteEntry::new("doc", "$.count"),
            ],
        })
        .expect_err("duplicate-target batch delete is rejected");
    assert_eq!(
        error.code(),
        "invalid_argument.executor.json_batch_duplicate_key"
    );
}

fn assert_empty_json_batches(executor: &mut Executor) {
    assert!(matches!(
        executor
            .execute(Command::JsonBatchSet {
                branch: None,
                space: None,
                entries: Vec::new(),
            })
            .expect("empty batch set succeeds"),
        Output::JsonBatchResults(results) if results.is_empty() && !results.applied()
    ));
    assert!(matches!(
        executor
            .execute(Command::JsonBatchGet {
                branch: None,
                space: None,
                entries: Vec::new(),
            })
            .expect("empty batch get succeeds"),
        Output::JsonBatchGetResults(results) if results.is_empty() && !results.applied()
    ));
    assert!(matches!(
        executor
            .execute(Command::JsonBatchDelete {
                branch: None,
                space: None,
                entries: Vec::new(),
            })
            .expect("empty batch delete succeeds"),
        Output::JsonBatchResults(results) if results.is_empty() && !results.applied()
    ));
}

fn assert_json_list_count_sample_edges(executor: &mut Executor) {
    write_json(
        executor,
        None,
        None,
        "zlist-a",
        "$",
        json!({"kind": "keep"}),
    );
    write_json(
        executor,
        None,
        None,
        "zlist-b",
        "$",
        json!({"kind": "drop"}),
    );
    write_json(
        executor,
        None,
        None,
        "zlist-c",
        "$",
        json!({"kind": "keep"}),
    );
    write_json(
        executor,
        None,
        None,
        "other-a",
        "$",
        json!({"kind": "other"}),
    );
    create_json_index(executor, "by-kind", "$.kind", JsonIndexType::Tag);
    assert!(execute_json_delete(executor, "zlist-b", "$"));

    let (first_page, has_more) = execute_json_list(executor, Some("zlist-"), None, 1);
    assert_eq!(first_page, vec!["zlist-a".to_owned()]);
    assert!(has_more);
    let (all_keys, _) = execute_json_list(executor, None, None, 100);
    assert!(all_keys.contains(&"zlist-a".to_owned()));
    assert!(all_keys.contains(&"zlist-c".to_owned()));
    assert!(!all_keys.contains(&"zlist-b".to_owned()));
    assert!(!all_keys.iter().any(|key| key.contains("by-kind")));

    assert_eq!(execute_json_count(executor, Some("zlist-")), 2);
    assert!(execute_json_count(executor, None) >= 3);

    let sample = execute_json_sample_items(executor, Some("zlist-"), 1);
    assert_eq!(sample.0, 2);
    assert_eq!(sample.1.len(), 1);
    assert!(sample.1[0].0.starts_with("zlist-"));
    assert!(sample.1[0].1.is_object());
}

fn assert_json_history_edges(executor: &mut Executor) {
    let first = write_json(
        executor,
        None,
        None,
        "history-edge",
        "$",
        json!({"name": "first", "drop": true}),
    );
    let second = write_json(
        executor,
        None,
        None,
        "history-edge",
        "$.name",
        json!("second"),
    );
    assert!(execute_json_delete(executor, "history-edge", "$.drop"));
    assert!(execute_json_delete(executor, "history-edge", "$"));

    assert_eq!(
        execute_json_get_as_of(executor, "history-edge", "$.name", first.timestamp),
        Some(json!("first"))
    );
    assert_eq!(
        execute_json_get_as_of(executor, "history-edge", "$.name", second.timestamp),
        Some(json!("second"))
    );
    assert_eq!(
        execute_json_list_as_of(executor, Some("history-"), first.timestamp),
        vec!["history-edge".to_owned()]
    );

    let history = execute_json_history(executor, "history-edge");
    assert_eq!(history.len(), 4);
    assert!(history[0].is_tombstone());
    assert_eq!(history[0].document_version(), None);
    assert_eq!(history[1].document_version(), Some(3));
    assert_eq!(history[2].document_version(), Some(2));
    assert_eq!(history[3].document_version(), Some(1));
    assert!(history
        .windows(2)
        .all(|window| window[0].version() > window[1].version()));
}

fn run_json_error_contract(executor: &mut Executor) {
    assert_json_command_error_classes(executor);
    assert_json_batch_item_error_boundaries(executor);
    assert_closed_handle_rejects_json_commands(executor);
}

fn assert_json_command_error_classes(executor: &mut Executor) {
    for command in invalid_input_json_commands() {
        let error = executor.execute(command).expect_err("command fails");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert!(
            error.code().contains(".engine.") || error.code().contains(".executor."),
            "unexpected public error code: {}",
            error.code()
        );
    }

    for command in missing_branch_json_commands() {
        let error = executor.execute(command).expect_err("missing branch fails");
        assert_eq!(error.class(), ExecutorErrorClass::NotFound);
    }
}

fn invalid_input_json_commands() -> Vec<Command> {
    vec![
        Command::JsonGet {
            branch: None,
            space: None,
            key: String::new(),
            path: "$".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonGet {
            branch: None,
            space: None,
            key: "bad-path".to_owned(),
            path: "$[".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonSet {
            branch: None,
            space: None,
            key: "too-deep".to_owned(),
            path: "$".to_owned(),
            value: deeply_nested_json(128),
        },
        Command::JsonSet {
            branch: None,
            space: None,
            key: "too-large".to_owned(),
            path: "$".to_owned(),
            value: Value::String("x".repeat(16 * 1024 * 1024 + 1)),
        },
        Command::JsonSet {
            branch: None,
            space: Some(String::new()),
            key: "bad-space".to_owned(),
            path: "$".to_owned(),
            value: json!(true),
        },
        Command::JsonSet {
            branch: None,
            space: Some("_system_".to_owned()),
            key: "bad-space".to_owned(),
            path: "$".to_owned(),
            value: json!(true),
        },
    ]
}

fn missing_branch_json_commands() -> Vec<Command> {
    vec![
        Command::JsonSet {
            branch: Some("missing".to_owned()),
            space: None,
            key: "doc".to_owned(),
            path: "$".to_owned(),
            value: json!({}),
        },
        Command::JsonDelete {
            branch: Some("missing".to_owned()),
            space: None,
            key: "doc".to_owned(),
            path: "$".to_owned(),
        },
        Command::JsonGet {
            branch: Some("missing".to_owned()),
            space: None,
            key: "doc".to_owned(),
            path: "$".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonList {
            branch: Some("missing".to_owned()),
            space: None,
            prefix: None,
            cursor: None,
            limit: Some(1),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonCount {
            branch: Some("missing".to_owned()),
            space: None,
            prefix: None,
            as_of: None,
            as_of_time: None,
        },
        Command::JsonSample {
            branch: Some("missing".to_owned()),
            space: None,
            prefix: None,
            count: Some(1),
        },
    ]
}

fn assert_json_batch_item_error_boundaries(executor: &mut Executor) {
    let Output::JsonBatchResults(results) = executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: vec![
                BatchJsonEntry::new("bad-path", "$[", json!(true)),
                BatchJsonEntry::new("bad-value", "$", deeply_nested_json(128)),
                BatchJsonEntry::new("valid-error-boundary", "$", json!({"ok": true})),
            ],
        })
        .expect("batch set returns positional errors")
    else {
        panic!("unexpected batch set output");
    };
    assert_eq!(results.len(), 3);
    assert!(results[0].error().is_some());
    assert!(results[1].error().is_some());
    assert_eq!(
        results[0].error_status().expect("path error status").code(),
        "invalid_argument.engine.json_path"
    );
    assert_eq!(
        results[1]
            .error_status()
            .expect("value error status")
            .code(),
        "invalid_argument.engine.json_document_too_deep"
    );
    assert!(results[2]
        .commit()
        .map(strata_executor::CommitReceipt::version)
        .is_some());
    assert!(
        results
            .iter()
            .filter_map(|result| result.error())
            .all(error_message_is_public),
        "batch item errors leaked lower-layer details: {results:?}"
    );
}

fn assert_closed_handle_rejects_json_commands(executor: &mut Executor) {
    executor.close().expect("close succeeds");
    for command in closed_handle_json_commands() {
        let error = executor.execute(command).expect_err("closed command fails");
        assert_eq!(error.class(), ExecutorErrorClass::ClosedHandle);
    }
}

fn closed_handle_json_commands() -> Vec<Command> {
    vec![
        Command::JsonSet {
            branch: None,
            space: None,
            key: "doc".to_owned(),
            path: "$".to_owned(),
            value: json!({}),
        },
        Command::JsonGet {
            branch: None,
            space: None,
            key: "doc".to_owned(),
            path: "$".to_owned(),
            as_of: None,
            as_of_time: None,
        },
        Command::JsonDelete {
            branch: None,
            space: None,
            key: "doc".to_owned(),
            path: "$".to_owned(),
        },
        Command::JsonHistory {
            branch: None,
            space: None,
            key: "doc".to_owned(),
        },
        Command::JsonExists {
            branch: None,
            space: None,
            key: "doc".to_owned(),
        },
        Command::JsonBatchSet {
            branch: None,
            space: None,
            entries: Vec::new(),
        },
        Command::JsonBatchGet {
            branch: None,
            space: None,
            entries: Vec::new(),
        },
        Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries: Vec::new(),
        },
        Command::JsonList {
            branch: None,
            space: None,
            prefix: None,
            cursor: None,
            limit: None,
            as_of: None,
            as_of_time: None,
        },
        Command::JsonCount {
            branch: None,
            space: None,
            prefix: None,
            as_of: None,
            as_of_time: None,
        },
        Command::JsonSample {
            branch: None,
            space: None,
            prefix: None,
            count: None,
        },
        Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
            field_path: "$.name".to_owned(),
            index_type: JsonIndexType::Text,
        },
        Command::JsonDropIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
        },
        Command::JsonListIndexes {
            branch: None,
            space: None,
        },
    ]
}

fn deeply_nested_json(depth: usize) -> Value {
    let mut value = Value::Null;
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    value
}

fn run_json_command_suite(executor: &mut Executor) {
    let first = seed_json_command_suite(executor);
    assert_json_read_commands(executor, first.timestamp);
    assert_json_batch_list_sample_commands(executor, first.timestamp);
    assert_json_delete_commands(executor);
    assert_json_index_commands(executor);
}

fn seed_json_command_suite(executor: &mut Executor) -> WriteFacts {
    let first = write_json(
        executor,
        None,
        None,
        "doc-alpha",
        "$",
        json!({"name": "Ada", "age": 36, "tags": ["math"]}),
    );
    let second = write_json(
        executor,
        None,
        None,
        "doc-alpha",
        "$.name",
        json!("Ada Lovelace"),
    );
    assert!(second.version > first.version);

    write_json(
        executor,
        None,
        None,
        "doc-bravo",
        "$",
        json!({"name": "Grace", "age": 37}),
    );
    write_json(
        executor,
        None,
        None,
        "doc-delete",
        "$",
        json!({"name": "Delete"}),
    );
    first
}

fn assert_json_read_commands(executor: &mut Executor, first_timestamp: u64) {
    assert_eq!(
        execute_json_get_value(executor, "doc-alpha", "$.name"),
        Some(json!("Ada Lovelace"))
    );
    assert_eq!(
        execute_json_get_as_of(executor, "doc-alpha", "$.name", first_timestamp),
        Some(json!("Ada"))
    );
    assert!(execute_json_exists(executor, "doc-alpha"));
    assert!(!execute_json_exists(executor, "missing"));
}

fn assert_json_batch_list_sample_commands(executor: &mut Executor, first_timestamp: u64) {
    batch_set_json(
        executor,
        vec![
            BatchJsonEntry::new("doc-batch-a", "$", json!({"name": "Katherine"})),
            BatchJsonEntry::new("doc-batch-b", "$", json!({"name": "Dorothy"})),
            BatchJsonEntry::new("doc-batch-a", "$.role", json!("lead")),
        ],
    );
    assert_eq!(
        execute_json_batch_get(
            executor,
            vec![
                BatchJsonGetEntry::new("doc-batch-a", "$.role"),
                BatchJsonGetEntry::new("missing", "$"),
                BatchJsonGetEntry::new("doc-batch-b", "$.name"),
            ],
        ),
        vec![Some(json!("lead")), None, Some(json!("Dorothy"))]
    );

    assert_eq!(
        execute_json_list(executor, Some("doc-batch-"), None, 1),
        (vec!["doc-batch-a".to_owned()], true)
    );
    assert_eq!(
        execute_json_list_as_of(executor, Some("doc-"), first_timestamp),
        vec!["doc-alpha".to_owned()]
    );

    assert_eq!(execute_json_count(executor, Some("doc-batch-")), 2);
    let sample = execute_json_sample(executor, Some("doc-batch-"), 1);
    assert_eq!(sample.0, 2);
    assert_eq!(sample.1.len(), 1);
    assert!(sample.1[0].starts_with("doc-batch-"));
}

fn assert_json_delete_commands(executor: &mut Executor) {
    let deleted = execute_json_delete(executor, "doc-delete", "$");
    assert!(deleted);
    let missing_deleted = execute_json_delete(executor, "doc-delete", "$");
    assert!(!missing_deleted);
    assert_json_history_has_tombstone(executor, "doc-delete");

    let delete_results = execute_json_batch_delete(
        executor,
        vec![
            BatchJsonDeleteEntry::new("doc-batch-a", "$.role"),
            BatchJsonDeleteEntry::new("missing", "$"),
        ],
    );
    assert_eq!(delete_results, vec![true, false]);
    assert_eq!(
        execute_json_get_value(executor, "doc-batch-a", "$.role"),
        None
    );
}

fn assert_json_index_commands(executor: &mut Executor) {
    let definition = create_json_index(executor, "by-name", "$.name", JsonIndexType::Text);
    assert_eq!(definition.name(), "by-name");
    assert_eq!(definition.index_type(), JsonIndexType::Text);

    let duplicate = executor
        .execute(Command::JsonCreateIndex {
            branch: None,
            space: None,
            name: "by-name".to_owned(),
            field_path: "$.name".to_owned(),
            index_type: JsonIndexType::Text,
        })
        .expect_err("duplicate index fails");
    assert_eq!(duplicate.class(), ExecutorErrorClass::Conflict);

    create_json_index(executor, "by-age", "$.age", JsonIndexType::Numeric);
    create_json_index(executor, "by-tag", "$.name", JsonIndexType::Tag);

    let indexes = list_json_indexes(executor);
    assert_eq!(indexes.len(), 3);
    assert_json_index(&indexes, "by-name", JsonIndexType::Text);
    assert_json_index(&indexes, "by-age", JsonIndexType::Numeric);
    assert_json_index(&indexes, "by-tag", JsonIndexType::Tag);

    assert!(!drop_json_index(executor, "missing"));
    assert!(drop_json_index(executor, "by-age"));
    assert!(drop_json_index(executor, "by-tag"));

    let indexes = list_json_indexes(executor);
    assert_eq!(indexes.len(), 1);
    assert_json_index(&indexes, "by-name", JsonIndexType::Text);
}

fn create_json_index(
    executor: &mut Executor,
    name: &str,
    field_path: &str,
    index_type: JsonIndexType,
) -> strata_executor::JsonIndexDefinition {
    create_json_index_in(executor, None, None, name, field_path, index_type)
}

fn create_json_index_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    name: &str,
    field_path: &str,
    index_type: JsonIndexType,
) -> strata_executor::JsonIndexDefinition {
    let Output::JsonIndexDefinition(definition) = executor
        .execute(Command::JsonCreateIndex {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            name: name.to_owned(),
            field_path: field_path.to_owned(),
            index_type,
        })
        .expect("index create succeeds")
    else {
        panic!("unexpected index create output");
    };
    definition
}

fn list_json_indexes(executor: &mut Executor) -> Vec<strata_executor::JsonIndexDefinition> {
    list_json_indexes_in(executor, None, None)
}

fn list_json_indexes_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
) -> Vec<strata_executor::JsonIndexDefinition> {
    let Output::JsonIndexList { items: indexes, .. } = executor
        .execute(Command::JsonListIndexes {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
        })
        .expect("index list succeeds")
    else {
        panic!("unexpected index list output");
    };
    indexes
}

fn drop_json_index(executor: &mut Executor, name: &str) -> bool {
    let Output::Bool(existed) = executor
        .execute(Command::JsonDropIndex {
            branch: None,
            space: None,
            name: name.to_owned(),
        })
        .expect("index drop succeeds")
    else {
        panic!("unexpected index drop output");
    };
    existed
}

fn assert_json_index(
    indexes: &[strata_executor::JsonIndexDefinition],
    name: &str,
    index_type: JsonIndexType,
) {
    assert!(
        indexes
            .iter()
            .any(|index| index.name() == name && index.index_type() == index_type),
        "missing JSON index `{name}` with type {index_type:?}: {indexes:?}"
    );
}

fn write_json(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    key: &str,
    path: &str,
    value: Value,
) -> WriteFacts {
    match executor
        .execute(Command::JsonSet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            key: key.to_owned(),
            path: path.to_owned(),
            value,
        })
        .expect("JSON set succeeds")
    {
        Output::JsonWriteResult { effect, commit, .. } => {
            assert!(effect.applied());
            WriteFacts {
                version: commit.version(),
                timestamp: commit.timestamp(),
            }
        }
        output => panic!("unexpected JSON set output: {output:?}"),
    }
}

fn run_executor_modes(exercise: fn(&mut Executor)) {
    let mut cache = Executor::open_cache().expect("cache executor opens");
    exercise(&mut cache);

    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");
    let mut durable = Executor::open_durable_local(&path).expect("durable executor opens");
    exercise(&mut durable);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WriteFacts {
    version: u64,
    timestamp: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct VersionedJsonFacts {
    value: Value,
    version: u64,
    timestamp: u64,
    document_version: u64,
}

fn batch_set_json(executor: &mut Executor, entries: Vec<BatchJsonEntry>) {
    match executor
        .execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries,
        })
        .expect("JSON batch set succeeds")
    {
        Output::JsonBatchResults(results) => {
            assert!(results.iter().all(|result| result
                .commit()
                .map(strata_executor::CommitReceipt::version)
                .is_some()));
        }
        output => panic!("unexpected JSON batch set output: {output:?}"),
    }
}

fn execute_json_get_value(executor: &mut Executor, key: &str, path: &str) -> Option<Value> {
    execute_json_get_value_in(executor, None, None, key, path)
}

fn execute_json_get_versioned(
    executor: &mut Executor,
    key: &str,
    path: &str,
) -> Option<VersionedJsonFacts> {
    match executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: key.to_owned(),
            path: path.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("JSON get succeeds")
    {
        Output::JsonVersionedValue(value) => value.value().map(|value| VersionedJsonFacts {
            value: value.value().clone(),
            version: value.version(),
            timestamp: value.timestamp(),
            document_version: value.document_version(),
        }),
        output => panic!("unexpected JSON get output: {output:?}"),
    }
}

fn execute_json_get_value_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    key: &str,
    path: &str,
) -> Option<Value> {
    match executor
        .execute(Command::JsonGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            key: key.to_owned(),
            path: path.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("JSON get succeeds")
    {
        Output::JsonVersionedValue(value) => value.value().map(|value| value.value().clone()),
        output => panic!("unexpected JSON get output: {output:?}"),
    }
}

fn execute_json_get_as_of(
    executor: &mut Executor,
    key: &str,
    path: &str,
    as_of: u64,
) -> Option<Value> {
    match executor
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: key.to_owned(),
            path: path.to_owned(),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("historical JSON get succeeds")
    {
        Output::JsonVersionedValue(value) => value.into_option().map(|value| value.value().clone()),
        output => panic!("unexpected historical JSON get output: {output:?}"),
    }
}

fn execute_json_delete(executor: &mut Executor, key: &str, path: &str) -> bool {
    match executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: key.to_owned(),
            path: path.to_owned(),
        })
        .expect("JSON delete succeeds")
    {
        Output::JsonDeleteResult { effect, .. } => effect.applied(),
        output => panic!("unexpected JSON delete output: {output:?}"),
    }
}

fn execute_json_batch_get(
    executor: &mut Executor,
    entries: Vec<BatchJsonGetEntry>,
) -> Vec<Option<Value>> {
    match executor
        .execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries,
        })
        .expect("JSON batch get succeeds")
    {
        Output::JsonBatchGetResults(results) => results
            .into_iter()
            .map(|result| result.value().cloned())
            .collect(),
        output => panic!("unexpected JSON batch get output: {output:?}"),
    }
}

fn execute_json_batch_delete(
    executor: &mut Executor,
    entries: Vec<BatchJsonDeleteEntry>,
) -> Vec<bool> {
    match executor
        .execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries,
        })
        .expect("JSON batch delete succeeds")
    {
        Output::JsonBatchResults(results) => results
            .into_iter()
            .map(|result| {
                result
                    .commit()
                    .map(strata_executor::CommitReceipt::version)
                    .is_some()
            })
            .collect(),
        output => panic!("unexpected JSON batch delete output: {output:?}"),
    }
}

fn execute_json_list(
    executor: &mut Executor,
    prefix: Option<&str>,
    cursor: Option<&str>,
    limit: u64,
) -> (Vec<String>, bool) {
    match executor
        .execute(Command::JsonList {
            branch: None,
            space: None,
            prefix: prefix.map(str::to_owned),
            cursor: cursor.map(str::to_owned),
            limit: Some(limit),
            as_of: None,
            as_of_time: None,
        })
        .expect("JSON list succeeds")
    {
        Output::JsonListResult { items: keys, page } => (keys, page.has_more()),
        output => panic!("unexpected JSON list output: {output:?}"),
    }
}

fn execute_json_list_as_of(
    executor: &mut Executor,
    prefix: Option<&str>,
    as_of: u64,
) -> Vec<String> {
    match executor
        .execute(Command::JsonList {
            branch: None,
            space: None,
            prefix: prefix.map(str::to_owned),
            cursor: None,
            limit: Some(100),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("historical JSON list succeeds")
    {
        Output::JsonListResult { items: keys, .. } => keys,
        output => panic!("unexpected historical JSON list output: {output:?}"),
    }
}

fn execute_json_count(executor: &mut Executor, prefix: Option<&str>) -> u64 {
    execute_json_count_in(executor, None, None, prefix)
}

fn execute_json_count_as_of(executor: &mut Executor, prefix: Option<&str>, as_of: u64) -> u64 {
    match executor
        .execute(Command::JsonCount {
            branch: None,
            space: None,
            prefix: prefix.map(str::to_owned),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("JSON count as_of succeeds")
    {
        Output::Uint(count) => count,
        output => panic!("unexpected JSON count output: {output:?}"),
    }
}

#[test]
fn json_count_as_of_returns_historical_count() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let first = write_json(&mut executor, None, None, "a", "$", json!({"n": 1}));
    write_json(&mut executor, None, None, "b", "$", json!({"n": 2}));
    write_json(&mut executor, None, None, "c", "$", json!({"n": 3}));
    assert_eq!(execute_json_count(&mut executor, None), 3);
    assert_eq!(
        execute_json_count_as_of(&mut executor, None, first.timestamp),
        1,
        "count as_of the first write sees only the first document"
    );
}

fn execute_json_count_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    prefix: Option<&str>,
) -> u64 {
    match executor
        .execute(Command::JsonCount {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            prefix: prefix.map(str::to_owned),
            as_of: None,
            as_of_time: None,
        })
        .expect("JSON count succeeds")
    {
        Output::Uint(count) => count,
        output => panic!("unexpected JSON count output: {output:?}"),
    }
}

fn execute_json_sample(
    executor: &mut Executor,
    prefix: Option<&str>,
    count: u64,
) -> (u64, Vec<String>) {
    let (total_count, items) = execute_json_sample_items(executor, prefix, count);
    (
        total_count,
        items.into_iter().map(|(key, _value)| key).collect(),
    )
}

fn execute_json_sample_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    prefix: Option<&str>,
    count: u64,
) -> (u64, Vec<String>) {
    let (total_count, items) = execute_json_sample_items_in(executor, branch, space, prefix, count);
    (
        total_count,
        items.into_iter().map(|(key, _value)| key).collect(),
    )
}

fn execute_json_sample_items(
    executor: &mut Executor,
    prefix: Option<&str>,
    count: u64,
) -> (u64, Vec<(String, Value)>) {
    execute_json_sample_items_in(executor, None, None, prefix, count)
}

fn execute_json_sample_items_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    prefix: Option<&str>,
    count: u64,
) -> (u64, Vec<(String, Value)>) {
    match executor
        .execute(Command::JsonSample {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            prefix: prefix.map(str::to_owned),
            count: Some(count),
        })
        .expect("JSON sample succeeds")
    {
        Output::JsonSampleResult {
            total_count, items, ..
        } => (
            total_count,
            items
                .into_iter()
                .map(|item| (item.key().to_owned(), item.value().clone()))
                .collect(),
        ),
        output => panic!("unexpected JSON sample output: {output:?}"),
    }
}

fn execute_json_exists(executor: &mut Executor, key: &str) -> bool {
    match executor
        .execute(Command::JsonExists {
            branch: None,
            space: None,
            key: key.to_owned(),
        })
        .expect("JSON exists succeeds")
    {
        Output::Bool(value) => value,
        output => panic!("unexpected JSON exists output: {output:?}"),
    }
}

fn assert_json_history_has_tombstone(executor: &mut Executor, key: &str) {
    let history = execute_json_history(executor, key);
    assert!(history
        .iter()
        .any(strata_executor::JsonHistoryItem::is_tombstone));
}

fn execute_json_history(
    executor: &mut Executor,
    key: &str,
) -> Vec<strata_executor::JsonHistoryItem> {
    match executor
        .execute(Command::JsonHistory {
            branch: None,
            space: None,
            key: key.to_owned(),
        })
        .expect("JSON history succeeds")
    {
        Output::JsonVersionHistory(Some(history)) => history,
        output => panic!("unexpected JSON history output: {output:?}"),
    }
}

fn error_message_is_public(message: &str) -> bool {
    ![
        "strata_storage",
        "StorageRuntime",
        "StorageKey",
        "StorageValue",
        "RowMutation",
        "StoragePersistence",
        "WAL",
        "Wal",
        "table",
        "compaction",
    ]
    .iter()
    .any(|forbidden| message.contains(forbidden))
}

#[allow(dead_code)]
fn _bytes(value: &str) -> Bytes {
    Bytes::from(value)
}

/// Pins the `JsonList` pagination contract: ascending key order, cursors as
/// durable plain positions (valid across interleaved writes and cursor-key
/// deletion, no duplicates), latest-state pages by default, snapshot-stable
/// enumeration under `as_of`, and terminal-page shape.
#[test]
fn json_list_cursor_contract_survives_interleaved_writes() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let mut seed_timestamp = 0;
    for key in ["doc-a", "doc-c", "doc-e"] {
        let output = executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: key.to_owned(),
                path: "$".to_owned(),
                value: json!({"seed": true}),
            })
            .expect("seed set succeeds");
        let Output::JsonWriteResult { commit, .. } = output else {
            panic!("unexpected seed output: {output:?}");
        };
        seed_timestamp = commit.timestamp();
    }

    let (items, has_more, cursor) = json_list_page(&mut executor, None, Some(2), None);
    assert_eq!(items, vec!["doc-a", "doc-c"]);
    assert!(has_more);
    assert_eq!(cursor.as_deref(), Some("doc-c"));

    // Interleave writes around the cursor position: behind it, ahead of it,
    // and a deletion ahead of it.
    for key in ["doc-b", "doc-d"] {
        executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: key.to_owned(),
                path: "$".to_owned(),
                value: json!({"interleaved": true}),
            })
            .expect("interleaved set succeeds");
    }
    executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "doc-e".to_owned(),
            path: "$".to_owned(),
        })
        .expect("interleaved delete succeeds");

    // Resuming strictly after the cursor reflects writes ahead of it (doc-d
    // appears, doc-e is gone) and never revisits keys behind it (doc-b does
    // not appear). The terminal page reports has_more false + null cursor.
    let (items, has_more, cursor) = json_list_page(&mut executor, cursor, Some(10), None);
    assert_eq!(items, vec!["doc-d"]);
    assert!(!has_more);
    assert!(cursor.is_none());

    // A fresh enumeration sees the merged latest state, in ascending order.
    let (items, _, _) = json_list_page(&mut executor, None, Some(10), None);
    assert_eq!(items, vec!["doc-a", "doc-b", "doc-c", "doc-d"]);

    // Cursors survive deletion of the cursor key itself.
    executor
        .execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: "doc-c".to_owned(),
            path: "$".to_owned(),
        })
        .expect("cursor-key delete succeeds");
    let (items, _, _) = json_list_page(&mut executor, Some("doc-c".to_owned()), Some(10), None);
    assert_eq!(items, vec!["doc-d"]);

    // as_of pins the enumeration to the seed snapshot regardless of every
    // write that happened since.
    let (items, _, _) = json_list_page(&mut executor, None, Some(10), Some(seed_timestamp));
    assert_eq!(items, vec!["doc-a", "doc-c", "doc-e"]);
}

fn json_list_page(
    executor: &mut Executor,
    cursor: Option<String>,
    limit: Option<u64>,
    as_of: Option<u64>,
) -> (Vec<String>, bool, Option<String>) {
    let output = executor
        .execute(Command::JsonList {
            branch: None,
            space: None,
            prefix: None,
            cursor,
            limit,
            as_of,
            as_of_time: None,
        })
        .expect("json list succeeds");
    let Output::JsonListResult { items, page } = output else {
        panic!("unexpected json list output: {output:?}");
    };
    (items, page.has_more(), page.cursor().cloned())
}

/// Pins the `JsonScan` contract: ordered key + full-value rows and honest cursor
/// pagination — a limit below the row count reports `has_more` with the first
/// unreturned key, and resuming from that inclusive cursor returns the rest with
/// no gap or overlap before a terminal page. Mirrors the KV scan contract.
#[test]
fn json_scan_returns_ordered_rows_and_paginates_honestly() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for i in 0..6 {
        executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: format!("doc-{i}"),
                path: "$".to_owned(),
                value: json!({ "n": i }),
            })
            .expect("set succeeds");
    }

    // A page below the row count returns ordered key+value rows and a cursor at
    // the first unreturned key.
    let (rows, cursor) = json_scan_page(&mut executor, None, 2);
    assert_eq!(
        rows,
        vec![
            ("doc-0".to_owned(), json!({ "n": 0 })),
            ("doc-1".to_owned(), json!({ "n": 1 })),
        ]
    );
    assert_eq!(cursor.as_deref(), Some("doc-2"));

    // Resume from the cursor until a terminal page. The union of pages is every
    // document, in ascending order, with no overlap or gap.
    let mut seen = rows;
    let mut start = cursor;
    let mut pages = 1;
    while let Some(next) = start {
        let (rows, cursor) = json_scan_page(&mut executor, Some(next), 2);
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
    let expected: Vec<(String, Value)> = (0..6)
        .map(|i| (format!("doc-{i}"), json!({ "n": i })))
        .collect();
    assert_eq!(seen, expected);
}

fn json_scan_page(
    executor: &mut Executor,
    start: Option<String>,
    limit: u64,
) -> (Vec<(String, Value)>, Option<String>) {
    let output = executor
        .execute(Command::JsonScan {
            branch: None,
            space: None,
            start,
            limit: Some(limit),
        })
        .expect("json scan succeeds");
    let Output::JsonScanResult { items, page } = output else {
        panic!("unexpected json scan output: {output:?}");
    };
    (
        items
            .iter()
            .map(|item| (item.key().to_owned(), item.value().clone()))
            .collect(),
        page.cursor().cloned(),
    )
}
