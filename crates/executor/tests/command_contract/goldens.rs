use super::output_cases::vector_index_diagnostics_fixture;
use super::support::*;

#[test]
fn shared_concept_goldens_match_public_json() {
    let durable_commit = CommitReceipt::new(7, 70, CommitDurability::Standard, 1, 0);
    let cache_commit = CommitReceipt::new(8, 80, CommitDurability::NotDurable, 1, 0);
    assert_json_fixture(
        &durable_commit,
        include_str!("../fixtures/responses/v1/shared/commit_receipt_durable.json"),
    );
    assert_json_fixture(
        &cache_commit,
        include_str!("../fixtures/responses/v1/shared/commit_receipt_cache.json"),
    );
    assert_json_fixture(
        &MutationEffect::created(),
        include_str!("../fixtures/responses/v1/shared/mutation_effect_created.json"),
    );
    assert_json_fixture(
        &MutationEffect::not_found(),
        include_str!("../fixtures/responses/v1/shared/mutation_effect_not_found.json"),
    );
    assert_json_fixture(
        &MaybeJsonValue::found(Value::Null),
        include_str!("../fixtures/responses/v1/shared/maybe_json_null.json"),
    );
    assert_json_fixture(
        &MaybeJsonValue::missing(),
        include_str!("../fixtures/responses/v1/shared/maybe_missing.json"),
    );
    assert_json_fixture(
        &PageInfo::<String>::terminal(),
        include_str!("../fixtures/responses/v1/shared/page_info_terminal.json"),
    );
    assert_json_fixture(
        &PageInfo::new(true, Some("cursor-1".to_owned())),
        include_str!("../fixtures/responses/v1/shared/page_info_continued.json"),
    );
    assert_json_fixture(
        &json_batch(vec![BatchItem::ok(
            0,
            true,
            Some(MutationEffect::created()),
            Some(durable_commit),
            JsonBatchItemResult::new(Some(1)),
        )]),
        include_str!("../fixtures/responses/v1/shared/batch_result_itemwise_ok.json"),
    );
    assert_json_fixture(
        &error_status_fixture(),
        include_str!("../fixtures/responses/v1/shared/error_status_invalid_argument.json"),
    );
}

/// The error-status golden is a committed wire sample; its class, retry
/// policy, commit outcome and suggested fix must be the registry row for its
/// code, because after #3244 nothing else can reach the wire.
#[test]
fn error_status_fixture_matches_the_registry_row() {
    let status = error_status_fixture();
    let entry = public_error_code_entry(status.code()).expect("fixture code is registered");
    assert_eq!(status.class(), entry.class);
    assert_eq!(status.retry_policy(), entry.retry_policy);
    assert_eq!(status.commit_outcome(), entry.commit_outcome);
    assert_eq!(status.suggested_fix(), entry.suggested_fix);
}

#[test]
#[allow(clippy::too_many_lines)]
fn public_response_family_goldens_match_public_json() {
    let cases = vec![
        (
            Output::DatabaseInfo(AdminDatabaseInfo {
                version: "1.0.0".to_owned(),
                target: AdminOpenTarget::Cache,
                created: true,
                durable: false,
                default_branch: "default".to_owned(),
                branch_count: 1,
                space_count: 1,
                memory_budget: strata_executor::AdminMemoryBudget {
                    total_bytes: 536_870_912,
                    source: strata_executor::AdminMemoryBudgetSource::DerivedFromHost,
                    usable_host_bytes: Some(2_147_483_648),
                },
                open: true,
            }),
            include_str!("../fixtures/responses/v1/admin/database_info_cache.json"),
        ),
        (
            Output::SpaceCreateResult {
                space: "tenant_a".to_owned(),
                effect: MutationEffect::created(),
                commit: Some(commit_receipt(2, 20, 1, 0)),
            },
            include_str!("../fixtures/responses/v1/spaces/space_create_applied.json"),
        ),
        (
            Output::Branch(branch_item("main")),
            include_str!("../fixtures/responses/v1/branches/branch_get.json"),
        ),
        (
            Output::WriteResult {
                key: bytes("a"),
                effect: MutationEffect::created(),
                // Mirrors the blessed cache-replay fixture: logical commit
                // clock 3, durable false (fixtures reproduce from a scratch
                // cache executor via `strata-idl verify-fixtures`).
                commit: CommitReceipt::new(3, 3, CommitDurability::NotDurable, 1, 0),
            },
            include_str!("../fixtures/responses/v1/kv/write_applied.json"),
        ),
        (
            Output::KeysPage {
                items: vec![bytes("a1"), bytes("a2")],
                page: PageInfo::terminal(),
            },
            include_str!("../fixtures/responses/v1/kv/list_keys.json"),
        ),
        (
            Output::KvVersionedValue(Maybe::found(VersionedValue::new(bytes("one"), 3, 3))),
            include_str!("../fixtures/responses/v1/kv/get_found.json"),
        ),
        (
            Output::DeleteResult {
                key: bytes("a"),
                effect: MutationEffect::not_found(),
                commit: None,
            },
            include_str!("../fixtures/responses/v1/kv/delete_missing.json"),
        ),
        (
            Output::JsonValue(MaybeJsonValue::found(json!({"name": "Ada"}))),
            include_str!("../fixtures/responses/v1/json/get_found.json"),
        ),
        (
            Output::VectorWriteResult {
                collection: "docs".to_owned(),
                key: "doc-a".to_owned(),
                effect: MutationEffect::created(),
                commit: CommitReceipt::new(4, 4, CommitDurability::NotDurable, 1, 0),
                vector_revision: 1,
            },
            include_str!("../fixtures/responses/v1/vector/upsert_applied.json"),
        ),
        (
            Output::VectorIndexQuery(VectorIndexQueryResult::new(
                vec![VectorMatch::new(
                    "doc-a".to_owned(),
                    1.0,
                    Some(json!({"kind": "doc"})),
                )],
                vector_index_diagnostics_fixture(),
            )),
            include_str!("../fixtures/responses/v1/vector/search_with_index_diagnostics.json"),
        ),
        (
            Output::EventAppendResult {
                sequence: 0,
                event_type: "user.created".to_owned(),
                effect: MutationEffect::created(),
                // Mirrors the blessed cache-replay fixture (event row +
                // type-index row + log metadata row in one commit).
                commit: CommitReceipt::new(3, 3, CommitDurability::NotDurable, 3, 0),
            },
            include_str!("../fixtures/responses/v1/event/append_applied.json"),
        ),
        (
            Output::GraphNodeWriteResult {
                graph: "deps".to_owned(),
                node_id: "node-a".to_owned(),
                effect: MutationEffect::created(),
                commit: commit_receipt(2, 20, 2, 0),
            },
            include_str!("../fixtures/responses/v1/graph/node_write_applied.json"),
        ),
        (
            Output::GraphOntologyResult(Some(graph_ontology_output("frozen"))),
            include_str!("../fixtures/responses/v1/graph/ontology_read.json"),
        ),
        (
            Output::GraphWccResult(graph_wcc_output()),
            include_str!("../fixtures/responses/v1/graph/wcc_result.json"),
        ),
        (
            Output::GraphBfsResult(graph_bfs_output()),
            include_str!("../fixtures/responses/v1/graph/bfs_result.json"),
        ),
        (
            Output::ArrowImportResult(ArrowImportResult::new(
                ArrowImportTarget::Kv,
                "input.parquet".to_owned(),
                10,
                1,
                2,
            )),
            include_str!("../fixtures/responses/v1/arrow/import_kv.json"),
        ),
        (
            Output::ArrowExportResult(ArrowExportResult::new(
                ArrowExportPrimitive::Graph,
                ArrowFileFormat::Parquet,
                vec![
                    "graph_nodes.parquet".to_owned(),
                    "graph_edges.parquet".to_owned(),
                ],
                11,
                1024,
            )),
            include_str!("../fixtures/responses/v1/arrow/export_graph.json"),
        ),
        (
            Output::Bool(true),
            include_str!("../fixtures/responses/v1/status/bool_true.json"),
        ),
        (
            Output::Uint(2),
            include_str!("../fixtures/responses/v1/status/uint_count.json"),
        ),
    ];

    for (output, expected) in cases {
        assert_json_fixture(&output, expected);
    }

    #[cfg(feature = "inference")]
    assert_json_fixture(
        &Output::InferenceText("hello".to_owned()),
        include_str!("../fixtures/responses/v1/inference/text.json"),
    );
}

#[test]
fn public_response_fixtures_are_pretty_printed() {
    for fixture in response_fixture_texts() {
        assert_pretty_fixture(fixture);
    }
}

#[test]
fn page_response_goldens_match_public_json() {
    let cases = [
        (
            Output::GraphNamePage {
                items: vec!["deps".to_owned()],
                page: PageInfo::new(true, Some("deps".to_owned())),
            },
            include_str!("../fixtures/responses/v1/pages/first_page.json"),
        ),
        (
            Output::JsonListResult {
                items: vec!["doc-b".to_owned()],
                page: PageInfo::new(true, Some("doc-b".to_owned())),
            },
            include_str!("../fixtures/responses/v1/pages/continued_page.json"),
        ),
        (
            Output::EventRangeResult {
                items: Vec::new(),
                page: PageInfo::terminal(),
            },
            include_str!("../fixtures/responses/v1/pages/terminal_page.json"),
        ),
    ];

    for (output, expected) in cases {
        assert_json_fixture(&output, expected);
    }
}

#[test]
fn batch_response_goldens_match_public_json() {
    let commit = commit_receipt(7, 70, 1, 0);
    let cases = [
        (
            Output::JsonBatchResults(json_batch(vec![BatchItem::ok(
                0,
                true,
                Some(MutationEffect::created()),
                Some(commit),
                JsonBatchItemResult::new(Some(1)),
            )])),
            include_str!("../fixtures/responses/v1/shared/batch_result_ok.json"),
        ),
        (
            Output::JsonBatchResults(json_batch(vec![
                BatchItem::ok(
                    0,
                    true,
                    Some(MutationEffect::created()),
                    Some(commit),
                    JsonBatchItemResult::new(Some(1)),
                ),
                BatchItem::ok(
                    1,
                    false,
                    Some(MutationEffect::not_found()),
                    None,
                    JsonBatchItemResult::new(None),
                ),
            ])),
            include_str!("../fixtures/responses/v1/shared/batch_result_partial.json"),
        ),
    ];

    for (output, expected) in cases {
        assert_json_fixture(&output, expected);
    }
}

#[test]
fn optional_read_goldens_match_public_json() {
    let cases = [
        (
            Output::JsonValue(MaybeJsonValue::missing()),
            include_str!("../fixtures/responses/v1/optional_reads/json_get_missing.json"),
        ),
        (
            Output::JsonValue(MaybeJsonValue::found(Value::Null)),
            include_str!("../fixtures/responses/v1/optional_reads/json_get_null.json"),
        ),
        (
            Output::KvVersionedValue(Maybe::missing()),
            include_str!("../fixtures/responses/v1/optional_reads/kv_get_versioned_missing.json"),
        ),
        (
            Output::VectorData(Maybe::missing()),
            include_str!("../fixtures/responses/v1/optional_reads/vector_get_missing.json"),
        ),
        (
            Output::EventRecord(Maybe::missing()),
            include_str!("../fixtures/responses/v1/optional_reads/event_get_missing.json"),
        ),
    ];

    for (output, expected) in cases {
        assert_json_fixture(&output, expected);
    }
}
