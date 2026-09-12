use std::sync::Arc;

pub(super) use serde_json::{json, Value};
pub(super) use strata_executor::{
    public_error_code_entry, with_error_render_config, AdminCapabilities, AdminConfig,
    AdminControlStatus, AdminDatabaseInfo, AdminDescribe, AdminGraph, AdminHealth,
    AdminHealthStatus, AdminMetrics, AdminOpenTarget, AdminPrimitives, AdminVectorCollection,
    ArrowExportPrimitive, ArrowExportResult, ArrowFileFormat, ArrowImportResult, ArrowImportTarget,
    BatchEventEntry, BatchExistsItemResult, BatchExistsPresence, BatchGetItemResult, BatchItem,
    BatchItemResult, BatchItemStatus, BatchJsonDeleteEntry, BatchJsonEntry, BatchJsonGetEntry,
    BatchKvEntry, BatchMode, BatchResult, BatchStatus, BatchVectorEntry, BranchCleanupItem,
    BranchItem, BranchParentItem, BranchStatus, Bytes, Command, CommitDurability, CommitReceipt,
    ErrorReferenceIdSource, ErrorRenderConfig, ErrorStatus, EventBatchAppendItemResult,
    EventChainVerification, EventData, EventRangeDirection, EventVersionedData, ExecutorError,
    GraphAnalyticsBudget, GraphBatchItemResult, GraphBatchOperation, GraphBfsData,
    GraphBfsEdgeData, GraphBindingHit, GraphBindingPrimitive, GraphBindingTarget, GraphBulkEdge,
    GraphBulkNode, GraphCdlpData, GraphDeletePolicy, GraphDirection, GraphEdgeData,
    GraphEdgeDataOutput, GraphEntityBinding, GraphInfoData, GraphLccData, GraphLinkTypeDefData,
    GraphLinkTypeSummaryData, GraphNeighborHit, GraphNodeData, GraphNodeDataOutput,
    GraphObjectTypeDefData, GraphObjectTypeSummaryData, GraphOntologyData,
    GraphOntologySummaryData, GraphPagerankData, GraphPropertyDef, GraphSsspData, GraphWccData,
    HistoryItem, HistoryResult, HubBranchHighlight, HubCloneProgress, HubCloneProgressStage,
    HubDatasetCard, HubDatasetPage, HubDatasetSort, HubDatasetSummary, HubInfo, HubProvenance,
    HubRefEntry, HubRefList, HubStrataFeatures, HubYankedEntry, HubYankedList,
    JsonBatchGetItemResult, JsonBatchItemResult, JsonHistoryItem, JsonIndexDefinition,
    JsonIndexType, JsonSampleItem, JsonVersionedValue, Maybe, MaybeJsonVersionedValue,
    MutationEffect, MutationEffectKind, Output, PageInfo, SampleItem, ScanItem,
    VectorBatchGetItemResult, VectorBatchItemResult, VectorCollectionInfo, VectorData,
    VectorDistanceMetric, VectorFilterCondition, VectorFilterOp, VectorHistoryItem,
    VectorHistoryResult, VectorIndexArtifactSource, VectorIndexDiagnostics, VectorIndexQueryResult,
    VectorMatch, VectorMetadataFilter, VectorScalar, VectorVersionedData, VersionedValue,
};

pub(super) fn assert_json_fixture<T: serde::Serialize>(actual: &T, expected: &str) {
    let actual = serde_json::to_value(actual).expect("actual value serializes");
    let expected: Value = serde_json::from_str(expected).expect("fixture is valid JSON");
    assert_eq!(actual, expected);
}

pub(super) fn assert_pretty_fixture(fixture: &str) {
    assert!(fixture.ends_with('\n'), "fixture must end with newline");
    assert!(fixture.contains("\n  "), "fixture must be indented");
    let _: Value = serde_json::from_str(fixture).expect("fixture is valid JSON");
}

pub(super) fn response_fixture_texts() -> Vec<&'static str> {
    let fixtures = vec![
        include_str!("../fixtures/responses/v1/admin/database_info_cache.json"),
        include_str!("../fixtures/responses/v1/arrow/export_graph.json"),
        include_str!("../fixtures/responses/v1/arrow/import_kv.json"),
        include_str!("../fixtures/responses/v1/branches/branch_get.json"),
        include_str!("../fixtures/responses/v1/event/append_applied.json"),
        include_str!("../fixtures/responses/v1/graph/node_write_applied.json"),
        include_str!("../fixtures/responses/v1/graph/ontology_read.json"),
        include_str!("../fixtures/responses/v1/hub/clone_progress_resolved.json"),
        include_str!("../fixtures/responses/v1/hub/dataset.json"),
        include_str!("../fixtures/responses/v1/hub/datasets.json"),
        include_str!("../fixtures/responses/v1/hub/info.json"),
        include_str!("../fixtures/responses/v1/hub/refs.json"),
        include_str!("../fixtures/responses/v1/hub/yanked.json"),
        include_str!("../fixtures/responses/v1/json/get_versioned_found.json"),
        include_str!("../fixtures/responses/v1/kv/delete_missing.json"),
        include_str!("../fixtures/responses/v1/kv/get_found.json"),
        include_str!("../fixtures/responses/v1/kv/list_keys.json"),
        include_str!("../fixtures/responses/v1/kv/write_applied.json"),
        include_str!("../fixtures/responses/v1/optional_reads/event_get_missing.json"),
        include_str!("../fixtures/responses/v1/optional_reads/json_get_missing.json"),
        include_str!("../fixtures/responses/v1/optional_reads/json_get_null.json"),
        include_str!("../fixtures/responses/v1/optional_reads/kv_get_versioned_missing.json"),
        include_str!("../fixtures/responses/v1/optional_reads/vector_get_missing.json"),
        include_str!("../fixtures/responses/v1/pages/continued_page.json"),
        include_str!("../fixtures/responses/v1/pages/first_page.json"),
        include_str!("../fixtures/responses/v1/pages/terminal_page.json"),
        include_str!("../fixtures/responses/v1/shared/batch_result_itemwise_ok.json"),
        include_str!("../fixtures/responses/v1/shared/batch_result_ok.json"),
        include_str!("../fixtures/responses/v1/shared/batch_result_partial.json"),
        include_str!("../fixtures/responses/v1/shared/commit_receipt_cache.json"),
        include_str!("../fixtures/responses/v1/shared/commit_receipt_durable.json"),
        include_str!("../fixtures/responses/v1/shared/error_status_invalid_argument.json"),
        include_str!("../fixtures/responses/v1/shared/maybe_missing.json"),
        include_str!("../fixtures/responses/v1/shared/mutation_effect_created.json"),
        include_str!("../fixtures/responses/v1/shared/mutation_effect_not_found.json"),
        include_str!("../fixtures/responses/v1/shared/page_info_continued.json"),
        include_str!("../fixtures/responses/v1/shared/page_info_terminal.json"),
        include_str!("../fixtures/responses/v1/spaces/space_create_applied.json"),
        include_str!("../fixtures/responses/v1/status/bool_true.json"),
        include_str!("../fixtures/responses/v1/status/uint_count.json"),
        include_str!("../fixtures/responses/v1/vector/search_with_index_diagnostics.json"),
        include_str!("../fixtures/responses/v1/vector/upsert_applied.json"),
    ];

    #[cfg(feature = "inference")]
    {
        let mut fixtures = fixtures;
        fixtures.push(include_str!("../fixtures/responses/v1/inference/text.json"));
        fixtures
    }

    #[cfg(not(feature = "inference"))]
    {
        fixtures
    }
}

/// The shared error-status golden, read back through the wire DTO so the
/// golden test round-trips it; `error_status_fixture_matches_the_registry_row`
/// pins its row fields to the registry.
pub(super) fn error_status_fixture() -> ErrorStatus {
    serde_json::from_str(include_str!(
        "../fixtures/responses/v1/shared/error_status_invalid_argument.json"
    ))
    .expect("error status fixture deserializes")
}

pub(super) fn bytes(value: &str) -> Bytes {
    Bytes::from(value)
}

pub(super) fn kv_batch(items: Vec<BatchItem<BatchItemResult>>) -> BatchResult<BatchItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn kv_batch_get(
    items: Vec<BatchItem<BatchGetItemResult>>,
) -> BatchResult<BatchGetItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn kv_batch_exists(
    items: Vec<BatchItem<BatchExistsItemResult>>,
) -> BatchResult<BatchExistsItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn presence_batch_exists(
    items: Vec<BatchItem<BatchExistsPresence>>,
) -> BatchResult<BatchExistsPresence> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn json_batch(
    items: Vec<BatchItem<JsonBatchItemResult>>,
) -> BatchResult<JsonBatchItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn json_batch_get(
    items: Vec<BatchItem<JsonBatchGetItemResult>>,
) -> BatchResult<JsonBatchGetItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn vector_batch(
    items: Vec<BatchItem<VectorBatchItemResult>>,
) -> BatchResult<VectorBatchItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn vector_batch_get(
    items: Vec<BatchItem<VectorBatchGetItemResult>>,
) -> BatchResult<VectorBatchGetItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn event_batch(
    items: Vec<BatchItem<EventBatchAppendItemResult>>,
) -> BatchResult<EventBatchAppendItemResult> {
    BatchResult::from_items(BatchMode::Itemwise, items)
}

pub(super) fn graph_batch(
    items: Vec<BatchItem<GraphBatchItemResult>>,
) -> BatchResult<GraphBatchItemResult> {
    BatchResult::from_items(BatchMode::Atomic, items)
}

#[derive(Debug)]
struct FixtureReferenceIdSource;

impl ErrorReferenceIdSource for FixtureReferenceIdSource {
    fn next_reference_id(&self) -> String {
        "err-test-000001".to_owned()
    }
}

/// Builds a normalized item error for failed batch fixtures through the real
/// boundary constructor (the registry row supplies everything but the message)
/// under a fixed reference id. The [`BatchItem`](strata_executor::BatchItem)
/// wrapper carries the error now that the inner item DTOs no longer restate it.
pub(super) fn item_error(message: &str) -> ErrorStatus {
    let config = ErrorRenderConfig::new("https://stratadb.org", Arc::new(FixtureReferenceIdSource));
    with_error_render_config(config, || {
        ExecutorError::new("invalid_argument.executor.batch_item", message)
    })
    .status()
    .clone()
}

pub(super) fn commit_receipt(
    version: u64,
    timestamp: u64,
    put_count: u64,
    delete_count: u64,
) -> CommitReceipt {
    CommitReceipt::new(
        version,
        timestamp,
        CommitDurability::Standard,
        put_count,
        delete_count,
    )
}

pub(super) fn unchanged_effect() -> MutationEffect {
    MutationEffect::new(false, MutationEffectKind::Unchanged, true, 0)
}

pub(super) fn deleted_count_effect(count: u64) -> MutationEffect {
    MutationEffect::new(true, MutationEffectKind::Deleted, true, count)
}

pub(super) fn event_versioned_data(
    sequence: u64,
    event_type: &str,
    version: u64,
    timestamp: u64,
) -> EventVersionedData {
    EventVersionedData::new(
        EventData::new(
            sequence,
            event_type.to_owned(),
            json!({"sequence": sequence, "nested": [{"ok": true}], "empty": {}}),
            timestamp,
            "00".repeat(32),
            "11".repeat(32),
        ),
        version,
        timestamp,
    )
}

pub(super) fn branch_item(name: &str) -> BranchItem {
    BranchItem::new(
        name.to_owned(),
        "00000000-0000-0000-0000-000000000000".to_owned(),
        1,
        BranchStatus::Active,
        Some(BranchParentItem::new(
            "default".to_owned(),
            "00000000-0000-0000-0000-000000000000".to_owned(),
            1,
            7,
            Some(99),
        )),
        None,
        Some(7),
        None,
        1,
    )
}

pub(super) fn json_index_definition(name: &str, index_type: JsonIndexType) -> JsonIndexDefinition {
    JsonIndexDefinition::new(
        name.to_owned(),
        "default".to_owned(),
        "name".to_owned(),
        index_type,
        1,
        10,
    )
}

pub(super) fn graph_binding_target() -> GraphBindingTarget {
    GraphBindingTarget::new(
        GraphBindingPrimitive::Json,
        Some("feature".to_owned()),
        "docs",
        "doc-a",
    )
}

pub(super) fn graph_binding() -> GraphEntityBinding {
    GraphEntityBinding::new(graph_binding_target())
}

pub(super) fn graph_node_output(graph: &str, node_id: &str) -> GraphNodeDataOutput {
    GraphNodeDataOutput::new(
        graph.to_owned(),
        node_id.to_owned(),
        Some(json!({"kind": "node"})),
        Some(graph_binding()),
        None,
        2,
        20,
    )
}

pub(super) fn graph_object_type_def() -> GraphObjectTypeDefData {
    GraphObjectTypeDefData::new(
        "Document".to_owned(),
        [(
            "title".to_owned(),
            GraphPropertyDef::new(Some("string".to_owned()), true),
        )]
        .into_iter()
        .collect(),
    )
}

pub(super) fn graph_link_type_def() -> GraphLinkTypeDefData {
    GraphLinkTypeDefData::new(
        "wrote".to_owned(),
        "Author".to_owned(),
        "Document".to_owned(),
        Some("one-to-many".to_owned()),
        std::collections::BTreeMap::new(),
    )
}

pub(super) fn graph_ontology_output(status: &str) -> GraphOntologyData {
    GraphOntologyData::new(
        "deps".to_owned(),
        status.to_owned(),
        vec![graph_object_type_def()],
        vec![graph_link_type_def()],
        2,
        20,
    )
}

pub(super) fn graph_ontology_summary_output() -> GraphOntologySummaryData {
    GraphOntologySummaryData::new(
        "deps".to_owned(),
        "frozen".to_owned(),
        vec![GraphObjectTypeSummaryData::new(graph_object_type_def(), 2)],
        vec![GraphLinkTypeSummaryData::new(graph_link_type_def(), 1)],
        2,
        20,
    )
}

pub(super) fn graph_edge_output(
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
) -> GraphEdgeDataOutput {
    GraphEdgeDataOutput::new(
        graph.to_owned(),
        src.to_owned(),
        edge_type.to_owned(),
        dst.to_owned(),
        2.5,
        Some(json!({"kind": "edge"})),
        3,
        30,
    )
}

pub(super) fn graph_wcc_output() -> GraphWccData {
    GraphWccData::new(
        "deps".to_owned(),
        [
            ("node-a".to_owned(), "node-a".to_owned()),
            ("node-b".to_owned(), "node-a".to_owned()),
            ("node-c".to_owned(), "node-c".to_owned()),
        ]
        .into_iter()
        .collect(),
        2,
    )
}

pub(super) fn graph_bfs_output() -> GraphBfsData {
    GraphBfsData::new(
        "deps".to_owned(),
        "node-a".to_owned(),
        vec!["node-a".to_owned(), "node-b".to_owned()],
        [("node-a".to_owned(), 0), ("node-b".to_owned(), 1)]
            .into_iter()
            .collect(),
        vec![GraphBfsEdgeData::new(
            "node-a".to_owned(),
            "node-b".to_owned(),
            "depends_on".to_owned(),
            1.0,
        )],
        false,
    )
}
