//! Executor handle and command dispatch.

use std::collections::BTreeSet;
use std::path::PathBuf;

use strata_core::{CommitVersion, Timestamp};
use strata_engine::{
    api::CommitOutcome, AdminCapabilitySummary as EngineAdminCapabilitySummary,
    AdminConfigSummary as EngineAdminConfigSummary, AdminDatabaseInfo as EngineAdminDatabaseInfo,
    AdminDescribeSummary as EngineAdminDescribeSummary,
    AdminGraphSummary as EngineAdminGraphSummary, AdminHealthStatus as EngineAdminHealthStatus,
    AdminHealthSummary as EngineAdminHealthSummary,
    AdminMetricsSummary as EngineAdminMetricsSummary,
    AdminPrimitiveSummary as EngineAdminPrimitiveSummary,
    AdminVectorCollectionSummary as EngineAdminVectorCollectionSummary, BranchCleanupSummary,
    BranchComparison as EngineBranchComparison, BranchName, BranchPreview as EngineBranchPreview,
    BranchStateSelector, BranchStatus as EngineBranchStatus, BranchSummary, CacheOpenOptions,
    ComparedCapability as EngineComparedCapability, ComparedEntity as EngineComparedEntity,
    ConflictKind as EngineConflictKind, ConflictStrategyResult as EngineConflictStrategyResult,
    ControlHealthStatus as EngineControlHealthStatus, Database,
    DatabaseOpenTarget as EngineDatabaseOpenTarget,
    DerivedStateDisposition as EngineDerivedStateDisposition,
    DerivedStateReport as EngineDerivedStateReport, DurableLocalOpenOptions,
    EmbeddingModelId as EngineEmbeddingModelId, EventAppendOutcome as EngineEventAppendOutcome,
    EventBatchAppendEntry as EngineEventBatchAppendEntry,
    EventBatchAppendItemOutcome as EngineEventBatchAppendItemOutcome,
    EventChainVerification as EngineEventChainVerification, EventPayload as EngineEventPayload,
    EventRangeDirection as EngineEventRangeDirection, EventRangePage as EngineEventRangePage,
    EventSequence as EngineEventSequence, EventService, EventType as EngineEventType,
    EventVersionedRecord as EngineEventVersionedRecord,
    GraphAdjacencyIndex as EngineGraphAdjacencyIndex,
    GraphAnalyticsBudget as EngineGraphAnalyticsBudget,
    GraphBatchOpOutcome as EngineGraphBatchOpOutcome,
    GraphBatchOperation as EngineGraphBatchOperation, GraphBatchWrite as EngineGraphBatchWrite,
    GraphBatchWriteOutcome as EngineGraphBatchWriteOutcome,
    GraphBfsOptions as EngineGraphBfsOptions, GraphBfsResult as EngineGraphBfsResult,
    GraphBinding as EngineGraphBinding, GraphBindingPage as EngineGraphBindingPage,
    GraphBindingPrimitive as EngineGraphBindingPrimitive,
    GraphBindingTarget as EngineGraphBindingTarget,
    GraphBulkInsertOutcome as EngineGraphBulkInsertOutcome,
    GraphCdlpOptions as EngineGraphCdlpOptions, GraphCdlpResult as EngineGraphCdlpResult,
    GraphDeleteOutcome as EngineGraphDeleteOutcome, GraphDeletePolicy as EngineGraphDeletePolicy,
    GraphDeletePolicyOutcome as EngineGraphDeletePolicyOutcome,
    GraphDirection as EngineGraphDirection, GraphEdge as EngineGraphEdge,
    GraphEdgeData as EngineGraphEdgeData, GraphEdgeType as EngineGraphEdgeType,
    GraphEdgeWriteOutcome as EngineGraphEdgeWriteOutcome,
    GraphEntityBinding as EngineGraphEntityBinding, GraphInfo as EngineGraphInfo,
    GraphLccResult as EngineGraphLccResult, GraphLinkTypeDef as EngineGraphLinkTypeDef,
    GraphName as EngineGraphName, GraphNamePage as EngineGraphNamePage,
    GraphNeighbor as EngineGraphNeighbor, GraphNeighborPage as EngineGraphNeighborPage,
    GraphNode as EngineGraphNode, GraphNodeData as EngineGraphNodeData,
    GraphNodeId as EngineGraphNodeId, GraphNodePage as EngineGraphNodePage,
    GraphObjectTypeDef as EngineGraphObjectTypeDef, GraphOntology as EngineGraphOntology,
    GraphOntologyFreezeOutcome as EngineGraphOntologyFreezeOutcome,
    GraphOntologyStatus as EngineGraphOntologyStatus,
    GraphOntologySummary as EngineGraphOntologySummary,
    GraphOntologyWriteOutcome as EngineGraphOntologyWriteOutcome,
    GraphPageRankOptions as EngineGraphPageRankOptions,
    GraphPageRankResult as EngineGraphPageRankResult, GraphProperties as EngineGraphProperties,
    GraphPropertyDef as EngineGraphPropertyDef, GraphService,
    GraphSsspResult as EngineGraphSsspResult, GraphTargetStatus as EngineGraphTargetStatus,
    GraphTypeName as EngineGraphTypeName, GraphWccResult as EngineGraphWccResult,
    GraphWriteOutcome as EngineGraphWriteOutcome, JsonDocumentId, JsonGetEntry, JsonHistory,
    JsonHistoryRow, JsonIndexDefinition as EngineJsonIndexDefinition, JsonIndexName,
    JsonIndexType as EngineJsonIndexType, JsonListPage, JsonPath, JsonSample as EngineJsonSample,
    JsonSampleRow, JsonService, JsonSetEntry, JsonValue as EngineJsonValue,
    JsonVersionedValue as EngineJsonVersionedValue, KvHistory, KvHistoryRow, KvKey, KvSample,
    KvScanRow, KvValue, KvVersionedValue, MemoryBudgetSource as EngineMemoryBudgetSource,
    PreviewConflict as EnginePreviewConflict, ProductSpace, PromotedEntity as EnginePromotedEntity,
    PromotionOutcome as EnginePromotionOutcome, PromotionStrategy as EnginePromotionStrategy,
    SpaceComparison as EngineSpaceComparison, SpaceCreateOutcome as EngineSpaceCreateOutcome,
    SpaceDeleteOutcome as EngineSpaceDeleteOutcome,
    VectorBulkDeleteOutcome as EngineVectorBulkDeleteOutcome,
    VectorCollectionInfo as EngineVectorCollectionInfo,
    VectorCollectionName as EngineVectorCollectionName, VectorConfig as EngineVectorConfig,
    VectorDistanceMetric as EngineVectorDistanceMetric, VectorEmbedding as EngineVectorEmbedding,
    VectorEntry as EngineVectorEntry, VectorFilter as EngineVectorFilter,
    VectorFilterCondition as EngineVectorFilterCondition, VectorFilterOp as EngineVectorFilterOp,
    VectorHistory as EngineVectorHistory, VectorHistoryRow as EngineVectorHistoryRow,
    VectorIndexDiagnostics as EngineVectorIndexDiagnostics, VectorKey as EngineVectorKey,
    VectorKeyPage as EngineVectorKeyPage, VectorMetadata as EngineVectorMetadata,
    VectorMetadataPatch as EngineVectorMetadataPatch, VectorScalar as EngineVectorScalar,
    VectorSearchMatch as EngineVectorSearchMatch, VectorService,
    VectorUpsertEntry as EngineVectorUpsertEntry,
    VectorVersionedEntry as EngineVectorVersionedEntry,
};

use crate::command::Command;
use crate::error::{engine_error_status, ExecutorError, ExecutorResult};
use crate::output::Output;
use crate::types::{
    AdminCapabilities as OutputAdminCapabilities, AdminConfig as OutputAdminConfig,
    AdminControlStatus as OutputAdminControlStatus, AdminDatabaseInfo as OutputAdminDatabaseInfo,
    AdminDescribe as OutputAdminDescribe, AdminGraph as OutputAdminGraph,
    AdminHealth as OutputAdminHealth, AdminHealthStatus as OutputAdminHealthStatus,
    AdminMemoryBudget as OutputAdminMemoryBudget,
    AdminMemoryBudgetSource as OutputAdminMemoryBudgetSource, AdminMetrics as OutputAdminMetrics,
    AdminOpenTarget as OutputAdminOpenTarget, AdminPrimitives as OutputAdminPrimitives,
    AdminVectorCollection as OutputAdminVectorCollection, ArrowExportPrimitive, ArrowFileFormat,
    ArrowImportTarget, BatchEventEntry, BatchExistsItemResult, BatchExistsPresence,
    BatchGetItemResult, BatchItem, BatchItemResult, BatchJsonDeleteEntry, BatchJsonEntry,
    BatchJsonGetEntry, BatchKvEntry, BatchMode, BatchResult, BatchVectorEntry, BranchCleanupItem,
    BranchComparisonItem, BranchItem, BranchMergeItem, BranchParentItem, BranchPreviewItem,
    BranchStatus, Bytes, CommitDurability, CommitReceipt, ComparedCapability, ComparedEntityItem,
    ConflictKind, ConflictStrategyResult, DerivedStateDisposition, DerivedStateReportItem,
    EventBatchAppendItemResult, EventChainVerification as OutputEventChainVerification, EventData,
    EventRangeDirection, EventVersionedData, GraphAnalyticsBudget, GraphBatchItemResult,
    GraphBatchOperation, GraphBfsData, GraphBfsEdgeData, GraphBindingHit, GraphBindingPrimitive,
    GraphBindingTarget, GraphBulkEdge, GraphBulkNode, GraphCdlpData, GraphDeletePolicy,
    GraphDirection, GraphEdgeData, GraphEdgeDataOutput, GraphEntityBinding, GraphInfoData,
    GraphLccData, GraphLinkTypeDefData, GraphLinkTypeSummaryData, GraphNeighborHit, GraphNodeData,
    GraphNodeDataOutput, GraphObjectTypeDefData, GraphObjectTypeSummaryData, GraphOntologyData,
    GraphOntologySummaryData, GraphPagerankData, GraphPropertyDef, GraphSsspData, GraphWccData,
    HistoryItem, HistoryResult, JsonBatchGetItemResult, JsonBatchItemResult, JsonHistoryItem,
    JsonIndexDefinition, JsonIndexType, JsonSampleItem,
    JsonVersionedValue as OutputJsonVersionedValue, Maybe, MaybeJsonValue, MaybeJsonVersionedValue,
    MutationEffect, MutationEffectKind, PageInfo, PreviewConflictItem, PromotedEntityItem,
    PromotionOutcomeItem, PromotionStrategy, SampleItem, ScanItem, SpaceComparisonItem,
    VectorBatchGetItemResult, VectorBatchItemResult,
    VectorCollectionInfo as OutputVectorCollectionInfo, VectorData, VectorDistanceMetric,
    VectorFilterOp, VectorHistoryItem, VectorHistoryResult,
    VectorIndexArtifactSource as OutputVectorIndexArtifactSource,
    VectorIndexDiagnostics as OutputVectorIndexDiagnostics, VectorIndexQueryResult, VectorMatch,
    VectorMetadataFilter, VectorScalar, VectorVersionedData, VersionedValue, DEFAULT_BRANCH,
    DEFAULT_SPACE,
};

const DEFAULT_JSON_LIST_LIMIT: usize = 100;
const DEFAULT_VECTOR_LIST_LIMIT: usize = 100;
const DEFAULT_GRAPH_LIST_LIMIT: usize = 100;

mod admin_branch;
mod admin_convert;
mod arrow_commands;
mod batch;
mod dispatch;
mod event;
mod event_convert;
mod facade;
mod graph;
mod graph_convert;
mod hub;
#[cfg(not(feature = "hub"))]
mod hub_disabled;
mod input_common;
mod json;
mod kv;
mod kv_json_convert;
mod services;
mod vector;
mod vector_convert;

use admin_convert::{
    output_admin_config, output_admin_describe, output_admin_health, output_admin_info,
    output_admin_metrics, output_space_create, output_space_delete,
};
use batch::{
    empty_batch_exists_results, empty_batch_get_results, empty_batch_results,
    empty_json_batch_get_results, empty_json_batch_results, empty_presence_exists_results,
    empty_vector_batch_get_results, empty_vector_batch_results, event_batch_result,
    finish_batch_exists_results, finish_batch_get_results, finish_batch_results,
    finish_json_batch_get_results, finish_json_batch_results, finish_presence_exists_results,
    finish_vector_batch_get_results, finish_vector_batch_results, graph_batch_result,
    json_batch_get_batch_result, json_batch_result, kv_batch_exists_result, kv_batch_get_result,
    kv_batch_result, presence_exists_failed, presence_exists_item, presence_exists_result,
    reject_duplicate_json_targets, reject_duplicate_valid_keys, reject_duplicate_vector_keys,
};
use event_convert::{
    engine_event_direction, engine_event_type, event_append_output, event_batch_append_item_result,
    event_batch_entry, event_chain_verification, event_payload, event_range_output, event_records,
    event_sequence, event_versioned_data, optional_engine_event_type,
};
use graph_convert::{
    engine_graph_batch_operation, engine_graph_bfs_options, engine_graph_binding_target,
    engine_graph_budget, engine_graph_bulk_edges, engine_graph_bulk_nodes,
    engine_graph_cdlp_options, engine_graph_delete_policy, engine_graph_direction,
    engine_graph_edge_data, engine_graph_node_data, engine_graph_pagerank_options,
    engine_graph_personalization, engine_graph_property_defs, graph_batch_operation_name,
    graph_batch_write_output, graph_bfs_output, graph_binding_page_output,
    graph_bulk_insert_output, graph_cdlp_output, graph_create_output, graph_delete_output,
    graph_delete_policy_output, graph_edge_data_output, graph_edge_type, graph_edge_write_output,
    graph_info_data, graph_lcc_output, graph_name, graph_name_page_output,
    graph_neighbor_page_output, graph_node_data_output, graph_node_id, graph_node_page_output,
    graph_node_write_output, graph_ontology_delete_output, graph_ontology_freeze_output,
    graph_ontology_output, graph_ontology_summary_output, graph_ontology_write_output,
    graph_pagerank_output, graph_sssp_output, graph_type_name, graph_wcc_output,
    optional_graph_edge_type, optional_graph_name, optional_graph_node_id,
};
use input_common::{
    branch_name, engine_json_index_type, json_document_id, json_get_entry, json_index_name,
    json_path, json_value, kv_key, kv_value, optional_json_document_id, optional_json_prefix,
    optional_key, optional_limit, output_json_index_type, product_space, required_usize,
};
use kv_json_convert::{
    batch_exists_failed, batch_exists_item, batch_get_failed, batch_get_result, batch_item_failed,
    batch_item_result, branch_cleanup_item, branch_comparison, branch_item, branch_preview,
    branch_promotion, bulk_delete_effect, bytes_from_key, commit_receipt, create_effect,
    delete_effect, delete_output, engine_promotion_strategy, history_result, json_batch_get_failed,
    json_batch_get_result, json_batch_item_failed, json_batch_item_result, json_delete_output,
    json_history_items, json_index_definition, json_list_output, json_sample_item,
    json_sample_output, json_value_output, json_versioned_value, json_write_output, sample_output,
    scan_item, update_effect, upsert_effect, usize_to_u64, versioned_value, write_output,
};
use vector_convert::{
    engine_vector_metric, optional_vector_key, optional_vector_metadata, output_vector_metric,
    query_embedding, require_vector_collection_info, resolve_vector_or_text,
    vector_batch_get_failed, vector_batch_get_item, vector_batch_item_failed,
    vector_batch_item_result, vector_bulk_delete_output, vector_collection, vector_collection_info,
    vector_dimension_mismatch_error, vector_embedding, vector_filter, vector_history_result,
    vector_index_diagnostics, vector_key, vector_key_page_output, vector_match,
    vector_metadata_patch, vector_upsert_entry, vector_versioned_data, vector_write_output,
};

/// Serialized command executor backed by an engine database handle.
pub struct Executor {
    database: Database,
    default_branch: String,
    default_space: String,
    /// Live IPC host state, injected by the owning `Connection` when it hosts a
    /// socket, so the `ipc_status` command can report it. `None` on a
    /// non-hosting executor (client, cache, or `IpcMode::Off`).
    ipc_host_state: Option<crate::IpcHostState>,
    /// Store-state watermark: bumped on every write-classified command attempt
    /// (see `execute`). Shared as an `Arc` so IPC version-tick subscribers can
    /// observe it lock-free, without holding the executor.
    state_version: std::sync::Arc<std::sync::atomic::AtomicU64>,
    #[cfg(feature = "inference")]
    inference: Box<dyn strata_inference::InferenceService>,
}

impl Executor {
    /// Opens a volatile cache-backed executor handle.
    pub fn open_cache() -> ExecutorResult<Self> {
        let outcome = Database::open_cache(CacheOpenOptions::new())?;
        Ok(Self::from_database(outcome.into_database()))
    }

    /// Opens a durable-local executor handle at the selected path.
    pub fn open_durable_local(path: impl Into<PathBuf>) -> ExecutorResult<Self> {
        Self::open_durable_local_with_options(path, DurableLocalOpenOptions::new())
    }

    /// Opens a durable-local executor handle with explicit open options
    /// (durability mode, memory budget, default branch).
    pub fn open_durable_local_with_options(
        path: impl Into<PathBuf>,
        options: DurableLocalOpenOptions,
    ) -> ExecutorResult<Self> {
        let outcome = Database::open_local(path, options)?;
        Ok(Self::from_database(outcome.into_database()))
    }

    /// Opens a durable-local executor returning the raw [`EngineError`] instead
    /// of an [`ExecutorError`], so the failure does NOT cross the logging
    /// boundary. The broker open dance uses this: an expected lock-contention
    /// loss that then recovers via the owner's socket must not be reported as
    /// an engine failure (#3071); only a definitive open failure is converted
    /// (and logged) by the caller.
    pub(crate) fn open_durable_local_quiet(
        path: impl Into<PathBuf>,
        options: DurableLocalOpenOptions,
    ) -> Result<Self, strata_engine::EngineError> {
        let outcome = Database::open_local(path, options)?;
        Ok(Self::from_database(outcome.into_database()))
    }

    /// Wraps an engine database handle.
    pub fn from_database(database: Database) -> Self {
        let default_branch = database.default_branch().as_str().to_owned();
        Self {
            database,
            default_branch,
            default_space: DEFAULT_SPACE.to_owned(),
            ipc_host_state: None,
            state_version: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "inference")]
            inference: Box::new(strata_inference::InferenceRuntime::default()),
        }
    }

    /// A shared handle to the store-state watermark, for observers (IPC
    /// version-tick subscriptions) that poll it without holding the executor.
    pub(crate) fn state_version_handle(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        self.state_version.clone()
    }

    /// Injects the live IPC host state (called by a hosting `Connection` after
    /// it starts the socket server) so `ipc_status` can report it.
    pub fn set_ipc_host_state(&mut self, state: crate::IpcHostState) {
        self.ipc_host_state = Some(state);
    }

    /// The injected IPC host state, if this executor is being hosted.
    pub(crate) const fn ipc_host_state(&self) -> Option<&crate::IpcHostState> {
        self.ipc_host_state.as_ref()
    }

    /// Stop hosting the IPC socket (`ipc_stop`): signal the server to stop and
    /// forget the host state, so `ipc_status` reports `hosting: false`. Returns
    /// whether a running host was stopped (`false` when not hosting).
    pub(crate) fn stop_ipc_hosting(&mut self) -> bool {
        match self.ipc_host_state.take() {
            Some(state) => {
                state.request_stop();
                true
            }
            None => false,
        }
    }

    /// Replaces the inference backend used by inference commands. Accepts any
    /// [`strata_inference::InferenceService`] — the real `InferenceRuntime` or a
    /// deterministic testkit fake.
    #[cfg(feature = "inference")]
    #[must_use]
    pub fn with_inference_runtime(
        mut self,
        inference: impl strata_inference::InferenceService + 'static,
    ) -> Self {
        self.inference = Box::new(inference);
        self
    }

    /// Sets the default branch used when commands omit their branch.
    pub fn with_default_branch(mut self, branch: impl Into<String>) -> ExecutorResult<Self> {
        self.set_default_branch(branch)?;
        Ok(self)
    }

    /// Sets the default branch in place (for a session whose scope changes).
    pub fn set_default_branch(&mut self, branch: impl Into<String>) -> ExecutorResult<()> {
        let branch = branch.into();
        let _validated = branch_name(Some(branch.as_str()), DEFAULT_BRANCH)?;
        self.default_branch = branch;
        Ok(())
    }

    /// Returns the default branch used for omitted command branches.
    #[must_use]
    pub fn default_branch(&self) -> &str {
        &self.default_branch
    }

    /// Sets the default product space used when commands omit their space.
    ///
    /// The executor owns branch and space session context (executor charter
    /// §2), so every frontend resolves omitted spaces uniformly instead of
    /// injecting a default per command.
    pub fn with_default_space(mut self, space: impl Into<String>) -> ExecutorResult<Self> {
        self.set_default_space(space)?;
        Ok(self)
    }

    /// Sets the default product space in place (for a session whose scope changes).
    pub fn set_default_space(&mut self, space: impl Into<String>) -> ExecutorResult<()> {
        let space = space.into();
        let _validated = product_space(Some(space.as_str()), DEFAULT_SPACE)?;
        self.default_space = space;
        Ok(())
    }

    /// Returns the default product space used for omitted command spaces.
    #[must_use]
    pub fn default_space(&self) -> &str {
        &self.default_space
    }

    /// Closes the underlying database handle.
    pub fn close(&mut self) -> ExecutorResult<()> {
        self.database.close()?;
        Ok(())
    }
}
