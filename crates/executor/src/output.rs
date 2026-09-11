//! Serializable command outputs.

use crate::types::{
    AdminConfig, AdminDatabaseInfo, AdminDescribe, AdminHealth, AdminIpcStatus, AdminIpcStop,
    AdminMetrics, AdminPing, ArrowExportResult, ArrowImportResult, BatchExistsItemResult,
    BatchExistsPresence, BatchGetItemResult, BatchItemResult, BatchResult, BranchCleanupItem,
    BranchComparisonItem, BranchItem, BranchPreviewItem, Bytes, CommitReceipt,
    EventBatchAppendItemResult, EventChainVerification, EventVersionedData, GraphBatchItemResult,
    GraphBfsData, GraphBindingHit, GraphCdlpData, GraphEdgeDataOutput, GraphInfoData, GraphLccData,
    GraphNeighborHit, GraphNodeDataOutput, GraphOntologyData, GraphOntologySummaryData,
    GraphPagerankData, GraphSsspData, GraphWccData, HistoryResult, HubCloneProgress,
    HubCloneResult, HubDatasetCard, HubDatasetPage, HubInfo, HubRefList, HubYankedList,
    JsonBatchGetItemResult, JsonBatchItemResult, JsonHistoryItem, JsonIndexDefinition,
    JsonSampleItem, Maybe, MaybeJsonValue, MaybeJsonVersionedValue, MutationEffect, PageInfo,
    PromotionOutcomeItem, SampleItem, ScanItem, VectorBatchGetItemResult, VectorBatchItemResult,
    VectorCollectionInfo, VectorHistoryResult, VectorIndexQueryResult, VectorMatch,
    VectorVersionedData, VersionedValue,
};
use serde::{Deserialize, Serialize};

/// Successful executor output.
// `Output` is the serialized wire enum; boxing the largest payload would change
// the generated JSON Schema, so the enum carries concrete DTOs directly.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Output {
    /// Lightweight admin liveness result.
    Pong(AdminPing),
    /// Database identity and catalog summary.
    DatabaseInfo(AdminDatabaseInfo),
    /// Multi-process IPC state for this process.
    IpcStatus(AdminIpcStatus),
    /// Result of stopping multi-process IPC hosting.
    IpcStop(AdminIpcStop),
    /// A completed hub clone.
    HubCloneResult(HubCloneResult),
    /// A hub clone progress event.
    HubCloneProgress(HubCloneProgress),
    /// Hub capability advertisement.
    HubInfo(HubInfo),
    /// Hub dataset listing page.
    HubDatasets(HubDatasetPage),
    /// Full hub dataset card.
    HubDataset(HubDatasetCard),
    /// Hub dataset refs.
    HubRefs(HubRefList),
    /// Hub yank deny-list.
    HubYanked(HubYankedList),
    /// Remote origin of a cloned database (`None` when never recorded).
    RemoteOriginResult {
        /// The recorded origin, when present.
        origin: Option<RemoteOriginInfo>,
    },
    /// Control-plane health facts.
    Health(AdminHealth),
    /// Lightweight database metrics.
    Metrics(AdminMetrics),
    /// Compact database description.
    Described(AdminDescribe),
    /// Sanitized configuration facts.
    Config(AdminConfig),
    /// Optional sanitized configuration value.
    ConfigValue(Option<String>),
    /// Product space list.
    SpaceList {
        /// Spaces in this page.
        items: Vec<String>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Product space create result.
    SpaceCreateResult {
        /// Product space name.
        space: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a catalog mutation was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Product space delete result.
    SpaceDeleteResult {
        /// Product space name.
        space: String,
        /// True when visible space data was force-deleted.
        force: bool,
        /// Number of visible space rows tombstoned, including primitive index/control rows.
        deleted_rows: u64,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a catalog mutation was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// One branch summary.
    Branch(BranchItem),
    /// Branch comparison result.
    BranchComparison(BranchComparisonItem),
    /// Branch promotion (merge) result.
    BranchMerge(PromotionOutcomeItem),
    /// Branch promotion preview result.
    BranchPreview(BranchPreviewItem),
    /// Branch list.
    Branches {
        /// Branches in this page.
        items: Vec<BranchItem>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Branch deletion result.
    BranchDeleteResult {
        /// True when the branch was deleted.
        deleted: bool,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Deleted branch summary.
        branch: BranchItem,
        /// Generation before delete.
        generation_before: Option<u64>,
        /// Generation after delete.
        generation_after: Option<u64>,
        /// Cleanup facts.
        cleanup: Option<BranchCleanupItem>,
    },
    /// KV point-read result: present value with commit metadata, or absence.
    KvVersionedValue(Maybe<VersionedValue>),
    /// Optional JSON value.
    JsonValue(MaybeJsonValue),
    /// Optional JSON value with commit metadata.
    JsonVersionedValue(MaybeJsonVersionedValue),
    /// Full version history for one key.
    VersionHistory(Option<HistoryResult>),
    /// Full JSON document version history.
    JsonVersionHistory(Option<Vec<JsonHistoryItem>>),
    /// Key list.
    ///
    /// A non-paginated list returns a terminal page (`has_more: false`,
    /// `cursor: null`); a paginated list carries the continuation cursor.
    KeysPage {
        /// Keys in this page.
        items: Vec<Bytes>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<Bytes>,
    },
    /// Write acknowledgement.
    WriteResult {
        /// Written key.
        key: Bytes,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Delete acknowledgement.
    DeleteResult {
        /// Target key.
        key: Bytes,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a delete was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// JSON document write acknowledgement.
    ///
    /// JSON document ids are textual, so the echoed key is a `String` rather
    /// than the `Bytes` used by the KV [`WriteResult`](Self::WriteResult)
    /// envelope (DSGN-5/DTO-2).
    JsonWriteResult {
        /// Written document id.
        key: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// JSON document delete acknowledgement.
    ///
    /// The echoed document id is a `String`, mirroring
    /// [`JsonWriteResult`](Self::JsonWriteResult).
    JsonDeleteResult {
        /// Target document id.
        key: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a delete was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// KV scan result.
    KvScanResult {
        /// Rows in this page.
        items: Vec<ScanItem>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<Bytes>,
    },
    /// Positional batch write/delete results.
    BatchResults(BatchResult<BatchItemResult>),
    /// Positional batch read results.
    BatchGetResults(BatchResult<BatchGetItemResult>),
    /// Positional JSON batch write/delete results.
    JsonBatchResults(BatchResult<JsonBatchItemResult>),
    /// Positional JSON batch read results.
    JsonBatchGetResults(BatchResult<JsonBatchGetItemResult>),
    /// Boolean result.
    Bool(bool),
    /// Positional batch existence results.
    BatchExistsResults(BatchResult<BatchExistsItemResult>),
    /// JSON batch document-existence results.
    JsonBatchExistsResults(BatchResult<BatchExistsPresence>),
    /// Vector batch key-existence results.
    VectorBatchExistsResults(BatchResult<BatchExistsPresence>),
    /// Unsigned integer result.
    Uint(u64),
    /// Sampled KV result.
    SampleResult {
        /// Total matching live rows.
        total_count: u64,
        /// Sampled rows.
        items: Vec<SampleItem>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<Bytes>,
    },
    /// Paginated JSON document key list.
    JsonListResult {
        /// Keys in this page.
        items: Vec<String>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// JSON document scan page.
    JsonScanResult {
        /// Documents in this page.
        items: Vec<JsonSampleItem>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Sampled JSON documents.
    JsonSampleResult {
        /// Total matching live documents.
        total_count: u64,
        /// Sampled documents.
        items: Vec<JsonSampleItem>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// JSON secondary index definition.
    JsonIndexDefinition(JsonIndexDefinition),
    /// JSON secondary index definitions.
    JsonIndexList {
        /// Indexes in this page.
        items: Vec<JsonIndexDefinition>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Vector write acknowledgement.
    VectorWriteResult {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
        /// Product vector revision.
        vector_revision: u64,
    },
    /// Vector metadata update acknowledgement.
    VectorMetadataUpdateResult {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when an update was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
        /// Product vector revision when an update was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        vector_revision: Option<u64>,
    },
    /// Vector delete acknowledgement.
    VectorDeleteResult {
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a delete was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Vector bulk delete acknowledgement.
    VectorBulkDeleteResult {
        /// Collection name.
        collection: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when deletes were applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Vector point-read result: present value with commit metadata, or absence.
    VectorData(Maybe<VectorVersionedData>),
    /// Sampled vectors.
    VectorSampleResult {
        /// Total live vectors in the collection.
        total_count: u64,
        /// Sampled vectors.
        items: Vec<VectorVersionedData>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Full vector history.
    VectorVersionHistory(Option<VectorHistoryResult>),
    /// Vector search matches.
    VectorMatches(Vec<VectorMatch>),
    /// Vector search matches plus index planner diagnostics.
    VectorIndexQuery(VectorIndexQueryResult),
    /// Paginated vector key list.
    VectorKeyPage {
        /// Keys in this page.
        items: Vec<String>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Vector scan page.
    VectorScanResult {
        /// Vectors in this page.
        items: Vec<VectorVersionedData>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Vector collection list.
    VectorCollectionList {
        /// Collections in this page.
        items: Vec<VectorCollectionInfo>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Positional vector batch write results.
    VectorBatchUpsertResults(BatchResult<VectorBatchItemResult>),
    /// Positional vector batch read results.
    VectorBatchGetResults(BatchResult<VectorBatchGetItemResult>),
    /// Positional vector batch delete results.
    VectorBatchDeleteResults(BatchResult<VectorBatchItemResult>),
    /// Event append acknowledgement.
    EventAppendResult {
        /// Assigned sequence.
        sequence: u64,
        /// Appended event type.
        event_type: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Event point-read result: present record with commit metadata, or absence.
    EventRecord(Maybe<EventVersionedData>),
    /// Event records.
    EventRecords {
        /// Events in this page.
        items: Vec<EventVersionedData>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<u64>,
    },
    /// Event log count.
    EventCount {
        /// Visible event count.
        count: u64,
    },
    /// Event type list.
    EventTypeList {
        /// Event types in this page.
        items: Vec<String>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Paginated event range.
    EventRangeResult {
        /// Events in this page.
        items: Vec<EventVersionedData>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<u64>,
    },
    /// Positional event batch append results.
    EventBatchAppendResults(BatchResult<EventBatchAppendItemResult>),
    /// Event hash-chain verification result.
    EventChainVerification(EventChainVerification),
    /// Graph create acknowledgement carrying the new graph metadata and commit
    /// receipt.
    GraphCreateResult {
        /// Created graph metadata.
        info: GraphInfoData,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Optional graph metadata.
    GraphInfoResult(Option<GraphInfoData>),
    /// Paginated graph name list.
    GraphNamePage {
        /// Graphs in this page.
        items: Vec<String>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Graph node point-read result: present node, or absence.
    GraphNodeResult(Maybe<GraphNodeDataOutput>),
    /// Paginated graph node list.
    GraphNodePage {
        /// Nodes in this page.
        items: Vec<GraphNodeDataOutput>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Sampled graph nodes.
    GraphSampleResult {
        /// Total live nodes in the graph.
        total_count: u64,
        /// Sampled nodes.
        items: Vec<GraphNodeDataOutput>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Graph edge point-read result: present edge, or absence.
    GraphEdgeResult(Maybe<GraphEdgeDataOutput>),
    /// Paginated graph neighbor list.
    GraphNeighborPage {
        /// Neighbor hits in this page.
        items: Vec<GraphNeighborHit>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Paginated graph binding list.
    GraphBindingPage {
        /// Binding hits in this page.
        items: Vec<GraphBindingHit>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Graph node write acknowledgement.
    GraphNodeWriteResult {
        /// Graph name.
        graph: String,
        /// Node id.
        node_id: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Graph edge write acknowledgement.
    GraphEdgeWriteResult {
        /// Graph name.
        graph: String,
        /// Source node id.
        src: String,
        /// Edge type.
        edge_type: String,
        /// Destination node id.
        dst: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Graph delete acknowledgement.
    GraphDeleteResult {
        /// Graph name.
        graph: String,
        /// Deleted node id for node deletes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        /// Deleted edge source for edge deletes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        src: Option<String>,
        /// Deleted edge type for edge deletes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_type: Option<String>,
        /// Deleted edge destination for edge deletes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dst: Option<String>,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a delete was applied.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Graph batch write acknowledgement.
    GraphBatchWriteResult {
        /// Graph name.
        graph: String,
        /// Shared batch result facts.
        #[serde(flatten)]
        batch: BatchResult<GraphBatchItemResult>,
    },
    /// Graph ontology read result (`None` before any type is defined).
    GraphOntologyResult(Option<GraphOntologyData>),
    /// Graph ontology summary result with per-type usage counts.
    GraphOntologySummaryResult(Option<GraphOntologySummaryData>),
    /// Graph ontology type definition acknowledgement.
    GraphOntologyWriteResult {
        /// Graph name.
        graph: String,
        /// Type kind: `object` or `link`.
        kind: String,
        /// Defined type name.
        type_name: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Weakly connected components computed over a graph snapshot.
    GraphWccResult(GraphWccData),
    /// Local clustering coefficients computed over a graph snapshot.
    GraphLccResult(GraphLccData),
    /// Shortest-path distances computed over a graph snapshot.
    GraphSsspResult(GraphSsspData),
    /// `PageRank` scores computed over a graph snapshot.
    GraphPagerankResult(GraphPagerankData),
    /// Community labels computed over a graph snapshot.
    GraphCdlpResult(GraphCdlpData),
    /// Breadth-first traversal computed over a graph snapshot.
    GraphBfsResult(GraphBfsData),
    /// Bulk ingest acknowledgement.
    GraphBulkInsertResult {
        /// Graph name.
        graph: String,
        /// How many node upserts the ingest applied.
        nodes_inserted: u64,
        /// How many edge upserts the ingest applied.
        edges_inserted: u64,
        /// How many chunk commits the ingest produced.
        commits: u64,
        /// Final chunk's commit receipt, when any chunk committed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Delete-policy application acknowledgement.
    GraphDeletePolicyResult {
        /// Applied policy: `cascade`, `detach`, or `keep_dangling`.
        policy: String,
        /// Mutation effect facts. The number of bound nodes the policy covered
        /// is reported by `effect.affected_count`.
        effect: MutationEffect,
        /// Commit receipt when rows changed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Graph ontology type deletion acknowledgement.
    GraphOntologyDeleteResult {
        /// Graph name.
        graph: String,
        /// Type kind: `object` or `link`.
        kind: String,
        /// Deleted type name.
        type_name: String,
        /// Mutation effect facts.
        effect: MutationEffect,
        /// Commit receipt when a row changed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        commit: Option<CommitReceipt>,
    },
    /// Graph ontology freeze acknowledgement.
    GraphOntologyFreezeResult {
        /// Graph name.
        graph: String,
        /// Frozen object type count.
        object_types: u64,
        /// Frozen link type count.
        link_types: u64,
        /// Commit receipt.
        commit: CommitReceipt,
    },
    /// Arrow import summary.
    ArrowImportResult(ArrowImportResult),
    /// Arrow export summary.
    ArrowExportResult(ArrowExportResult),
    /// Inference model list.
    #[cfg(feature = "inference")]
    InferenceModels {
        /// Models in this page.
        items: Vec<strata_inference::ModelInfo>,
        /// Shared page continuation facts.
        #[serde(flatten)]
        page: PageInfo<String>,
    },
    /// Inference model pull output.
    #[cfg(feature = "inference")]
    InferenceModelPulled(strata_inference::PullModelOutput),
    /// Inference capability facts.
    #[cfg(feature = "inference")]
    InferenceCapability(strata_inference::InferenceCapability),
    /// Inference generation output.
    #[cfg(feature = "inference")]
    InferenceGeneration(strata_inference::ChatResponse),
    /// Inference token ids.
    #[cfg(feature = "inference")]
    InferenceTokenIds(Vec<u32>),
    /// Inference detokenized text.
    #[cfg(feature = "inference")]
    InferenceText(String),
    /// Inference embedding output.
    #[cfg(feature = "inference")]
    InferenceEmbeddings(strata_inference::EmbeddingsResponse),
    /// Inference ranking output.
    #[cfg(feature = "inference")]
    InferenceRanking(strata_inference::RankResponse),
    /// Inference unload result.
    #[cfg(feature = "inference")]
    InferenceUnloadResult(InferenceUnloadResult),
    /// Inference runtime cache diagnostics.
    #[cfg(feature = "inference")]
    InferenceCacheStatus(strata_inference::ModelCacheStatus),
    /// What this binary can do before anything is attempted.
    #[cfg(feature = "inference")]
    InferenceStatus(strata_inference::InferenceStatus),
}

/// Result of evicting a model from the inference runtime cache.
#[cfg(feature = "inference")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct InferenceUnloadResult {
    /// True when a cached entry was removed.
    pub unloaded: bool,
}

/// Serialized view of a database's recorded remote origin.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct RemoteOriginInfo {
    /// The remote host the database was fetched from.
    pub remote_url: String,
    /// The remote dataset name.
    pub dataset: String,
    /// The branch the clone opened by default.
    pub branch: String,
    /// The fetched bundle's manifest hash.
    pub manifest_hash: String,
    /// When the fetch happened (Unix microseconds).
    pub fetched_at_micros: u64,
    /// Per-branch base frontier recorded at the sync point.
    pub base_frontier: Vec<RemoteOriginFrontierInfo>,
}

/// One fetched branch in the recorded base frontier.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct RemoteOriginFrontierInfo {
    /// The fetched branch name.
    pub branch: String,
    /// The bundle's per-branch base token.
    pub base: String,
    /// Local head version at the sync point, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_version: Option<u64>,
}
