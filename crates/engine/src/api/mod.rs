//! Executor-facing engine API.

mod admin;
mod branch;
mod control;
mod database;
mod dataset_readme;
mod event;
mod graph;
mod json;
mod kv;
mod options;
mod space;
mod vector;

pub use admin::{
    AdminCapabilitySummary, AdminConfigSummary, AdminDatabaseInfo, AdminDescribeSummary,
    AdminGraphSummary, AdminHealthStatus, AdminHealthSummary, AdminMetricsSummary,
    AdminPingSummary, AdminPrimitiveSummary, AdminService, AdminVectorCollectionSummary,
};
pub(crate) use branch::BranchWorkflowCoverage;
pub use branch::{
    BranchCleanupSummary, BranchComparison, BranchCreateOutcome, BranchDeleteOutcome,
    BranchMergeSummary, BranchParentSummary, BranchPreview, BranchStateSelector, BranchStatus,
    BranchSummary, ComparedCapability, ComparedEntity, ConflictKind, ConflictStrategyResult,
    DerivedStateDisposition, DerivedStateReport, PreviewConflict, PromotedEntity, PromotionOutcome,
    PromotionStrategy, SpaceComparison,
};
pub use control::{ControlDiagnostics, ControlHealthStatus, SpaceCatalogDiagnostics};
pub use database::{
    CloseOutcome, Database, DatabaseOpenOutcome, DatabaseOpenSummary, DatabaseOpenTarget,
    MemoryBudgetSource,
};
pub use event::{
    EventAppendOutcome, EventBatchAppendEntry, EventBatchAppendItemOutcome,
    EventBatchAppendOutcome, EventChainVerification, EventLength, EventPayload,
    EventRangeDirection, EventRangePage, EventSequence, EventService, EventType, EventTypeList,
    EventVersionedRecord,
};
pub use graph::{
    GraphAdjacencyEdge, GraphAdjacencyIndex, GraphAnalyticsBudget, GraphBatchOpOutcome,
    GraphBatchOperation, GraphBatchWrite, GraphBatchWriteOutcome, GraphBfsOptions, GraphBfsResult,
    GraphBinding, GraphBindingPage, GraphBindingPrimitive, GraphBindingTarget,
    GraphBulkInsertOutcome, GraphCdlpOptions, GraphCdlpResult, GraphDeleteOutcome,
    GraphDeletePolicy, GraphDeletePolicyOutcome, GraphDirection, GraphEdge, GraphEdgeData,
    GraphEdgePage, GraphEdgeType, GraphEdgeWriteOutcome, GraphEntityBinding, GraphInfo,
    GraphLccResult, GraphLinkTypeDef, GraphLinkTypeSummary, GraphName, GraphNamePage,
    GraphNeighbor, GraphNeighborPage, GraphNode, GraphNodeData, GraphNodeId, GraphNodePage,
    GraphObjectTypeDef, GraphObjectTypeSummary, GraphOntology, GraphOntologyFreezeOutcome,
    GraphOntologyStatus, GraphOntologySummary, GraphOntologyWriteOutcome, GraphPageRankOptions,
    GraphPageRankResult, GraphProperties, GraphPropertyDef, GraphService, GraphSsspOptions,
    GraphSsspResult, GraphSubgraphResult, GraphTargetStatus, GraphTraversalEdge, GraphTypeName,
    GraphWccResult, GraphWriteOutcome,
};
pub use json::{
    JsonBatchDeleteOutcome, JsonBatchSetItemOutcome, JsonBatchSetOutcome, JsonDeleteOutcome,
    JsonDocumentId, JsonGetEntry, JsonHistory, JsonHistoryRow, JsonIndexDefinition, JsonIndexName,
    JsonIndexType, JsonListPage, JsonPath, JsonPathSegment, JsonSample, JsonSampleRow, JsonService,
    JsonSetEntry, JsonValue, JsonVersionedValue, JsonWriteOutcome,
};
pub use kv::{
    KvBatchDeleteOutcome, KvBatchPutOutcome, KvDeleteOutcome, KvHistory, KvHistoryRow, KvKey,
    KvListPage, KvSample, KvScanRow, KvService, KvValue, KvVersionedValue, KvWriteOutcome,
    ProductSpace,
};
pub use options::{
    CacheOpenOptions, CachePreheat, DurabilityMode, DurableLocalOpenOptions, VersionRetention,
};
pub use space::{SpaceCreateOutcome, SpaceDeleteOutcome, SpaceService, SpaceUsageSummary};
pub use vector::{
    EmbeddingModelId, VectorArtifactSourceDiagnostic, VectorBatchDeleteOutcome,
    VectorBatchGetOutcome, VectorBatchUpsertOutcome, VectorBulkDeleteOutcome, VectorCollectionInfo,
    VectorCollectionName, VectorConfig, VectorDeleteOutcome, VectorDistanceMetric, VectorEmbedding,
    VectorEmbeddingUpdateOutcome, VectorEntry, VectorFilter, VectorFilterCondition, VectorFilterOp,
    VectorHistory, VectorHistoryRow, VectorIndexDiagnostics, VectorKey, VectorKeyPage,
    VectorMetadata, VectorMetadataPatch, VectorMetadataUpdateOutcome, VectorScalar,
    VectorSearchMatch, VectorSearchResult, VectorService, VectorUpsertEntry, VectorVersionedEntry,
    VectorWriteOutcome,
};

pub use crate::branch::{BranchName, BranchService};
pub use crate::commit::{CommitDurability, CommitOutcome};
pub use crate::diagnostics::{
    error_code_registry_entries, error_code_registry_entry, CommitOutcomeStatus, EngineError,
    EngineErrorClass, EngineErrorStatus, EngineResult, ErrorClass, ErrorCodeRegistryEntry,
    ErrorDetail, RetryPolicy,
};
