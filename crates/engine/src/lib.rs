//! Executor-facing engine contract built over the storage persistence boundary.

#![deny(unsafe_code)]
// `EngineResult<T>` is the stable product API shape; boxing `EngineError`
// would be a deliberate contract change rather than a local lint cleanup.
#![allow(clippy::result_large_err)]

pub mod api;
pub mod artifact;

mod branch;
mod commit;
mod config;
mod control;
mod data;
mod diagnostics;
mod persistence;
mod runtime;
mod time_compat;

#[cfg(any(test, feature = "testkit"))]
pub mod test_support;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

/// Core value types that appear in this crate's public API.
///
/// A write acknowledgement hands back a [`CommitVersion`] and a [`Timestamp`];
/// a branch resolves to a [`BranchId`]. They are defined in `strata-core`, and
/// are re-exported here so a caller can name the type of their own result
/// without taking a second dependency (#3190).
pub use strata_core::{
    BranchId, BranchIdError, CommitVersion, ParseCommitVersionError, ParseTimestampError, Timestamp,
};

pub use api::{
    error_code_registry_entries, error_code_registry_entry, AdminCapabilitySummary,
    AdminConfigSummary, AdminDatabaseInfo, AdminDescribeSummary, AdminGraphSummary,
    AdminHealthStatus, AdminHealthSummary, AdminMetricsSummary, AdminPingSummary,
    AdminPrimitiveSummary, AdminService, AdminVectorCollectionSummary, BranchCleanupSummary,
    BranchComparison, BranchCreateOutcome, BranchDeleteOutcome, BranchMergeSummary, BranchName,
    BranchParentSummary, BranchPreview, BranchService, BranchStateSelector, BranchStatus,
    BranchSummary, CacheOpenOptions, CachePreheat, CloseOutcome, CommitDurability,
    CommitOutcomeStatus, ComparedCapability, ComparedEntity, ConflictKind, ConflictStrategyResult,
    ControlDiagnostics, ControlHealthStatus, Database, DatabaseOpenOutcome, DatabaseOpenSummary,
    DatabaseOpenTarget, DerivedStateDisposition, DerivedStateReport, DurabilityMode,
    DurableLocalOpenOptions, EmbeddingModelId, EngineError, EngineErrorClass, EngineErrorStatus,
    EngineResult, ErrorClass, ErrorCodeRegistryEntry, ErrorDetail, EventAppendOutcome,
    EventBatchAppendEntry, EventBatchAppendItemOutcome, EventBatchAppendOutcome,
    EventChainVerification, EventLength, EventPayload, EventRangeDirection, EventRangePage,
    EventSequence, EventService, EventType, EventTypeList, EventVersionedRecord,
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
    GraphPageRankResult, GraphProperties, GraphPropertyDef, GraphService, GraphSsspResult,
    GraphSubgraphResult, GraphTargetStatus, GraphTraversalEdge, GraphTypeName, GraphWccResult,
    GraphWriteOutcome, JsonBatchDeleteOutcome, JsonBatchSetItemOutcome, JsonBatchSetOutcome,
    JsonDeleteOutcome, JsonDocumentId, JsonGetEntry, JsonHistory, JsonHistoryRow,
    JsonIndexDefinition, JsonIndexName, JsonIndexType, JsonListPage, JsonPath, JsonPathSegment,
    JsonSample, JsonSampleRow, JsonService, JsonSetEntry, JsonValue, JsonVersionedValue,
    JsonWriteOutcome, KvBatchDeleteOutcome, KvBatchPutOutcome, KvDeleteOutcome, KvHistory,
    KvHistoryRow, KvKey, KvListPage, KvSample, KvScanRow, KvService, KvValue, KvVersionedValue,
    KvWriteOutcome, MemoryBudgetSource, PreviewConflict, ProductSpace, PromotedEntity,
    PromotionOutcome, PromotionStrategy, RetryPolicy, SpaceCatalogDiagnostics, SpaceComparison,
    SpaceCreateOutcome, SpaceDeleteOutcome, SpaceService, SpaceUsageSummary,
    VectorArtifactSourceDiagnostic, VectorBatchDeleteOutcome, VectorBatchGetOutcome,
    VectorBatchUpsertOutcome, VectorBulkDeleteOutcome, VectorCollectionInfo, VectorCollectionName,
    VectorConfig, VectorDeleteOutcome, VectorDistanceMetric, VectorEmbedding,
    VectorEmbeddingUpdateOutcome, VectorEntry, VectorFilter, VectorFilterCondition, VectorFilterOp,
    VectorHistory, VectorHistoryRow, VectorIndexDiagnostics, VectorKey, VectorKeyPage,
    VectorMetadata, VectorMetadataPatch, VectorMetadataUpdateOutcome, VectorScalar,
    VectorSearchMatch, VectorSearchResult, VectorService, VectorUpsertEntry, VectorVersionedEntry,
    VectorWriteOutcome, VersionRetention,
};

#[cfg(test)]
mod mutation_gate_error_value {
    //! Mirrors the `error_values` entry the mutation gate substitutes for
    //! `Err(..)` in this crate (#3258). If the expression stops compiling,
    //! the lane entry has gone silently inert — every `Result` fn in the
    //! crate reverts to unviable-only body mutants; if the strings drift,
    //! the entry no longer names this expression.

    #[test]
    fn configured_expression_is_constructible_and_verbatim() {
        drop(crate::EngineError::new(
            "internal.engine.persistence",
            "mutated",
        ));
        let declared = |expression: &str| {
            include_str!("../../../.cargo/mutants.toml")
                .lines()
                .map(str::trim)
                .filter(|line| !line.starts_with('#'))
                .any(|line| line.contains(expression))
        };
        assert!(
            declared(r#"crate::EngineError::new("internal.engine.persistence", "mutated")"#),
            "lane A's error_values must carry this mirror verbatim, uncommented"
        );
    }
}
