//! Strata: an embedded, branchable, time-traveling database.
//!
//! `stratadb` is the embedded-library facade and the only published Rust
//! surface. (`strata-engine` beneath it is internal; the command-level surface
//! is the `strata` binary from `strata-cli`.)
//!
//! # A first write
//!
//! ```
//! use stratadb::prelude::*;
//!
//! let database = Database::open_cache(CacheOpenOptions::new())?.into_database();
//! let mut kv = database.kv(BranchName::new("default")?, ProductSpace::new("default")?)?;
//! kv.put(KvKey::new("greeting")?, KvValue::new("hello"))?;
//! assert!(kv.get(&KvKey::new("greeting")?)?.is_some());
//! # Ok::<(), stratadb::EngineError>(())
//! ```
//!
//! # Reading the past
//!
//! Every write acknowledges a commit. Hand its timestamp back to `get_at` to
//! read the database as it was at that point.
//!
//! ```
//! use stratadb::prelude::*;
//!
//! let database = Database::open_cache(CacheOpenOptions::new())?.into_database();
//! let mut kv = database.kv(BranchName::new("default")?, ProductSpace::new("default")?)?;
//!
//! let first = kv.put(KvKey::new("greeting")?, KvValue::new("hello"))?;
//! kv.put(KvKey::new("greeting")?, KvValue::new("goodbye"))?;
//!
//! let then = kv.get_at(&KvKey::new("greeting")?, first.commit().timestamp())?;
//! assert_eq!(then.expect("present then").as_bytes(), b"hello");
//! assert_eq!(kv.get(&KvKey::new("greeting")?)?.expect("present now").as_bytes(), b"goodbye");
//! # Ok::<(), stratadb::EngineError>(())
//! ```
//!
//! # Branching
//!
//! A branch is a cheap fork of the whole database — from now, or from any
//! commit version a write handed back.
//!
//! ```
//! use stratadb::prelude::*;
//!
//! let mut database = Database::open_cache(CacheOpenOptions::new())?.into_database();
//! let (default, space) = (BranchName::new("default")?, ProductSpace::new("default")?);
//!
//! let seed = database
//!     .kv(default.clone(), space)?
//!     .put(KvKey::new("k")?, KvValue::new("v"))?;
//!
//! database.branches()?.fork_current(&default, BranchName::new("experiment")?)?;
//! database.branches()?.fork_at_version(
//!     &default,
//!     BranchName::new("at-seed")?,
//!     seed.commit().version(),
//! )?;
//! # Ok::<(), stratadb::EngineError>(())
//! ```
//!
//! The whole branch verb set on [`branch::BranchService`], named once so none of
//! it stays hidden behind the CLI's own vocabulary. A test pins this list
//! against the service, so a verb cannot be added or removed without it:
//!
//! ```text
//! list, get, create, fork_current, fork_at_version, fork_at_timestamp,
//! compare, preview, promote, delete
//! ```
//!
//! `create` makes an empty branch; the `fork_*` verbs branch from an existing
//! one, at its head or at a given point.
//!
//! `fork_at_version` takes a [`CommitVersion`] and `fork_at_timestamp` a
//! [`Timestamp`] — both come off the commit a write acknowledged.
//!
//! `promote` carries KV, JSON, and vector rows. It does **not** carry events
//! (promotion would break the sequence and hash chain) or graph rows, though
//! `compare` still reports differences in both, so a comparison is not a dry
//! run of a promotion.
//!
//! # Several capabilities at once
//!
//! Capability services come from a shared borrow, so they compose.
//!
//! ```
//! use stratadb::prelude::*;
//!
//! let database = Database::open_cache(CacheOpenOptions::new())?.into_database();
//! let (branch, space) = (BranchName::new("default")?, ProductSpace::new("default")?);
//!
//! let mut kv = database.kv(branch.clone(), space.clone())?;
//! let mut json = database.json(branch.clone(), space.clone())?;
//! let mut events = database.event(branch, space)?;
//! # let _ = (&mut kv, &mut json, &mut events);
//! # Ok::<(), stratadb::EngineError>(())
//! ```
//!
//! # How this surface is organised
//!
//! The root holds what nearly every program touches: [`Database`], the open
//! options, the error types, and [`BranchName`] / [`ProductSpace`]. Everything
//! else is namespaced by the capability it belongs to — [`kv`], [`json`],
//! [`event`], [`vector`], [`graph`], [`branch`], [`space`], [`admin`], and
//! [`artifact`] — so a reader can find a type by the thing it is about rather
//! than scanning one list of two hundred names.
//!
//! [`prelude`] re-exports the handful needed for a first write.
//!
//! Re-exports are explicit rather than glob, so this file is a readable
//! inventory of what is published (#3140), and adding a public type to the
//! engine does not silently publish it.

// `missing_docs` and `unreachable_pub` come from `[workspace.lints]`, which is
// the single source of truth for lint config; only the two attributes the
// workspace does not carry are set here.
#![deny(unsafe_code)]
// `EngineResult<T>` is the product's error shape; boxing it would be a
// contract change rather than a lint fix. Matches the engine's own allow.
#![allow(clippy::result_large_err)]

// Core value types — what the API HANDS BACK. A commit version from a write
// ack, a timestamp for `get_at`. Previously reachable through no path at all,
// so a caller could not name the type of their own result (#3190).
pub use strata_engine::{
    BranchId, BranchIdError, CommitVersion, ParseCommitVersionError, ParseTimestampError, Timestamp,
};

// Opening a database, closing it, and the errors every call can return.
pub use strata_engine::{
    error_code_registry_entries, error_code_registry_entry, BranchName, CacheOpenOptions,
    CachePreheat, CloseOutcome, CommitDurability, CommitOutcomeStatus, Database,
    DatabaseOpenOutcome, DatabaseOpenSummary, DatabaseOpenTarget, DurabilityMode,
    DurableLocalOpenOptions, EngineError, EngineErrorClass, EngineErrorStatus, EngineResult,
    ErrorClass, ErrorCodeRegistryEntry, ErrorDetail, MemoryBudgetSource, ProductSpace, RetryPolicy,
    VersionRetention,
};

/// The types a first program needs, in one import.
///
/// ```
/// use stratadb::prelude::*;
/// ```
///
/// Deliberately small: opening a database, naming a branch and space, and
/// reading and writing KV. Reach into [`crate::json`], [`crate::graph`] and
/// the rest for the other capabilities.
pub mod prelude {
    pub use crate::kv::{KvKey, KvValue};
    pub use crate::{
        BranchName, CacheOpenOptions, Database, DurableLocalOpenOptions, EngineError, EngineResult,
        ProductSpace,
    };
}

/// Key-value capability.
///
/// Byte keys and values with full version history.
pub mod kv {
    pub use strata_engine::{
        KvBatchDeleteOutcome, KvBatchPutOutcome, KvDeleteOutcome, KvHistory, KvHistoryRow, KvKey,
        KvListPage, KvSample, KvScanRow, KvService, KvValue, KvVersionedValue, KvWriteOutcome,
    };
}

/// JSON document capability.
///
/// Documents addressed by id and JSON path, with indexes.
pub mod json {
    pub use strata_engine::{
        JsonBatchDeleteOutcome, JsonBatchSetItemOutcome, JsonBatchSetOutcome, JsonDeleteOutcome,
        JsonDocumentId, JsonGetEntry, JsonHistory, JsonHistoryRow, JsonIndexDefinition,
        JsonIndexName, JsonIndexType, JsonListPage, JsonPath, JsonPathSegment, JsonSample,
        JsonSampleRow, JsonService, JsonSetEntry, JsonValue, JsonVersionedValue, JsonWriteOutcome,
    };
}

/// Event log capability.
///
/// Append-only, hash-chained event streams.
pub mod event {
    pub use strata_engine::{
        EventAppendOutcome, EventBatchAppendEntry, EventBatchAppendItemOutcome,
        EventBatchAppendOutcome, EventChainVerification, EventLength, EventPayload,
        EventRangeDirection, EventRangePage, EventSequence, EventService, EventType, EventTypeList,
        EventVersionedRecord,
    };
}

/// Vector capability.
///
/// Embeddings, collections, and similarity search.
pub mod vector {
    pub use strata_engine::{
        EmbeddingModelId, VectorArtifactSourceDiagnostic, VectorBatchDeleteOutcome,
        VectorBatchGetOutcome, VectorBatchUpsertOutcome, VectorBulkDeleteOutcome,
        VectorCollectionInfo, VectorCollectionName, VectorConfig, VectorDeleteOutcome,
        VectorDistanceMetric, VectorEmbedding, VectorEmbeddingUpdateOutcome, VectorEntry,
        VectorFilter, VectorFilterCondition, VectorFilterOp, VectorHistory, VectorHistoryRow,
        VectorIndexDiagnostics, VectorKey, VectorKeyPage, VectorMetadata, VectorMetadataPatch,
        VectorMetadataUpdateOutcome, VectorScalar, VectorSearchMatch, VectorSearchResult,
        VectorService, VectorUpsertEntry, VectorVersionedEntry, VectorWriteOutcome,
    };
}

/// Graph capability.
///
/// Nodes, edges, traversal, and analytics.
pub mod graph {
    pub use strata_engine::{
        GraphAdjacencyEdge, GraphAdjacencyIndex, GraphAnalyticsBudget, GraphBatchOpOutcome,
        GraphBatchOperation, GraphBatchWrite, GraphBatchWriteOutcome, GraphBfsOptions,
        GraphBfsResult, GraphBinding, GraphBindingPage, GraphBindingPrimitive, GraphBindingTarget,
        GraphBulkInsertOutcome, GraphCdlpOptions, GraphCdlpResult, GraphDeleteOutcome,
        GraphDeletePolicy, GraphDeletePolicyOutcome, GraphDirection, GraphEdge, GraphEdgeData,
        GraphEdgePage, GraphEdgeType, GraphEdgeWriteOutcome, GraphEntityBinding, GraphInfo,
        GraphLccResult, GraphLinkTypeDef, GraphLinkTypeSummary, GraphName, GraphNamePage,
        GraphNeighbor, GraphNeighborPage, GraphNode, GraphNodeData, GraphNodeId, GraphNodePage,
        GraphObjectTypeDef, GraphObjectTypeSummary, GraphOntology, GraphOntologyFreezeOutcome,
        GraphOntologyStatus, GraphOntologySummary, GraphOntologyWriteOutcome, GraphPageRankOptions,
        GraphPageRankResult, GraphProperties, GraphPropertyDef, GraphService, GraphSsspResult,
        GraphSubgraphResult, GraphTargetStatus, GraphTraversalEdge, GraphTypeName, GraphWccResult,
        GraphWriteOutcome,
    };
}

/// Branching.
///
/// Fork, compare, preview, and promote whole databases.
pub mod branch {
    pub use strata_engine::{
        BranchCleanupSummary, BranchComparison, BranchCreateOutcome, BranchDeleteOutcome,
        BranchMergeSummary, BranchParentSummary, BranchPreview, BranchService, BranchStateSelector,
        BranchStatus, BranchSummary, ComparedCapability, ComparedEntity, ConflictKind,
        ConflictStrategyResult, DerivedStateDisposition, DerivedStateReport, PreviewConflict,
        PromotedEntity, PromotionOutcome, PromotionStrategy,
    };
}

/// Product spaces.
///
/// Named namespaces within a branch.
pub mod space {
    pub use strata_engine::{
        SpaceCatalogDiagnostics, SpaceComparison, SpaceCreateOutcome, SpaceDeleteOutcome,
        SpaceService, SpaceUsageSummary,
    };
}

/// Administration and introspection.
///
/// Health, metrics, describe, and config.
pub mod admin {
    pub use strata_engine::{
        AdminCapabilitySummary, AdminConfigSummary, AdminDatabaseInfo, AdminDescribeSummary,
        AdminGraphSummary, AdminHealthStatus, AdminHealthSummary, AdminMetricsSummary,
        AdminPingSummary, AdminPrimitiveSummary, AdminService, AdminVectorCollectionSummary,
        ControlDiagnostics, ControlHealthStatus,
    };
}

/// Clone artifacts.
///
/// Export a branch to a portable artifact and import it elsewhere.
pub mod artifact {
    pub use strata_engine::artifact::{
        decode_section, export_branch, import_branch, import_branches, ArtifactModel,
        ArtifactRecord, ArtifactRecordIter, ArtifactSection, BranchArtifact, BranchImportSummary,
        RemoteOrigin, RemoteOriginFrontierEntry, ARTIFACT_FORMAT_VERSION,
    };
}
