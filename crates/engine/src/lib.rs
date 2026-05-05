//! Database engine for Strata
//!
//! This crate orchestrates all lower layers:
//! - Database: Main database struct with open/close
//! - Branch lifecycle: begin_branch, end_branch, fork_branch (Epic 5)
//! - Transaction coordination
//! - Recovery integration
//! - Background tasks (snapshots, TTL cleanup)
//!
//! The engine is the only component that knows about:
//! - Branch management
//! - Cross-layer coordination (storage + WAL + recovery)
//! - Replay logic
//!
//! # Performance Instrumentation
//!
//! Enable the `perf-trace` feature for per-operation timing:
//!
//! ```bash
//! cargo build --features perf-trace
//! ```
//!
//! # API Stability
//!
//! `strata_engine` is a workspace-internal crate (`publish = false`). Application
//! code should use `strata_executor::Strata::open()` as the only supported entry
//! point. Direct use of `Database::open_runtime()` is for workspace-internal
//! components only.

pub mod background;
pub mod branch_domain;
mod coordinator;
pub mod database;
pub mod error;
pub mod instrumentation;
pub mod limits;
pub mod recovery;
pub mod semantics;
mod sensitive;
pub mod transaction;
pub mod transaction_ops; // TransactionOps Trait Definition

pub use background::{BackgroundScheduler, BackpressureError, SchedulerStats, TaskPriority};
pub use branch_domain::{
    is_system_branch, BranchControlRecord, BranchEventOffsets, BranchGeneration,
    BranchLifecycleStatus, BranchRef, CherryPickRecord, DagBranchInfo, DagBranchStatus, DagEventId,
    ForkAnchor, ForkRecord, MergeRecord, RevertRecord, BRANCH_DAG_GRAPH, SYSTEM_BRANCH,
};
pub use coordinator::TransactionMetrics;
pub use database::branch_service::{BranchService, ForkOptions, MergeOptions};
pub use database::profile::{
    apply_hardware_profile_if_defaults, apply_profile_if_defaults, detect_hardware, HardwareInfo,
    Profile,
};
pub use database::{
    AccessMode, BranchRetentionEntry, CacheMetrics, Database, DatabaseDiskUsage,
    DegradedPrimitiveEntry, ErrorRole, HealthReport, LossyErrorKind, LossyRecoveryReport,
    ModelConfig, OpenOptions, OrphanReason, OrphanStorageEntry, PrimitiveDegradationEntry,
    PrimitiveDegradationRegistry, PrimitiveDegradedEvent, PrimitiveDegradedObserver,
    PrimitiveDegradedObserverRegistry, ReclaimStatus, RecoveryError, RetentionBlocker,
    RetentionReport, RetentionTotals, StorageConfig, StorageMetricsSummary, StrataConfig,
    SubsystemHealth, SubsystemStatus, SystemMetrics, WalWriterHealth,
};
pub use error::{
    ConstraintReason, DetailValue, ErrorCode, ErrorDetails, PrimitiveDegradedReason, StrataError,
    StrataResult,
};
pub use instrumentation::PerfTrace;
pub use limits::{LimitError, Limits};
pub use recovery::Subsystem;
pub use semantics::event::ChainVerification;
pub use semantics::json::{
    apply_patches, delete_at_path, get_at_path, get_at_path_mut, merge_patch, set_at_path,
    JsonLimitError, JsonPatch, JsonPath, JsonPathError, JsonValue, PathParseError, PathSegment,
    MAX_ARRAY_SIZE, MAX_DOCUMENT_SIZE, MAX_NESTING_DEPTH, MAX_PATH_LENGTH,
};
pub use semantics::value::extractable_text;
pub use semantics::vector::{
    CollectionId, CollectionInfo, DistanceMetric, FilterCondition, FilterOp, JsonScalar,
    MetadataFilter, StorageDtype, VectorConfig, VectorEntry, VectorId, VectorMatch,
};
pub use sensitive::SensitiveString;
pub use strata_storage::durability::wal::DurabilityMode;
pub use strata_storage::durability::WalCounters;
pub use strata_storage::StorageIterator;
pub use strata_storage::VersionedEntry;
pub use strata_storage::{DegradationClass, RecoveryHealth, TransactionContext};
pub use transaction::{ScopedTransaction, Transaction, TransactionPool, MAX_POOL_SIZE};
pub use transaction_ops::TransactionOps;

mod branch_ops;
pub(crate) mod branch_retention;
pub mod bundle;
pub mod primitives;
pub mod recipe_store;
pub mod search;
pub mod system_space;

// Re-export search types at crate root for convenience
pub use search::{SearchBudget, SearchHit, SearchMode, SearchRequest, SearchResponse, SearchStats};

// Re-export branch ops types at crate root for convenience
pub use branch_ops::MaterializeInfo;

// Re-export search subsystem implementation
pub use search::SearchSubsystem;

// Re-export submodules for `strata_engine::extensions::*` access
pub use primitives::extensions;

// Re-export refresh types for secondary index subsystems and follower management
pub use database::refresh::{
    AdvanceError, BlockReason, BlockedTxn, FollowerStatus, NoopPreparedRefresh, PreparedRefresh,
    RefreshHook, RefreshHookError, RefreshHooks, RefreshOutcome, UnblockError,
};

// Re-export primitive types at crate root for convenience
pub use primitives::{
    build_search_response,
    build_search_response_with_index,
    build_search_response_with_scorer,
    BM25LiteScorer,
    // Handles
    BranchHandle,
    BranchMetadata,
    BranchStatus,
    BranchTransaction,
    Event,
    EventLog,
    EventLogExt,
    // Index
    InvertedIndex,
    JsonDoc,
    JsonStore,
    JsonStoreExt,
    // Primitives
    KVStore,
    // Extension traits
    KVStoreExt,
    PostingEntry,
    PostingList,
    Scorer,
    ScorerContext,
    SearchCandidate,
    SearchDoc,
    // Search & Scoring
    Searchable,
    SimpleScorer,
    SpaceIndex,
};

// Re-export bundle types at crate root
pub use bundle::{BundleInfo, ExportInfo, ImportInfo};

// Re-export branch_ops types at crate root
pub use branch_ops::{
    BranchDiffEntry, BranchDiffResult, CherryPickFilter, CherryPickInfo, ConflictEntry, DiffFilter,
    DiffOptions, DiffSummary, ForkInfo, MergeAction, MergeActionKind, MergeBase, MergeBaseInfo,
    MergeInfo, MergeStrategy, NoteInfo, RevertInfo, SpaceDiff, TagInfo, ThreeWayChange,
    ThreeWayDiffEntry, ThreeWayDiffResult, TypedEntries, TypedEntryCell,
};

// Graph and vector merge handler types. Registration happens via per-database
// `MergeHandlerRegistry` during subsystem `initialize()`.
pub use branch_ops::primitive_merge::{
    GraphMergePlanFn, MergePlanCtx, PrimitiveMergePlan, VectorMergePostCommitFn,
    VectorMergePrecheckFn,
};

#[cfg(feature = "perf-trace")]
pub use instrumentation::{PerfBreakdown, PerfStats};
