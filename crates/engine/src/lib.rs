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
mod coordinator;
pub mod database;
pub mod instrumentation;
pub mod recovery;
pub mod transaction;
pub mod transaction_ops; // TransactionOps Trait Definition

pub use background::{BackgroundScheduler, BackpressureError, SchedulerStats, TaskPriority};
pub use coordinator::TransactionMetrics;
pub use database::branch_service::{BranchService, ForkOptions, MergeOptions};
pub use database::profile::{
    apply_hardware_profile_if_defaults, apply_profile_if_defaults, detect_hardware, HardwareInfo,
    Profile,
};
pub use database::{
    CacheMetrics, Database, DatabaseDiskUsage, HealthReport, ModelConfig, StorageConfig,
    StorageMetricsSummary, StrataConfig, SubsystemHealth, SubsystemStatus, SystemMetrics,
    WalWriterHealth,
};
pub use instrumentation::PerfTrace;
pub use recovery::Subsystem;
pub use strata_concurrency::TransactionContext;
pub use strata_durability::wal::DurabilityMode;
pub use strata_durability::WalCounters;
pub use strata_storage::StorageIterator;
pub use strata_storage::VersionedEntry;
pub use transaction::{ScopedTransaction, Transaction, TransactionPool, MAX_POOL_SIZE};
pub use transaction_ops::TransactionOps;

mod branch_ops;
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

// Re-export branch_dag types from core at crate root for convenience
pub use strata_core::branch_dag::{
    is_system_branch, DagBranchInfo, DagBranchStatus, DagEventId, ForkRecord, MergeRecord,
    BRANCH_DAG_GRAPH, SYSTEM_BRANCH,
};

#[cfg(feature = "perf-trace")]
pub use instrumentation::{PerfBreakdown, PerfStats};
