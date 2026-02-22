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

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod background;
pub mod coordinator;
pub mod database;
pub mod instrumentation;
pub mod recovery;
pub mod transaction;
pub mod transaction_ops; // TransactionOps Trait Definition

pub use background::{BackgroundScheduler, BackpressureError, SchedulerStats, TaskPriority};
pub use coordinator::{TransactionCoordinator, TransactionMetrics};
pub use database::{Database, ModelConfig, RetryConfig, StrataConfig};
pub use instrumentation::PerfTrace;
pub use recovery::{
    diff_views, recover_all_participants, register_recovery_participant, BranchDiff, BranchError,
    DiffEntry, ReadOnlyView, RecoveryFn, RecoveryParticipant, ReplayBranchIndex, ReplayError,
};
pub use strata_durability::wal::DurabilityMode;
pub use strata_durability::WalCounters;
// Note: Use strata_core::PrimitiveType for DiffEntry.primitive field
pub use strata_concurrency::TransactionContext;
pub use transaction::{Transaction, TransactionPool, MAX_POOL_SIZE};
pub use transaction_ops::TransactionOps;

pub mod branch_ops;
pub mod bundle;
pub mod graph;
pub mod primitives;
pub mod search;

// Re-export search types at crate root for convenience
pub use search::{SearchBudget, SearchHit, SearchMode, SearchRequest, SearchResponse, SearchStats};

// Re-export search recovery registration
pub use search::register_search_recovery;

// Re-export submodules for `strata_engine::vector::*` and `strata_engine::extensions::*` access
pub use primitives::extensions;
pub use primitives::vector;

// Re-export primitive types at crate root for convenience
pub use primitives::{
    build_search_response,
    build_search_response_with_index,
    build_search_response_with_scorer,
    // Recovery
    register_vector_recovery,
    validate_collection_name,
    validate_vector_key,
    BM25LiteScorer,
    // Handles
    BranchHandle,
    BranchIndex,
    BranchMetadata,
    BranchStatus,
    BruteForceBackend,
    CollectionId,
    CollectionInfo,
    CollectionRecord,
    DistanceMetric,
    Event,
    EventHandle,
    EventLog,
    EventLogExt,
    FilterCondition,
    FilterOp,
    HnswBackend,
    HnswConfig,
    IndexBackendFactory,
    // Index
    InvertedIndex,
    JsonDoc,
    JsonHandle,
    JsonScalar,
    JsonStore,
    JsonStoreExt,
    // Primitives
    KVStore,
    // Extension traits
    KVStoreExt,
    KvHandle,
    MetadataFilter,
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
    State,
    StateCell,
    StateCellExt,
    StateHandle,
    StorageDtype,
    VectorBackendState,
    // Vector types
    VectorConfig,
    VectorConfigSerde,
    VectorEntry,
    VectorError,
    VectorHeap,
    VectorId,
    VectorIndexBackend,
    VectorMatch,
    VectorMatchWithSource,
    VectorRecord,
    VectorResult,
    VectorStore,
    VectorStoreExt,
};

// Re-export graph types at crate root
pub use graph::boost::{apply_boost, compute_proximity_map, GraphBoost};
pub use graph::types::{
    BfsOptions, BfsResult, CascadePolicy, Direction, Edge, EdgeData, GraphAlgorithm, GraphMeta,
    GraphSnapshot, Neighbor, NodeData,
};
pub use graph::GraphStore;

// Re-export bundle types at crate root
pub use bundle::{BundleInfo, ExportInfo, ImportInfo};

// Re-export branch_ops types at crate root
pub use branch_ops::{
    BranchDiffEntry, BranchDiffResult, ConflictEntry, DiffSummary, ForkInfo, MergeInfo,
    MergeStrategy, SpaceDiff,
};

#[cfg(feature = "perf-trace")]
pub use instrumentation::{PerfBreakdown, PerfStats};
