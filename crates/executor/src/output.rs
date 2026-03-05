//! Output enum for command execution results.
//!
//! Every command produces exactly one output type. This mapping is deterministic:
//! the same command always produces the same output variant (though the values
//! may differ based on database state).

use serde::{Deserialize, Serialize};
use strata_core::Value;
use strata_engine::branch_ops::{BranchDiffResult, ForkInfo, MergeInfo};
use strata_engine::{StrataConfig, WalCounters};

use crate::types::*;

/// Successful command execution results.
///
/// Each [`Command`](crate::Command) variant maps to exactly one `Output` variant.
/// This mapping is deterministic and documented in the command definitions.
///
/// # Example
///
/// ```text
/// use strata_executor::{Command, Output, Executor};
///
/// let result = executor.execute(Command::KvGet { branch, key })?;
///
/// match result {
///     Output::Maybe(Some(v)) => println!("Found: {:?}", v),
///     Output::Maybe(None) => println!("Not found"),
///     _ => unreachable!("KvGet always returns Maybe"),
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Output {
    // ==================== Primitive Results ====================
    /// No return value (delete, flush, compact)
    Unit,

    /// Optional value (for get operations that may not find a key)
    Maybe(Option<Value>),

    /// Optional versioned value (most common for get operations)
    MaybeVersioned(Option<VersionedValue>),

    /// Optional version number (for CAS operations)
    MaybeVersion(Option<u64>),

    /// Version number
    Version(u64),

    /// Boolean result
    Bool(bool),

    /// Unsigned integer result (for count operations)
    Uint(u64),

    // ==================== Collections ====================
    /// List of versioned values (history operations)
    VersionedValues(Vec<VersionedValue>),

    /// Version history result (getv/readv operations).
    /// None if the key/cell/document doesn't exist.
    VersionHistory(Option<Vec<VersionedValue>>),

    /// List of keys
    Keys(Vec<String>),

    // ==================== Scan Results ====================
    /// JSON list result with cursor
    JsonListResult {
        /// Matching document keys.
        keys: Vec<String>,
        /// Cursor for fetching the next page, if more results exist.
        cursor: Option<String>,
    },

    // ==================== Search Results ====================
    /// Vector search matches
    VectorMatches(Vec<VectorMatch>),

    // ==================== Vector-specific ====================
    /// Single vector data
    VectorData(Option<VersionedVectorData>),

    /// List of vector collections
    VectorCollectionList(Vec<CollectionInfo>),

    /// Multiple version numbers (for batch operations)
    Versions(Vec<u64>),

    /// Per-item results for batch operations (positionally maps to input entries)
    BatchResults(Vec<BatchItemResult>),

    /// Per-item results for batch get operations (includes values)
    BatchGetResults(Vec<BatchGetItemResult>),

    // ==================== Branch-specific ====================
    /// Optional versioned branch info (for branch_get which may not find a branch)
    MaybeBranchInfo(Option<VersionedBranchInfo>),

    /// List of versioned branch infos
    BranchInfoList(Vec<VersionedBranchInfo>),

    /// Branch creation result (info + version)
    BranchWithVersion {
        /// Newly created branch metadata.
        info: BranchInfo,
        /// Version number assigned to the creation event.
        version: u64,
    },

    /// Branch fork result
    BranchForked(ForkInfo),

    /// Branch diff result
    BranchDiff(BranchDiffResult),

    /// Branch merge result
    BranchMerged(MergeInfo),

    /// Database configuration snapshot
    Config(StrataConfig),

    /// Single configuration value (for ConfigureGetKey).
    /// None if the key is not set.
    ConfigValue(Option<String>),

    /// WAL durability counters
    DurabilityCounters(WalCounters),

    // ==================== Transaction-specific ====================
    /// Transaction info
    TxnInfo(Option<TransactionInfo>),

    /// Transaction successfully begun
    TxnBegun,

    /// Transaction committed with version
    TxnCommitted {
        /// Commit version number.
        version: u64,
    },

    /// Transaction aborted
    TxnAborted,

    // ==================== Database-specific ====================
    /// Database info
    DatabaseInfo(DatabaseInfo),

    /// Ping response
    Pong {
        /// Database engine version string.
        version: String,
    },

    // ==================== Intelligence ====================
    /// Search results across primitives
    SearchResults(Vec<SearchResultHit>),

    // ==================== Space ====================
    /// List of space names
    SpaceList(Vec<String>),

    // ==================== Bundle ====================
    /// Branch export result
    BranchExported(BranchExportResult),

    /// Branch import result
    BranchImported(BranchImportResult),

    /// Bundle validation result
    BundleValidated(BundleValidateResult),

    /// Time range for a branch (oldest and latest timestamps in microseconds since epoch)
    TimeRange {
        /// Oldest timestamp, or None if branch has no data.
        oldest_ts: Option<u64>,
        /// Latest timestamp, or None if branch has no data.
        latest_ts: Option<u64>,
    },

    /// Embedding pipeline status
    EmbedStatus(EmbedStatusInfo),

    /// Single embedding vector
    Embedding(Vec<f32>),

    /// Batch of embedding vectors
    Embeddings(Vec<Vec<f32>>),

    /// List of available models
    ModelsList(Vec<ModelInfoOutput>),

    /// Text generation result
    Generated(GenerationResult),

    /// Tokenization result (text → token IDs)
    TokenIds(TokenizeResult),

    /// Plain text result (detokenization)
    Text(String),

    /// Graph neighbor query results
    GraphNeighbors(Vec<GraphNeighborHit>),

    /// Graph BFS traversal results
    GraphBfs(GraphBfsResult),

    /// Graph bulk insert result
    GraphBulkInsertResult {
        /// Number of nodes inserted.
        nodes_inserted: u64,
        /// Number of edges inserted.
        edges_inserted: u64,
    },

    /// Model successfully pulled/downloaded
    ModelsPulled {
        /// Name of the model that was pulled.
        name: String,
        /// Local file path where the model was saved.
        path: String,
    },

    /// Graph analytics result with u64 values (WCC, CDLP).
    GraphAnalyticsU64(GraphAnalyticsU64Result),

    /// Graph analytics result with f64 values (PageRank, LCC, SSSP).
    GraphAnalyticsF64(GraphAnalyticsF64Result),

    /// Graph paginated result (items + optional cursor).
    GraphPage {
        /// Items in this page.
        items: Vec<String>,
        /// Cursor for the next page (None = last page).
        next_cursor: Option<String>,
    },
}

/// Snapshot of the embedding pipeline status.
///
/// Returned by [`Command::EmbedStatus`](crate::Command::EmbedStatus).
/// Users can derive:
/// - **Progress:** `total_embedded / total_queued`
/// - **In-flight:** `total_queued - total_embedded - total_failed`
/// - **Is idle:** `pending == 0 && scheduler_active_tasks == 0 && scheduler_queue_depth == 0`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbedStatusInfo {
    /// Whether auto-embedding is currently enabled.
    pub auto_embed: bool,
    /// Configured batch size for embedding.
    pub batch_size: usize,
    /// Number of items currently waiting in the buffer.
    pub pending: usize,
    /// Cumulative count of items pushed into the buffer.
    pub total_queued: u64,
    /// Cumulative count of items successfully embedded.
    pub total_embedded: u64,
    /// Cumulative count of items that failed embedding.
    pub total_failed: u64,
    /// Number of tasks waiting in the scheduler queue.
    pub scheduler_queue_depth: usize,
    /// Number of tasks currently being executed by scheduler workers.
    pub scheduler_active_tasks: usize,
}
