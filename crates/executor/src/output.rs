//! Output enum for command execution results.
//!
//! Every command produces exactly one output type. This mapping is deterministic:
//! the same command always produces the same output variant (though the values
//! may differ based on database state).

use serde::{Deserialize, Serialize};
use strata_core::Value;
use strata_engine::branch_ops::{BranchDiffResult, ForkInfo, MergeInfo};
use strata_engine::{HealthReport, StrataConfig, SystemMetrics, WalCounters};

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

    // ==================== Write Results ====================
    /// Write acknowledgment — echoes key + version
    WriteResult {
        /// Key or cell that was written.
        key: String,
        /// Version number assigned to the write.
        version: u64,
    },

    /// Delete acknowledgment — echoes key + whether it existed
    DeleteResult {
        /// Key or cell that was deleted.
        key: String,
        /// Whether the key existed before deletion.
        deleted: bool,
    },

    /// Event append acknowledgment — echoes event type + sequence
    EventAppendResult {
        /// Sequence number assigned to the appended event.
        sequence: u64,
        /// Event type that was appended.
        event_type: String,
    },

    /// Vector write acknowledgment — echoes collection + key + version
    VectorWriteResult {
        /// Collection name.
        collection: String,
        /// Key that was written.
        key: String,
        /// Version number assigned to the write.
        version: u64,
    },

    /// Vector delete acknowledgment — echoes collection + key + existed
    VectorDeleteResult {
        /// Collection name.
        collection: String,
        /// Key that was deleted.
        key: String,
        /// Whether the key existed before deletion.
        deleted: bool,
    },

    // ==================== Scan Results ====================
    /// Paginated key list with has_more indicator
    KeysPage {
        /// Keys in this page.
        keys: Vec<String>,
        /// Whether more results exist beyond this page.
        has_more: bool,
        /// Cursor for fetching the next page.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },

    /// JSON list result with cursor
    JsonListResult {
        /// Matching document keys.
        keys: Vec<String>,
        /// Whether more results exist beyond this page.
        has_more: bool,
        /// Cursor for fetching the next page, if more results exist.
        #[serde(default, skip_serializing_if = "Option::is_none")]
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

    /// Configuration set acknowledgment — echoes key + effective value.
    ConfigSetResult {
        /// Configuration key that was set.
        key: String,
        /// Effective new value (normalized, e.g. "miniLM" not "minilm").
        new_value: String,
    },

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

    /// Database introspection snapshot
    Described(DescribeResult),

    /// Ping response
    Pong {
        /// Database engine version string.
        version: String,
    },

    /// Health check report with per-subsystem status.
    Health(HealthReport),

    /// Unified database metrics snapshot.
    Metrics(SystemMetrics),

    // ==================== Intelligence ====================
    /// Search results across primitives
    SearchResults {
        /// Ranked search hits.
        hits: Vec<SearchResultHit>,
        /// Execution statistics.
        stats: SearchStatsOutput,
    },

    /// Sample of entries from a primitive (keys + values for shape discovery)
    SampleResult {
        /// Total number of entries in the source.
        total_count: u64,
        /// Sampled items.
        items: Vec<SampleItem>,
    },

    /// Graph node write result (add_node)
    GraphWriteResult {
        /// Node ID that was written.
        node_id: String,
        /// Whether a new node was created (false = updated existing).
        created: bool,
    },

    /// Graph edge write result (add_edge)
    GraphEdgeWriteResult {
        /// Source node ID.
        src: String,
        /// Destination node ID.
        dst: String,
        /// Edge type label.
        edge_type: String,
        /// Whether a new edge was created (false = updated existing).
        created: bool,
    },

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

    /// Result of re-indexing all embeddings
    ReindexResult {
        /// KV entries queued for re-embedding
        kv_queued: u64,
        /// JSON documents queued for re-embedding
        json_queued: u64,
        /// Events queued for re-embedding
        event_queued: u64,
        /// New embedding dimension
        new_dimension: usize,
    },

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

    /// Graph grouping analytics summary (WCC, CDLP).
    GraphGroupSummary(GraphGroupSummary),

    /// Graph score-based analytics summary (PageRank, LCC, SSSP).
    GraphScoreSummary(GraphScoreSummary),

    /// Graph paginated result (items + optional cursor).
    GraphPage {
        /// Items in this page.
        items: Vec<String>,
        /// Cursor for the next page (None = last page).
        next_cursor: Option<String>,
    },

    /// Data export result (CSV/JSON/JSONL).
    Exported(ExportResult),
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
