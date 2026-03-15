//! Supporting types for commands and outputs.
//!
//! These types are used in command parameters and output values.
//! All types are serializable for cross-language use.

use serde::{Deserialize, Serialize};
use strata_core::Value;

// =============================================================================
// Branch Types
// =============================================================================

/// Branch identifier.
///
/// Can be "default" for the default branch, or a UUID string for named branches.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchId(
    /// The underlying string identifier.
    pub String,
);

impl Default for BranchId {
    fn default() -> Self {
        BranchId("default".to_string())
    }
}

impl BranchId {
    /// Check if this is the default branch.
    pub fn is_default(&self) -> bool {
        self.0 == "default"
    }

    /// Get the string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for BranchId {
    fn from(s: String) -> Self {
        BranchId(s)
    }
}

impl From<&str> for BranchId {
    fn from(s: &str) -> Self {
        BranchId(s.to_string())
    }
}

impl std::fmt::Display for BranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Branch status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    /// Branch is active and accepting reads/writes.
    Active,
}

/// Branch information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchInfo {
    /// Unique branch identifier.
    pub id: BranchId,
    /// Current branch status.
    pub status: BranchStatus,
    /// Unix timestamp when the branch was created.
    pub created_at: u64,
    /// Unix timestamp of the last update.
    pub updated_at: u64,
    /// Parent branch, if this branch was forked.
    pub parent_id: Option<BranchId>,
}

/// Versioned branch information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedBranchInfo {
    /// The branch metadata.
    pub info: BranchInfo,
    /// Version counter for this branch record.
    pub version: u64,
    /// Unix timestamp of this version.
    pub timestamp: u64,
}

// =============================================================================
// Versioned Types
// =============================================================================

/// A value with version metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedValue {
    /// The stored value.
    pub value: Value,
    /// Monotonic version counter.
    pub version: u64,
    /// Unix timestamp when this version was written.
    pub timestamp: u64,
}

// =============================================================================
// Vector Types
// =============================================================================

/// Distance metric for vector similarity search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    /// Cosine similarity (default).
    #[default]
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Dot product similarity.
    DotProduct,
}

/// Metadata filter for vector search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// Metadata field name to filter on.
    pub field: String,
    /// Comparison operator.
    pub op: FilterOp,
    /// Value to compare against.
    pub value: Value,
}

/// Filter operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Gte,
    /// Less than.
    Lt,
    /// Less than or equal.
    Lte,
    /// Value is in a set.
    In,
    /// String/array contains value.
    Contains,
}

/// Vector data (embedding + metadata)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorData {
    /// The embedding vector.
    pub embedding: Vec<f32>,
    /// Optional metadata associated with the vector.
    pub metadata: Option<Value>,
}

/// Versioned vector data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedVectorData {
    /// Vector key.
    pub key: String,
    /// Embedding and metadata.
    pub data: VectorData,
    /// Monotonic version counter.
    pub version: u64,
    /// Unix timestamp when this version was written.
    pub timestamp: u64,
}

/// Vector search match result
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorMatch {
    /// Key of the matched vector.
    pub key: String,
    /// Similarity score (higher is more similar).
    pub score: f32,
    /// Optional metadata of the matched vector.
    pub metadata: Option<Value>,
}

/// Vector collection information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Collection name.
    pub name: String,
    /// Vector dimensionality.
    pub dimension: usize,
    /// Distance metric used for search.
    pub metric: DistanceMetric,
    /// Number of vectors in the collection.
    pub count: u64,
    /// Index type (e.g., "brute_force", "hnsw")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
    /// Approximate memory usage in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
}

/// Batch vector entry for bulk upsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchVectorEntry {
    /// Vector key.
    pub key: String,
    /// The embedding vector.
    pub vector: Vec<f32>,
    /// Optional metadata.
    pub metadata: Option<Value>,
}

// =============================================================================
// Batch Types
// =============================================================================

/// Entry for batch KV put operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchKvEntry {
    /// Key to write.
    pub key: String,
    /// Value to store.
    pub value: Value,
}

/// Entry for batch event append operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchEventEntry {
    /// Event type tag (e.g. "user.created").
    pub event_type: String,
    /// Event payload data (must be a JSON object).
    pub payload: Value,
}

/// Entry for batch state set operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchStateEntry {
    /// Cell name.
    pub cell: String,
    /// Value to store.
    pub value: Value,
}

/// Entry for batch JSON set operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchJsonEntry {
    /// Document key.
    pub key: String,
    /// JSON path within the document.
    pub path: String,
    /// Value to set at the path.
    pub value: Value,
}

/// Entry for batch JSON get operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchJsonGetEntry {
    /// Document key.
    pub key: String,
    /// JSON path within the document.
    pub path: String,
}

/// Entry for batch JSON delete operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchJsonDeleteEntry {
    /// Document key.
    pub key: String,
    /// JSON path within the document.
    pub path: String,
}

/// Per-item result for batch get operations (includes value, not just version).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchGetItemResult {
    /// The retrieved value, if found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Version of the retrieved value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// Timestamp of the retrieved value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    /// Error message if the item failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Per-item result for batch operations.
///
/// Positionally maps to input entries: `results[i]` corresponds to `entries[i]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchItemResult {
    /// Version assigned if the item succeeded.
    pub version: Option<u64>,
    /// Error message if the item failed.
    pub error: Option<String>,
}

// =============================================================================
// Transaction Types
// =============================================================================

/// Transaction options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TxnOptions {
    /// If true, the transaction only permits reads.
    pub read_only: bool,
}

/// Transaction information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionInfo {
    /// Transaction identifier.
    pub id: String,
    /// Current transaction status.
    pub status: TxnStatus,
    /// Unix timestamp when the transaction began.
    pub started_at: u64,
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnStatus {
    /// Transaction is in progress.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been rolled back.
    RolledBack,
}

// =============================================================================
// Database Types
// =============================================================================

/// Database information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatabaseInfo {
    /// Database engine version string.
    pub version: String,
    /// Seconds since the database was opened.
    pub uptime_secs: u64,
    /// Total number of branches.
    pub branch_count: u64,
    /// Total number of keys across all branches.
    pub total_keys: u64,
}

// =============================================================================
// Bundle Types
// =============================================================================

/// Information about a branch export operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchExportResult {
    /// Exported branch identifier.
    pub branch_id: String,
    /// File path of the created bundle.
    pub path: String,
    /// Number of entries in the bundle.
    pub entry_count: u64,
    /// Bundle file size in bytes.
    pub bundle_size: u64,
}

/// Information about a branch import operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchImportResult {
    /// Imported branch identifier.
    pub branch_id: String,
    /// Number of transactions replayed.
    pub transactions_applied: u64,
    /// Total keys written during import.
    pub keys_written: u64,
}

/// Information about bundle validation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleValidateResult {
    /// Branch identifier found in the bundle.
    pub branch_id: String,
    /// Bundle format version.
    pub format_version: u32,
    /// Number of entries in the bundle.
    pub entry_count: u64,
    /// Whether all checksums passed validation.
    pub checksums_valid: bool,
}

// =============================================================================
// Describe Types
// =============================================================================

/// Structured database snapshot for agent introspection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DescribeResult {
    /// Database engine version string.
    pub version: String,
    /// Path to the database directory.
    pub path: String,
    /// Current branch name.
    pub branch: String,
    /// All branch names.
    pub branches: Vec<String>,
    /// All space names on the current branch.
    pub spaces: Vec<String>,
    /// Whether the database is in read-only follower mode.
    pub follower: bool,
    /// Per-primitive summaries.
    pub primitives: PrimitiveSummary,
    /// Configuration summary.
    pub config: ConfigSummary,
    /// Capability flags.
    pub capabilities: CapabilitySummary,
}

/// Summary of all primitive stores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrimitiveSummary {
    /// KV store summary.
    pub kv: CountSummary,
    /// JSON store summary.
    pub json: CountSummary,
    /// Event log summary.
    pub events: CountSummary,
    /// State cell summary.
    pub state: StateSummary,
    /// Vector store summary.
    pub vector: VectorSummary,
    /// Graph store summary.
    pub graph: GraphSummary,
}

/// Simple count summary for a primitive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CountSummary {
    /// Number of entries.
    pub count: u64,
}

/// State cell summary with cell names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateSummary {
    /// Number of state cells.
    pub count: u64,
    /// Names of all state cells.
    pub cells: Vec<String>,
}

/// Vector store summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorSummary {
    /// Per-collection summaries.
    pub collections: Vec<VectorCollectionSummary>,
}

/// Summary of a single vector collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorCollectionSummary {
    /// Collection name.
    pub name: String,
    /// Vector dimensionality.
    pub dimension: usize,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// Number of vectors.
    pub count: u64,
}

/// Graph store summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSummary {
    /// Per-graph summaries.
    pub graphs: Vec<GraphSummaryEntry>,
}

/// Summary of a single graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSummaryEntry {
    /// Graph name.
    pub name: String,
    /// Number of nodes.
    pub nodes: u64,
    /// Number of edges.
    pub edges: u64,
    /// Ontology object types.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub object_types: Vec<String>,
    /// Ontology link types.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub link_types: Vec<String>,
}

/// Configuration summary (safe subset — no API keys).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigSummary {
    /// Generation provider name.
    pub provider: String,
    /// Default generation model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    /// Whether auto-embed is enabled.
    pub auto_embed: bool,
    /// Embedding model name.
    pub embed_model: String,
    /// Durability mode.
    pub durability: String,
}

/// Capability flags indicating what features are available.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilitySummary {
    /// Whether keyword search is available.
    pub search: bool,
    /// Whether vector similarity search is available.
    pub vector_search: bool,
    /// Whether text generation is available.
    pub generation: bool,
    /// Whether auto-embed is enabled.
    pub auto_embed: bool,
}

// =============================================================================
// Intelligence Types
// =============================================================================

/// Time range specified as ISO 8601 datetime strings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeRangeInput {
    /// Range start (inclusive). ISO 8601, e.g. "2026-02-07T00:00:00Z".
    pub start: String,
    /// Range end (inclusive). ISO 8601, e.g. "2026-02-09T23:59:59Z".
    pub end: String,
}

/// Structured search query — the canonical JSON interface for search.
///
/// All fields except `query` are optional with sensible defaults.
///
/// # Example JSON
///
/// ```json
/// {
///   "query": "user authentication errors",
///   "k": 10,
///   "primitives": ["kv", "json", "event"],
///   "time_range": { "start": "2026-02-07T00:00:00Z", "end": "2026-02-09T00:00:00Z" },
///   "mode": "hybrid",
///   "expand": true,
///   "rerank": true
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchQuery {
    /// Natural-language or keyword query string.
    pub query: String,

    /// Number of results to return (default: 10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u64>,

    /// Restrict to specific primitives (e.g. ["kv", "json", "event"]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primitives: Option<Vec<String>>,

    /// Time range filter (ISO 8601 datetime strings).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<TimeRangeInput>,

    /// Search mode: "keyword" or "hybrid" (default: "hybrid").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Enable/disable query expansion. Absent = auto (use if model configured).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expand: Option<bool>,

    /// Enable/disable reranking. Absent = auto (use if model configured).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank: Option<bool>,

    /// Precomputed query embedding (skips embedder call in hybrid search).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precomputed_embedding: Option<Vec<f32>>,
}

/// Information about a model in the registry (serializable output type).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelInfoOutput {
    /// Model name (e.g., "miniLM", "qwen3:8b").
    pub name: String,
    /// Model task ("embed" or "generate").
    pub task: String,
    /// Architecture (e.g., "bert", "llama").
    pub architecture: String,
    /// Default quantization variant.
    pub default_quant: String,
    /// Embedding dimension (0 for generation models).
    pub embedding_dim: usize,
    /// Whether the model has been downloaded locally.
    pub is_local: bool,
    /// Approximate model size in bytes.
    pub size_bytes: u64,
}

// =============================================================================
// Generation Types
// =============================================================================

/// Result of text generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GenerationResult {
    /// Generated text.
    pub text: String,
    /// Why generation stopped ("eos", "max_tokens", "context_length", "cancelled").
    pub stop_reason: String,
    /// Number of tokens in the prompt.
    pub prompt_tokens: usize,
    /// Number of tokens generated.
    pub completion_tokens: usize,
    /// Model name used for generation.
    pub model: String,
}

/// Result of tokenization (text → token IDs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenizeResult {
    /// Token IDs.
    pub ids: Vec<u32>,
    /// Number of tokens.
    pub count: usize,
    /// Model name used for tokenization.
    pub model: String,
}

/// A single hit from a cross-primitive search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResultHit {
    /// Entity identifier string
    pub entity: String,
    /// Primitive type that produced this hit
    pub primitive: String,
    /// Relevance score (higher = more relevant)
    pub score: f32,
    /// Rank in result set (1-indexed)
    pub rank: u32,
    /// Optional text snippet
    pub snippet: Option<String>,
}

/// Execution statistics for a search operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchStatsOutput {
    /// Elapsed wall time in milliseconds.
    pub elapsed_ms: f64,
    /// Total candidates considered across all primitives.
    pub candidates_considered: usize,
    /// Per-primitive candidate counts (e.g. "kv" -> 150).
    pub candidates_by_primitive: std::collections::HashMap<String, usize>,
    /// Whether a search index was used (vs. full scan).
    pub index_used: bool,
    /// Whether results were truncated due to budget limits.
    pub truncated: bool,
    /// Search mode ("keyword" or "hybrid").
    pub mode: String,
    /// Whether query expansion was used.
    pub expansion_used: bool,
    /// Whether re-ranking was used.
    pub rerank_used: bool,
    /// Model name used for query expansion (when expansion_used is true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expansion_model: Option<String>,
    /// Model name used for re-ranking (when rerank_used is true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rerank_model: Option<String>,
}

/// A single sample item from a primitive (key + value for shape discovery).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SampleItem {
    /// Key or identifier.
    pub key: String,
    /// Value at the key.
    pub value: Value,
}

// =============================================================================
// Graph Types
// =============================================================================

/// A node with its score (used in analytics summaries).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeScore {
    /// Node identifier.
    pub node_id: String,
    /// Score value.
    pub score: f64,
}

/// A group of nodes sharing a label/component (used in WCC/CDLP summaries).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupInfo {
    /// Group identifier (component ID or label hash).
    pub id: u64,
    /// Number of nodes in this group.
    pub size: usize,
    /// Sample node IDs from this group.
    pub sample_nodes: Vec<String>,
}

/// Statistical distribution of scores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreDistribution {
    /// Minimum score.
    pub min: f64,
    /// Maximum score.
    pub max: f64,
    /// Arithmetic mean.
    pub mean: f64,
    /// Median (50th percentile).
    pub median: f64,
    /// 90th percentile.
    pub p90: f64,
    /// 99th percentile.
    pub p99: f64,
}

/// Summary of a grouping analytics result (WCC, CDLP).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphGroupSummary {
    /// Algorithm name (e.g. "wcc", "cdlp").
    pub algorithm: String,
    /// Graph name.
    pub graph: String,
    /// Total number of nodes.
    pub node_count: usize,
    /// Number of distinct groups.
    pub group_count: usize,
    /// Size of the largest group.
    pub largest_group_size: usize,
    /// Top groups sorted by size descending, with sample nodes.
    pub groups: Vec<GroupInfo>,
    /// Full raw results (only when `include_all` is true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all: Option<std::collections::HashMap<String, u64>>,
}

/// Summary of a score-based analytics result (PageRank, LCC, SSSP).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphScoreSummary {
    /// Algorithm name (e.g. "pagerank", "lcc", "sssp").
    pub algorithm: String,
    /// Graph name.
    pub graph: String,
    /// Total number of nodes.
    pub node_count: usize,
    /// Top nodes by score.
    pub top_nodes: Vec<NodeScore>,
    /// Score distribution statistics.
    pub distribution: ScoreDistribution,
    /// Number of iterations (PageRank).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<usize>,
    /// Whether the algorithm converged (PageRank).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged: Option<bool>,
    /// Global clustering coefficient — mean of all coefficients (LCC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_clustering_coefficient: Option<f64>,
    /// Count of nodes with zero coefficient (LCC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero_count: Option<usize>,
    /// Source node (SSSP).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Farthest reachable nodes (SSSP).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub farthest: Option<Vec<NodeScore>>,
    /// Full raw results (only when `include_all` is true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all: Option<std::collections::HashMap<String, f64>>,
}

// =============================================================================
// Export Types
// =============================================================================

/// Output format for data export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportFormat {
    /// Comma-separated values.
    Csv,
    /// Pretty-printed JSON array.
    Json,
    /// Newline-delimited JSON (one object per line).
    Jsonl,
}

/// Which primitive to export data from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportPrimitive {
    /// Key-value store.
    Kv,
    /// JSON document store.
    Json,
    /// State cells.
    State,
    /// Event log.
    Events,
}

impl ExportPrimitive {
    /// Returns the primitive name as a static string.
    pub fn as_str(&self) -> &'static str {
        match self {
            ExportPrimitive::Kv => "kv",
            ExportPrimitive::Json => "json",
            ExportPrimitive::State => "state",
            ExportPrimitive::Events => "events",
        }
    }
}

/// Result of a data export operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExportResult {
    /// Number of rows exported.
    pub row_count: u64,
    /// Output format used.
    pub format: ExportFormat,
    /// Primitive exported from.
    pub primitive: ExportPrimitive,
    /// Rendered data (inline). Present when no file path was specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    /// File path where data was written. Present when a path was specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Size of the rendered data in bytes. Present when written to file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// A neighbor entry returned by graph neighbor queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphNeighborHit {
    /// The neighbor node ID.
    pub node_id: String,
    /// The edge type connecting to this neighbor.
    pub edge_type: String,
    /// Edge weight.
    pub weight: f64,
}

/// BFS traversal result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphBfsResult {
    /// Visited node IDs in BFS order.
    pub visited: Vec<String>,
    /// Depth at which each node was first discovered.
    pub depths: std::collections::HashMap<String, usize>,
    /// Edges traversed: (src, dst, edge_type).
    pub edges: Vec<(String, String, String)>,
}

/// A node entry for bulk graph insertion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BulkGraphNode {
    /// Node identifier.
    pub node_id: String,
    /// Optional entity reference URI (e.g. `"kv://main/key"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_ref: Option<String>,
    /// Optional properties to attach to the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Value>,
    /// Optional ontology object type (e.g. `"Patient"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_type: Option<String>,
}

/// An edge entry for bulk graph insertion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BulkGraphEdge {
    /// Source node ID.
    pub src: String,
    /// Destination node ID.
    pub dst: String,
    /// Edge type label.
    pub edge_type: String,
    /// Optional edge weight (default 1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
    /// Optional properties to attach to the edge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Value>,
}
