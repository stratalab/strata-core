//! Command enum defining all Strata operations.
//!
//! Commands are the "instruction set" of Strata. Every operation that can be
//! performed on the database is represented as a variant of this enum.
//!
//! Commands are:
//! - **Self-contained**: All parameters needed for execution are in the variant
//! - **Serializable**: Can be converted to/from JSON for cross-language use
//! - **Typed**: No generic fallback, every operation has explicit types
//! - **Pure data**: No closures or executable code

use serde::{Deserialize, Serialize};
use strata_core::Value;
use strata_engine::MergeStrategy;

use crate::types::*;

/// A command is a self-contained, serializable operation.
///
/// This is the "instruction set" of Strata - every operation that can be
/// performed on the database is represented here.
///
/// # Command Categories
///
/// | Category | Count | Description |
/// |----------|-------|-------------|
/// | KV | 4 | Key-value operations |
/// | JSON | 17 | JSON document operations |
/// | Event | 4 | Event log operations (MVP) |
/// | State | 4 | State cell operations (MVP) |
/// | Vector | 7 | Vector store operations (MVP) |
/// | Branch | 5 | Branch lifecycle operations (MVP) |
/// | Transaction | 5 | Transaction control |
/// | Retention | 3 | Retention policy |
/// | Database | 4 | Database-level operations |
///
/// # Branch field
///
/// Data-scoped commands have an optional `branch` field. When omitted (or `None`),
/// the executor resolves it to the default branch before dispatch. JSON
/// with `"branch": "default"` works; new callers can simply omit the field.
///
/// Branch lifecycle commands (BranchGet, BranchDelete, etc.) keep a required
/// `branch: BranchId` since they explicitly operate on a specific branch.
///
/// # Example
///
/// ```text
/// use strata_executor::{Command, BranchId};
/// use strata_core::Value;
///
/// // Explicit branch
/// let cmd = Command::KvPut {
///     branch: Some(BranchId::default()),
///     key: "foo".into(),
///     value: Value::Int(42),
/// };
///
/// // Omit branch (defaults to "default")
/// let cmd = Command::KvPut {
///     branch: None,
///     key: "foo".into(),
///     value: Value::Int(42),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Command {
    // ==================== KV (4) ====================
    /// Put a key-value pair.
    /// Returns: `Output::Version`
    KvPut {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key to write.
        key: String,
        /// Value to store.
        value: Value,
    },

    /// Get a value by key.
    /// Returns: `Output::MaybeValue`
    KvGet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key to look up.
        key: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Delete a key.
    /// Returns: `Output::Bool` (true if key existed)
    KvDelete {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key to delete.
        key: String,
    },

    /// List keys with optional prefix filter.
    /// Returns: `Output::Keys`
    KvList {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix filter.
        prefix: Option<String>,
        /// Pagination cursor from a previous response.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        /// Maximum number of keys to return.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Batch put multiple key-value pairs in a single transaction.
    /// Returns: `Output::BatchResults`
    KvBatchPut {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Entries to write.
        entries: Vec<BatchKvEntry>,
    },

    /// Get full version history for a key.
    /// Returns: `Output::VersionHistory`
    KvGetv {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Key to retrieve history for.
        key: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    // ==================== JSON (4 MVP) ====================
    /// Set a value at a path in a JSON document.
    /// Returns: `Output::Version`
    JsonSet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// JSON path within the document.
        path: String,
        /// Value to set at the path.
        value: Value,
    },

    /// Get a value at a path from a JSON document.
    /// Returns: `Output::MaybeVersioned`
    JsonGet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// JSON path to read.
        path: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Delete a value at a path from a JSON document.
    /// Returns: `Output::Uint` (count of elements removed)
    JsonDelete {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// JSON path to delete.
        path: String,
    },

    /// Get full version history for a JSON document.
    /// Returns: `Output::VersionHistory`
    JsonGetv {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Document key.
        key: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Batch set multiple JSON documents in a single transaction.
    /// Returns: `Output::BatchResults`
    JsonBatchSet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// JSON entries to set.
        entries: Vec<BatchJsonEntry>,
    },

    /// Batch get multiple JSON document values.
    /// Returns: `Output::BatchGetResults`
    JsonBatchGet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// JSON entries to get.
        entries: Vec<BatchJsonGetEntry>,
    },

    /// Batch delete multiple JSON documents or paths.
    /// Returns: `Output::BatchResults`
    JsonBatchDelete {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// JSON entries to delete.
        entries: Vec<BatchJsonDeleteEntry>,
    },

    /// List JSON documents with cursor-based pagination.
    /// Returns: `Output::JsonListResult`
    JsonList {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix filter.
        prefix: Option<String>,
        /// Pagination cursor from a previous response.
        cursor: Option<String>,
        /// Maximum number of documents to return.
        limit: u64,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    // ==================== Event (4 MVP + 1 batch) ====================
    // MVP: append, read, get_by_type, len
    /// Batch append multiple events in a single transaction.
    /// Returns: `Output::BatchResults`
    EventBatchAppend {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event entries to append.
        entries: Vec<BatchEventEntry>,
    },

    /// Append an event to the log.
    /// Returns: `Output::Version`
    EventAppend {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event type tag (e.g. "user.created").
        event_type: String,
        /// Event payload data.
        payload: Value,
    },

    /// Read a specific event by sequence number.
    /// Returns: `Output::MaybeVersioned`
    EventGet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event sequence number.
        sequence: u64,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Read all events of a specific type.
    /// Returns: `Output::VersionedValues`
    EventGetByType {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Event type to filter by.
        event_type: String,
        /// Maximum number of events to return.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// Only return events after this sequence number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_sequence: Option<u64>,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Get the total count of events in the log.
    /// Returns: `Output::Uint`
    EventLen {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
    },

    // ==================== State (4 MVP + 1 batch) ====================
    // MVP: set, read, cas, init
    /// Batch set multiple state cells in a single transaction.
    /// Returns: `Output::BatchResults`
    StateBatchSet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// State cell entries to set.
        entries: Vec<BatchStateEntry>,
    },

    /// Set a state cell value (unconditional write).
    /// Returns: `Output::Version`
    StateSet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Cell name.
        cell: String,
        /// Value to store.
        value: Value,
    },

    /// Read a state cell value.
    /// Returns: `Output::MaybeVersioned`
    StateGet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Cell name.
        cell: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Compare-and-swap on a state cell.
    /// Returns: `Output::MaybeVersion`
    StateCas {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Cell name.
        cell: String,
        /// Expected version counter (`None` means cell must not exist).
        expected_counter: Option<u64>,
        /// New value to swap in.
        value: Value,
    },

    /// Get full version history for a state cell.
    /// Returns: `Output::VersionHistory`
    StateGetv {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Cell name.
        cell: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Initialize a state cell (only if it doesn't exist).
    /// Returns: `Output::Version`
    StateInit {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Cell name.
        cell: String,
        /// Initial value.
        value: Value,
    },

    /// Delete a state cell.
    /// Returns: `Output::Bool` (true if cell existed)
    StateDelete {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Cell name.
        cell: String,
    },

    /// List state cell names with optional prefix filter.
    /// Returns: `Output::Keys`
    StateList {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional cell name prefix filter.
        prefix: Option<String>,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    // ==================== Vector (7 MVP) ====================
    // MVP: upsert, get, delete, search, create_collection, delete_collection, list_collections
    /// Insert or update a vector.
    /// Returns: `Output::Version`
    VectorUpsert {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Embedding vector data.
        vector: Vec<f32>,
        /// Optional metadata to associate with the vector.
        metadata: Option<Value>,
    },

    /// Get a vector by key.
    /// Returns: `Output::MaybeVectorData`
    VectorGet {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Delete a vector.
    /// Returns: `Output::Bool`
    VectorDelete {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector key.
        key: String,
    },

    /// Search for similar vectors.
    /// Returns: `Output::VectorMatches`
    VectorSearch {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection to search.
        collection: String,
        /// Query embedding vector.
        query: Vec<f32>,
        /// Number of nearest neighbors to return.
        k: u64,
        /// Optional metadata filters.
        filter: Option<Vec<MetadataFilter>>,
        /// Optional distance metric override.
        metric: Option<DistanceMetric>,
        /// Optional timestamp for time-travel reads (microseconds since epoch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Create a collection with explicit configuration.
    /// Returns: `Output::Version`
    VectorCreateCollection {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector dimensionality.
        dimension: u64,
        /// Distance metric for similarity search.
        metric: DistanceMetric,
    },

    /// Delete a collection.
    /// Returns: `Output::Bool`
    VectorDeleteCollection {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
    },

    /// List all collections in a branch.
    /// Returns: `Output::VectorCollectionList`
    VectorListCollections {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
    },

    /// Get detailed statistics for a single collection.
    /// Returns: `Output::VectorCollectionList` (with single entry)
    VectorCollectionStats {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
    },

    /// Batch insert or update multiple vectors.
    /// Returns: `Output::Versions`
    VectorBatchUpsert {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Vector entries to upsert.
        entries: Vec<BatchVectorEntry>,
    },

    // ==================== Branch (5 MVP) ====================
    /// Create a new branch.
    /// Returns: `Output::BranchWithVersion`
    BranchCreate {
        /// Optional branch name (auto-generated UUID if omitted).
        branch_id: Option<String>,
        /// Optional metadata to attach to the branch.
        metadata: Option<Value>,
    },

    /// Get branch info.
    /// Returns: `Output::MaybeBranchInfo`
    BranchGet {
        /// Branch to look up.
        branch: BranchId,
    },

    /// List all branches.
    /// Returns: `Output::BranchInfoList`
    BranchList {
        /// Optional status filter.
        state: Option<BranchStatus>,
        /// Maximum number of branches to return.
        limit: Option<u64>,
        /// Number of branches to skip.
        offset: Option<u64>,
    },

    /// Check if a branch exists.
    /// Returns: `Output::Bool`
    BranchExists {
        /// Branch to check.
        branch: BranchId,
    },

    /// Delete a branch and all its data (cascading delete).
    /// Returns: `Output::Unit`
    BranchDelete {
        /// Branch to delete.
        branch: BranchId,
    },

    /// Fork a branch, creating a complete copy of all its data.
    /// Returns: `Output::BranchForked`
    BranchFork {
        /// Source branch name.
        source: String,
        /// Destination branch name.
        destination: String,
        /// Optional message describing the fork.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// Optional creator identifier.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        creator: Option<String>,
    },

    /// Compare two branches and return structured differences.
    /// Returns: `Output::BranchDiff`
    BranchDiff {
        /// First branch to compare.
        branch_a: String,
        /// Second branch to compare.
        branch_b: String,
        /// Optional filter by primitive types.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter_primitives: Option<Vec<strata_core::PrimitiveType>>,
        /// Optional filter by space names.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter_spaces: Option<Vec<String>>,
        /// Optional point-in-time timestamp (microseconds).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
    },

    /// Merge data from source branch into target branch.
    /// Returns: `Output::BranchMerged`
    BranchMerge {
        /// Source branch name.
        source: String,
        /// Target branch name.
        target: String,
        /// Conflict resolution strategy.
        strategy: MergeStrategy,
        /// Optional message describing the merge.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// Optional creator identifier.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        creator: Option<String>,
    },

    // ==================== Transaction (5) ====================
    /// Begin a new transaction.
    /// Returns: `Output::TxnBegun`
    TxnBegin {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Transaction configuration options.
        options: Option<TxnOptions>,
    },

    /// Commit the current transaction.
    /// Returns: `Output::Version`
    TxnCommit,

    /// Rollback the current transaction.
    /// Returns: `Output::Unit`
    TxnRollback,

    /// Get current transaction info.
    /// Returns: `Output::MaybeTxnInfo`
    TxnInfo,

    /// Check if a transaction is active.
    /// Returns: `Output::Bool`
    TxnIsActive,

    // ==================== Retention (3) ====================
    // Note: Branch-level retention is handled via BranchSetRetention/BranchGetRetention
    // These are database-wide retention operations
    /// Apply retention policy (trigger garbage collection).
    /// Returns: `Output::RetentionResult`
    RetentionApply {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Get retention statistics.
    /// Returns: `Output::RetentionStats`
    RetentionStats {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Preview what would be deleted by retention policy.
    /// Returns: `Output::RetentionPreview`
    RetentionPreview {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    // ==================== Database (5) ====================
    /// Ping the database to check connectivity
    Ping,

    /// Get database information
    Info,

    /// Run health checks on all subsystems.
    /// Returns: `Output::Health`
    Health,

    /// Flush pending writes to disk
    Flush,

    /// Trigger compaction
    Compact,

    /// Return a structured snapshot of the database for agent introspection.
    /// Returns: `Output::Described`
    Describe {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Get the available time range for a branch.
    /// Returns: `Output::TimeRange`
    TimeRange {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Export primitive data to CSV, JSON, or JSONL.
    /// Returns: `Output::Exported`
    DbExport {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Which primitive to export from.
        primitive: ExportPrimitive,
        /// Output format.
        format: ExportFormat,
        /// Optional key/doc prefix filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Maximum number of rows to export.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// File path to write to. If omitted, data is returned inline.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },

    // ==================== Bundle (3) ====================
    /// Export a branch to a .branchbundle.tar.zst archive.
    /// Returns: `Output::BranchExported`
    BranchExport {
        /// Branch to export.
        branch_id: String,
        /// Output file path.
        path: String,
    },

    /// Import a branch from a .branchbundle.tar.zst archive.
    /// Returns: `Output::BranchImported`
    BranchImport {
        /// Path to the bundle archive.
        path: String,
    },

    /// Validate a .branchbundle.tar.zst archive without importing.
    /// Returns: `Output::BundleValidated`
    BranchBundleValidate {
        /// Path to the bundle archive.
        path: String,
    },

    // ==================== Intelligence (2) ====================
    /// Configure an external model endpoint for query expansion.
    /// Returns: `Output::Unit`
    ConfigureModel {
        /// OpenAI-compatible API endpoint (e.g. "http://localhost:11434/v1").
        endpoint: String,
        /// Model name (e.g. "qwen3:1.7b").
        model: String,
        /// Optional API key for authenticated endpoints.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        api_key: Option<String>,
        /// Request timeout in milliseconds (default: 5000).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },

    /// Search across multiple primitives using a structured query.
    /// Returns: `Output::SearchResults`
    Search {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Structured search query.
        search: SearchQuery,
    },

    // ==================== Data Introspection ====================
    /// Count KV keys matching a prefix.
    /// Returns: `Output::Uint`
    KvCount {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },

    /// Count JSON documents matching a prefix.
    /// Returns: `Output::Uint`
    JsonCount {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },

    /// Sample KV entries for shape discovery.
    /// Returns: `Output::SampleResult`
    KvSample {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Number of samples to return (default: 5).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<usize>,
    },

    /// Sample JSON documents for shape discovery.
    /// Returns: `Output::SampleResult`
    JsonSample {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Optional key prefix filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Number of samples to return (default: 5).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<usize>,
    },

    /// Sample vector entries for shape discovery (returns metadata, not embeddings).
    /// Returns: `Output::SampleResult`
    VectorSample {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Target space.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        space: Option<String>,
        /// Collection name.
        collection: String,
        /// Number of samples to return (default: 5).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<usize>,
    },

    // ==================== Embedding (2) ====================
    /// Embed a single text string.
    /// Returns: `Output::Embedding`
    Embed {
        /// Text to embed.
        text: String,
    },

    /// Embed a batch of text strings.
    /// Returns: `Output::Embeddings`
    EmbedBatch {
        /// Texts to embed.
        texts: Vec<String>,
    },

    // ==================== Model Management (2) ====================
    /// List all available models in the registry.
    /// Returns: `Output::ModelsList`
    ModelsList,

    /// Download a model by name.
    /// Returns: `Output::ModelsPulled`
    ModelsPull {
        /// Model name to download (e.g., "miniLM", "nomic-embed").
        name: String,
    },

    // ==================== Generation (3) ====================
    /// Generate text from a prompt using a local model.
    /// Returns: `Output::Generated`
    Generate {
        /// Model name (e.g. "qwen3:8b").
        model: String,
        /// Input prompt text.
        prompt: String,
        /// Maximum tokens to generate.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_tokens: Option<usize>,
        /// Sampling temperature (0.0 = greedy).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        temperature: Option<f32>,
        /// Top-K sampling (0 = disabled).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_k: Option<usize>,
        /// Top-P (nucleus) sampling (1.0 = disabled).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_p: Option<f32>,
        /// Random seed for reproducibility.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seed: Option<u64>,
        /// Stop generation when any of these token IDs are produced.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_tokens: Option<Vec<u32>>,
        /// Stop generation when any of these text sequences are produced.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_sequences: Option<Vec<String>>,
    },

    /// Tokenize text into token IDs using a model's tokenizer.
    /// Returns: `Output::TokenIds`
    Tokenize {
        /// Model name.
        model: String,
        /// Text to tokenize.
        text: String,
        /// Whether to add special tokens (default: true).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        add_special_tokens: Option<bool>,
    },

    /// Detokenize token IDs back to text.
    /// Returns: `Output::Text`
    Detokenize {
        /// Model name.
        model: String,
        /// Token IDs to decode.
        ids: Vec<u32>,
    },

    /// Unload a generation model from memory.
    /// Returns: `Output::Bool` (true if model was loaded)
    GenerateUnload {
        /// Model name to unload.
        model: String,
    },

    /// List locally downloaded models.
    /// Returns: `Output::ModelsList`
    ModelsLocal,

    // ==================== Space (4) ====================
    /// List spaces in a branch.
    /// Returns: `Output::SpaceList`
    SpaceList {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Create a space explicitly.
    /// Returns: `Output::Unit`
    SpaceCreate {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Space name.
        space: String,
    },

    /// Get embedding pipeline status.
    /// Returns: `Output::EmbedStatus`
    EmbedStatus,

    /// Re-index all embeddings with the currently configured model.
    /// Drops existing shadow collections, recreates with new dimensions,
    /// and queues all data for re-embedding.
    /// Returns: `Output::ReindexResult`
    ReindexEmbeddings {
        /// Target branch (defaults to current branch).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Get the current database configuration.
    /// Returns: `Output::Config`
    ConfigGet,

    /// Set a configuration key to a value.
    /// Returns: `Output::Unit`
    ///
    /// Supported keys: `provider`, `default_model`, `anthropic_api_key`,
    /// `openai_api_key`, `google_api_key`, `embed_model`, `durability`,
    /// `auto_embed`, `bm25_k1`, `bm25_b`, `embed_batch_size`,
    /// `model_endpoint`, `model_name`, `model_api_key`, `model_timeout_ms`.
    ///
    /// Returns: `Output::ConfigSetResult`
    ConfigureSet {
        /// Configuration key name.
        key: String,
        /// Configuration value.
        value: String,
    },

    /// Get the value of a configuration key.
    /// Returns: `Output::ConfigValue`
    ConfigureGetKey {
        /// Configuration key name.
        key: String,
    },

    /// Enable or disable auto-embedding.
    /// Returns: `Output::Unit`
    ConfigSetAutoEmbed {
        /// Whether to enable auto-embedding.
        enabled: bool,
    },

    /// Check whether auto-embedding is enabled.
    /// Returns: `Output::Bool`
    AutoEmbedStatus,

    /// Get WAL durability counters.
    /// Returns: `Output::DurabilityCounters`
    DurabilityCounters,

    /// Delete a space (must be empty unless force=true).
    /// Returns: `Output::Unit`
    SpaceDelete {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Space name.
        space: String,
        /// If true, delete even if the space is non-empty.
        #[serde(default)]
        force: bool,
    },

    /// Check if a space exists.
    /// Returns: `Output::Bool`
    SpaceExists {
        /// Target branch (defaults to "default").
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Space name.
        space: String,
    },

    // ==================== Graph ====================
    /// Create a new graph.
    /// Returns: `Output::Unit`
    GraphCreate {
        /// Target branch (resolved from context if absent).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Optional cascade policy: `"cascade"`, `"detach"`, or `"ignore"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cascade_policy: Option<String>,
    },

    /// Delete a graph and all its data.
    /// Returns: `Output::Unit`
    GraphDelete {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// List all graphs.
    /// Returns: `Output::Keys`
    GraphList {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
    },

    /// Get graph metadata.
    /// Returns: `Output::Maybe`
    GraphGetMeta {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// Add or update a node.
    /// Returns: `Output::Unit`
    GraphAddNode {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Node identifier.
        node_id: String,
        /// Optional entity reference URI (e.g. `"kv://main/key"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entity_ref: Option<String>,
        /// Optional properties to attach to the node.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        properties: Option<Value>,
        /// Optional ontology object type (e.g. `"Patient"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_type: Option<String>,
    },

    /// Get a node.
    /// Returns: `Output::Maybe`
    GraphGetNode {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Node identifier.
        node_id: String,
    },

    /// Remove a node and its incident edges.
    /// Returns: `Output::Unit`
    GraphRemoveNode {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Node identifier.
        node_id: String,
    },

    /// List all node IDs in a graph.
    /// Returns: `Output::Keys`
    GraphListNodes {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// List node IDs with cursor-based pagination.
    /// Returns: `Output::GraphPage`
    GraphListNodesPaginated {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Maximum number of items per page.
        limit: usize,
        /// Cursor for continuation (None = first page).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },

    /// Add or update an edge.
    /// Returns: `Output::Unit`
    GraphAddEdge {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Source node ID.
        src: String,
        /// Destination node ID.
        dst: String,
        /// Edge type label.
        edge_type: String,
        /// Optional edge weight (default 1.0).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        weight: Option<f64>,
        /// Optional properties to attach to the edge.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        properties: Option<Value>,
    },

    /// Remove an edge.
    /// Returns: `Output::Unit`
    GraphRemoveEdge {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Source node ID.
        src: String,
        /// Destination node ID.
        dst: String,
        /// Edge type label.
        edge_type: String,
    },

    /// Get neighbors of a node.
    /// Returns: `Output::GraphNeighbors`
    GraphNeighbors {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Node identifier.
        node_id: String,
        /// Direction: `"outgoing"`, `"incoming"`, or `"both"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<String>,
        /// Optional edge type filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_type: Option<String>,
    },

    /// Bulk insert nodes and edges into a graph.
    /// Returns: `Output::GraphBulkInsertResult`
    GraphBulkInsert {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Nodes to insert.
        nodes: Vec<BulkGraphNode>,
        /// Edges to insert.
        edges: Vec<BulkGraphEdge>,
        /// Optional chunk size for batching (default 10,000).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        chunk_size: Option<usize>,
    },

    /// BFS traversal.
    /// Returns: `Output::GraphBfs`
    GraphBfs {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Start node ID.
        start: String,
        /// Maximum traversal depth.
        max_depth: usize,
        /// Maximum number of nodes to visit.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_nodes: Option<usize>,
        /// Only traverse edges of these types.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_types: Option<Vec<String>>,
        /// Traversal direction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<String>,
    },

    // ==================== Graph Ontology ====================
    /// Define an object type in the graph's ontology.
    /// Returns: `Output::Unit`
    GraphDefineObjectType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Object type definition as JSON.
        definition: Value,
    },

    /// Get an object type definition.
    /// Returns: `Output::Maybe`
    GraphGetObjectType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Type name.
        name: String,
    },

    /// List all object type names.
    /// Returns: `Output::Keys`
    GraphListObjectTypes {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// Delete an object type definition.
    /// Returns: `Output::Unit`
    GraphDeleteObjectType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Type name.
        name: String,
    },

    /// Define a link type in the graph's ontology.
    /// Returns: `Output::Unit`
    GraphDefineLinkType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Link type definition as JSON.
        definition: Value,
    },

    /// Get a link type definition.
    /// Returns: `Output::Maybe`
    GraphGetLinkType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Type name.
        name: String,
    },

    /// List all link type names.
    /// Returns: `Output::Keys`
    GraphListLinkTypes {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// Delete a link type definition.
    /// Returns: `Output::Unit`
    GraphDeleteLinkType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Type name.
        name: String,
    },

    /// Freeze the graph's ontology.
    /// Returns: `Output::Unit`
    GraphFreezeOntology {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// Get the ontology status of a graph.
    /// Returns: `Output::Maybe`
    GraphOntologyStatus {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// Get a complete ontology summary for AI orientation.
    /// Returns: `Output::Maybe`
    GraphOntologySummary {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// List all ontology types (both object and link types).
    /// Returns: `Output::Keys`
    GraphListOntologyTypes {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
    },

    /// Get all node IDs of a given object type.
    /// Returns: `Output::Keys`
    GraphNodesByType {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Object type name.
        object_type: String,
    },

    // ==================== Graph Analytics ====================
    /// Weakly Connected Components (union-find).
    /// Returns: `Output::GraphGroupSummary`
    GraphWcc {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Number of top groups to return (default 10).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_n: Option<usize>,
        /// Include full raw results.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_all: Option<bool>,
    },

    /// Community Detection via Label Propagation.
    /// Returns: `Output::GraphGroupSummary`
    GraphCdlp {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Maximum number of iterations.
        max_iterations: usize,
        /// Direction: `"outgoing"`, `"incoming"`, or `"both"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<String>,
        /// Number of top groups to return (default 10).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_n: Option<usize>,
        /// Include full raw results.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_all: Option<bool>,
    },

    /// PageRank importance scoring.
    /// Returns: `Output::GraphScoreSummary`
    GraphPagerank {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Damping factor (default 0.85).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        damping: Option<f64>,
        /// Maximum number of iterations (default 20).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_iterations: Option<usize>,
        /// Convergence tolerance (default 1e-6).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tolerance: Option<f64>,
        /// Number of top nodes to return (default 10).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_n: Option<usize>,
        /// Include full raw results.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_all: Option<bool>,
    },

    /// Local Clustering Coefficient.
    /// Returns: `Output::GraphScoreSummary`
    GraphLcc {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Number of top nodes to return (default 10).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_n: Option<usize>,
        /// Include full raw results.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_all: Option<bool>,
    },

    /// Single-Source Shortest Path (Dijkstra).
    /// Returns: `Output::GraphScoreSummary`
    GraphSssp {
        /// Target branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchId>,
        /// Graph name.
        graph: String,
        /// Source node ID.
        source: String,
        /// Direction: `"outgoing"`, `"incoming"`, or `"both"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<String>,
        /// Number of top nodes to return (default 10).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        top_n: Option<usize>,
        /// Include full raw results.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        include_all: Option<bool>,
    },
}

impl Command {
    /// Returns `true` if this command performs a write operation.
    ///
    /// Used by the access-mode guard to reject writes when the database
    /// is opened in read-only mode.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Command::KvPut { .. }
                | Command::KvBatchPut { .. }
                | Command::KvDelete { .. }
                | Command::JsonSet { .. }
                | Command::JsonBatchSet { .. }
                | Command::JsonBatchDelete { .. }
                | Command::JsonDelete { .. }
                | Command::EventAppend { .. }
                | Command::EventBatchAppend { .. }
                | Command::StateSet { .. }
                | Command::StateBatchSet { .. }
                | Command::StateCas { .. }
                | Command::StateInit { .. }
                | Command::StateDelete { .. }
                | Command::VectorUpsert { .. }
                | Command::VectorDelete { .. }
                | Command::VectorCreateCollection { .. }
                | Command::VectorDeleteCollection { .. }
                | Command::VectorBatchUpsert { .. }
                | Command::BranchCreate { .. }
                | Command::BranchDelete { .. }
                | Command::BranchFork { .. }
                | Command::BranchMerge { .. }
                | Command::ConfigSetAutoEmbed { .. }
                | Command::SpaceCreate { .. }
                | Command::SpaceDelete { .. }
                | Command::TxnBegin { .. }
                | Command::TxnCommit
                | Command::TxnRollback
                | Command::RetentionApply { .. }
                | Command::Flush
                | Command::Compact
                | Command::BranchExport { .. }
                | Command::BranchImport { .. }
                | Command::ConfigureModel { .. }
                | Command::ConfigureSet { .. }
                | Command::ModelsPull { .. }
                | Command::GraphCreate { .. }
                | Command::GraphDelete { .. }
                | Command::GraphAddNode { .. }
                | Command::GraphRemoveNode { .. }
                | Command::GraphAddEdge { .. }
                | Command::GraphRemoveEdge { .. }
                | Command::GraphBulkInsert { .. }
                | Command::GraphDefineObjectType { .. }
                | Command::GraphDeleteObjectType { .. }
                | Command::GraphDefineLinkType { .. }
                | Command::GraphDeleteLinkType { .. }
                | Command::GraphFreezeOntology { .. }
                | Command::ReindexEmbeddings { .. }
        )
    }

    /// Returns the variant name as a static string.
    ///
    /// The exhaustive match ensures the compiler flags any new `Command`
    /// variant that is added without a corresponding name.
    pub fn name(&self) -> &'static str {
        match self {
            Command::KvPut { .. } => "KvPut",
            Command::KvBatchPut { .. } => "KvBatchPut",
            Command::KvGet { .. } => "KvGet",
            Command::KvDelete { .. } => "KvDelete",
            Command::KvList { .. } => "KvList",
            Command::KvGetv { .. } => "KvGetv",
            Command::JsonSet { .. } => "JsonSet",
            Command::JsonBatchSet { .. } => "JsonBatchSet",
            Command::JsonBatchGet { .. } => "JsonBatchGet",
            Command::JsonBatchDelete { .. } => "JsonBatchDelete",
            Command::JsonGet { .. } => "JsonGet",
            Command::JsonDelete { .. } => "JsonDelete",
            Command::JsonGetv { .. } => "JsonGetv",
            Command::JsonList { .. } => "JsonList",
            Command::EventAppend { .. } => "EventAppend",
            Command::EventBatchAppend { .. } => "EventBatchAppend",
            Command::EventGet { .. } => "EventGet",
            Command::EventGetByType { .. } => "EventGetByType",
            Command::EventLen { .. } => "EventLen",
            Command::StateSet { .. } => "StateSet",
            Command::StateBatchSet { .. } => "StateBatchSet",
            Command::StateGet { .. } => "StateGet",
            Command::StateCas { .. } => "StateCas",
            Command::StateGetv { .. } => "StateGetv",
            Command::StateInit { .. } => "StateInit",
            Command::StateDelete { .. } => "StateDelete",
            Command::StateList { .. } => "StateList",
            Command::VectorUpsert { .. } => "VectorUpsert",
            Command::VectorGet { .. } => "VectorGet",
            Command::VectorDelete { .. } => "VectorDelete",
            Command::VectorSearch { .. } => "VectorSearch",
            Command::VectorCreateCollection { .. } => "VectorCreateCollection",
            Command::VectorDeleteCollection { .. } => "VectorDeleteCollection",
            Command::VectorListCollections { .. } => "VectorListCollections",
            Command::VectorCollectionStats { .. } => "VectorCollectionStats",
            Command::VectorBatchUpsert { .. } => "VectorBatchUpsert",
            Command::BranchCreate { .. } => "BranchCreate",
            Command::BranchGet { .. } => "BranchGet",
            Command::BranchList { .. } => "BranchList",
            Command::BranchExists { .. } => "BranchExists",
            Command::BranchDelete { .. } => "BranchDelete",
            Command::BranchFork { .. } => "BranchFork",
            Command::BranchDiff { .. } => "BranchDiff",
            Command::BranchMerge { .. } => "BranchMerge",
            Command::TxnBegin { .. } => "TxnBegin",
            Command::TxnCommit => "TxnCommit",
            Command::TxnRollback => "TxnRollback",
            Command::TxnInfo => "TxnInfo",
            Command::TxnIsActive => "TxnIsActive",
            Command::RetentionApply { .. } => "RetentionApply",
            Command::RetentionStats { .. } => "RetentionStats",
            Command::RetentionPreview { .. } => "RetentionPreview",
            Command::Ping => "Ping",
            Command::Info => "Info",
            Command::Health => "Health",
            Command::Flush => "Flush",
            Command::Compact => "Compact",
            Command::Describe { .. } => "Describe",
            Command::TimeRange { .. } => "TimeRange",
            Command::DbExport { .. } => "DbExport",
            Command::BranchExport { .. } => "BranchExport",
            Command::BranchImport { .. } => "BranchImport",
            Command::BranchBundleValidate { .. } => "BranchBundleValidate",
            Command::ConfigureModel { .. } => "ConfigureModel",
            Command::Search { .. } => "Search",
            Command::KvCount { .. } => "KvCount",
            Command::JsonCount { .. } => "JsonCount",
            Command::KvSample { .. } => "KvSample",
            Command::JsonSample { .. } => "JsonSample",
            Command::VectorSample { .. } => "VectorSample",
            Command::EmbedStatus => "EmbedStatus",
            Command::ReindexEmbeddings { .. } => "ReindexEmbeddings",
            Command::ConfigGet => "ConfigGet",
            Command::ConfigureSet { .. } => "ConfigureSet",
            Command::ConfigureGetKey { .. } => "ConfigureGetKey",
            Command::ConfigSetAutoEmbed { .. } => "ConfigSetAutoEmbed",
            Command::AutoEmbedStatus => "AutoEmbedStatus",
            Command::DurabilityCounters => "DurabilityCounters",
            Command::Embed { .. } => "Embed",
            Command::EmbedBatch { .. } => "EmbedBatch",
            Command::ModelsList => "ModelsList",
            Command::ModelsPull { .. } => "ModelsPull",
            Command::Generate { .. } => "Generate",
            Command::Tokenize { .. } => "Tokenize",
            Command::Detokenize { .. } => "Detokenize",
            Command::GenerateUnload { .. } => "GenerateUnload",
            Command::ModelsLocal => "ModelsLocal",
            Command::SpaceList { .. } => "SpaceList",
            Command::SpaceCreate { .. } => "SpaceCreate",
            Command::SpaceDelete { .. } => "SpaceDelete",
            Command::SpaceExists { .. } => "SpaceExists",
            Command::GraphCreate { .. } => "GraphCreate",
            Command::GraphDelete { .. } => "GraphDelete",
            Command::GraphList { .. } => "GraphList",
            Command::GraphGetMeta { .. } => "GraphGetMeta",
            Command::GraphAddNode { .. } => "GraphAddNode",
            Command::GraphGetNode { .. } => "GraphGetNode",
            Command::GraphRemoveNode { .. } => "GraphRemoveNode",
            Command::GraphListNodes { .. } => "GraphListNodes",
            Command::GraphListNodesPaginated { .. } => "GraphListNodesPaginated",
            Command::GraphAddEdge { .. } => "GraphAddEdge",
            Command::GraphRemoveEdge { .. } => "GraphRemoveEdge",
            Command::GraphNeighbors { .. } => "GraphNeighbors",
            Command::GraphBulkInsert { .. } => "GraphBulkInsert",
            Command::GraphBfs { .. } => "GraphBfs",
            Command::GraphDefineObjectType { .. } => "GraphDefineObjectType",
            Command::GraphGetObjectType { .. } => "GraphGetObjectType",
            Command::GraphListObjectTypes { .. } => "GraphListObjectTypes",
            Command::GraphListOntologyTypes { .. } => "GraphListOntologyTypes",
            Command::GraphDeleteObjectType { .. } => "GraphDeleteObjectType",
            Command::GraphDefineLinkType { .. } => "GraphDefineLinkType",
            Command::GraphGetLinkType { .. } => "GraphGetLinkType",
            Command::GraphListLinkTypes { .. } => "GraphListLinkTypes",
            Command::GraphDeleteLinkType { .. } => "GraphDeleteLinkType",
            Command::GraphFreezeOntology { .. } => "GraphFreezeOntology",
            Command::GraphOntologyStatus { .. } => "GraphOntologyStatus",
            Command::GraphOntologySummary { .. } => "GraphOntologySummary",
            Command::GraphNodesByType { .. } => "GraphNodesByType",
            Command::GraphWcc { .. } => "GraphWcc",
            Command::GraphCdlp { .. } => "GraphCdlp",
            Command::GraphPagerank { .. } => "GraphPagerank",
            Command::GraphLcc { .. } => "GraphLcc",
            Command::GraphSssp { .. } => "GraphSssp",
        }
    }

    /// Fill in the default branch and space for any data command where they are `None`.
    ///
    /// Called by the executor before dispatch so handlers always receive a
    /// concrete `BranchId` and space name.
    pub fn resolve_defaults(&mut self) {
        macro_rules! resolve_branch {
            ($branch:expr) => {
                if $branch.is_none() {
                    *$branch = Some(BranchId::default());
                }
            };
        }
        macro_rules! resolve_space {
            ($space:expr) => {
                if $space.is_none() {
                    *$space = Some("default".to_string());
                }
            };
        }

        match self {
            // KV
            Command::KvPut { branch, space, .. }
            | Command::KvBatchPut { branch, space, .. }
            | Command::KvGet { branch, space, .. }
            | Command::KvDelete { branch, space, .. }
            | Command::KvList { branch, space, .. }
            | Command::KvGetv { branch, space, .. }
            // JSON
            | Command::JsonSet { branch, space, .. }
            | Command::JsonBatchSet { branch, space, .. }
            | Command::JsonBatchGet { branch, space, .. }
            | Command::JsonBatchDelete { branch, space, .. }
            | Command::JsonGet { branch, space, .. }
            | Command::JsonGetv { branch, space, .. }
            | Command::JsonDelete { branch, space, .. }
            | Command::JsonList { branch, space, .. }
            // Event
            | Command::EventAppend { branch, space, .. }
            | Command::EventBatchAppend { branch, space, .. }
            | Command::EventGet { branch, space, .. }
            | Command::EventGetByType { branch, space, .. }
            | Command::EventLen { branch, space, .. }
            // State
            | Command::StateSet { branch, space, .. }
            | Command::StateBatchSet { branch, space, .. }
            | Command::StateGet { branch, space, .. }
            | Command::StateGetv { branch, space, .. }
            | Command::StateCas { branch, space, .. }
            | Command::StateInit { branch, space, .. }
            | Command::StateDelete { branch, space, .. }
            | Command::StateList { branch, space, .. }
            // Vector (7 MVP)
            | Command::VectorUpsert { branch, space, .. }
            | Command::VectorGet { branch, space, .. }
            | Command::VectorDelete { branch, space, .. }
            | Command::VectorSearch { branch, space, .. }
            | Command::VectorCreateCollection { branch, space, .. }
            | Command::VectorDeleteCollection { branch, space, .. }
            | Command::VectorListCollections { branch, space, .. }
            | Command::VectorCollectionStats { branch, space, .. }
            | Command::VectorBatchUpsert { branch, space, .. }
            // Intelligence
            | Command::Search { branch, space, .. }
            // Data introspection
            | Command::KvCount { branch, space, .. }
            | Command::JsonCount { branch, space, .. }
            | Command::KvSample { branch, space, .. }
            | Command::JsonSample { branch, space, .. }
            | Command::VectorSample { branch, space, .. }
            // Export
            | Command::DbExport { branch, space, .. } => {
                resolve_branch!(branch);
                resolve_space!(space);
            }

            // Retention, Transaction begin, TimeRange, Describe — only have branch, no space
            Command::RetentionApply { branch, .. }
            | Command::RetentionStats { branch, .. }
            | Command::RetentionPreview { branch, .. }
            | Command::TxnBegin { branch, .. }
            | Command::TimeRange { branch, .. }
            | Command::Describe { branch, .. } => {
                resolve_branch!(branch);
            }

            // Space commands — only have branch, space is explicit
            Command::SpaceList { branch, .. }
            | Command::SpaceCreate { branch, .. }
            | Command::SpaceDelete { branch, .. }
            | Command::SpaceExists { branch, .. }
            | Command::ReindexEmbeddings { branch, .. } => {
                resolve_branch!(branch);
            }

            // Graph commands — only have branch
            Command::GraphCreate { branch, .. }
            | Command::GraphDelete { branch, .. }
            | Command::GraphList { branch, .. }
            | Command::GraphGetMeta { branch, .. }
            | Command::GraphAddNode { branch, .. }
            | Command::GraphGetNode { branch, .. }
            | Command::GraphRemoveNode { branch, .. }
            | Command::GraphListNodes { branch, .. }
            | Command::GraphListNodesPaginated { branch, .. }
            | Command::GraphAddEdge { branch, .. }
            | Command::GraphRemoveEdge { branch, .. }
            | Command::GraphNeighbors { branch, .. }
            | Command::GraphBulkInsert { branch, .. }
            | Command::GraphBfs { branch, .. }
            | Command::GraphDefineObjectType { branch, .. }
            | Command::GraphGetObjectType { branch, .. }
            | Command::GraphListObjectTypes { branch, .. }
            | Command::GraphListOntologyTypes { branch, .. }
            | Command::GraphDeleteObjectType { branch, .. }
            | Command::GraphDefineLinkType { branch, .. }
            | Command::GraphGetLinkType { branch, .. }
            | Command::GraphListLinkTypes { branch, .. }
            | Command::GraphDeleteLinkType { branch, .. }
            | Command::GraphFreezeOntology { branch, .. }
            | Command::GraphOntologyStatus { branch, .. }
            | Command::GraphOntologySummary { branch, .. }
            | Command::GraphNodesByType { branch, .. }
            | Command::GraphWcc { branch, .. }
            | Command::GraphCdlp { branch, .. }
            | Command::GraphPagerank { branch, .. }
            | Command::GraphLcc { branch, .. }
            | Command::GraphSssp { branch, .. } => {
                resolve_branch!(branch);
            }

            // Branch lifecycle, Transaction, and Database commands have no
            // optional branch to resolve.
            Command::BranchCreate { .. }
            | Command::BranchGet { .. }
            | Command::BranchList { .. }
            | Command::BranchExists { .. }
            | Command::BranchDelete { .. }
            | Command::BranchFork { .. }
            | Command::BranchDiff { .. }
            | Command::BranchMerge { .. }
            | Command::TxnCommit
            | Command::TxnRollback
            | Command::TxnInfo
            | Command::TxnIsActive
            | Command::Ping
            | Command::Info
            | Command::Health
            | Command::Flush
            | Command::Compact
            | Command::EmbedStatus
            | Command::ConfigGet
            | Command::ConfigSetAutoEmbed { .. }
            | Command::AutoEmbedStatus
            | Command::DurabilityCounters
            | Command::Embed { .. }
            | Command::EmbedBatch { .. }
            | Command::ModelsList
            | Command::ModelsPull { .. }
            | Command::Generate { .. }
            | Command::Tokenize { .. }
            | Command::Detokenize { .. }
            | Command::GenerateUnload { .. }
            | Command::ModelsLocal
            | Command::BranchExport { .. }
            | Command::BranchImport { .. }
            | Command::BranchBundleValidate { .. }
            | Command::ConfigureModel { .. }
            | Command::ConfigureSet { .. }
            | Command::ConfigureGetKey { .. } => {}
        }
    }

    /// Backwards-compatible alias for resolve_defaults
    pub fn resolve_default_branch(&mut self) {
        self.resolve_defaults();
    }

    /// Return the resolved branch for data-scoped commands, if any.
    ///
    /// Must be called *after* `resolve_defaults()`. Returns `None` for
    /// branch-lifecycle, transaction, and database-level commands.
    pub fn resolved_branch(&self) -> Option<&BranchId> {
        match self {
            // Data commands with branch + space
            Command::KvPut { branch, .. }
            | Command::KvBatchPut { branch, .. }
            | Command::KvGet { branch, .. }
            | Command::KvDelete { branch, .. }
            | Command::KvList { branch, .. }
            | Command::KvGetv { branch, .. }
            | Command::JsonSet { branch, .. }
            | Command::JsonBatchSet { branch, .. }
            | Command::JsonBatchGet { branch, .. }
            | Command::JsonBatchDelete { branch, .. }
            | Command::JsonGet { branch, .. }
            | Command::JsonGetv { branch, .. }
            | Command::JsonDelete { branch, .. }
            | Command::JsonList { branch, .. }
            | Command::EventAppend { branch, .. }
            | Command::EventBatchAppend { branch, .. }
            | Command::EventGet { branch, .. }
            | Command::EventGetByType { branch, .. }
            | Command::EventLen { branch, .. }
            | Command::StateSet { branch, .. }
            | Command::StateBatchSet { branch, .. }
            | Command::StateGet { branch, .. }
            | Command::StateGetv { branch, .. }
            | Command::StateCas { branch, .. }
            | Command::StateInit { branch, .. }
            | Command::StateDelete { branch, .. }
            | Command::StateList { branch, .. }
            | Command::VectorUpsert { branch, .. }
            | Command::VectorGet { branch, .. }
            | Command::VectorDelete { branch, .. }
            | Command::VectorSearch { branch, .. }
            | Command::VectorCreateCollection { branch, .. }
            | Command::VectorDeleteCollection { branch, .. }
            | Command::VectorListCollections { branch, .. }
            | Command::VectorCollectionStats { branch, .. }
            | Command::VectorBatchUpsert { branch, .. }
            | Command::Search { branch, .. }
            | Command::KvCount { branch, .. }
            | Command::JsonCount { branch, .. }
            | Command::KvSample { branch, .. }
            | Command::JsonSample { branch, .. }
            | Command::VectorSample { branch, .. }
            | Command::DbExport { branch, .. } => branch.as_ref(),

            // Commands with branch only (no space)
            Command::RetentionApply { branch, .. }
            | Command::RetentionStats { branch, .. }
            | Command::RetentionPreview { branch, .. }
            | Command::TxnBegin { branch, .. }
            | Command::TimeRange { branch, .. }
            | Command::Describe { branch, .. } => branch.as_ref(),

            // Space commands + reindex
            Command::SpaceList { branch, .. }
            | Command::SpaceCreate { branch, .. }
            | Command::SpaceDelete { branch, .. }
            | Command::SpaceExists { branch, .. }
            | Command::ReindexEmbeddings { branch, .. } => branch.as_ref(),

            // Graph commands
            Command::GraphCreate { branch, .. }
            | Command::GraphDelete { branch, .. }
            | Command::GraphList { branch, .. }
            | Command::GraphGetMeta { branch, .. }
            | Command::GraphAddNode { branch, .. }
            | Command::GraphGetNode { branch, .. }
            | Command::GraphRemoveNode { branch, .. }
            | Command::GraphListNodes { branch, .. }
            | Command::GraphListNodesPaginated { branch, .. }
            | Command::GraphAddEdge { branch, .. }
            | Command::GraphRemoveEdge { branch, .. }
            | Command::GraphNeighbors { branch, .. }
            | Command::GraphBulkInsert { branch, .. }
            | Command::GraphBfs { branch, .. }
            | Command::GraphDefineObjectType { branch, .. }
            | Command::GraphGetObjectType { branch, .. }
            | Command::GraphListObjectTypes { branch, .. }
            | Command::GraphListOntologyTypes { branch, .. }
            | Command::GraphDeleteObjectType { branch, .. }
            | Command::GraphDefineLinkType { branch, .. }
            | Command::GraphGetLinkType { branch, .. }
            | Command::GraphListLinkTypes { branch, .. }
            | Command::GraphDeleteLinkType { branch, .. }
            | Command::GraphFreezeOntology { branch, .. }
            | Command::GraphOntologyStatus { branch, .. }
            | Command::GraphOntologySummary { branch, .. }
            | Command::GraphNodesByType { branch, .. }
            | Command::GraphWcc { branch, .. }
            | Command::GraphCdlp { branch, .. }
            | Command::GraphPagerank { branch, .. }
            | Command::GraphLcc { branch, .. }
            | Command::GraphSssp { branch, .. } => branch.as_ref(),

            // Branch lifecycle, Transaction, Database — no data branch
            _ => None,
        }
    }
}
