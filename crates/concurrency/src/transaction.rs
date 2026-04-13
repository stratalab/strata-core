//! Transaction context for OCC
//!
//! This module implements the core transaction data structure for optimistic
//! concurrency control. TransactionContext tracks all reads, writes, deletes,
//! and CAS operations for a transaction, enabling validation at commit time.
//!
//! See `docs/architecture/M2_TRANSACTION_SEMANTICS.md` for the full specification.

use crate::validation::{validate_transaction, ValidationResult};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use strata_core::id::{CommitVersion, TxnId};
use strata_core::primitives::json::{get_at_path, JsonPatch, JsonPath, JsonValue};
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{BranchId, Key};
use strata_core::value::Value;
use strata_core::StrataError;
use strata_core::StrataResult;
use strata_core::{Version, Versioned, VersionedValue};
use strata_storage::SegmentedStore;

/// Error type for commit failures
///
/// Per spec Core Invariants:
/// - All-or-nothing commit: transaction either commits or aborts entirely
/// - First-committer-wins: conflicts are detected based on read-set
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum CommitError {
    /// Transaction aborted due to validation conflicts
    ///
    /// Per spec Section 3: Conflicts detected in read-set or CAS-set
    ValidationFailed(ValidationResult),

    /// Transaction was not in correct state for commit
    ///
    /// Commit requires Active state to transition to Validating
    InvalidState(String),

    /// WAL write failed during commit
    ///
    /// Per spec Section 5: WAL must be written before storage for durability.
    /// If WAL write fails, the transaction cannot be durably committed.
    WALError(String),

    /// Storage error during validation
    ///
    /// A storage I/O error occurred while reading current versions for
    /// conflict detection. The transaction is aborted to prevent incorrect commits.
    StorageError(String),

    /// Counter overflow (transaction ID or version counter reached u64::MAX)
    CounterOverflow(String),

    /// Storage application failed after WAL commit (#1725)
    ///
    /// The transaction IS durable (WAL record written) and will be recovered
    /// on restart, but it is NOT visible to reads in the current process.
    /// The caller must not assume the data is immediately readable.
    DurableButNotVisible(String),

    /// Branch is being deleted (#1916)
    ///
    /// The target branch has been marked for deletion. Commits are rejected
    /// to prevent data resurrection on a deleted branch.
    BranchDeleting(strata_core::types::BranchId),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::ValidationFailed(result) => {
                write!(f, "Commit failed: {} conflict(s)", result.conflict_count())
            }
            CommitError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            CommitError::WALError(msg) => write!(f, "WAL error: {}", msg),
            CommitError::StorageError(msg) => write!(f, "Storage error during validation: {}", msg),
            CommitError::CounterOverflow(msg) => write!(f, "Counter overflow: {}", msg),
            CommitError::DurableButNotVisible(msg) => {
                write!(
                    f,
                    "Durable but not visible (will recover on restart): {}",
                    msg
                )
            }
            CommitError::BranchDeleting(branch_id) => {
                write!(f, "Branch {} is being deleted", branch_id)
            }
        }
    }
}

impl std::error::Error for CommitError {}

// Conversion to StrataError
impl From<CommitError> for StrataError {
    fn from(e: CommitError) -> Self {
        match e {
            CommitError::ValidationFailed(result) => StrataError::TransactionAborted {
                reason: format!("Validation failed: {} conflict(s)", result.conflict_count()),
            },
            CommitError::InvalidState(msg) => StrataError::TransactionNotActive { state: msg },
            CommitError::WALError(msg) => StrataError::Storage {
                message: format!("WAL error: {}", msg),
                source: None,
            },
            CommitError::StorageError(msg) => StrataError::Storage {
                message: format!("Storage error during validation: {}", msg),
                source: None,
            },
            CommitError::CounterOverflow(msg) => {
                StrataError::capacity_exceeded(msg, usize::MAX, usize::MAX)
            }
            CommitError::DurableButNotVisible(msg) => StrataError::Storage {
                message: format!("Durable but not visible (will recover on restart): {}", msg),
                source: None,
            },
            CommitError::BranchDeleting(branch_id) => StrataError::TransactionAborted {
                reason: format!("Branch {} is being deleted", branch_id),
            },
        }
    }
}

/// Result of applying transaction writes to storage
///
/// Per spec Section 6.1: All keys in a transaction get the same commit version.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    /// Version assigned to all writes in this transaction
    pub commit_version: CommitVersion,
    /// Number of puts applied
    pub puts_applied: usize,
    /// Number of deletes applied
    pub deletes_applied: usize,
    /// Number of CAS operations applied
    pub cas_applied: usize,
}

impl ApplyResult {
    /// Total number of operations applied
    pub fn total_operations(&self) -> usize {
        self.puts_applied + self.deletes_applied + self.cas_applied
    }
}

/// Summary of pending operations that would be rolled back on abort
///
/// This is useful for debugging, logging, or providing feedback before
/// aborting a transaction. It shows what operations are buffered and
/// would be discarded if the transaction were aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingOperations {
    /// Number of pending put operations
    pub puts: usize,
    /// Number of pending delete operations
    pub deletes: usize,
    /// Number of pending CAS operations
    pub cas: usize,
}

impl PendingOperations {
    /// Total number of pending operations
    pub fn total(&self) -> usize {
        self.puts + self.deletes + self.cas
    }

    /// Check if there are no pending operations
    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

/// Status of a transaction in its lifecycle
///
/// State transitions:
/// - `Active` → `Validating` (begin commit)
/// - `Validating` → `Committed` (validation passed)
/// - `Validating` → `Aborted` (conflict detected)
/// - `Active` → `Aborted` (user abort or error)
///
/// Terminal states (no transitions allowed):
/// - `Committed`
/// - `Aborted`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Transaction is executing, can read/write
    Active,
    /// Transaction is being validated for conflicts
    Validating,
    /// Transaction committed successfully
    Committed,
    /// Transaction was aborted
    Aborted {
        /// Human-readable reason for abort
        reason: String,
    },
}

/// A compare-and-swap operation to be validated at commit
///
/// CAS operations are buffered until commit time. At commit:
/// 1. Validate that the key's current version equals `expected_version`
/// 2. If valid, write `new_value`
/// 3. If invalid, abort the transaction
///
/// Note: CAS does NOT automatically add to read_set. If you want read-set
/// protection in addition to CAS validation, explicitly read the key first.
#[derive(Debug, Clone)]
pub struct CASOperation {
    /// Key to CAS
    pub key: Key,
    /// Expected version (ZERO = key must not exist)
    pub expected_version: CommitVersion,
    /// New value to write if version matches
    pub new_value: Value,
}

// ============================================================================
// JSON Transaction Types (M5 Epic 30)
// ============================================================================

/// Record of a JSON path read (for conflict detection)
///
/// Tracks which paths within JSON documents were read during a transaction.
/// Used for fine-grained conflict detection at commit time.
#[derive(Debug, Clone)]
pub struct JsonPathRead {
    /// Key of the JSON document
    pub key: Key,
    /// Path that was read
    pub path: JsonPath,
    /// Version of the document when read
    pub version: CommitVersion,
}

impl JsonPathRead {
    /// Create a new JSON path read record
    pub fn new(key: Key, path: JsonPath, version: CommitVersion) -> Self {
        Self { key, path, version }
    }
}

/// Record of a JSON patch operation (for commit)
///
/// Stores a patch to be applied to a JSON document at commit time.
/// Patches are applied in order to compute the final document state.
#[derive(Debug, Clone)]
pub struct JsonPatchEntry {
    /// Key of the JSON document
    pub key: Key,
    /// Patch to apply
    pub patch: JsonPatch,
    /// Version the document will have after this patch
    pub resulting_version: CommitVersion,
}

impl JsonPatchEntry {
    /// Create a new JSON patch entry
    pub fn new(key: Key, patch: JsonPatch, resulting_version: CommitVersion) -> Self {
        Self {
            key,
            patch,
            resulting_version,
        }
    }
}

// ============================================================================
// JsonStoreExt Trait (M5 Epic 30)
// ============================================================================

/// Extension trait for JSON operations within transactions (M5 Rule 3)
///
/// This trait enables JSON operations to be performed within a TransactionContext,
/// allowing atomic cross-primitive transactions between JSON and other primitives.
///
/// # Architecture (M5 Architecture Rule 3)
///
/// Per M5 Rule 3: "Add `JsonStoreExt` trait to TransactionContext. NO separate
/// JsonTransaction type." This enables cross-primitive atomic transactions
/// without additional coordination.
///
/// # Usage
///
/// ```no_run
/// # use strata_concurrency::{TransactionContext, JsonStoreExt};
/// # use strata_core::types::{BranchId, Key, Namespace, TypeTag};
/// # use strata_core::value::Value;
/// # use strata_core::primitives::json::JsonPath;
/// # use std::sync::Arc;
/// # fn example(txn: &mut TransactionContext) -> strata_core::StrataResult<()> {
/// # let ns = Arc::new(Namespace::for_branch(BranchId::default()));
/// # let key = Key::new(ns.clone(), TypeTag::Json, b"doc".to_vec());
/// # let path = JsonPath::root();
/// # let other_key = Key::new_kv(ns, "other");
/// // JSON operation
/// let value = txn.json_get(&key, &path)?;
/// txn.json_set(&key, &path, serde_json::json!({"updated": true}).into())?;
///
/// // KV operation in same transaction
/// txn.put(other_key, Value::Bytes(b"done".to_vec()))?;
/// # Ok(())
/// # }
/// ```
///
/// # Read-Your-Writes
///
/// JSON operations support read-your-writes semantics:
/// - `json_set` writes are visible to subsequent `json_get` calls in the same transaction
/// - Writes are buffered until commit
///
/// # Conflict Detection
///
/// JSON reads and writes are tracked for region-based conflict detection:
/// - All reads track the document version at read time
/// - At commit time, if any read document's version has changed, conflict is detected
pub trait JsonStoreExt {
    /// Get a value at a JSON path within a document
    ///
    /// # Arguments
    /// * `key` - Key of the JSON document
    /// * `path` - JSON path to read from
    ///
    /// # Returns
    /// - `Ok(Some(value))` if path exists
    /// - `Ok(None)` if document exists but path doesn't
    /// - `Err` if document doesn't exist or transaction is invalid
    fn json_get(&mut self, key: &Key, path: &JsonPath) -> StrataResult<Option<JsonValue>>;

    /// Set a value at a JSON path within a document
    ///
    /// # Arguments
    /// * `key` - Key of the JSON document
    /// * `path` - JSON path to write to
    /// * `value` - Value to set
    ///
    /// # Returns
    /// - `Ok(())` on success
    /// - `Err` if document doesn't exist or transaction is invalid
    fn json_set(&mut self, key: &Key, path: &JsonPath, value: JsonValue) -> StrataResult<()>;

    /// Delete a value at a JSON path within a document
    ///
    /// # Arguments
    /// * `key` - Key of the JSON document
    /// * `path` - JSON path to delete
    ///
    /// # Returns
    /// - `Ok(())` on success (even if path didn't exist)
    /// - `Err` if document doesn't exist or transaction is invalid
    fn json_delete(&mut self, key: &Key, path: &JsonPath) -> StrataResult<()>;

    /// Get the entire JSON document
    ///
    /// # Arguments
    /// * `key` - Key of the JSON document
    ///
    /// # Returns
    /// - `Ok(Some(value))` if document exists
    /// - `Ok(None)` if document doesn't exist
    fn json_get_document(&mut self, key: &Key) -> StrataResult<Option<JsonValue>>;

    /// Check if a JSON document exists
    ///
    /// # Arguments
    /// * `key` - Key of the JSON document
    fn json_exists(&mut self, key: &Key) -> StrataResult<bool>;
}

/// Transaction context for OCC with snapshot isolation
///
/// Tracks all reads, writes, deletes, and CAS operations for a transaction.
/// Validation and commit happen at transaction end.
///
/// # Read-Your-Writes Semantics
///
/// When reading a key, the transaction checks in order:
/// 1. **write_set**: Returns uncommitted write from this transaction
/// 2. **delete_set**: Returns None for uncommitted delete
/// 3. **snapshot**: Returns value from snapshot, tracks in read_set
///
/// # Read-Set Tracking
///
/// All reads from the snapshot are tracked in `read_set` with the version read.
/// At commit time, these versions are validated against current storage.
/// If any version changed, the transaction has a read-write conflict.
///
/// # Lifecycle
///
/// 1. **BEGIN**: Create with `with_store()`, status is `Active`
/// 2. **READ/WRITE**: Use `get()`, `put()`, `delete()`, `cas()`
/// 3. **VALIDATE**: Call `mark_validating()`, check for conflicts
/// 4. **COMMIT/ABORT**: Call `mark_committed()` or `mark_aborted()`
///
/// # Future: Savepoints
///
/// A future enhancement could add savepoint support (partial rollback
/// within a transaction). This would allow `SAVEPOINT name` /
/// `ROLLBACK TO name` semantics by snapshotting the write/delete/cas
/// sets at savepoint time and restoring them on rollback. Deferred
/// to a later milestone.
pub struct TransactionContext {
    // Identity
    /// Unique transaction ID
    pub txn_id: TxnId,
    /// Branch this transaction belongs to
    pub branch_id: BranchId,

    // Snapshot isolation
    /// Version at transaction start (snapshot version)
    ///
    /// All reads see data as of this version. Used for conflict detection.
    pub start_version: CommitVersion,

    /// Backing store for snapshot reads
    ///
    /// Reads are bounded by `start_version` for MVCC isolation.
    store: Option<Arc<SegmentedStore>>,

    // Operation tracking
    /// Keys read and their versions (for validation)
    ///
    /// At commit time, we check that each key's current version still matches
    /// the version we read. If not, there's a read-write conflict.
    ///
    /// Version 0 means the key did not exist when read.
    pub read_set: HashMap<Key, CommitVersion>,

    /// Keys written with their new values (buffered)
    ///
    /// These writes are not visible to other transactions until commit.
    /// At commit, they are applied atomically to storage.
    pub write_set: HashMap<Key, Value>,

    /// Keys to delete (buffered)
    ///
    /// Deletes are buffered like writes. A deleted key returns None
    /// when read within this transaction (read-your-deletes).
    pub delete_set: HashSet<Key>,

    /// CAS operations to validate and apply
    ///
    /// Each CAS is validated at commit time against the current storage
    /// version, independent of the read_set.
    pub cas_set: Vec<CASOperation>,

    // Event state tracking (lazy allocation, like JSON fields)
    /// Tracks the cumulative event sequence count across Transaction instances.
    /// This allows multiple Transaction::new() calls within the same session
    /// transaction to continue from the correct sequence number.
    event_sequence_count: Option<u64>,

    /// Tracks the last event hash for hash chaining across Transaction instances.
    event_last_hash: Option<[u8; 32]>,

    // JSON Operations (M5 - lazy allocation for zero overhead when not using JSON)
    /// JSON path reads for fine-grained conflict detection
    ///
    /// Only allocated when JSON operations are performed.
    json_reads: Option<Vec<JsonPathRead>>,

    /// JSON patches to apply at commit
    ///
    /// Only allocated when JSON operations are performed.
    json_writes: Option<Vec<JsonPatchEntry>>,

    /// Snapshot versions of JSON documents at read time
    ///
    /// Maps document key to the version observed during read.
    /// Only allocated when JSON operations are performed.
    json_snapshot_versions: Option<HashMap<Key, CommitVersion>>,

    // State
    /// Current transaction status
    pub status: TransactionStatus,

    // Timing
    /// When this transaction was created
    start_time: Instant,

    /// Maximum entries allowed in the write buffer (0 = unlimited).
    /// Counts puts + deletes + CAS operations.
    max_write_entries: usize,

    /// Per-transaction read-only mode.
    ///
    /// When true, writes are rejected and read-set tracking is skipped,
    /// saving memory on large scan workloads.
    read_only: bool,

    /// Per-key write mode overrides. Keys not present use WriteMode::Append.
    ///
    /// Keys in this map will use the specified WriteMode at commit time
    /// instead of the default Append. Used for internal data like graph
    /// adjacency lists where version history wastes memory.
    key_write_modes: HashMap<Key, WriteMode>,

    /// Per-key TTL in milliseconds. Keys not present have no TTL (ttl_ms = 0).
    pub ttl_map: HashMap<Key, u64>,

    /// Allow operations on keys from branches other than `self.branch_id`.
    ///
    /// Default `false` — all keys must match the transaction's branch.
    /// Internal admin operations (branch deletion) set this to `true`
    /// because they intentionally reach into other branches' namespaces.
    ///
    /// **WARNING**: Cross-branch reads are NOT protected by the per-branch
    /// commit lock, so TOCTOU is possible. Only enable for operations where
    /// concurrent access to the target branch is ruled out by design.
    allow_cross_branch: bool,
}

impl TransactionContext {
    /// Create a new transaction context without a snapshot
    ///
    /// This constructor is primarily for testing or for transactions
    /// that don't need to read from storage.
    ///
    /// For normal transactions, use `with_store()`.
    ///
    /// # Arguments
    /// * `txn_id` - Unique transaction identifier
    /// * `branch_id` - Branch this transaction belongs to
    /// * `start_version` - Snapshot version at transaction start
    ///
    /// # Example
    ///
    /// ```
    /// use strata_concurrency::TransactionContext;
    /// use strata_core::types::BranchId;
    /// use strata_core::id::{TxnId, CommitVersion};
    ///
    /// let branch_id = BranchId::new();
    /// let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
    /// assert!(txn.is_active());
    /// ```
    pub fn new(txn_id: TxnId, branch_id: BranchId, start_version: CommitVersion) -> Self {
        TransactionContext {
            txn_id,
            branch_id,
            start_version,
            store: None,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            delete_set: HashSet::new(),
            cas_set: Vec::new(),
            event_sequence_count: None,
            event_last_hash: None,
            json_reads: None,
            json_writes: None,
            json_snapshot_versions: None,
            status: TransactionStatus::Active,
            start_time: Instant::now(),
            max_write_entries: 0,
            read_only: false,
            key_write_modes: HashMap::new(),
            ttl_map: HashMap::new(),
            allow_cross_branch: false,
        }
    }

    /// Create a new transaction context with a snapshot
    ///
    /// This is the primary constructor for transactions that need to read
    /// from storage. The snapshot provides a consistent point-in-time view.
    ///
    /// # Arguments
    /// * `txn_id` - Unique transaction identifier
    /// * `branch_id` - Branch this transaction belongs to
    /// * `snapshot` - Snapshot view for this transaction
    ///
    /// # Example
    ///
    /// ```
    /// use strata_concurrency::TransactionContext;
    /// use strata_core::types::BranchId;
    /// use strata_core::id::TxnId;
    /// use strata_storage::SegmentedStore;
    /// use std::sync::Arc;
    ///
    /// let branch_id = BranchId::new();
    /// let store = Arc::new(SegmentedStore::new());
    /// let txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
    /// assert!(txn.is_active());
    /// ```
    pub fn with_store(txn_id: TxnId, branch_id: BranchId, store: Arc<SegmentedStore>) -> Self {
        let start_version = store.current_version();
        TransactionContext {
            txn_id,
            branch_id,
            start_version,
            store: Some(store),
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            delete_set: HashSet::new(),
            cas_set: Vec::new(),
            event_sequence_count: None,
            event_last_hash: None,
            json_reads: None,
            json_writes: None,
            json_snapshot_versions: None,
            status: TransactionStatus::Active,
            start_time: Instant::now(),
            max_write_entries: 0,
            read_only: false,
            key_write_modes: HashMap::new(),
            ttl_map: HashMap::new(),
            allow_cross_branch: false,
        }
    }

    // === Read Operations ===

    /// Get a value from the transaction
    ///
    /// Implements read-your-writes semantics:
    /// 1. Check write_set (uncommitted writes from this txn) - NO read_set entry
    /// 2. Check delete_set (uncommitted deletes from this txn) - NO read_set entry
    /// 3. Read from snapshot - tracks in read_set
    ///
    /// # Read-Set Tracking
    ///
    /// Only reads from the snapshot are tracked in read_set:
    /// - If key exists in snapshot: tracks `(key, version)`
    /// - If key doesn't exist in snapshot: tracks `(key, 0)`
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::types::{BranchId, Key, Namespace};
    /// # use std::sync::Arc;
    /// # fn example(txn: &mut TransactionContext) -> strata_core::StrataResult<()> {
    /// # let key = Key::new_kv(Arc::new(Namespace::for_branch(BranchId::default())), "key");
    /// let value = txn.get(&key)?;
    /// if let Some(v) = value {
    ///     // Process value
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get(&mut self, key: &Key) -> StrataResult<Option<Value>> {
        self.guard(key)?;

        // 1. Check write_set first (read-your-writes)
        // No read_set entry - we're reading our own uncommitted write
        if let Some(value) = self.write_set.get(key) {
            return Ok(Some(value.clone()));
        }

        // 2. Check delete_set (return None if deleted in this txn)
        // No read_set entry - we're reading our own uncommitted delete
        if self.delete_set.contains(key) {
            return Ok(None);
        }

        // 3. Read from snapshot
        self.read_from_snapshot(key)
    }

    /// Read from store and track in read_set
    ///
    /// This is the core read path that tracks reads for conflict detection.
    /// Reads are bounded by `start_version` for MVCC isolation.
    fn read_from_snapshot(&mut self, key: &Key) -> StrataResult<Option<Value>> {
        let store = self.store.as_ref().ok_or_else(|| {
            StrataError::invalid_input("Transaction has no store for reads".to_string())
        })?;

        match store.get_versioned(key, self.start_version)? {
            Some(vv) => {
                // Key exists - track its version for conflict detection
                if !self.read_only {
                    self.read_set
                        .insert(key.clone(), CommitVersion(vv.version.as_u64()));
                }
                Ok(Some(vv.value))
            }
            None => {
                // Key doesn't exist - track with version 0
                // This is important: if someone creates this key before we commit,
                // we have a conflict (we assumed it didn't exist)
                if !self.read_only {
                    self.read_set.insert(key.clone(), CommitVersion::ZERO);
                }
                Ok(None)
            }
        }
    }

    /// Get a value with version metadata from the transaction
    ///
    /// Implements read-your-writes semantics like `get()` but preserves
    /// version information:
    /// 1. **write_set hit:** returns `VersionedValue` with `Version::Txn(0)` placeholder
    /// 2. **delete_set hit:** returns `None`
    /// 3. **snapshot read:** returns full `VersionedValue` from snapshot, tracks in read_set
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    pub fn get_versioned(&mut self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        self.guard(key)?;

        // 1. Check write_set first (read-your-writes)
        if let Some(value) = self.write_set.get(key) {
            return Ok(Some(Versioned {
                value: value.clone(),
                version: Version::Txn(0),
                timestamp: strata_core::Timestamp::from_micros(0),
            }));
        }

        // 2. Check delete_set (return None if deleted in this txn)
        if self.delete_set.contains(key) {
            return Ok(None);
        }

        // 3. Read from snapshot with version info
        self.get_versioned_from_snapshot(key)
    }

    /// Read from store preserving version metadata, and track in read_set
    fn get_versioned_from_snapshot(&mut self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        let store = self.store.as_ref().ok_or_else(|| {
            StrataError::invalid_input("Transaction has no store for reads".to_string())
        })?;

        let versioned = store.get_versioned(key, self.start_version)?;

        // Track in read_set for conflict detection (skip in read-only mode)
        if !self.read_only {
            if let Some(ref vv) = versioned {
                self.read_set
                    .insert(key.clone(), CommitVersion(vv.version.as_u64()));
            } else {
                self.read_set.insert(key.clone(), CommitVersion::ZERO);
            }
        }

        Ok(versioned)
    }

    /// Check if a key exists in the transaction's view
    ///
    /// This is a convenience method that calls `get()` and checks for Some.
    /// Note: This DOES track the read in read_set if the key is read from snapshot.
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    pub fn exists(&mut self, key: &Key) -> StrataResult<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Scan keys with a prefix
    ///
    /// Returns all keys matching the prefix, implementing read-your-writes:
    /// - Includes uncommitted writes from this transaction matching prefix
    /// - Excludes uncommitted deletes from this transaction
    /// - Tracks all scanned keys from snapshot in read_set
    ///
    /// Results are sorted by key order.
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active or has no snapshot.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::types::{BranchId, Key, Namespace};
    /// # use std::sync::Arc;
    /// # fn example(txn: &mut TransactionContext) -> strata_core::StrataResult<()> {
    /// let namespace = Arc::new(Namespace::for_branch(BranchId::default()));
    /// let prefix = Key::new_kv(namespace, "user:");
    /// let users = txn.scan_prefix(&prefix)?;
    /// for (key, value) in users {
    ///     // Process each user
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan_prefix(&mut self, prefix: &Key) -> StrataResult<Vec<(Key, Value)>> {
        self.guard(prefix)?;

        let store = self.store.as_ref().ok_or_else(|| {
            StrataError::invalid_input("Transaction has no store for reads".to_string())
        })?;

        // Get all matching keys from store (bounded by start_version)
        // Storage returns results already sorted by Key via MergeIterator + MvccIterator.
        let snapshot_results = store.scan_prefix(prefix, self.start_version)?;

        // Pre-allocate read_set capacity to reduce rehashing
        if !self.read_only {
            self.read_set.reserve(snapshot_results.len());
        }

        // Track all snapshot keys in read_set, filter out deleted keys
        let mut snapshot_filtered: Vec<(Key, Value)> = Vec::with_capacity(snapshot_results.len());
        for (key, vv) in snapshot_results {
            if !self.read_only {
                self.read_set
                    .insert(key.clone(), CommitVersion(vv.version.as_u64()));
            }
            if !self.delete_set.contains(&key) {
                snapshot_filtered.push((key, vv.value));
            }
        }

        // Collect write_set entries matching prefix
        let mut write_matches: Vec<(Key, Value)> = self
            .write_set
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        // Fast path: no buffered writes overlap, snapshot is already sorted
        if write_matches.is_empty() {
            return Ok(snapshot_filtered);
        }

        write_matches.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Sorted merge: write_matches takes precedence on key collision
        let mut result = Vec::with_capacity(snapshot_filtered.len() + write_matches.len());
        let mut snap_iter = snapshot_filtered.into_iter().peekable();
        let mut write_iter = write_matches.into_iter().peekable();

        while let (Some((sk, _)), Some((wk, _))) = (snap_iter.peek(), write_iter.peek()) {
            match sk.cmp(wk) {
                std::cmp::Ordering::Less => {
                    result.push(snap_iter.next().unwrap());
                }
                std::cmp::Ordering::Greater => {
                    result.push(write_iter.next().unwrap());
                }
                std::cmp::Ordering::Equal => {
                    snap_iter.next(); // discard snapshot, write_set wins
                    result.push(write_iter.next().unwrap());
                }
            }
        }
        result.extend(snap_iter);
        result.extend(write_iter);

        Ok(result)
    }

    /// Get the version that was read for a key (from read_set)
    ///
    /// Returns None if the key hasn't been read from snapshot.
    /// Returns Some(CommitVersion::ZERO) if the key was read but didn't exist.
    pub fn get_read_version(&self, key: &Key) -> Option<CommitVersion> {
        self.read_set.get(key).copied()
    }

    // === Write Operations ===

    /// Set the maximum number of write buffer entries (0 = unlimited).
    pub fn set_max_write_entries(&mut self, max: usize) {
        self.max_write_entries = max;
    }

    /// Enable or disable per-transaction read-only mode.
    ///
    /// When enabled, write operations (`put`, `delete`, `cas`) are rejected
    /// and read-set tracking is skipped, saving memory on large scans.
    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    /// Allow operations on keys whose branch_id differs from this transaction's.
    ///
    /// **Internal only.** Normal user transactions must NOT enable this.
    /// Cross-branch reads bypass the per-branch commit lock, so TOCTOU
    /// protection is lost (see #1709). Only safe when the caller guarantees
    /// no concurrent mutation of the target branch (e.g., branch deletion).
    pub fn set_allow_cross_branch(&mut self, allow: bool) {
        self.allow_cross_branch = allow;
    }

    /// Check if this transaction was explicitly set to read-only mode.
    ///
    /// This reflects the **configured mode** set via [`set_read_only`](Self::set_read_only),
    /// not whether the transaction has actually performed writes. When `true`, writes are
    /// rejected at the API level and read-set tracking is skipped for memory savings.
    ///
    /// See also [`is_read_only`](Self::is_read_only) which checks whether the transaction
    /// has *actually* performed any mutations (regardless of mode).
    pub fn is_read_only_mode(&self) -> bool {
        self.read_only
    }

    /// Check if the write buffer has exceeded the configured limit.
    ///
    /// For put/delete operations, pass the key to allow overwrites of already-tracked
    /// keys (the operation won't increase the total count). For CAS, pass `None`
    /// because CAS always appends to the cas_set.
    #[inline]
    fn check_write_limit(&self, key: Option<&Key>) -> StrataResult<()> {
        if self.max_write_entries == 0 {
            return Ok(());
        }
        // If the key is already tracked in write_set or delete_set, the mutation
        // won't increase the total count — allow it unconditionally.
        if let Some(k) = key {
            if self.write_set.contains_key(k) || self.delete_set.contains(k) {
                return Ok(());
            }
        }
        let total = self.write_set.len() + self.delete_set.len() + self.cas_set.len();
        if total >= self.max_write_entries {
            return Err(StrataError::capacity_exceeded(
                "transaction_write_buffer",
                self.max_write_entries,
                total + 1,
            ));
        }
        Ok(())
    }

    /// Buffer a write operation
    ///
    /// The write is NOT applied to storage until commit.
    /// Other transactions will NOT see this write (OCC isolation).
    ///
    /// # Semantics
    /// - If the key was previously deleted in this txn, remove from delete_set
    /// - Add/overwrite in write_set (latest value wins)
    /// - Writes are "blind" - no read_set entry unless you explicitly read first
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::types::{BranchId, Key, Namespace};
    /// # use strata_core::value::Value;
    /// # use std::sync::Arc;
    /// # fn example(txn: &mut TransactionContext) -> strata_core::StrataResult<()> {
    /// # let key = Key::new_kv(Arc::new(Namespace::for_branch(BranchId::default())), "key");
    /// txn.put(key, Value::Bytes(b"value".to_vec()))?;
    /// // Value is NOT visible to other transactions yet
    /// // Will be visible after successful commit
    /// # Ok(())
    /// # }
    /// ```
    pub fn put(&mut self, key: Key, value: Value) -> StrataResult<()> {
        self.guard(&key)?;
        if self.read_only {
            return Err(StrataError::invalid_input(
                "Cannot write in a read-only transaction",
            ));
        }
        self.check_write_limit(Some(&key))?;

        self.validate_no_cas_conflict(&key)?;

        // Remove from delete_set if previously deleted in this txn
        self.delete_set.remove(&key);
        // Clear any previous TTL — a plain put() has no TTL
        self.ttl_map.remove(&key);

        // Add to write_set (overwrites any previous write to same key)
        self.write_set.insert(key, value);
        Ok(())
    }

    /// Buffer a write with a TTL (time-to-live) in milliseconds.
    ///
    /// Same as `put()`, but also records a TTL so the entry is serialized
    /// into the WAL and preserved through recovery. A `ttl_ms` of 0 means
    /// no TTL (equivalent to `put()`).
    pub fn put_with_ttl(&mut self, key: Key, value: Value, ttl_ms: u64) -> StrataResult<()> {
        if ttl_ms > 0 {
            self.ttl_map.insert(key.clone(), ttl_ms);
        }
        self.put(key, value)
    }

    /// Buffer a write with a single-version retention hint.
    ///
    /// Same as `put()`, but marks this key for `WriteMode::KeepLast(1)`.
    /// The write still appends a new MVCC version at commit time; older
    /// versions are pruned later during compaction, not at write time.
    /// Use for internal data (e.g., graph adjacency lists) where version
    /// history is not needed and would waste memory.
    pub fn put_replace(&mut self, key: Key, value: Value) -> StrataResult<()> {
        self.key_write_modes
            .insert(key.clone(), WriteMode::KeepLast(1));
        self.put(key, value)
    }

    /// Buffer a delete operation
    ///
    /// The delete is NOT applied to storage until commit.
    /// Other transactions will NOT see this delete (OCC isolation).
    ///
    /// # Semantics
    /// - If the key was previously written in this txn, remove from write_set
    /// - Add to delete_set
    /// - At commit, creates a tombstone in storage
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::types::{BranchId, Key, Namespace};
    /// # use std::sync::Arc;
    /// # fn example(txn: &mut TransactionContext) -> strata_core::StrataResult<()> {
    /// # let key = Key::new_kv(Arc::new(Namespace::for_branch(BranchId::default())), "key");
    /// txn.delete(key)?;
    /// // Key is NOT deleted from storage yet
    /// // Will be deleted after successful commit
    /// // Reading this key within this txn returns None (read-your-deletes)
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete(&mut self, key: Key) -> StrataResult<()> {
        self.guard(&key)?;
        if self.read_only {
            return Err(StrataError::invalid_input(
                "Cannot write in a read-only transaction",
            ));
        }
        self.check_write_limit(Some(&key))?;

        self.validate_no_cas_conflict(&key)?;

        // Remove from write_set if previously written in this txn
        self.write_set.remove(&key);
        // Clean up any write mode override and TTL for this key
        self.key_write_modes.remove(&key);
        self.ttl_map.remove(&key);

        // Add to delete_set
        self.delete_set.insert(key);
        Ok(())
    }

    /// Buffer a compare-and-swap operation
    ///
    /// CAS operations are validated at COMMIT time, not call time.
    /// This allows multiple CAS operations to be batched in a single transaction.
    ///
    /// # Semantics
    /// - `expected_version = 0` means "key must not exist"
    /// - `expected_version = N` means "key must be at version N"
    /// - CAS does NOT automatically add to read_set
    /// - If you need read-set protection, explicitly read the key first
    /// - Multiple CAS operations on different keys are allowed
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::types::{BranchId, Key, Namespace};
    /// # use strata_core::id::CommitVersion;
    /// # use strata_core::value::Value;
    /// # use std::sync::Arc;
    /// # fn example(txn: &mut TransactionContext) -> strata_core::StrataResult<()> {
    /// # let ns = Arc::new(Namespace::for_branch(BranchId::default()));
    /// # let key = Key::new_kv(ns.clone(), "key");
    /// # let other_key = Key::new_kv(ns, "other");
    /// // Create key only if it doesn't exist (expected_version = 0)
    /// txn.cas(key, CommitVersion::ZERO, Value::Bytes(b"initial".to_vec()))?;
    ///
    /// // Update key only if at version 5
    /// txn.cas(other_key, CommitVersion(5), Value::Bytes(b"updated".to_vec()))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cas(
        &mut self,
        key: Key,
        expected_version: CommitVersion,
        new_value: Value,
    ) -> StrataResult<()> {
        self.guard(&key)?;
        if self.read_only {
            return Err(StrataError::invalid_input(
                "Cannot write in a read-only transaction",
            ));
        }
        self.check_write_limit(None)?;

        self.validate_no_write_conflict(&key)?;

        self.cas_set.push(CASOperation {
            key,
            expected_version,
            new_value,
        });
        Ok(())
    }

    /// Compare-and-swap with automatic read-set tracking.
    ///
    /// Like `cas()`, but also reads the key from the snapshot to populate
    /// the read-set. This gives BOTH CAS validation AND read-set protection.
    /// Equivalent to `txn.get(&key)?; txn.cas(key, expected_version, value)?;`
    pub fn cas_with_read(
        &mut self,
        key: Key,
        expected_version: CommitVersion,
        new_value: Value,
    ) -> StrataResult<()> {
        self.guard(&key)?;
        if self.read_only {
            return Err(StrataError::invalid_input(
                "Cannot write in a read-only transaction",
            ));
        }
        self.check_write_limit(None)?;

        self.validate_no_write_conflict(&key)?;

        self.read_from_snapshot(&key)?;
        self.cas_set.push(CASOperation {
            key,
            expected_version,
            new_value,
        });
        Ok(())
    }

    // === Event State Tracking ===

    /// Get the current event sequence count tracked across Transaction instances.
    ///
    /// Returns 0 if no events have been appended in this transaction context.
    pub fn event_sequence_count(&self) -> u64 {
        self.event_sequence_count.unwrap_or(0)
    }

    /// Get the last event hash tracked across Transaction instances.
    ///
    /// Returns the zero hash if no events have been appended in this transaction context.
    pub fn event_last_hash(&self) -> [u8; 32] {
        self.event_last_hash.unwrap_or([0u8; 32])
    }

    /// Update the event state after an event append.
    ///
    /// Called by Transaction::event_append() to persist event continuity
    /// across multiple Transaction instances within the same session transaction.
    pub fn set_event_state(&mut self, count: u64, last_hash: [u8; 32]) {
        self.event_sequence_count = Some(count);
        self.event_last_hash = Some(last_hash);
    }

    // === JSON Operations (M5 Epic 30) ===

    /// Check if this transaction has any JSON operations
    ///
    /// Returns true if any JSON reads, writes, or snapshot versions are recorded.
    /// Useful for determining if JSON-specific validation is needed.
    pub fn has_json_ops(&self) -> bool {
        self.json_reads.is_some()
            || self.json_writes.is_some()
            || self.json_snapshot_versions.is_some()
    }

    /// Get JSON patch writes (immutable)
    ///
    /// Returns an empty slice if no JSON writes have been recorded.
    pub fn json_writes(&self) -> &[JsonPatchEntry] {
        self.json_writes.as_deref().unwrap_or(&[])
    }

    /// Get JSON snapshot versions (immutable)
    ///
    /// Returns None if no JSON snapshot versions have been recorded.
    pub fn json_snapshot_versions(&self) -> Option<&HashMap<Key, CommitVersion>> {
        self.json_snapshot_versions.as_ref()
    }

    /// Ensure json_reads is initialized and return mutable reference
    ///
    /// Lazily allocates the Vec on first use.
    pub fn ensure_json_reads(&mut self) -> &mut Vec<JsonPathRead> {
        self.json_reads.get_or_insert_with(Vec::new)
    }

    /// Ensure json_writes is initialized and return mutable reference
    ///
    /// Lazily allocates the Vec on first use.
    pub fn ensure_json_writes(&mut self) -> &mut Vec<JsonPatchEntry> {
        self.json_writes.get_or_insert_with(Vec::new)
    }

    /// Ensure json_snapshot_versions is initialized and return mutable reference
    ///
    /// Lazily allocates the HashMap on first use.
    pub fn ensure_json_snapshot_versions(&mut self) -> &mut HashMap<Key, CommitVersion> {
        self.json_snapshot_versions.get_or_insert_with(HashMap::new)
    }

    /// Record a JSON path read for conflict detection
    ///
    /// This should be called when reading a specific path from a JSON document.
    /// The read will be validated at commit time to detect conflicts.
    pub fn record_json_read(&mut self, key: Key, path: JsonPath, version: CommitVersion) {
        self.ensure_json_reads()
            .push(JsonPathRead::new(key, path, version));
    }

    /// Record a JSON patch for commit
    ///
    /// This should be called when modifying a JSON document via patch.
    /// The patch will be applied at commit time.
    pub fn record_json_write(&mut self, key: Key, patch: JsonPatch, resulting_version: CommitVersion) {
        self.ensure_json_writes()
            .push(JsonPatchEntry::new(key, patch, resulting_version));
    }

    /// Record JSON document snapshot version
    ///
    /// Tracks the version of a JSON document when it was first read.
    /// Used for document-level conflict detection.
    pub fn record_json_snapshot_version(&mut self, key: Key, version: CommitVersion) {
        self.ensure_json_snapshot_versions().insert(key, version);
    }

    /// Ensure the snapshot version for a JSON document is tracked.
    ///
    /// If the document's version is not yet recorded, reads it from the store
    /// and records it for conflict detection at commit time. Errors from the
    /// store are silently ignored (best-effort tracking).
    fn ensure_json_snapshot_tracked(&mut self, key: &Key) {
        if self
            .json_snapshot_versions()
            .map_or(true, |v| !v.contains_key(key))
        {
            if let Some(store) = &self.store {
                match store.get_versioned(key, self.start_version) {
                    Ok(Some(vv)) => {
                        self.record_json_snapshot_version(
                            key.clone(),
                            CommitVersion(vv.version.as_u64()),
                        );
                    }
                    Ok(None) => {
                        self.record_json_snapshot_version(key.clone(), CommitVersion::ZERO);
                    }
                    Err(_) => {}
                }
            }
        }
    }

    /// Clear all buffered operations
    ///
    /// This is useful for retry scenarios where you want to restart
    /// a transaction's operations without creating a new transaction.
    ///
    /// Clears: read_set, write_set, delete_set, cas_set, and all JSON operation sets
    ///
    /// Note: Does not change transaction state or snapshot.
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not active.
    pub fn clear_operations(&mut self) -> StrataResult<()> {
        self.ensure_active()?;

        self.read_set.clear();
        self.write_set.clear();
        self.delete_set.clear();
        self.cas_set.clear();
        // Clear JSON sets (set to None to deallocate)
        self.json_reads = None;
        self.json_writes = None;
        self.json_snapshot_versions = None;
        Ok(())
    }

    // === State Management ===

    /// Check if transaction is in Active state
    ///
    /// Only active transactions can accept new read/write operations.
    pub fn is_active(&self) -> bool {
        matches!(self.status, TransactionStatus::Active)
    }

    /// Check if transaction is committed
    pub fn is_committed(&self) -> bool {
        matches!(self.status, TransactionStatus::Committed)
    }

    /// Check if transaction is aborted
    pub fn is_aborted(&self) -> bool {
        matches!(self.status, TransactionStatus::Aborted { .. })
    }

    /// Check if transaction can be rolled back
    ///
    /// A transaction can be rolled back if it's in Active or Validating state.
    /// Once committed or aborted, rollback is not possible.
    pub fn can_rollback(&self) -> bool {
        matches!(
            self.status,
            TransactionStatus::Active | TransactionStatus::Validating
        )
    }

    // === Timeout Support ===

    /// Check if this transaction has exceeded the given timeout
    ///
    /// Returns true if the elapsed time since transaction creation
    /// exceeds the specified timeout duration.
    ///
    /// # Arguments
    /// * `timeout` - Maximum allowed duration for this transaction
    ///
    /// # Example
    /// ```
    /// use strata_concurrency::TransactionContext;
    /// use strata_core::id::{TxnId, CommitVersion};
    /// use strata_core::types::BranchId;
    /// use std::time::Duration;
    ///
    /// let branch_id = BranchId::new();
    /// let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
    ///
    /// // Should not be expired immediately
    /// assert!(!txn.is_expired(Duration::from_secs(1)));
    /// ```
    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.start_time.elapsed() > timeout
    }

    /// Get the elapsed time since transaction started
    ///
    /// Returns the duration since this transaction was created.
    ///
    /// # Example
    /// ```
    /// use strata_concurrency::TransactionContext;
    /// use strata_core::id::{TxnId, CommitVersion};
    /// use strata_core::types::BranchId;
    /// use std::time::Duration;
    ///
    /// let branch_id = BranchId::new();
    /// let txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
    ///
    /// // Elapsed should be very small initially
    /// assert!(txn.elapsed() < Duration::from_secs(1));
    /// ```
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Check if transaction can accept operations
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if transaction is not in `Active` state.
    pub fn ensure_active(&self) -> StrataResult<()> {
        if self.is_active() {
            Ok(())
        } else {
            Err(StrataError::invalid_input(format!(
                "Transaction {} is not active: {:?}",
                self.txn_id, self.status
            )))
        }
    }

    /// Verify a key belongs to this transaction's branch.
    ///
    /// Commit serialization locks only `self.branch_id`. Allowing keys from
    /// other branches would let cross-branch reads bypass that lock, creating
    /// a TOCTOU vulnerability (#1709).
    fn enforce_branch_scope(&self, key: &Key) -> StrataResult<()> {
        if !self.allow_cross_branch && key.namespace.branch_id != self.branch_id {
            return Err(StrataError::invalid_input(format!(
                "Key branch {} does not match transaction branch {} — \
                 cross-branch operations are not supported in a single transaction",
                key.namespace.branch_id, self.branch_id
            )));
        }
        Ok(())
    }

    /// Common pre-operation guard: verify the transaction is active and the key
    /// belongs to this transaction's branch.
    fn guard(&self, key: &Key) -> StrataResult<()> {
        self.ensure_active()?;
        self.enforce_branch_scope(key)
    }

    /// Reject if the same key already has a CAS operation in this transaction.
    /// Used by `put()` and `delete()` to prevent ambiguous CAS+write mixing (#1739 OCC-M1).
    fn validate_no_cas_conflict(&self, key: &Key) -> StrataResult<()> {
        if self.cas_set.iter().any(|op| op.key == *key) {
            return Err(StrataError::invalid_input(
                "Key already has a CAS operation in this transaction; \
                 mixing put/delete and CAS on the same key is ambiguous",
            ));
        }
        Ok(())
    }

    /// Reject if the same key already has a put or delete in this transaction.
    /// Used by `cas()` and `cas_with_read()` to prevent ambiguous write+CAS mixing (#1739 OCC-M1).
    fn validate_no_write_conflict(&self, key: &Key) -> StrataResult<()> {
        if self.write_set.contains_key(key) || self.delete_set.contains(key) {
            return Err(StrataError::invalid_input(
                "Key already has a put/delete in this transaction; \
                 mixing put/delete and CAS on the same key is ambiguous",
            ));
        }
        Ok(())
    }

    /// Transition to Validating state
    ///
    /// This is the first step of the commit process. After marking validating,
    /// the transaction should be validated against current storage state.
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if not in `Active` state.
    ///
    /// # State Transition
    /// `Active` → `Validating`
    pub fn mark_validating(&mut self) -> StrataResult<()> {
        self.ensure_active()?;
        self.status = TransactionStatus::Validating;
        Ok(())
    }

    /// Transition to Committed state
    ///
    /// Called after successful validation. The transaction's writes have been
    /// applied to storage.
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if not in `Validating` state.
    ///
    /// # State Transition
    /// `Validating` → `Committed`
    pub fn mark_committed(&mut self) -> StrataResult<()> {
        match &self.status {
            TransactionStatus::Validating => {
                self.status = TransactionStatus::Committed;
                Ok(())
            }
            _ => Err(StrataError::invalid_input(format!(
                "Cannot commit transaction {} from state {:?}",
                self.txn_id, self.status
            ))),
        }
    }

    /// Abort the transaction and clean up
    ///
    /// Per spec:
    /// - Aborted transactions write nothing to storage
    /// - Aborted transactions write nothing to WAL
    /// - All buffered operations are discarded
    ///
    /// Can be called from `Active` (user abort) or `Validating` (conflict detected).
    ///
    /// # Arguments
    /// * `reason` - Human-readable reason for abort
    ///
    /// # Errors
    /// Returns `StrataError::invalid_input` if already `Committed` or `Aborted`.
    ///
    /// # State Transitions
    /// - `Active` → `Aborted`
    /// - `Validating` → `Aborted`
    pub fn mark_aborted(&mut self, reason: String) -> StrataResult<()> {
        match &self.status {
            TransactionStatus::Committed => Err(StrataError::invalid_input(format!(
                "Cannot abort committed transaction {}",
                self.txn_id
            ))),
            TransactionStatus::Aborted { .. } => Err(StrataError::invalid_input(format!(
                "Transaction {} already aborted",
                self.txn_id
            ))),
            _ => {
                self.status = TransactionStatus::Aborted { reason };

                // Clear all buffered operations per spec
                // Aborted transactions write nothing
                self.write_set.clear();
                self.delete_set.clear();
                self.cas_set.clear();

                // Note: read_set is kept for debugging/diagnostics

                Ok(())
            }
        }
    }

    /// Get summary of pending operations
    ///
    /// Useful for debugging and logging before abort/commit.
    /// Returns counts of buffered operations that would be applied on commit
    /// or discarded on abort.
    pub fn pending_operations(&self) -> PendingOperations {
        PendingOperations {
            puts: self.write_set.len(),
            deletes: self.delete_set.len(),
            cas: self.cas_set.len(),
        }
    }

    // === Commit Operation ===

    /// Commit the transaction
    ///
    /// Per spec Section 3 and Core Invariants:
    /// 1. Transition to Validating state
    /// 2. Run validation against current storage
    /// 3. If valid: transition to Committed
    /// 4. If invalid: transition to Aborted
    ///
    /// # Arguments
    /// * `store` - Storage to validate against
    ///
    /// # Returns
    /// - Ok(()) if transaction committed successfully
    /// - Err(CommitError::ValidationFailed) if transaction aborted due to conflicts
    /// - Err(CommitError::InvalidState) if not in Active state
    ///
    /// # Note
    /// This method performs validation and state transitions only.
    /// Actual write application is handled separately in .
    /// Full atomic commit with WAL is implemented in .
    ///
    /// # Spec Reference
    /// - Section 3.1: When conflicts occur
    /// - Section 3.3: First-committer-wins rule
    /// - Core Invariants: All-or-nothing commit
    pub fn commit<S: Storage>(&mut self, store: &S) -> std::result::Result<(), CommitError> {
        // Step 1: Transition to Validating
        if !self.is_active() {
            return Err(CommitError::InvalidState(format!(
                "Cannot commit transaction {} from {:?} state - must be Active",
                self.txn_id, self.status
            )));
        }
        self.status = TransactionStatus::Validating;

        // Step 2: Validate against current storage state
        let validation_result = validate_transaction(self, store)
            .map_err(|e| CommitError::StorageError(e.to_string()))?;

        if !validation_result.is_valid() {
            // Step 3a: Validation failed - abort
            let conflict_count = validation_result.conflict_count();
            self.status = TransactionStatus::Aborted {
                reason: format!("Commit failed: {} conflict(s) detected", conflict_count),
            };
            return Err(CommitError::ValidationFailed(validation_result));
        }

        // Step 3b: Validation passed - mark committed
        self.status = TransactionStatus::Committed;

        Ok(())
    }

    /// Apply all buffered writes to storage
    ///
    /// Per spec Section 6.1:
    /// - Global version incremented ONCE for the whole transaction
    /// - All keys in this transaction get the same commit version
    ///
    /// Per spec Section 6.5:
    /// - Deletes create tombstones with the commit version
    ///
    /// # Arguments
    /// * `store` - Storage to apply writes to
    /// * `commit_version` - Version to assign to all writes
    ///
    /// # Returns
    /// ApplyResult with counts of applied operations
    ///
    /// # Preconditions
    /// - Transaction must be in Committed state (validation passed)
    ///
    /// # Errors
    /// - StrataError::invalid_input if transaction is not in Committed state
    /// - Error from storage operations if they fail
    pub fn apply_writes<S: Storage>(
        &mut self,
        store: &S,
        commit_version: CommitVersion,
    ) -> StrataResult<ApplyResult> {
        if !self.is_committed() {
            return Err(StrataError::invalid_input(format!(
                "Cannot apply writes: transaction {} is {:?}, must be Committed",
                self.txn_id, self.status
            )));
        }

        let puts_count = self.write_set.len();
        let deletes_count = self.delete_set.len();
        let cas_count = self.cas_set.len();

        // Collect puts (write_set + CAS) into a single batch — drain for zero-copy moves.
        let mut writes: Vec<(Key, Value, WriteMode)> = Vec::with_capacity(puts_count + cas_count);
        let mut put_ttls: Vec<u64> = Vec::with_capacity(puts_count + cas_count);

        for (key, value) in self.write_set.drain() {
            let mode = self
                .key_write_modes
                .remove(&key)
                .unwrap_or(WriteMode::Append);
            let ttl_ms = self.ttl_map.remove(&key).unwrap_or(0);
            writes.push((key, value, mode));
            put_ttls.push(ttl_ms);
        }

        // CAS operations always use Append mode (validation already passed).
        for cas_op in self.cas_set.drain(..) {
            let ttl_ms = self.ttl_map.remove(&cas_op.key).unwrap_or(0);
            writes.push((cas_op.key, cas_op.new_value, WriteMode::Append));
            put_ttls.push(ttl_ms);
        }

        // Collect deletes into batch
        let deletes: Vec<Key> = self.delete_set.drain().collect();

        // Apply all puts and deletes atomically — the global version is advanced
        // only after every entry is installed, preventing partial-state visibility (#1706).
        store.apply_writes_atomic(writes, deletes, commit_version, &put_ttls)?;

        Ok(ApplyResult {
            commit_version,
            puts_applied: puts_count,
            deletes_applied: deletes_count,
            cas_applied: cas_count,
        })
    }

    /// Materialize JSON patches into the write_set as full-document puts.
    ///
    /// For each key in `json_writes`, reads the base document from the snapshot
    /// store, applies all patches in order, serializes the result to msgpack bytes,
    /// and inserts into `write_set`. After this call, `json_writes` is cleared and
    /// the existing `TransactionPayload` / `apply_writes` path handles persistence.
    ///
    /// Must be called after validation but before building `TransactionPayload`.
    pub fn materialize_json_writes(&mut self) -> StrataResult<()> {
        use strata_core::primitives::json::{apply_patches, JsonValue};

        let json_writes = match self.json_writes.take() {
            Some(w) if !w.is_empty() => w,
            _ => return Ok(()),
        };

        // Group patches by key, preserving insertion order.
        let mut patches_by_key: Vec<(Key, Vec<strata_core::primitives::json::JsonPatch>)> =
            Vec::new();
        for entry in json_writes {
            if let Some((_k, patches)) = patches_by_key.iter_mut().find(|(k, _)| *k == entry.key) {
                patches.push(entry.patch);
            } else {
                patches_by_key.push((entry.key, vec![entry.patch]));
            }
        }

        let store = self.store.as_ref().ok_or_else(|| {
            StrataError::internal("Cannot materialize JSON writes: no snapshot store")
        })?;

        for (key, patches) in patches_by_key {
            // Read the base document from the snapshot.
            let mut doc: JsonValue =
                if let Some(vv) = store.get_versioned(&key, self.start_version)? {
                    match &vv.value {
                        Value::Bytes(b) => rmp_serde::from_slice(b).map_err(|e| {
                            StrataError::internal(format!(
                                "Failed to deserialize JSON document for materialization: {}",
                                e
                            ))
                        })?,
                        _ => JsonValue::object(),
                    }
                } else {
                    JsonValue::object()
                };

            apply_patches(&mut doc, &patches).map_err(|e| {
                StrataError::internal(format!("Failed to apply JSON patches: {}", e))
            })?;

            let doc_bytes = rmp_serde::to_vec(&doc).map_err(|e| {
                StrataError::internal(format!(
                    "Failed to serialize materialized JSON document: {}",
                    e
                ))
            })?;

            self.write_set.insert(key, Value::Bytes(doc_bytes));
        }

        Ok(())
    }

    // === Introspection ===

    /// Get the number of keys in the read set
    pub fn read_count(&self) -> usize {
        self.read_set.len()
    }

    /// Get the number of keys in the write set
    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }

    /// Get the number of keys in the delete set
    pub fn delete_count(&self) -> usize {
        self.delete_set.len()
    }

    /// Get the number of CAS operations
    pub fn cas_count(&self) -> usize {
        self.cas_set.len()
    }

    /// Check if transaction has any pending operations
    ///
    /// Returns true if there are buffered writes, deletes, or CAS operations
    /// that would need to be applied at commit.
    pub fn has_pending_operations(&self) -> bool {
        !self.write_set.is_empty() || !self.delete_set.is_empty() || !self.cas_set.is_empty()
    }

    /// Check if this transaction has performed no mutations.
    ///
    /// Returns `true` when the write set, delete set, and CAS set are all empty,
    /// meaning the transaction has only performed reads. Such transactions always
    /// commit successfully since they cannot conflict.
    ///
    /// This is a **behavioral query** — it reflects what the transaction has done,
    /// not how it was configured. See [`is_read_only_mode`](Self::is_read_only_mode)
    /// for the configured mode flag.
    pub fn is_read_only(&self) -> bool {
        self.write_set.is_empty() && self.delete_set.is_empty() && self.cas_set.is_empty()
    }

    /// Get the abort reason if transaction is aborted
    pub fn abort_reason(&self) -> Option<&str> {
        match &self.status {
            TransactionStatus::Aborted { reason } => Some(reason),
            _ => None,
        }
    }

    // ========================================================================
    // Pooling Support (M4 )
    // ========================================================================

    /// Reset context for reuse (M4 pooling optimization)
    ///
    /// Clears all transaction state without deallocating memory.
    /// HashMap::clear() and Vec::clear() preserve capacity, which is
    /// the key optimization for transaction pooling.
    ///
    /// After reset, the context is ready for a new transaction with:
    /// - New txn_id, branch_id, start_version
    /// - New snapshot
    /// - Empty read_set, write_set, delete_set, cas_set (with preserved capacity)
    /// - Active status
    /// - Fresh start_time
    ///
    /// # Arguments
    ///
    /// * `txn_id` - New transaction ID
    /// * `branch_id` - New branch ID
    /// * `snapshot` - New snapshot view (optional for testing)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::types::BranchId;
    /// # use strata_core::id::{TxnId, CommitVersion};
    /// # use strata_storage::SegmentedStore;
    /// # use std::sync::Arc;
    /// let branch_id = BranchId::default();
    /// let mut ctx = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
    /// // ... use the context ...
    ///
    /// // Reset for reuse - capacity is preserved!
    /// let new_branch_id = BranchId::default();
    /// let store = Arc::new(SegmentedStore::new());
    /// ctx.reset(TxnId(2), new_branch_id, Some(store));
    /// ```
    pub fn reset(
        &mut self,
        txn_id: TxnId,
        branch_id: BranchId,
        store: Option<Arc<SegmentedStore>>,
    ) {
        // Update identity
        self.txn_id = txn_id;
        self.branch_id = branch_id;

        // Update store and version
        self.start_version = store
            .as_ref()
            .map(|s| s.current_version())
            .unwrap_or(CommitVersion::ZERO);
        self.store = store;

        // Clear collections but preserve capacity - this is the key optimization!
        // HashMap::clear() and HashSet::clear() keep the allocated buckets
        // Vec::clear() keeps the allocated buffer
        self.read_set.clear();
        self.write_set.clear();
        self.delete_set.clear();
        self.cas_set.clear();
        self.key_write_modes.clear();
        self.ttl_map.clear();

        // Reclaim memory if a large transaction inflated capacity beyond threshold.
        // Normal workloads (< 4096 entries) keep their allocations intact.
        //
        // 4096 is chosen as a power-of-two that comfortably covers typical OLTP
        // transactions (tens to low-hundreds of keys) while capping idle memory
        // at ~128 KiB per map (4096 × ~32 B per entry). Shrinking to half the
        // threshold avoids oscillation when capacity hovers near the boundary.
        const SHRINK_THRESHOLD: usize = 4096;
        macro_rules! shrink_if_large {
            ($collection:expr) => {
                if $collection.capacity() > SHRINK_THRESHOLD {
                    $collection.shrink_to(SHRINK_THRESHOLD / 2);
                }
            };
        }
        shrink_if_large!(self.read_set);
        shrink_if_large!(self.write_set);
        shrink_if_large!(self.delete_set);
        shrink_if_large!(self.key_write_modes);
        shrink_if_large!(self.ttl_map);

        // Clear event state (deallocate, since event ops are rare)
        self.event_sequence_count = None;
        self.event_last_hash = None;

        // Clear JSON fields (deallocate, since JSON ops are rare)
        self.json_reads = None;
        self.json_writes = None;
        self.json_snapshot_versions = None;

        // Reset state
        self.status = TransactionStatus::Active;
        self.start_time = Instant::now();
        self.read_only = false;
        self.allow_cross_branch = false;
    }

    /// Get current capacity of internal collections (for debugging/testing)
    ///
    /// Returns (read_set_capacity, write_set_capacity, delete_set_capacity, cas_set_capacity).
    /// Used to verify that `reset()` preserves capacity.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use strata_concurrency::TransactionContext;
    /// # use strata_core::id::{TxnId, CommitVersion};
    /// # use strata_core::types::BranchId;
    /// let branch_id = BranchId::default();
    /// let ctx = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
    /// let (read_cap, write_cap, delete_cap, cas_cap) = ctx.capacity();
    /// ```
    pub fn capacity(&self) -> (usize, usize, usize, usize) {
        (
            self.read_set.capacity(),
            self.write_set.capacity(),
            self.delete_set.capacity(),
            self.cas_set.capacity(),
        )
    }
}

// ============================================================================
// JsonStoreExt Implementation (M5 Epic 30)
// ============================================================================

impl JsonStoreExt for TransactionContext {
    fn json_get(&mut self, key: &Key, path: &JsonPath) -> StrataResult<Option<JsonValue>> {
        self.guard(key)?;

        // Check write set first (read-your-writes)
        // Look for the most recent write that affects this path
        if let Some(writes) = &self.json_writes {
            for entry in writes.iter().rev() {
                if entry.key == *key {
                    // Check if the patch affects this path
                    match &entry.patch {
                        JsonPatch::Set {
                            path: set_path,
                            value,
                        } if set_path.is_ancestor_of(path) => {
                            // If set_path equals our path, return the value directly
                            if set_path == path {
                                return Ok(Some(value.clone()));
                            }
                            // Navigate into the written value using the relative path
                            // Build a relative path by skipping the set_path segments
                            let relative_segments: Vec<_> = path
                                .segments()
                                .iter()
                                .skip(set_path.len())
                                .cloned()
                                .collect();
                            let relative_path = JsonPath::from_segments(relative_segments);
                            return Ok(get_at_path(value, &relative_path).cloned());
                        }
                        JsonPatch::Delete { path: del_path } if del_path.is_ancestor_of(path) => {
                            // Path was deleted
                            return Ok(None);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Read from store
        let store = self.store.as_ref().ok_or_else(|| {
            StrataError::invalid_input("Transaction has no store for reads".to_string())
        })?;

        // Get the document from store (bounded by start_version)
        let versioned = store.get_versioned(key, self.start_version)?;
        let Some(vv) = versioned else {
            // Document doesn't exist — record version ZERO so OCC detects
            // concurrent creation (mirrors KV get() behavior for missing keys)
            self.record_json_snapshot_version(key.clone(), CommitVersion::ZERO);
            return Ok(None);
        };

        // Track the document version for conflict detection
        let doc_version = CommitVersion(vv.version.as_u64());
        self.record_json_snapshot_version(key.clone(), doc_version);
        self.record_json_read(key.clone(), path.clone(), doc_version);

        // Deserialize the document
        let doc_bytes = match &vv.value {
            Value::Bytes(b) => b,
            _ => {
                return Err(StrataError::invalid_input(
                    "Expected JSON document to be stored as bytes".to_string(),
                ))
            }
        };

        // Deserialize using MessagePack
        let doc_value: JsonValue = rmp_serde::from_slice(doc_bytes).map_err(|e| {
            StrataError::invalid_input(format!("Failed to deserialize JSON document: {}", e))
        })?;

        // Get value at path
        Ok(get_at_path(&doc_value, path).cloned())
    }

    fn json_set(&mut self, key: &Key, path: &JsonPath, value: JsonValue) -> StrataResult<()> {
        self.guard(key)?;
        self.ensure_json_snapshot_tracked(key);

        // Record the write
        let patch = JsonPatch::set_at(path.clone(), value);
        // We don't know the resulting version until commit, use 0 as placeholder
        self.record_json_write(key.clone(), patch, CommitVersion(0));

        Ok(())
    }

    fn json_delete(&mut self, key: &Key, path: &JsonPath) -> StrataResult<()> {
        self.guard(key)?;
        self.ensure_json_snapshot_tracked(key);

        // Record the delete
        let patch = JsonPatch::delete_at(path.clone());
        self.record_json_write(key.clone(), patch, CommitVersion(0));

        Ok(())
    }

    fn json_get_document(&mut self, key: &Key) -> StrataResult<Option<JsonValue>> {
        // Get the root path
        let root = JsonPath::root();
        self.json_get(key, &root)
    }

    fn json_exists(&mut self, key: &Key) -> StrataResult<bool> {
        self.guard(key)?;

        // Check write buffer first (read-your-writes)
        // Look for root-level Set or Delete operations on this key
        if let Some(writes) = &self.json_writes {
            for entry in writes.iter().rev() {
                if entry.key == *key {
                    match &entry.patch {
                        JsonPatch::Set { path, .. } if path.is_root() => {
                            // Document was created/replaced in this transaction
                            return Ok(true);
                        }
                        JsonPatch::Delete { path } if path.is_root() => {
                            // Document was deleted in this transaction
                            return Ok(false);
                        }
                        _ => {
                            // Partial update - continue checking for root operations
                        }
                    }
                }
            }
        }

        // Fall back to store
        let store = self.store.as_ref().ok_or_else(|| {
            StrataError::invalid_input("Transaction has no store for reads".to_string())
        })?;

        match store.get_versioned(key, self.start_version)? {
            Some(vv) => {
                self.record_json_snapshot_version(key.clone(), CommitVersion(vv.version.as_u64()));
                Ok(true)
            }
            None => {
                self.record_json_snapshot_version(key.clone(), CommitVersion::ZERO);
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::id::{CommitVersion, TxnId};
    use strata_core::types::{BranchId, Namespace, TypeTag};
    use strata_core::value::Value;

    fn test_namespace_for(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn test_key(ns: &Arc<Namespace>, name: &str) -> Key {
        Key::new(ns.clone(), TypeTag::KV, name.as_bytes().to_vec())
    }

    /// Create a store with a single key-value at the given version.
    fn store_with_key(key: &Key, value: Value, version: u64) -> Arc<SegmentedStore> {
        let store = Arc::new(SegmentedStore::new());
        store
            .put_with_version_mode(
                key.clone(),
                value,
                CommitVersion(version),
                None,
                WriteMode::Append,
            )
            .unwrap();
        store
    }

    /// Create an empty store (no data).
    fn empty_store() -> Arc<SegmentedStore> {
        Arc::new(SegmentedStore::new())
    }

    #[test]
    fn test_get_versioned_from_snapshot() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = store_with_key(&key, Value::Int(42), 5);
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        let result = txn.get_versioned(&key).unwrap();
        let vv = result.unwrap();
        assert_eq!(vv.value, Value::Int(42));
        assert_eq!(vv.version, Version::Txn(5));
    }

    #[test]
    fn test_get_versioned_from_write_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = empty_store();
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        txn.put(key.clone(), Value::String("written".into()))
            .unwrap();

        let result = txn.get_versioned(&key).unwrap();
        let vv = result.unwrap();
        assert_eq!(vv.value, Value::String("written".into()));
        assert_eq!(vv.version, Version::Txn(0)); // placeholder
    }

    #[test]
    fn test_get_versioned_from_delete_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = store_with_key(&key, Value::Int(42), 5);
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        txn.delete(key.clone()).unwrap();

        let result = txn.get_versioned(&key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_versioned_nonexistent() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "missing");
        let store = empty_store();
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        let result = txn.get_versioned(&key).unwrap();
        assert!(result.is_none());
        // Tracks version 0 in read_set for conflict detection
        assert_eq!(txn.read_set.get(&key), Some(&CommitVersion::ZERO));
    }

    #[test]
    fn test_get_versioned_tracks_read_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = store_with_key(&key, Value::Int(7), 15);
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        let _ = txn.get_versioned(&key).unwrap();
        // Verify version tracked for conflict detection
        assert_eq!(txn.read_set.get(&key), Some(&CommitVersion(15)));
    }

    // ========================================================================
    // Write Buffer Limit Tests
    // ========================================================================

    #[test]
    fn test_write_buffer_limit_rejects_at_max() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
        txn.set_max_write_entries(3);

        // Put 3 entries — should succeed
        for i in 0..3 {
            txn.put(test_key(&ns, &format!("k{}", i)), Value::Int(i as i64))
                .unwrap();
        }
        // 4th should fail
        let err = txn
            .put(test_key(&ns, "overflow"), Value::Int(99))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("transaction_write_buffer"),
            "Error should mention transaction_write_buffer: {}",
            err
        );
    }

    #[test]
    fn test_write_buffer_limit_counts_deletes() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
        txn.set_max_write_entries(2);

        txn.put(test_key(&ns, "k1"), Value::Int(1)).unwrap();
        txn.delete(test_key(&ns, "k2")).unwrap();
        // Total = 2 (1 write + 1 delete), next should fail
        let err = txn.put(test_key(&ns, "k3"), Value::Int(3)).unwrap_err();
        assert!(format!("{}", err).contains("transaction_write_buffer"));
    }

    #[test]
    fn test_write_buffer_limit_zero_unlimited() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
        // max_write_entries = 0 (default, unlimited)

        for i in 0..1000 {
            txn.put(test_key(&ns, &format!("k{}", i)), Value::Int(i as i64))
                .unwrap();
        }
        assert_eq!(txn.write_set.len(), 1000);
    }

    // ========================================================================
    // Conditional Shrink Tests
    // ========================================================================

    #[test]
    fn test_reset_shrinks_large_capacity() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

        // Inflate capacity well beyond the 4096 threshold
        for i in 0..5000 {
            txn.put(test_key(&ns, &format!("k{}", i)), Value::Int(i as i64))
                .unwrap();
        }
        assert!(txn.write_set.capacity() >= 5000);

        txn.reset(TxnId(2), branch_id, Some(empty_store()));

        // After reset, capacity should be shrunk below threshold
        assert!(
            txn.write_set.capacity() <= 4096,
            "write_set capacity should be shrunk: {}",
            txn.write_set.capacity()
        );
    }

    #[test]
    fn test_write_buffer_limit_counts_cas() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
        txn.set_max_write_entries(2);

        // 1 put + 1 CAS = 2
        txn.put(test_key(&ns, "k1"), Value::Int(1)).unwrap();
        txn.cas(test_key(&ns, "k2"), CommitVersion::ZERO, Value::Int(2))
            .unwrap();

        // 3rd operation should fail (total = 2 >= limit of 2)
        let err = txn.put(test_key(&ns, "k3"), Value::Int(3)).unwrap_err();
        assert!(format!("{}", err).contains("transaction_write_buffer"));
    }

    #[test]
    fn test_write_buffer_overwrite_at_limit() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
        txn.set_max_write_entries(2);

        // Fill to limit
        txn.put(test_key(&ns, "k1"), Value::Int(1)).unwrap();
        txn.put(test_key(&ns, "k2"), Value::Int(2)).unwrap();

        // Overwriting an existing key should succeed (net-zero change)
        txn.put(test_key(&ns, "k1"), Value::Int(10)).unwrap();
        // Value should be updated
        assert_eq!(
            txn.write_set.get(&test_key(&ns, "k1")).unwrap().clone(),
            Value::Int(10)
        );
    }

    #[test]
    fn test_write_buffer_delete_existing_at_limit() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));
        txn.set_max_write_entries(2);

        // Fill: 1 put + 1 delete = 2
        txn.put(test_key(&ns, "k1"), Value::Int(1)).unwrap();
        txn.delete(test_key(&ns, "k2")).unwrap();

        // Delete k2 again — already in delete_set, should succeed
        txn.delete(test_key(&ns, "k2")).unwrap();
        // Put to k1 again — already in write_set, should succeed
        txn.put(test_key(&ns, "k1"), Value::Int(99)).unwrap();

        // A truly new key should still fail
        let err = txn.put(test_key(&ns, "k3"), Value::Int(3)).unwrap_err();
        assert!(format!("{}", err).contains("transaction_write_buffer"));
    }

    #[test]
    fn test_reset_preserves_normal_capacity() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

        // Insert a modest number of entries (below threshold)
        for i in 0..100 {
            txn.put(test_key(&ns, &format!("k{}", i)), Value::Int(i as i64))
                .unwrap();
        }
        let cap_before = txn.write_set.capacity();

        txn.reset(TxnId(2), branch_id, Some(empty_store()));

        // Capacity should be preserved (not shrunk)
        assert_eq!(txn.write_set.capacity(), cap_before);
    }

    // ========================================================================
    // KeepLast / key_write_modes Tests (Issue #1389)
    // ========================================================================

    #[test]
    fn test_reset_clears_key_write_modes() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let store = empty_store();
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

        // Mark some keys for replace
        txn.put_replace(test_key(&ns, "adj1"), Value::Int(1))
            .unwrap();
        txn.put_replace(test_key(&ns, "adj2"), Value::Int(2))
            .unwrap();
        assert_eq!(txn.key_write_modes.len(), 2);

        // Reset should clear key_write_modes
        txn.reset(TxnId(2), branch_id, Some(store.clone()));
        assert!(txn.key_write_modes.is_empty());

        // After reset, a normal put should NOT use KeepLast — verify no stale leak
        let key = test_key(&ns, "adj1"); // same key name as before
        txn.put(key.clone(), Value::Int(99)).unwrap();
        assert!(
            !txn.key_write_modes.contains_key(&key),
            "normal put after reset should not inherit stale KeepLast mode"
        );
    }

    #[test]
    fn test_put_replace_uses_keep_last() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

        let key = test_key(&ns, "graph_adj");
        txn.put_replace(key.clone(), Value::Int(42)).unwrap();

        assert_eq!(txn.key_write_modes.get(&key), Some(&WriteMode::KeepLast(1)));
        // Verify the value is also in write_set
        assert_eq!(txn.write_set.get(&key), Some(&Value::Int(42)));
    }

    #[test]
    fn test_delete_cleans_up_key_write_modes() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(100));

        let key = test_key(&ns, "adj_to_delete");
        txn.put_replace(key.clone(), Value::Int(1)).unwrap();
        assert!(txn.key_write_modes.contains_key(&key));

        // Delete should clean up the write mode entry
        txn.delete(key.clone()).unwrap();
        assert!(
            !txn.key_write_modes.contains_key(&key),
            "delete should remove orphaned key_write_modes entry"
        );
        assert!(!txn.write_set.contains_key(&key));
        assert!(txn.delete_set.contains(&key));
    }

    #[test]
    fn test_apply_writes_with_keep_last_mode() {
        use strata_core::traits::Storage;

        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let store = empty_store();

        // First: write 3 versions of a key to storage (simulating prior commits)
        let key = test_key(&ns, "adj_list");
        for v in 1..=3u64 {
            store
                .put_with_version_mode(
                    key.clone(),
                    Value::Int(v as i64),
                    CommitVersion(v),
                    None,
                    WriteMode::Append,
                )
                .unwrap();
        }
        store.set_version(CommitVersion(3));

        // Verify 3 versions exist
        let history = Storage::get_history(&*store, &key, None, None).unwrap();
        assert_eq!(history.len(), 3);

        // Now create a transaction that uses put_replace
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store.clone());
        txn.put_replace(key.clone(), Value::Int(99)).unwrap();

        // Force status to Committed for apply_writes
        txn.status = TransactionStatus::Committed;

        let result = txn.apply_writes(&*store, CommitVersion(4)).unwrap();
        assert_eq!(result.puts_applied, 1);

        // KeepLast(1) is a retention hint — write still appends (issue #1700)
        let latest = Storage::get_versioned(&*store, &key, CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(latest.value, Value::Int(99));
        assert_eq!(latest.version.as_u64(), 4);

        // SegmentedStore appends all versions (pruning happens at compaction),
        // so all 4 versions are present (3 original + 1 from put_replace).
        let history_after = Storage::get_history(&*store, &key, None, None).unwrap();
        assert_eq!(
            history_after.len(),
            4,
            "SegmentedStore keeps all versions; expected 4, got {}",
            history_after.len()
        );
        assert_eq!(history_after[0].value, Value::Int(99));
        assert_eq!(history_after[0].version.as_u64(), 4);
    }

    // ========================================================================
    // Read-Only Mode Tests
    // ========================================================================

    #[test]
    fn test_read_only_skips_read_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = store_with_key(&key, Value::Int(42), 1);
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);
        txn.set_read_only(true);

        // Read should succeed
        let val = txn.get(&key).unwrap();
        assert_eq!(val, Some(Value::Int(42)));

        // But read_set should be empty
        assert!(
            txn.read_set.is_empty(),
            "read_set should be empty in read-only mode"
        );
    }

    #[test]
    fn test_read_only_rejects_put() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion::ZERO);
        txn.set_read_only(true);

        let key = test_key(&ns, "k1");
        let result = txn.put(key, Value::Int(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_only_rejects_delete() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion::ZERO);
        txn.set_read_only(true);

        let key = test_key(&ns, "k1");
        let result = txn.delete(key);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_only_rejects_cas() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion::ZERO);
        txn.set_read_only(true);

        let key = test_key(&ns, "k1");
        let result = txn.cas(key, CommitVersion::ZERO, Value::Int(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_only_reset_clears_flag() {
        let branch_id = BranchId::new();
        let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion::ZERO);
        txn.set_read_only(true);
        assert!(txn.is_read_only_mode());

        txn.reset(TxnId(2), branch_id, Some(empty_store()));
        assert!(
            !txn.is_read_only_mode(),
            "read_only should be cleared after reset"
        );
    }

    // ========================================================================
    // CAS with Read Tests
    // ========================================================================

    #[test]
    fn test_cas_with_read_populates_read_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = store_with_key(&key, Value::Int(10), 5);
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        txn.cas_with_read(key.clone(), CommitVersion(5), Value::Int(20))
            .unwrap();

        // read_set should contain the key (from the read)
        assert_eq!(txn.read_set.get(&key), Some(&CommitVersion(5)));
        // cas_set should have the CAS operation
        assert_eq!(txn.cas_set.len(), 1);
        assert_eq!(txn.cas_set[0].expected_version, CommitVersion(5));
    }

    #[test]
    fn test_cas_with_read_nonexistent_key_tracks_version_zero() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "missing");
        let store = empty_store();
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);

        txn.cas_with_read(key.clone(), CommitVersion::ZERO, Value::Int(1))
            .unwrap();

        // Non-existent key should be tracked with version 0
        assert_eq!(txn.read_set.get(&key), Some(&CommitVersion::ZERO));
        assert_eq!(txn.cas_set.len(), 1);
    }

    #[test]
    fn test_cas_with_read_rejects_read_only() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let store = empty_store();
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);
        txn.set_read_only(true);

        let key = test_key(&ns, "k1");
        let result = txn.cas_with_read(key, CommitVersion::ZERO, Value::Int(1));
        assert!(result.is_err());
        assert!(
            format!("{}", result.unwrap_err()).contains("read-only"),
            "Error should mention read-only"
        );
    }

    #[test]
    fn test_read_only_get_versioned_skips_read_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let key = test_key(&ns, "k1");
        let store = store_with_key(&key, Value::Int(42), 7);
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);
        txn.set_read_only(true);

        // get_versioned should return the value
        let result = txn.get_versioned(&key).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().version.as_u64(), 7);

        // But read_set should remain empty
        assert!(
            txn.read_set.is_empty(),
            "read_set should be empty after get_versioned in read-only mode"
        );
    }

    #[test]
    fn test_read_only_get_versioned_nonexistent_skips_read_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let store = empty_store();
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);
        txn.set_read_only(true);

        let key = test_key(&ns, "missing");
        let result = txn.get_versioned(&key).unwrap();
        assert!(result.is_none());

        // Non-existent key lookup should also skip read_set
        assert!(
            txn.read_set.is_empty(),
            "read_set should be empty for non-existent key in read-only mode"
        );
    }

    #[test]
    fn test_read_only_scan_prefix_skips_read_set() {
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);
        let store = Arc::new(SegmentedStore::new());
        for i in 0..3 {
            let key = test_key(&ns, &format!("pfx:{}", i));
            store
                .put_with_version_mode(
                    key,
                    Value::Int(i as i64),
                    CommitVersion(1),
                    None,
                    WriteMode::Append,
                )
                .unwrap();
        }
        let mut txn = TransactionContext::with_store(TxnId(1), branch_id, store);
        txn.set_read_only(true);

        let prefix = test_key(&ns, "pfx:");
        let results = txn.scan_prefix(&prefix).unwrap();
        assert_eq!(results.len(), 3);

        // All 3 keys scanned, but read_set should be empty
        assert!(
            txn.read_set.is_empty(),
            "read_set should be empty after scan_prefix in read-only mode"
        );
    }

    // ========================================================================
    // Cross-Branch Key Rejection Tests (#1709)
    // ========================================================================

    #[test]
    fn test_issue_1709_get_rejects_cross_branch_key() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_key = Key::new(ns_b, TypeTag::KV, b"flag".to_vec());

        let store = Arc::new(SegmentedStore::new());
        let mut txn = TransactionContext::with_store(TxnId(1), branch_a, store);

        // Reading a key from branch B inside a branch A transaction must fail
        let err = txn.get(&cross_key).unwrap_err();
        assert!(
            format!("{}", err).contains("branch"),
            "Error should mention branch mismatch: {}",
            err
        );
    }

    #[test]
    fn test_issue_1709_put_rejects_cross_branch_key() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_key = Key::new(ns_b, TypeTag::KV, b"flag".to_vec());

        let mut txn = TransactionContext::new(TxnId(1), branch_a, CommitVersion(100));

        // Writing a key from branch B inside a branch A transaction must fail
        let err = txn.put(cross_key, Value::Int(1)).unwrap_err();
        assert!(
            format!("{}", err).contains("branch"),
            "Error should mention branch mismatch: {}",
            err
        );
    }

    #[test]
    fn test_issue_1709_delete_rejects_cross_branch_key() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_key = Key::new(ns_b, TypeTag::KV, b"flag".to_vec());

        let mut txn = TransactionContext::new(TxnId(1), branch_a, CommitVersion(100));

        // Deleting a key from branch B inside a branch A transaction must fail
        let err = txn.delete(cross_key).unwrap_err();
        assert!(
            format!("{}", err).contains("branch"),
            "Error should mention branch mismatch: {}",
            err
        );
    }

    #[test]
    fn test_issue_1709_cas_rejects_cross_branch_key() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_key = Key::new(ns_b, TypeTag::KV, b"flag".to_vec());

        let mut txn = TransactionContext::new(TxnId(1), branch_a, CommitVersion(100));

        // CAS on a key from branch B inside a branch A transaction must fail
        let err = txn
            .cas(cross_key, CommitVersion(1), Value::Int(2))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("branch"),
            "Error should mention branch mismatch: {}",
            err
        );
    }

    #[test]
    fn test_issue_1709_cas_with_read_rejects_cross_branch_key() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_key = Key::new(ns_b, TypeTag::KV, b"flag".to_vec());

        let store = Arc::new(SegmentedStore::new());
        let mut txn = TransactionContext::with_store(TxnId(1), branch_a, store);

        // cas_with_read on a key from branch B must fail
        let err = txn
            .cas_with_read(cross_key, CommitVersion::ZERO, Value::Int(2))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("branch"),
            "Error should mention branch mismatch: {}",
            err
        );
    }

    #[test]
    fn test_issue_1709_scan_prefix_rejects_cross_branch_key() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_prefix = Key::new(ns_b, TypeTag::KV, b"pfx:".to_vec());

        let store = Arc::new(SegmentedStore::new());
        let mut txn = TransactionContext::with_store(TxnId(1), branch_a, store);

        let err = txn.scan_prefix(&cross_prefix).unwrap_err();
        assert!(
            format!("{}", err).contains("branch"),
            "Error should mention branch mismatch: {}",
            err
        );
    }

    #[test]
    fn test_issue_1709_allow_cross_branch_bypasses_check() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns_b = Arc::new(Namespace::new(branch_b, "default".to_string()));
        let cross_key = Key::new(ns_b, TypeTag::KV, b"flag".to_vec());

        let store = Arc::new(SegmentedStore::new());
        let mut txn = TransactionContext::with_store(TxnId(1), branch_a, store);
        txn.set_allow_cross_branch(true);

        // With allow_cross_branch, cross-branch get should succeed (returns None, no data)
        assert!(txn.get(&cross_key).unwrap().is_none());
    }

    #[test]
    fn test_issue_1766_scan_prefix_sorted_merge_with_write_set() {
        // Verify scan_prefix returns correctly sorted, deduplicated results
        // when write_set entries interleave with and override snapshot entries.
        let branch_id = BranchId::new();
        let ns = test_namespace_for(branch_id);

        let store = Arc::new(SegmentedStore::new());
        // Insert snapshot keys: a, c, e, g
        for (name, val) in [("a", 1), ("c", 3), ("e", 5), ("g", 7)] {
            store
                .put_with_version_mode(
                    test_key(&ns, name),
                    Value::Int(val),
                    CommitVersion(10),
                    None,
                    WriteMode::Append,
                )
                .unwrap();
        }

        let mut txn = TransactionContext::with_store(TxnId(100), branch_id, store);

        // Buffer writes that interleave (b, d) and override (c, e)
        txn.put(test_key(&ns, "b"), Value::Int(20)).unwrap();
        txn.put(test_key(&ns, "d"), Value::Int(40)).unwrap();
        txn.put(test_key(&ns, "c"), Value::Int(30)).unwrap(); // overrides snapshot c=3
        txn.put(test_key(&ns, "e"), Value::Int(50)).unwrap(); // overrides snapshot e=5

        // Delete snapshot key g
        txn.delete(test_key(&ns, "g")).unwrap();

        let prefix = Key::new(ns.clone(), TypeTag::KV, vec![]);
        let results = txn.scan_prefix(&prefix).unwrap();

        let keys: Vec<String> = results
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        let vals: Vec<&Value> = results.iter().map(|(_, v)| v).collect();

        // Must be sorted: a, b, c, d, e (g deleted)
        assert_eq!(keys, vec!["a", "b", "c", "d", "e"]);
        // Snapshot values for a; write_set values for b, c, d, e
        assert_eq!(
            vals,
            vec![
                &Value::Int(1),
                &Value::Int(20),
                &Value::Int(30),
                &Value::Int(40),
                &Value::Int(50),
            ]
        );

        // read_set should contain snapshot keys (a, c, e, g) but NOT write-only keys (b, d)
        assert!(txn.read_set.contains_key(&test_key(&ns, "a")));
        assert!(txn.read_set.contains_key(&test_key(&ns, "c")));
        assert!(txn.read_set.contains_key(&test_key(&ns, "e")));
        assert!(txn.read_set.contains_key(&test_key(&ns, "g"))); // tracked even though deleted
        assert!(!txn.read_set.contains_key(&test_key(&ns, "b")));
        assert!(!txn.read_set.contains_key(&test_key(&ns, "d")));
    }
}
