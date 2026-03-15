//! JsonStore: JSON document storage primitive
//!
//! ## Design: STATELESS FACADE
//!
//! JsonStore holds ONLY `Arc<Database>`. No internal state, no caches,
//! no maps, no locks. All data lives in ShardedStore via Key::new_json().
//!
//! ## Branch Isolation
//!
//! All operations are scoped to a branch_id. Keys are prefixed with the
//! branch's namespace, ensuring complete isolation between branches.
//!
//! ## Thread Safety
//!
//! JsonStore is `Send + Sync` and can be safely shared across threads.
//! Multiple JsonStore instances on the same Database are safe.
//!
//! ## API
//!
//! All operations go through `db.transaction()` for consistency:
//! - `create`, `get`, `set`, `delete_at_path`, `destroy`, `list`, `exists`
//!
//! ## Architectural Rules
//!
//! This implementation follows the architectural rules:
//! 1. JSON lives in ShardedStore via Key::new_json()
//! 2. JsonStore is stateless (Arc<Database> only)
//! 3. JSON extends TransactionContext (no separate type)
//! 4. Path semantics in API layer (not storage)
//! 5. WAL remains unified (entry types 0x20-0x23)
//! 6. JSON API feels like other primitives

use crate::database::Database;
use crate::primitives::extensions::JsonStoreExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Version, Versioned};
use strata_core::primitives::json::{
    delete_at_path, get_at_path, set_at_path, JsonLimitError, JsonPath, JsonValue,
};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::StrataError;
use strata_core::{StrataResult, VersionedHistory};

// =============================================================================
// Limit Validation Helpers
// =============================================================================

/// Convert a JsonLimitError to a StrataError
fn limit_error_to_error(e: JsonLimitError) -> StrataError {
    StrataError::invalid_input(e.to_string())
}

// =============================================================================
// JsonDoc - Internal Document Representation
// =============================================================================

/// Internal representation of a JSON document
///
/// Stored as serialized bytes in ShardedStore.
/// Version is used for optimistic concurrency control.
///
/// # Design
///
/// - **Document-level versioning**: Single version for entire document
/// - **Timestamps**: Track creation and modification times
/// - **Serializable**: Uses MessagePack for efficient storage
///
/// # Example
///
/// ```text
/// use strata_engine::JsonDoc;
/// use strata_core::primitives::json::JsonValue;
///
/// let doc = JsonDoc::new("my-document", JsonValue::from(42i64));
/// assert_eq!(doc.version, 1);
/// assert_eq!(doc.id, "my-document");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonDoc {
    /// Document unique identifier (user-provided string key)
    pub id: String,
    /// The JSON value (root of document)
    pub value: JsonValue,
    /// Document version (increments on any change)
    pub version: u64,
    /// Creation timestamp (microseconds since epoch)
    pub created_at: u64,
    /// Last modification timestamp (microseconds since epoch)
    pub updated_at: u64,
}

/// Result of listing JSON documents
///
/// Contains document IDs and an optional cursor for pagination.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonListResult {
    /// Document IDs returned (user-provided string keys)
    pub doc_ids: Vec<String>,
    /// Cursor for next page, if more results exist
    pub next_cursor: Option<String>,
}

impl JsonDoc {
    /// Create a new document with initial value
    ///
    /// Initializes version to 1 and sets timestamps to current time.
    pub fn new(id: impl Into<String>, value: JsonValue) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        JsonDoc {
            id: id.into(),
            value,
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    /// Increment version and update timestamp
    ///
    /// Call this after any modification to the document.
    pub fn touch(&mut self) {
        self.version += 1;
        self.updated_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
    }
}

/// JSON document storage primitive
///
/// STATELESS FACADE over Database - all state lives in unified ShardedStore.
/// Multiple JsonStore instances on same Database are safe.
///
/// # Design
///
/// JsonStore does NOT own storage. It is a facade that:
/// - Uses `Arc<Database>` for all operations
/// - Stores documents via `Key::new_json()` in ShardedStore
/// - Uses MVCC versioned reads for fast path
/// - Participates in cross-primitive transactions
///
/// # Example
///
/// ```text
/// use strata_primitives::JsonStore;
/// use crate::database::Database;
/// use strata_core::types::BranchId;
/// use strata_core::primitives::json::JsonValue;
///
/// let db = Database::cache()?;
/// let json = JsonStore::new(db);
/// let branch_id = BranchId::new();
///
/// // Create and read document
/// json.create(&branch_id, "default", "my-doc", JsonValue::object())?;
/// let value = json.get(&branch_id, "default", "my-doc", &JsonPath::root())?;
/// ```
#[derive(Clone)]
pub struct JsonStore {
    db: Arc<Database>, // ONLY state: reference to database
}

impl JsonStore {
    /// Create new JsonStore instance
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Get the underlying database reference
    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Build namespace for branch+space-scoped operations
    fn namespace_for(&self, branch_id: &BranchId, space: &str) -> Namespace {
        Namespace::for_branch_space(*branch_id, space)
    }

    /// Build key for JSON document
    fn key_for(&self, branch_id: &BranchId, space: &str, doc_id: &str) -> Key {
        Key::new_json(Arc::new(self.namespace_for(branch_id, space)), doc_id)
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    /// Serialize document for storage
    ///
    /// Uses MessagePack for efficient binary serialization.
    pub(crate) fn serialize_doc(doc: &JsonDoc) -> StrataResult<Value> {
        let bytes =
            rmp_serde::to_vec(doc).map_err(|e| StrataError::serialization(e.to_string()))?;
        Ok(Value::Bytes(bytes))
    }

    /// Deserialize document from storage
    ///
    /// Expects Value::Bytes containing MessagePack-encoded JsonDoc.
    pub(crate) fn deserialize_doc(value: &Value) -> StrataResult<JsonDoc> {
        match value {
            Value::Bytes(bytes) => {
                rmp_serde::from_slice(bytes).map_err(|e| StrataError::serialization(e.to_string()))
            }
            _ => Err(StrataError::invalid_input("expected bytes for JsonDoc")),
        }
    }

    // ========================================================================
    // Document Operations
    // ========================================================================

    /// Create a new JSON document
    ///
    /// Creates a new document with version 1. Fails if a document with
    /// the same ID already exists.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - BranchId for namespace isolation
    /// * `space` - Logical space within the branch
    /// * `doc_id` - Unique document identifier
    /// * `value` - Initial JSON value for the document
    ///
    /// # Returns
    ///
    /// * `Ok(Version)` - Document created with version
    /// * `Err(InvalidOperation)` - Document already exists
    ///
    /// # Example
    ///
    /// ```text
    /// let version = json.create(&branch_id, "default", &doc_id, JsonValue::object())?;
    /// assert_eq!(version, Version::counter(1));
    /// ```
    pub fn create(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        value: JsonValue,
    ) -> StrataResult<Version> {
        // Validate document limits (Issue #440)
        value.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);
        let doc = JsonDoc::new(doc_id, value.clone());

        self.db.transaction(*branch_id, |txn| {
            // Check if document already exists
            if txn.get(&key)?.is_some() {
                return Err(StrataError::invalid_input(format!(
                    "JSON document {} already exists",
                    doc_id
                )));
            }

            let serialized = Self::serialize_doc(&doc)?;
            txn.put(key.clone(), serialized)?;
            Ok(Version::counter(doc.version))
        })
    }

    // ========================================================================
    // Reads
    // ========================================================================

    /// Get value at path in a document.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - BranchId for namespace isolation
    /// * `space` - Logical space within the branch
    /// * `doc_id` - Document to read from
    /// * `path` - Path within the document (use JsonPath::root() for whole doc)
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` - Value at path
    /// * `Ok(None)` - Document doesn't exist or path not found
    /// * `Err` - On deserialization error
    ///
    /// Use `getv()` to access version metadata and history.
    pub fn get(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
    ) -> StrataResult<Option<JsonValue>> {
        // Validate path limits (Issue #440)
        path.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);

        self.db.transaction(*branch_id, |txn| match txn.get(&key)? {
            Some(value) => {
                let doc = Self::deserialize_doc(&value)?;
                Ok(get_at_path(&doc.value, path).cloned())
            }
            None => Ok(None),
        })
    }

    /// Get value at path in a document, with version metadata.
    ///
    /// Reads directly from the committed store (non-transactional) to
    /// retrieve the document value together with its version and timestamp.
    /// Returns `None` if the document doesn't exist or path not found.
    pub fn get_versioned(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
    ) -> StrataResult<Option<Versioned<JsonValue>>> {
        use strata_core::contract::Versioned;

        let key = self.key_for(branch_id, space, doc_id);
        use strata_core::Storage;
        match self.db.storage().get_versioned(&key, u64::MAX)? {
            Some(vv) => {
                let doc = Self::deserialize_doc(&vv.value)?;
                match get_at_path(&doc.value, path).cloned() {
                    Some(json_val) => Ok(Some(Versioned::with_timestamp(
                        json_val,
                        strata_core::contract::Version::counter(doc.version),
                        vv.timestamp,
                    ))),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Get full version history for a JSON document.
    ///
    /// Returns `None` if the document doesn't exist. Index with `[0]` = latest,
    /// `[1]` = previous, etc. Reads directly from storage (non-transactional).
    ///
    /// History is per-document (not per-path). To inspect a path within a
    /// specific version, index into the result and navigate the `JsonValue`.
    pub fn getv(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
    ) -> StrataResult<Option<VersionedHistory<JsonValue>>> {
        let key = self.key_for(branch_id, space, doc_id);
        let history = self.db.get_history(&key, None, None)?;
        let versions: Vec<Versioned<JsonValue>> = history
            .iter()
            .filter_map(|vv| {
                let doc = Self::deserialize_doc(&vv.value).ok()?;
                Some(Versioned::with_timestamp(
                    doc.value,
                    Version::counter(doc.version),
                    vv.timestamp,
                ))
            })
            .collect();
        Ok(VersionedHistory::new(versions))
    }

    /// Set value at path, creating the document if it doesn't exist.
    ///
    /// Combines exists-check, create, and set into a single atomic transaction,
    /// producing exactly 1 WAL append regardless of whether the document exists.
    ///
    /// - If doc doesn't exist and path is root: creates document with value
    /// - If doc doesn't exist and path is non-root: creates with empty object, sets at path
    /// - If doc exists: sets value at path
    pub fn set_or_create(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version> {
        path.validate().map_err(limit_error_to_error)?;
        value.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);

        self.db.transaction(*branch_id, |txn| {
            match txn.get(&key)? {
                Some(stored) => {
                    // Document exists — set at path
                    let mut doc = Self::deserialize_doc(&stored)?;
                    set_at_path(&mut doc.value, path, value)
                        .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
                    doc.touch();
                    let serialized = Self::serialize_doc(&doc)?;
                    txn.put(key.clone(), serialized)?;
                    Ok(Version::counter(doc.version))
                }
                None => {
                    // Document doesn't exist — create with value at path
                    let initial = if path.is_root() {
                        value
                    } else {
                        let mut obj = JsonValue::object();
                        set_at_path(&mut obj, path, value).map_err(|e| {
                            StrataError::invalid_input(format!("Path error: {}", e))
                        })?;
                        obj
                    };
                    let doc = JsonDoc::new(doc_id, initial);
                    let serialized = Self::serialize_doc(&doc)?;
                    txn.put(key.clone(), serialized)?;
                    Ok(Version::counter(doc.version))
                }
            }
        })
    }

    /// Set multiple documents in a single transaction.
    ///
    /// Each entry does a set_or_create: creates the document if it doesn't exist,
    /// or sets the value at the given path if it does. All writes share one
    /// lock acquisition, one WAL record, and one commit.
    pub fn batch_set_or_create(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: Vec<(String, JsonPath, JsonValue)>,
    ) -> StrataResult<Vec<Result<Version, String>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        self.db.transaction(*branch_id, |txn| {
            let mut versions = Vec::with_capacity(entries.len());
            for (doc_id, path, value) in &entries {
                let key = self.key_for(branch_id, space, doc_id);

                let result = match txn.get(&key)? {
                    Some(stored) => {
                        let mut doc = Self::deserialize_doc(&stored)?;
                        set_at_path(&mut doc.value, path, value.clone()).map_err(|e| {
                            StrataError::invalid_input(format!("Path error: {}", e))
                        })?;
                        doc.touch();
                        let serialized = Self::serialize_doc(&doc)?;
                        txn.put(key, serialized)?;
                        Version::counter(doc.version)
                    }
                    None => {
                        let initial = if path.is_root() {
                            value.clone()
                        } else {
                            let mut obj = JsonValue::object();
                            set_at_path(&mut obj, path, value.clone()).map_err(|e| {
                                StrataError::invalid_input(format!("Path error: {}", e))
                            })?;
                            obj
                        };
                        let doc = JsonDoc::new(doc_id, initial);
                        let serialized = Self::serialize_doc(&doc)?;
                        txn.put(key, serialized)?;
                        Version::counter(doc.version)
                    }
                };
                versions.push(Ok(result));
            }
            Ok(versions)
        })
    }

    /// Check if document exists.
    pub fn exists(&self, branch_id: &BranchId, space: &str, doc_id: &str) -> StrataResult<bool> {
        let key = self.key_for(branch_id, space, doc_id);
        self.db
            .transaction(*branch_id, |txn| Ok(txn.get(&key)?.is_some()))
    }

    // ========================================================================
    // Mutations
    // ========================================================================

    /// Set value at path in a document
    ///
    /// Uses transaction for atomic read-modify-write.
    /// Increments document version on success.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - BranchId for namespace isolation
    /// * `space` - Logical space within the branch
    /// * `doc_id` - Document to modify
    /// * `path` - Path to set value at (creates intermediate objects/arrays)
    /// * `value` - New value to set
    ///
    /// # Returns
    ///
    /// * `Ok(Version)` - New document version after modification
    /// * `Err(InvalidOperation)` - Document doesn't exist
    /// * `Err` - On path error or serialization error
    pub fn set(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version> {
        // Validate path and value limits (Issue #440)
        path.validate().map_err(limit_error_to_error)?;
        value.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);

        self.db.transaction(*branch_id, |txn| {
            // Load existing document
            let stored = txn.get(&key)?.ok_or_else(|| {
                StrataError::invalid_input(format!("JSON document {} not found", doc_id))
            })?;
            let mut doc = Self::deserialize_doc(&stored)?;

            // Apply mutation
            set_at_path(&mut doc.value, path, value)
                .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
            doc.touch();

            // Store updated document
            let serialized = Self::serialize_doc(&doc)?;
            txn.put(key.clone(), serialized)?;

            Ok(Version::counter(doc.version))
        })
    }

    /// Delete value at path in a document
    ///
    /// Uses transaction for atomic read-modify-write.
    /// Increments document version on success.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - BranchId for namespace isolation
    /// * `space` - Logical space within the branch
    /// * `doc_id` - Document to modify
    /// * `path` - Path to delete (must not be root)
    ///
    /// # Returns
    ///
    /// * `Ok(Version)` - New document version after deletion
    /// * `Err(InvalidOperation)` - Document doesn't exist or path error
    ///
    /// # Example
    ///
    /// ```text
    /// // Remove a field from an object
    /// json.delete_at_path(&branch_id, "default", &doc_id, &"user.temp".parse().unwrap())?;
    /// ```
    pub fn delete_at_path(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
    ) -> StrataResult<Version> {
        // Validate path limits (Issue #440)
        path.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);

        self.db.transaction(*branch_id, |txn| {
            // Load existing document
            let stored = txn.get(&key)?.ok_or_else(|| {
                StrataError::invalid_input(format!("JSON document {} not found", doc_id))
            })?;
            let mut doc = Self::deserialize_doc(&stored)?;

            // Apply deletion
            delete_at_path(&mut doc.value, path)
                .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
            doc.touch();

            // Store updated document
            let serialized = Self::serialize_doc(&doc)?;
            txn.put(key.clone(), serialized)?;

            Ok(Version::counter(doc.version))
        })
    }

    /// Destroy (delete) an entire document
    ///
    /// Removes the document from storage. This operation is final.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - BranchId for namespace isolation
    /// * `space` - Logical space within the branch
    /// * `doc_id` - Document to destroy
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Document existed and was destroyed
    /// * `Ok(false)` - Document did not exist
    ///
    /// # Example
    ///
    /// ```text
    /// let existed = json.destroy(&branch_id, "default", &doc_id)?;
    /// assert!(existed);
    /// ```
    pub fn destroy(&self, branch_id: &BranchId, space: &str, doc_id: &str) -> StrataResult<bool> {
        let key = self.key_for(branch_id, space, doc_id);

        self.db.transaction(*branch_id, |txn| {
            // Check if document exists
            if txn.get(&key)?.is_none() {
                return Ok(false);
            }

            // Delete the document
            txn.delete(key.clone())?;
            Ok(true)
        })
    }

    // ========================================================================
    // Introspection
    // ========================================================================

    /// List documents in the store with cursor-based pagination
    ///
    /// Supports Primitive Contract Invariant 6: Introspectable.
    /// Returns document IDs for a branch, optionally filtered by prefix.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - BranchId for namespace isolation
    /// * `space` - Logical space within the branch
    /// * `prefix` - Optional prefix to filter document IDs (compared as strings)
    /// * `cursor` - Resume pagination from this cursor (doc_id to start after)
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// * `Ok(JsonListResult)` - Document IDs and optional next cursor
    ///
    /// # Example
    ///
    /// ```text
    /// // List first 10 documents
    /// let result = json.list(&branch_id, "default", None, None, 10)?;
    ///
    /// // List documents with prefix "user:"
    /// let result = json.list(&branch_id, "default", Some("user:"), None, 10)?;
    ///
    /// // Paginate through results
    /// let page1 = json.list(&branch_id, "default", None, None, 10)?;
    /// if let Some(cursor) = page1.next_cursor {
    ///     let page2 = json.list(&branch_id, "default", None, Some(&cursor), 10)?;
    /// }
    /// ```
    pub fn list(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> StrataResult<JsonListResult> {
        let ns = self.namespace_for(branch_id, space);
        // Narrow scan at storage level: if prefix is given, only scan matching keys
        let scan_prefix = Key::new_json(Arc::new(ns), prefix.unwrap_or(""));

        self.db.transaction(*branch_id, |txn| {
            let mut doc_ids = Vec::with_capacity(limit + 1);
            let mut past_cursor = cursor.is_none();

            for (key, _value) in txn.scan_prefix(&scan_prefix)? {
                // Extract doc_id from Key instead of deserializing the full Value
                let doc_id = match key.user_key_string() {
                    Some(id) => id,
                    None => continue,
                };

                // Handle cursor: skip until we're past the cursor
                if !past_cursor {
                    if cursor == Some(doc_id.as_str()) {
                        past_cursor = true;
                    }
                    continue;
                }

                doc_ids.push(doc_id);

                // Collect limit + 1 to detect if there are more
                if doc_ids.len() > limit {
                    break;
                }
            }

            // If we have more than limit, pop the last and use it as cursor
            let next_cursor = if doc_ids.len() > limit {
                doc_ids.pop();
                doc_ids.last().cloned()
            } else {
                None
            };

            Ok(JsonListResult {
                doc_ids,
                next_cursor,
            })
        })
    }
    // ========== Time-Travel API ==========

    /// Get value at path in a document as of a past timestamp.
    ///
    /// Returns the document value at the given timestamp, or None if the
    /// document didn't exist then or path wasn't found.
    pub fn get_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
        as_of_ts: u64,
    ) -> StrataResult<Option<JsonValue>> {
        path.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);
        let result = self.db.get_at_timestamp(&key, as_of_ts)?;
        match result {
            Some(vv) => {
                let doc = Self::deserialize_doc(&vv.value)?;
                Ok(get_at_path(&doc.value, path).cloned())
            }
            None => Ok(None),
        }
    }

    /// List document IDs as of a past timestamp.
    pub fn list_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        as_of_ts: u64,
    ) -> StrataResult<Vec<String>> {
        let ns = self.namespace_for(branch_id, space);
        // Narrow scan at storage level: if prefix is given, only scan matching keys
        let scan_key = Key::new_json(Arc::new(ns), prefix.unwrap_or(""));
        let results = self.db.scan_prefix_at_timestamp(&scan_key, as_of_ts)?;
        let mut doc_ids = Vec::new();
        for (key, _vv) in results {
            if let Some(doc_id) = key.user_key_string() {
                doc_ids.push(doc_id);
            }
        }
        Ok(doc_ids)
    }
}

// ========== Searchable Trait Implementation ==========

impl crate::search::Searchable for JsonStore {
    fn search(
        &self,
        _req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        // Search is handled by the intelligence layer, not the primitive
        Ok(crate::SearchResponse::empty())
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Json
    }
}

// =============================================================================
// JsonStoreExt Implementation
// =============================================================================
//
// Extension trait implementation for cross-primitive transactions.
// Allows JSON operations within a TransactionContext.

impl JsonStoreExt for TransactionContext {
    fn json_get(&mut self, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>> {
        // Validate path limits (Issue #440)
        path.validate().map_err(limit_error_to_error)?;

        let key = Key::new_json(Arc::new(Namespace::for_branch(self.branch_id)), doc_id);

        // Read from transaction context (respects read-your-writes)
        match self.get(&key)? {
            Some(value) => {
                let doc = JsonStore::deserialize_doc(&value)?;
                Ok(get_at_path(&doc.value, path).cloned())
            }
            None => Ok(None),
        }
    }

    fn json_set(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version> {
        // Validate path and value limits (Issue #440)
        path.validate().map_err(limit_error_to_error)?;
        value.validate().map_err(limit_error_to_error)?;

        let key = Key::new_json(Arc::new(Namespace::for_branch(self.branch_id)), doc_id);

        // Load existing document from transaction context
        let stored = self.get(&key)?.ok_or_else(|| {
            StrataError::invalid_input(format!("JSON document {} not found", doc_id))
        })?;
        let mut doc = JsonStore::deserialize_doc(&stored)?;

        // Apply mutation
        set_at_path(&mut doc.value, path, value)
            .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
        doc.touch();

        // Store updated document in transaction write set
        let serialized = JsonStore::serialize_doc(&doc)?;
        self.put(key, serialized)?;

        Ok(Version::counter(doc.version))
    }

    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> StrataResult<Version> {
        // Validate document limits (Issue #440)
        value.validate().map_err(limit_error_to_error)?;

        let key = Key::new_json(Arc::new(Namespace::for_branch(self.branch_id)), doc_id);
        let doc = JsonDoc::new(doc_id, value);

        // Check if document already exists
        if self.get(&key)?.is_some() {
            return Err(StrataError::invalid_input(format!(
                "JSON document {} already exists",
                doc_id
            )));
        }

        // Store new document
        let serialized = JsonStore::serialize_doc(&doc)?;
        self.put(key, serialized)?;

        Ok(Version::counter(doc.version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jsonstore_is_stateless() {
        // JsonStore should have size of single Arc pointer
        assert_eq!(
            std::mem::size_of::<JsonStore>(),
            std::mem::size_of::<Arc<Database>>()
        );
    }

    #[test]
    fn test_jsonstore_is_clone() {
        let db = Database::cache().unwrap();
        let store1 = JsonStore::new(db.clone());
        let store2 = store1.clone();
        assert!(Arc::ptr_eq(store1.database(), store2.database()));
    }

    #[test]
    fn test_jsonstore_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<JsonStore>();
    }

    #[test]
    fn test_key_for_branch_isolation() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let doc_id = "test-doc";

        let key1 = store.key_for(&branch1, "default", &doc_id);
        let key2 = store.key_for(&branch2, "default", &doc_id);

        // Keys for different branches should be different even for same doc_id
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_key_for_same_branch() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let key1 = store.key_for(&branch_id, "default", &doc_id);
        let key2 = store.key_for(&branch_id, "default", &doc_id);

        // Same branch and doc_id should produce same key
        assert_eq!(key1, key2);
    }

    // ========================================
    // JsonDoc Tests
    // ========================================

    #[test]
    fn test_json_doc_new() {
        let id = "test-doc";
        let value = JsonValue::from(42i64);
        let doc = JsonDoc::new(id, value.clone());

        assert_eq!(doc.id, id);
        assert_eq!(doc.value, value);
        assert_eq!(doc.version, 1);
        assert!(doc.created_at > 0);
        assert_eq!(doc.created_at, doc.updated_at);
    }

    #[test]
    fn test_json_doc_touch() {
        let id = "test-doc";
        let value = JsonValue::from(42i64);
        let mut doc = JsonDoc::new(id, value);

        let old_version = doc.version;
        let old_updated = doc.updated_at;

        // Sleep a tiny bit to ensure timestamp changes
        std::thread::sleep(std::time::Duration::from_millis(2));
        doc.touch();

        assert_eq!(doc.version, old_version + 1);
        assert!(doc.updated_at >= old_updated);
        // created_at should not change
        assert_eq!(doc.created_at, doc.created_at);
    }

    #[test]
    fn test_json_doc_touch_multiple() {
        let id = "test-doc";
        let value = JsonValue::object();
        let mut doc = JsonDoc::new(id, value);

        for i in 0..5 {
            doc.touch();
            assert_eq!(doc.version, 2 + i);
        }
        assert_eq!(doc.version, 6);
    }

    // ========================================
    // Serialization Tests
    // ========================================

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let db = Database::cache().unwrap();
        let _store = JsonStore::new(db);

        let doc = JsonDoc::new("test-doc", JsonValue::from("test value"));

        let serialized = JsonStore::serialize_doc(&doc).unwrap();
        let deserialized = JsonStore::deserialize_doc(&serialized).unwrap();

        assert_eq!(doc.id, deserialized.id);
        assert_eq!(doc.value, deserialized.value);
        assert_eq!(doc.version, deserialized.version);
        assert_eq!(doc.created_at, deserialized.created_at);
        assert_eq!(doc.updated_at, deserialized.updated_at);
    }

    #[test]
    fn test_serialize_complex_document() {
        let db = Database::cache().unwrap();
        let _store = JsonStore::new(db);

        let value: JsonValue = serde_json::json!({
            "string": "hello",
            "number": 42,
            "boolean": true,
            "null": null,
            "array": [1, 2, 3],
            "nested": {
                "foo": "bar"
            }
        })
        .into();

        let doc = JsonDoc::new("test-doc", value);

        let serialized = JsonStore::serialize_doc(&doc).unwrap();
        let deserialized = JsonStore::deserialize_doc(&serialized).unwrap();

        assert_eq!(doc.value, deserialized.value);
    }

    #[test]
    fn test_deserialize_invalid_type() {
        let db = Database::cache().unwrap();
        let _store = JsonStore::new(db);

        // Try to deserialize a non-bytes value
        let invalid = Value::Int(42);
        let result = JsonStore::deserialize_doc(&invalid);

        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_invalid_bytes() {
        let db = Database::cache().unwrap();
        let _store = JsonStore::new(db);

        // Try to deserialize garbage bytes
        let invalid = Value::Bytes(vec![0, 1, 2, 3, 4, 5]);
        let result = JsonStore::deserialize_doc(&invalid);

        assert!(result.is_err());
    }

    #[test]
    fn test_serialized_size_is_compact() {
        let db = Database::cache().unwrap();
        let _store = JsonStore::new(db);

        let doc = JsonDoc::new("test-doc", JsonValue::from(42i64));

        let serialized = JsonStore::serialize_doc(&doc).unwrap();

        match serialized {
            Value::Bytes(bytes) => {
                // MessagePack should produce reasonably compact output
                // UUID (16 bytes) + value + version + timestamps should be < 100 bytes
                assert!(bytes.len() < 100);
            }
            _ => panic!("Expected bytes"),
        }
    }

    // ========================================
    // Create Tests
    // ========================================

    #[test]
    fn test_create_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let version = store
            .create(&branch_id, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();
        assert_eq!(version, Version::counter(1));
    }

    #[test]
    fn test_create_object_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "name": "Alice",
            "age": 30
        })
        .into();

        let version = store.create(&branch_id, "default", &doc_id, value).unwrap();
        assert_eq!(version, Version::counter(1));
    }

    #[test]
    fn test_create_duplicate_fails() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        // First create succeeds
        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(1i64))
            .unwrap();

        // Second create with same ID fails
        let result = store.create(&branch_id, "default", &doc_id, JsonValue::from(2i64));
        assert!(result.is_err());
    }

    #[test]
    fn test_create_different_docs() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        let doc1 = "doc-1";
        let doc2 = "doc-2";

        let v1 = store
            .create(&branch_id, "default", &doc1, JsonValue::from(1i64))
            .unwrap();
        let v2 = store
            .create(&branch_id, "default", &doc2, JsonValue::from(2i64))
            .unwrap();

        assert_eq!(v1, Version::counter(1));
        assert_eq!(v2, Version::counter(1));
    }

    #[test]
    fn test_create_branch_isolation() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let doc_id = "test-doc";

        // Same doc_id can be created in different branches
        let v1 = store
            .create(&branch1, "default", &doc_id, JsonValue::from(1i64))
            .unwrap();
        let v2 = store
            .create(&branch2, "default", &doc_id, JsonValue::from(2i64))
            .unwrap();

        assert_eq!(v1, Version::counter(1));
        assert_eq!(v2, Version::counter(1));
    }

    #[test]
    fn test_create_null_value() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let version = store
            .create(&branch_id, "default", &doc_id, JsonValue::null())
            .unwrap();
        assert_eq!(version, Version::counter(1));
    }

    #[test]
    fn test_create_empty_object() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let version = store
            .create(&branch_id, "default", &doc_id, JsonValue::object())
            .unwrap();
        assert_eq!(version, Version::counter(1));
    }

    #[test]
    fn test_create_empty_array() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let version = store
            .create(&branch_id, "default", &doc_id, JsonValue::array())
            .unwrap();
        assert_eq!(version, Version::counter(1));
    }

    // ========================================
    // Get Tests
    // ========================================

    #[test]
    fn test_get_root() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();

        let value = store
            .get(&branch_id, "default", &doc_id, &JsonPath::root())
            .unwrap();
        assert_eq!(value.and_then(|v| v.as_i64()), Some(42));
    }

    #[test]
    fn test_get_at_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "name": "Alice",
            "age": 30
        })
        .into();

        store.create(&branch_id, "default", &doc_id, value).unwrap();

        let name = store
            .get(&branch_id, "default", &doc_id, &"name".parse().unwrap())
            .unwrap();
        assert_eq!(
            name.and_then(|v| v.as_str().map(String::from)),
            Some("Alice".to_string())
        );

        let age = store
            .get(&branch_id, "default", &doc_id, &"age".parse().unwrap())
            .unwrap();
        assert_eq!(age.and_then(|v| v.as_i64()), Some(30));
    }

    #[test]
    fn test_get_nested_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "user": {
                "profile": {
                    "name": "Bob"
                }
            }
        })
        .into();

        store.create(&branch_id, "default", &doc_id, value).unwrap();

        let name = store
            .get(
                &branch_id,
                "default",
                &doc_id,
                &"user.profile.name".parse().unwrap(),
            )
            .unwrap();
        assert_eq!(
            name.and_then(|v| v.as_str().map(String::from)),
            Some("Bob".to_string())
        );
    }

    #[test]
    fn test_get_array_element() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "items": ["a", "b", "c"]
        })
        .into();

        store.create(&branch_id, "default", &doc_id, value).unwrap();

        let item = store
            .get(&branch_id, "default", &doc_id, &"items[1]".parse().unwrap())
            .unwrap();
        assert_eq!(
            item.and_then(|v| v.as_str().map(String::from)),
            Some("b".to_string())
        );
    }

    #[test]
    fn test_get_missing_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let result = store
            .get(&branch_id, "default", &doc_id, &JsonPath::root())
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_missing_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::object())
            .unwrap();

        let result = store
            .get(
                &branch_id,
                "default",
                &doc_id,
                &"nonexistent".parse().unwrap(),
            )
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_exists() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        assert!(!store.exists(&branch_id, "default", &doc_id).unwrap());

        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();

        assert!(store.exists(&branch_id, "default", &doc_id).unwrap());
    }

    #[test]
    fn test_exists_branch_isolation() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch1, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();

        // Document exists in branch1 but not in branch2
        assert!(store.exists(&branch1, "default", &doc_id).unwrap());
        assert!(!store.exists(&branch2, "default", &doc_id).unwrap());
    }

    // ========================================
    // Set Tests
    // ========================================

    #[test]
    fn test_set_at_root() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();

        let v2 = store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &JsonPath::root(),
                JsonValue::from(100i64),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let value = store
            .get(&branch_id, "default", &doc_id, &JsonPath::root())
            .unwrap();
        assert_eq!(value.and_then(|v| v.as_i64()), Some(100));
    }

    #[test]
    fn test_set_at_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::object())
            .unwrap();

        let v2 = store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &"name".parse().unwrap(),
                JsonValue::from("Alice"),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let name = store
            .get(&branch_id, "default", &doc_id, &"name".parse().unwrap())
            .unwrap();
        assert_eq!(
            name.and_then(|v| v.as_str().map(String::from)),
            Some("Alice".to_string())
        );
    }

    #[test]
    fn test_set_nested_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::object())
            .unwrap();

        // Creates intermediate objects automatically
        let v2 = store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &"user.profile.name".parse().unwrap(),
                JsonValue::from("Bob"),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let name = store
            .get(
                &branch_id,
                "default",
                &doc_id,
                &"user.profile.name".parse().unwrap(),
            )
            .unwrap();
        assert_eq!(
            name.and_then(|v| v.as_str().map(String::from)),
            Some("Bob".to_string())
        );
    }

    #[test]
    fn test_set_increments_version() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let v1 = store
            .create(&branch_id, "default", &doc_id, JsonValue::object())
            .unwrap();
        assert_eq!(v1, Version::counter(1));

        let v2 = store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &"a".parse().unwrap(),
                JsonValue::from(1i64),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let v3 = store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &"b".parse().unwrap(),
                JsonValue::from(2i64),
            )
            .unwrap();
        assert_eq!(v3, Version::counter(3));
    }

    #[test]
    fn test_set_missing_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let result = store.set(
            &branch_id,
            "default",
            &doc_id,
            &"name".parse().unwrap(),
            JsonValue::from("test"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_set_overwrites_value() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({ "name": "Alice" }).into();
        store.create(&branch_id, "default", &doc_id, value).unwrap();

        store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &"name".parse().unwrap(),
                JsonValue::from("Bob"),
            )
            .unwrap();

        let name = store
            .get(&branch_id, "default", &doc_id, &"name".parse().unwrap())
            .unwrap();
        assert_eq!(
            name.and_then(|v| v.as_str().map(String::from)),
            Some("Bob".to_string())
        );
    }

    #[test]
    fn test_set_array_element() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({ "items": [1, 2, 3] }).into();
        store.create(&branch_id, "default", &doc_id, value).unwrap();

        store
            .set(
                &branch_id,
                "default",
                &doc_id,
                &"items[1]".parse().unwrap(),
                JsonValue::from(999i64),
            )
            .unwrap();

        let item = store
            .get(&branch_id, "default", &doc_id, &"items[1]".parse().unwrap())
            .unwrap();
        assert_eq!(item.and_then(|v| v.as_i64()), Some(999));
    }

    // ========================================
    // Delete at Path Tests
    // ========================================

    #[test]
    fn test_delete_at_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "name": "Alice",
            "age": 30
        })
        .into();
        store.create(&branch_id, "default", &doc_id, value).unwrap();

        // Delete the "age" field
        let v2 = store
            .delete_at_path(&branch_id, "default", &doc_id, &"age".parse().unwrap())
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        // Verify "age" is gone but "name" remains
        assert!(store
            .get(&branch_id, "default", &doc_id, &"age".parse().unwrap())
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .get(&branch_id, "default", &doc_id, &"name".parse().unwrap())
                .unwrap()
                .and_then(|v| v.as_str().map(String::from)),
            Some("Alice".to_string())
        );
    }

    #[test]
    fn test_delete_at_nested_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "user": {
                "profile": {
                    "name": "Bob",
                    "temp": "to_delete"
                }
            }
        })
        .into();
        store.create(&branch_id, "default", &doc_id, value).unwrap();

        // Delete nested field
        let v2 = store
            .delete_at_path(
                &branch_id,
                "default",
                &doc_id,
                &"user.profile.temp".parse().unwrap(),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        // Verify "temp" is gone
        assert!(store
            .get(
                &branch_id,
                "default",
                &doc_id,
                &"user.profile.temp".parse().unwrap()
            )
            .unwrap()
            .is_none());

        // Verify "name" remains
        assert_eq!(
            store
                .get(
                    &branch_id,
                    "default",
                    &doc_id,
                    &"user.profile.name".parse().unwrap()
                )
                .unwrap()
                .and_then(|v| v.as_str().map(String::from)),
            Some("Bob".to_string())
        );
    }

    #[test]
    fn test_delete_at_path_array_element() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "items": ["a", "b", "c"]
        })
        .into();
        store.create(&branch_id, "default", &doc_id, value).unwrap();

        // Delete middle element
        let v2 = store
            .delete_at_path(&branch_id, "default", &doc_id, &"items[1]".parse().unwrap())
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        // Array should now be ["a", "c"]
        let items = store
            .get(&branch_id, "default", &doc_id, &"items".parse().unwrap())
            .unwrap()
            .unwrap();
        let arr = items.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].as_str(), Some("a"));
        assert_eq!(arr[1].as_str(), Some("c"));
    }

    #[test]
    fn test_delete_at_path_increments_version() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "a": 1,
            "b": 2,
            "c": 3
        })
        .into();
        let v1 = store.create(&branch_id, "default", &doc_id, value).unwrap();
        assert_eq!(v1, Version::counter(1));

        let v2 = store
            .delete_at_path(&branch_id, "default", &doc_id, &"a".parse().unwrap())
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let v3 = store
            .delete_at_path(&branch_id, "default", &doc_id, &"b".parse().unwrap())
            .unwrap();
        assert_eq!(v3, Version::counter(3));
    }

    #[test]
    fn test_delete_at_path_missing_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let result =
            store.delete_at_path(&branch_id, "default", &doc_id, &"field".parse().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_at_path_missing_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::object())
            .unwrap();

        // Deleting a nonexistent path is idempotent (succeeds, increments version)
        let v2 = store
            .delete_at_path(
                &branch_id,
                "default",
                &doc_id,
                &"nonexistent".parse().unwrap(),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2)); // Version still increments even though nothing was removed
    }

    // ========================================
    // Destroy Tests
    // ========================================

    #[test]
    fn test_destroy_existing_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();
        assert!(store.exists(&branch_id, "default", &doc_id).unwrap());

        let existed = store.destroy(&branch_id, "default", &doc_id).unwrap();
        assert!(existed);
        assert!(!store.exists(&branch_id, "default", &doc_id).unwrap());
    }

    #[test]
    fn test_destroy_nonexistent_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let existed = store.destroy(&branch_id, "default", &doc_id).unwrap();
        assert!(!existed);
    }

    #[test]
    fn test_destroy_branch_isolation() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let doc_id = "test-doc";

        // Create document in both branches
        store
            .create(&branch1, "default", &doc_id, JsonValue::from(1i64))
            .unwrap();
        store
            .create(&branch2, "default", &doc_id, JsonValue::from(2i64))
            .unwrap();

        // Destroy in branch1
        store.destroy(&branch1, "default", &doc_id).unwrap();

        // Document should be gone from branch1 but still exist in branch2
        assert!(!store.exists(&branch1, "default", &doc_id).unwrap());
        assert!(store.exists(&branch2, "default", &doc_id).unwrap());
    }

    #[test]
    fn test_destroy_then_recreate() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        // Create, destroy, recreate
        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(1i64))
            .unwrap();
        store.destroy(&branch_id, "default", &doc_id).unwrap();

        // Should be able to recreate with new value
        let version = store
            .create(&branch_id, "default", &doc_id, JsonValue::from(2i64))
            .unwrap();
        assert_eq!(version, Version::counter(1)); // Fresh document starts at version 1

        let value = store
            .get(&branch_id, "default", &doc_id, &JsonPath::root())
            .unwrap();
        assert_eq!(value.and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn test_destroy_complex_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let value: JsonValue = serde_json::json!({
            "user": {
                "name": "Alice",
                "items": [1, 2, 3],
                "nested": {
                    "deep": {
                        "value": true
                    }
                }
            }
        })
        .into();
        store.create(&branch_id, "default", &doc_id, value).unwrap();

        let existed = store.destroy(&branch_id, "default", &doc_id).unwrap();
        assert!(existed);
        assert!(!store.exists(&branch_id, "default", &doc_id).unwrap());
    }

    #[test]
    fn test_destroy_idempotent() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", &doc_id, JsonValue::from(42i64))
            .unwrap();

        // First destroy returns true
        assert!(store.destroy(&branch_id, "default", &doc_id).unwrap());

        // Subsequent destroys return false
        assert!(!store.destroy(&branch_id, "default", &doc_id).unwrap());
        assert!(!store.destroy(&branch_id, "default", &doc_id).unwrap());
    }

    // ========== Batch API Tests ==========

    #[test]
    fn test_batch_set_or_create_basic() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        let entries = vec![
            ("doc1".to_string(), JsonPath::root(), JsonValue::from(42i64)),
            (
                "doc2".to_string(),
                JsonPath::root(),
                JsonValue::from("hello"),
            ),
        ];

        let results = store
            .batch_set_or_create(&branch_id, "default", entries)
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());

        // Verify persistence
        let v1 = store
            .get(&branch_id, "default", "doc1", &JsonPath::root())
            .unwrap();
        assert_eq!(v1.and_then(|v| v.as_i64()), Some(42));

        let v2 = store
            .get(&branch_id, "default", "doc2", &JsonPath::root())
            .unwrap();
        assert_eq!(
            v2.and_then(|v| v.as_str().map(String::from)),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_batch_set_or_create_empty() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        let results = store
            .batch_set_or_create(&branch_id, "default", vec![])
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_set_or_create_updates_existing() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        store
            .create(&branch_id, "default", "existing", JsonValue::from("old"))
            .unwrap();

        let entries = vec![(
            "existing".to_string(),
            JsonPath::root(),
            JsonValue::from("new"),
        )];

        let results = store
            .batch_set_or_create(&branch_id, "default", entries)
            .unwrap();
        assert_eq!(*results[0].as_ref().unwrap(), Version::counter(2));

        let v = store
            .get(&branch_id, "default", "existing", &JsonPath::root())
            .unwrap();
        assert_eq!(
            v.and_then(|v| v.as_str().map(String::from)),
            Some("new".to_string())
        );
    }

    // ========== Time-Travel Boundary Tests ==========

    #[test]
    fn test_get_at_exact_versioned_timestamp() {
        // Reproduces: get_versioned returns timestamp T1 (JsonDoc.updated_at),
        // but get_at compares against storage-layer timestamp T2 (StoredValue).
        // Since T2 > T1, using T1 as as_of fails to find the value.
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "tt-doc";

        let root = JsonPath::root();

        store
            .create(&branch_id, "default", doc_id, JsonValue::from("v1"))
            .unwrap();

        // Capture the timestamp returned by get_versioned
        let versioned = store
            .get_versioned(&branch_id, "default", doc_id, &root)
            .unwrap()
            .unwrap();
        let ts = versioned.timestamp.as_micros();

        // Update to v2
        store
            .set(&branch_id, "default", doc_id, &root, JsonValue::from("v2"))
            .unwrap();

        // Reading at the exact timestamp of v1 should return v1, not None
        let result = store
            .get_at(&branch_id, "default", doc_id, &root, ts)
            .unwrap();
        assert!(
            result.is_some(),
            "get_at with exact versioned timestamp returned None — \
             timestamp mismatch between JsonDoc.updated_at and StoredValue.timestamp"
        );
        assert_eq!(result.unwrap(), JsonValue::from("v1"));
    }
}
