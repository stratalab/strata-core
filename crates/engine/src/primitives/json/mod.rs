//! JsonStore: JSON document storage primitive
//!
//! ## Design: STATELESS FACADE
//!
//! JsonStore holds ONLY `Arc<Database>`. No internal state, no caches,
//! no maps, no locks. All data lives in SegmentedStore via Key::new_json().
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
//! 1. JSON lives in SegmentedStore via Key::new_json()
//! 2. JsonStore is stateless (Arc<Database> only)
//! 3. JSON extends TransactionContext (no separate type)
//! 4. Path semantics in API layer (not storage)
//! 5. WAL remains unified (entry types 0x20-0x23)
//! 6. JSON API feels like other primitives

pub mod index;

use crate::database::Database;
use crate::primitives::extensions::JsonStoreExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Version, Versioned};
use strata_core::id::CommitVersion;
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
/// Stored as serialized bytes in SegmentedStore.
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
    /// Document unique identifier (user-provided string key).
    ///
    /// NOTE: This field is redundant with the storage key (Key::new_json).
    /// It costs ~20-100 bytes per document. Kept for error messages and
    /// debugging convenience. Consider removing in a future format version.
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
    pub(crate) fn touch(&mut self) {
        self.version += 1;
        self.updated_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
    }
}

/// JSON document storage primitive
///
/// STATELESS FACADE over Database - all state lives in unified SegmentedStore.
/// Multiple JsonStore instances on same Database are safe.
///
/// # Design
///
/// JsonStore does NOT own storage. It is a facade that:
/// - Uses `Arc<Database>` for all operations
/// - Stores documents via `Key::new_json()` in SegmentedStore
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
    fn namespace_for(&self, branch_id: &BranchId, space: &str) -> Arc<Namespace> {
        Arc::new(Namespace::for_branch_space(*branch_id, space))
    }

    /// Build key for JSON document
    fn key_for(&self, branch_id: &BranchId, space: &str, doc_id: &str) -> Key {
        Key::new_json(self.namespace_for(branch_id, space), doc_id)
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    /// Format version byte for JsonDoc serialization.
    ///
    /// Version 1: MessagePack-encoded JsonDoc (original format, no header).
    /// Version 2: 1-byte version prefix (0x02) + MessagePack-encoded JsonDoc.
    ///
    /// On read, if the first byte is NOT a valid version tag, we fall back to
    /// v1 (raw MessagePack) for backward compatibility with existing data.
    const FORMAT_VERSION: u8 = 0x02;

    /// Serialize document for storage
    ///
    /// Writes a version byte prefix followed by MessagePack payload.
    pub(crate) fn serialize_doc(doc: &JsonDoc) -> StrataResult<Value> {
        let payload =
            rmp_serde::to_vec(doc).map_err(|e| StrataError::serialization(e.to_string()))?;
        let mut bytes = Vec::with_capacity(1 + payload.len());
        bytes.push(Self::FORMAT_VERSION);
        bytes.extend_from_slice(&payload);
        Ok(Value::Bytes(bytes))
    }

    /// Deserialize document from storage
    ///
    /// Dispatches to version-specific deserializer based on format version byte.
    /// Falls back to v1 (raw MessagePack) if no recognized version tag is found,
    /// ensuring backward compatibility with documents written before versioning.
    pub(crate) fn deserialize_doc(value: &Value) -> StrataResult<JsonDoc> {
        match value {
            Value::Bytes(bytes) if bytes.is_empty() => {
                Err(StrataError::serialization("empty JsonDoc bytes"))
            }
            Value::Bytes(bytes) => {
                if bytes[0] == Self::FORMAT_VERSION {
                    // v2: skip version byte, deserialize payload
                    rmp_serde::from_slice(&bytes[1..])
                        .map_err(|e| StrataError::serialization(e.to_string()))
                } else {
                    // v1 fallback: no version header, raw MessagePack
                    rmp_serde::from_slice(bytes)
                        .map_err(|e| StrataError::serialization(e.to_string()))
                }
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

        let version = self.db.transaction(*branch_id, |txn| {
            crate::primitives::space::ensure_space_registered_in_txn(txn, branch_id, space)?;

            // Check if document already exists
            if txn.get(&key)?.is_some() {
                return Err(StrataError::invalid_input(format!(
                    "JSON document {} already exists",
                    doc_id
                )));
            }

            let serialized = Self::serialize_doc(&doc)?;
            txn.put(key.clone(), serialized)?;

            // Update secondary indexes
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;
            if !indexes.is_empty() {
                Self::update_index_entries(
                    txn,
                    branch_id,
                    space,
                    doc_id,
                    None,
                    Some(&doc.value),
                    &indexes,
                )?;
            }

            Ok(Version::counter(doc.version))
        })?;

        // Update inverted index for BM25 search (zero overhead when disabled)
        Self::index_json_doc(&self.db, branch_id, space, doc_id, &value)?;

        Ok(version)
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
        match self.db.storage().get_versioned(&key, CommitVersion::MAX)? {
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
        let mut corrupt_count: u32 = 0;
        let versions: Vec<Versioned<JsonValue>> = history
            .iter()
            .filter_map(|vv| match Self::deserialize_doc(&vv.value) {
                Ok(doc) => Some(Versioned::with_timestamp(
                    doc.value,
                    Version::counter(doc.version),
                    vv.timestamp,
                )),
                Err(e) => {
                    corrupt_count += 1;
                    tracing::warn!(
                        target: "strata::json",
                        doc_id = doc_id,
                        error = %e,
                        corrupt_count = corrupt_count,
                        "Skipping corrupt document entry in version history"
                    );
                    None
                }
            })
            .collect();
        if corrupt_count > 0 {
            tracing::error!(
                target: "strata::json",
                doc_id = doc_id,
                corrupt_count = corrupt_count,
                total_versions = versions.len() + corrupt_count as usize,
                "Version history contains corrupt entries"
            );
        }
        Ok(VersionedHistory::new(versions))
    }

    /// Index a JSON document into the InvertedIndex for BM25 search.
    /// Zero overhead when the index is disabled.
    ///
    /// `pub(crate)` so `branch_ops::primitive_merge::JsonMergeHandler` can
    /// refresh BM25 index entries for documents touched by a JSON merge.
    pub(crate) fn index_json_doc(
        db: &Arc<Database>,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        value: &JsonValue,
    ) -> StrataResult<()> {
        let idx = db.extension::<crate::search::InvertedIndex>()?;
        if idx.is_enabled() {
            let text = serde_json::to_string(value.as_inner()).unwrap_or_default();
            let entity_ref = crate::search::EntityRef::Json {
                branch_id: *branch_id,
                space: space.to_string(),
                doc_id: doc_id.to_string(),
            };
            idx.index_document(&entity_ref, &text, None);
        }
        Ok(())
    }

    /// Remove a JSON document from the InvertedIndex for BM25 search.
    /// Used when a merge deletes a document on the target side.
    ///
    /// `pub(crate)` so `branch_ops::primitive_merge::JsonMergeHandler` can
    /// drop BM25 index entries for documents removed by a JSON merge.
    pub(crate) fn deindex_json_doc(
        db: &Arc<Database>,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
    ) -> StrataResult<()> {
        let idx = db.extension::<crate::search::InvertedIndex>()?;
        if idx.is_enabled() {
            let entity_ref = crate::search::EntityRef::Json {
                branch_id: *branch_id,
                space: space.to_string(),
                doc_id: doc_id.to_string(),
            };
            idx.remove_document(&entity_ref);
        }
        Ok(())
    }

    /// Core read-modify-write for a single JSON document within a transaction.
    ///
    /// If the document exists, applies `set_at_path` and increments version.
    /// If `create_if_missing` is true and the document doesn't exist, creates it.
    /// Returns the resulting document version and the full document value
    /// (avoids a re-read when the caller needs the complete doc, e.g. for embedding).
    #[allow(clippy::too_many_arguments)]
    fn set_in_txn(
        txn: &mut TransactionContext,
        key: &Key,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
        create_if_missing: bool,
        branch_id: &BranchId,
        space: &str,
        indexes: &[index::IndexDef],
    ) -> StrataResult<(Version, JsonValue)> {
        match txn.get(key)? {
            Some(stored) => {
                let mut doc = Self::deserialize_doc(&stored)?;
                let old_value = if !indexes.is_empty() {
                    Some(doc.value.clone())
                } else {
                    None
                };
                set_at_path(&mut doc.value, path, value)
                    .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
                doc.value.validate().map_err(limit_error_to_error)?;
                doc.touch();
                let serialized = Self::serialize_doc(&doc)?;
                txn.put(key.clone(), serialized)?;

                if !indexes.is_empty() {
                    Self::update_index_entries(
                        txn,
                        branch_id,
                        space,
                        doc_id,
                        old_value.as_ref(),
                        Some(&doc.value),
                        indexes,
                    )?;
                }

                Ok((Version::counter(doc.version), doc.value))
            }
            None if create_if_missing => {
                let initial = if path.is_root() {
                    value
                } else {
                    let mut obj = JsonValue::object();
                    set_at_path(&mut obj, path, value)
                        .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
                    obj
                };
                initial.validate().map_err(limit_error_to_error)?;
                let doc = JsonDoc::new(doc_id, initial);
                let serialized = Self::serialize_doc(&doc)?;
                txn.put(key.clone(), serialized)?;

                if !indexes.is_empty() {
                    Self::update_index_entries(
                        txn,
                        branch_id,
                        space,
                        doc_id,
                        None,
                        Some(&doc.value),
                        indexes,
                    )?;
                }

                Ok((Version::counter(doc.version), doc.value))
            }
            None => Err(StrataError::invalid_input(format!(
                "JSON document {} not found",
                doc_id
            ))),
        }
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
    ) -> StrataResult<(Version, JsonValue)> {
        path.validate().map_err(limit_error_to_error)?;
        value.validate().map_err(limit_error_to_error)?;

        let key = self.key_for(branch_id, space, doc_id);
        let result = self.db.transaction(*branch_id, |txn| {
            crate::primitives::space::ensure_space_registered_in_txn(txn, branch_id, space)?;
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;
            Self::set_in_txn(
                txn, &key, doc_id, path, value, true, branch_id, space, &indexes,
            )
        })?;

        // Update inverted index for BM25 search
        Self::index_json_doc(&self.db, branch_id, space, doc_id, &result.1)?;

        Ok(result)
    }

    /// Set multiple documents in a single transaction.
    ///
    /// Each entry does a set_or_create: creates the document if it doesn't exist,
    /// or sets the value at the given path if it does. All writes share one
    /// lock acquisition, one WAL record, and one commit.
    ///
    /// Returns (versions, full_doc_values) — the full document after each write,
    /// for use by callers that need the complete content (e.g. embedding hooks).
    pub fn batch_set_or_create(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: Vec<(String, JsonPath, JsonValue)>,
    ) -> StrataResult<Vec<(Version, JsonValue)>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Validate all paths and values upfront (matching set_or_create)
        for (_, path, value) in &entries {
            path.validate().map_err(limit_error_to_error)?;
            value.validate().map_err(limit_error_to_error)?;
        }

        // Keep doc_ids for post-commit indexing
        let doc_ids: Vec<String> = entries.iter().map(|(id, _, _)| id.clone()).collect();

        let results = self.db.transaction(*branch_id, |txn| {
            crate::primitives::space::ensure_space_registered_in_txn(txn, branch_id, space)?;
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;
            let mut results = Vec::with_capacity(entries.len());
            for (doc_id, path, value) in &entries {
                let key = self.key_for(branch_id, space, doc_id);
                let result = Self::set_in_txn(
                    txn,
                    &key,
                    doc_id,
                    path,
                    value.clone(),
                    true,
                    branch_id,
                    space,
                    &indexes,
                )?;
                results.push(result);
            }
            Ok(results)
        })?;

        // Post-commit: update inverted index for BM25 search
        for (doc_id, (_, doc_value)) in doc_ids.iter().zip(results.iter()) {
            Self::index_json_doc(&self.db, branch_id, space, doc_id, doc_value)?;
        }

        Ok(results)
    }

    /// Get multiple documents in a single transaction.
    ///
    /// Each entry is (doc_id, path). Returns a Vec of Option<Versioned<JsonValue>>
    /// in the same order as the input entries.
    ///
    /// NOTE: The timestamp in each `Versioned` is the document's `updated_at`
    /// (set inside the writing transaction), not the storage-layer commit
    /// timestamp. This differs slightly from `get_versioned()` which uses the
    /// storage timestamp. The difference is sub-millisecond and the trade-off
    /// gives transactional consistency (all reads in one snapshot).
    pub fn batch_get(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: &[(String, JsonPath)],
    ) -> StrataResult<Vec<Option<Versioned<JsonValue>>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Validate all paths upfront
        for (_, path) in entries {
            path.validate().map_err(limit_error_to_error)?;
        }

        self.db.transaction(*branch_id, |txn| {
            let mut results = Vec::with_capacity(entries.len());
            for (doc_id, path) in entries {
                let key = self.key_for(branch_id, space, doc_id);
                match txn.get(&key)? {
                    Some(stored) => {
                        let doc = Self::deserialize_doc(&stored)?;
                        match get_at_path(&doc.value, path).cloned() {
                            Some(json_val) => {
                                results.push(Some(Versioned::with_timestamp(
                                    json_val,
                                    Version::counter(doc.version),
                                    doc.updated_at.into(),
                                )));
                            }
                            None => results.push(None),
                        }
                    }
                    None => results.push(None),
                }
            }
            Ok(results)
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
        let (version, doc_value) = self.db.transaction(*branch_id, |txn| {
            crate::primitives::space::ensure_space_registered_in_txn(txn, branch_id, space)?;
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;
            Self::set_in_txn(
                txn, &key, doc_id, path, value, false, branch_id, space, &indexes,
            )
        })?;

        // Update inverted index for BM25 search
        Self::index_json_doc(&self.db, branch_id, space, doc_id, &doc_value)?;

        Ok(version)
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
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;

            // Load existing document
            let stored = txn.get(&key)?.ok_or_else(|| {
                StrataError::invalid_input(format!("JSON document {} not found", doc_id))
            })?;
            let mut doc = Self::deserialize_doc(&stored)?;
            let old_value = if !indexes.is_empty() {
                Some(doc.value.clone())
            } else {
                None
            };

            // Apply deletion
            delete_at_path(&mut doc.value, path)
                .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
            doc.touch();

            // Store updated document
            let serialized = Self::serialize_doc(&doc)?;
            txn.put(key.clone(), serialized)?;

            // Update secondary indexes
            if !indexes.is_empty() {
                Self::update_index_entries(
                    txn,
                    branch_id,
                    space,
                    doc_id,
                    old_value.as_ref(),
                    Some(&doc.value),
                    &indexes,
                )?;
            }

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
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;

            // Check if document exists and get value for index cleanup
            let stored = match txn.get(&key)? {
                Some(v) => v,
                None => return Ok(false),
            };

            // Remove index entries before deleting the document
            if !indexes.is_empty() {
                let doc = Self::deserialize_doc(&stored)?;
                Self::update_index_entries(
                    txn,
                    branch_id,
                    space,
                    doc_id,
                    Some(&doc.value),
                    None,
                    &indexes,
                )?;
            }

            // Delete the document
            txn.delete(key.clone())?;
            Ok(true)
        })
    }

    /// Destroy multiple documents in a single transaction.
    ///
    /// Returns a Vec<bool> indicating whether each document existed (and was
    /// destroyed). All deletes share one lock, one WAL record, one commit.
    pub fn batch_destroy(
        &self,
        branch_id: &BranchId,
        space: &str,
        doc_ids: &[String],
    ) -> StrataResult<Vec<bool>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }

        self.db.transaction(*branch_id, |txn| {
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;
            let mut results = Vec::with_capacity(doc_ids.len());
            for doc_id in doc_ids {
                let key = self.key_for(branch_id, space, doc_id);
                match txn.get(&key)? {
                    Some(stored) => {
                        if !indexes.is_empty() {
                            let doc = Self::deserialize_doc(&stored)?;
                            Self::update_index_entries(
                                txn,
                                branch_id,
                                space,
                                doc_id,
                                Some(&doc.value),
                                None,
                                &indexes,
                            )?;
                        }
                        txn.delete(key)?;
                        results.push(true);
                    }
                    None => {
                        results.push(false);
                    }
                }
            }
            Ok(results)
        })
    }

    /// Delete at path for multiple documents in a single transaction.
    ///
    /// Returns a Vec<Result<Version>> for each entry. All deletes share
    /// one transaction for atomicity.
    pub fn batch_delete_at_path(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: &[(String, JsonPath)],
    ) -> StrataResult<Vec<StrataResult<Version>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Validate all paths upfront
        for (_, path) in entries {
            path.validate().map_err(limit_error_to_error)?;
        }

        self.db.transaction(*branch_id, |txn| {
            let indexes = Self::load_indexes(self.db.as_ref(), txn, branch_id, space)?;
            let mut results = Vec::with_capacity(entries.len());
            for (doc_id, path) in entries {
                let key = self.key_for(branch_id, space, doc_id);
                let result = (|| {
                    let stored = txn.get(&key)?.ok_or_else(|| {
                        StrataError::invalid_input(format!("JSON document {} not found", doc_id))
                    })?;
                    let mut doc = Self::deserialize_doc(&stored)?;
                    let old_value = if !indexes.is_empty() {
                        Some(doc.value.clone())
                    } else {
                        None
                    };
                    delete_at_path(&mut doc.value, path)
                        .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
                    doc.touch();
                    let serialized = Self::serialize_doc(&doc)?;
                    txn.put(key, serialized)?;

                    if !indexes.is_empty() {
                        Self::update_index_entries(
                            txn,
                            branch_id,
                            space,
                            doc_id,
                            old_value.as_ref(),
                            Some(&doc.value),
                            &indexes,
                        )?;
                    }

                    Ok(Version::counter(doc.version))
                })();
                results.push(result);
            }
            Ok(results)
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
        let scan_prefix = Key::new_json(ns, prefix.unwrap_or(""));

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
    /// Count documents matching an optional prefix in a single scan.
    ///
    /// More efficient than paginated `list()` calls — opens one transaction,
    /// scans keys without collecting doc IDs or deserializing values.
    pub fn count(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
    ) -> StrataResult<u64> {
        let ns = self.namespace_for(branch_id, space);
        let scan_prefix = Key::new_json(ns, prefix.unwrap_or(""));

        self.db.transaction(*branch_id, |txn| {
            let results = txn.scan_prefix(&scan_prefix)?;
            Ok(results
                .into_iter()
                .filter(|(key, _)| key.user_key_string().is_some())
                .count() as u64)
        })
    }

    /// Sample JSON documents with evenly-spaced selection from a single scan.
    ///
    /// Performs one `scan_prefix` and reads values inline, avoiding the
    /// separate `list()` + K `get()` round-trips.
    pub fn sample(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        count: usize,
    ) -> StrataResult<(u64, Vec<(String, JsonValue)>)> {
        let ns = self.namespace_for(branch_id, space);
        let scan_prefix = Key::new_json(ns, prefix.unwrap_or(""));

        self.db.transaction(*branch_id, |txn| {
            let results = txn.scan_prefix(&scan_prefix)?;

            // Collect entries, deserializing inline
            let entries: Vec<(String, JsonValue)> = results
                .into_iter()
                .filter_map(|(key, value)| {
                    let doc_id = key.user_key_string()?;
                    let doc = Self::deserialize_doc(&value).ok()?;
                    Some((doc_id, doc.value))
                })
                .collect();

            let total = entries.len() as u64;

            if count == 0 || entries.is_empty() {
                return Ok((total, Vec::new()));
            }
            if count >= entries.len() {
                return Ok((total, entries));
            }

            // Evenly-spaced sampling
            let step = entries.len() as f64 / count as f64;
            let sampled = (0..count)
                .map(|i| {
                    let idx = (i as f64 * step) as usize;
                    entries[idx].clone()
                })
                .collect();

            Ok((total, sampled))
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

    /// List document IDs as of a past timestamp, with cursor-based pagination.
    ///
    /// Matches the `list()` signature with cursor/limit support.
    pub fn list_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        as_of_ts: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> StrataResult<JsonListResult> {
        let ns = self.namespace_for(branch_id, space);
        let scan_key = Key::new_json(ns, prefix.unwrap_or(""));
        let results = self.db.scan_prefix_at_timestamp(&scan_key, as_of_ts)?;

        let mut doc_ids = Vec::with_capacity(limit + 1);
        let mut past_cursor = cursor.is_none();

        for (key, _vv) in results {
            let doc_id = match key.user_key_string() {
                Some(id) => id,
                None => continue,
            };

            if !past_cursor {
                if cursor == Some(doc_id.as_str()) {
                    past_cursor = true;
                }
                continue;
            }

            doc_ids.push(doc_id);
            if doc_ids.len() > limit {
                break;
            }
        }

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
    }

    // ========================================================================
    // Secondary Index Operations
    // ========================================================================

    /// Create a secondary index on a JSON field.
    ///
    /// Index metadata is stored in `_idx_meta_{space}` and index entries
    /// are stored in `_idx_{space}_{name}` — both in the KV layer.
    ///
    /// Returns error if an index with the same name already exists.
    pub fn create_index(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
        field_path: &str,
        index_type: index::IndexType,
    ) -> StrataResult<index::IndexDef> {
        // Validate index name
        if name.is_empty() {
            return Err(StrataError::invalid_input("Index name must not be empty"));
        }
        if name.contains('/') || name.contains('\0') {
            return Err(StrataError::invalid_input(
                "Index name must not contain '/' or null characters",
            ));
        }

        // Validate the field path is parseable
        let def = index::IndexDef::new(name, space, field_path, index_type);
        def.json_path()?; // validates

        let meta_space = index::index_meta_space_name(space);
        let meta_key = Key::new_json(
            Arc::new(Namespace::for_branch_space(*branch_id, &meta_space)),
            name,
        );

        self.db.transaction(*branch_id, |txn| {
            // Check if index already exists
            if txn.get(&meta_key)?.is_some() {
                return Err(StrataError::invalid_input(format!(
                    "Index '{}' already exists in space '{}'",
                    name, space
                )));
            }

            // Store index metadata as a JSON document
            let meta_json =
                serde_json::to_vec(&def).map_err(|e| StrataError::serialization(e.to_string()))?;
            txn.put(meta_key.clone(), Value::Bytes(meta_json))?;
            Ok(def.clone())
        })
    }

    /// Drop a secondary index, removing metadata and all index entries.
    pub fn drop_index(&self, branch_id: &BranchId, space: &str, name: &str) -> StrataResult<bool> {
        let meta_space = index::index_meta_space_name(space);
        let meta_key = Key::new_json(
            Arc::new(Namespace::for_branch_space(*branch_id, &meta_space)),
            name,
        );

        self.db.transaction(*branch_id, |txn| {
            // Check if index exists
            if txn.get(&meta_key)?.is_none() {
                return Ok(false);
            }

            // Delete metadata
            txn.delete(meta_key.clone())?;

            // Delete all index entries by scanning the index space
            let scan_prefix = index::index_scan_prefix(branch_id, space, name);
            let entries = txn.scan_prefix(&scan_prefix)?;
            for (key, _) in entries {
                txn.delete(key)?;
            }

            Ok(true)
        })
    }

    /// List all secondary indexes defined on a collection space.
    pub fn list_indexes(
        &self,
        branch_id: &BranchId,
        space: &str,
    ) -> StrataResult<Vec<index::IndexDef>> {
        let meta_space = index::index_meta_space_name(space);
        let meta_ns = Arc::new(Namespace::for_branch_space(*branch_id, &meta_space));
        let scan_prefix = Key::new_json(meta_ns, "");

        self.db.transaction(*branch_id, |txn| {
            let entries = txn.scan_prefix(&scan_prefix)?;
            let mut indexes = Vec::new();
            for (_, value) in entries {
                if let Value::Bytes(bytes) = &value {
                    let def = serde_json::from_slice::<index::IndexDef>(bytes).map_err(|e| {
                        StrataError::serialization(format!("corrupt index metadata: {}", e))
                    })?;
                    indexes.push(def);
                }
            }
            Ok(indexes)
        })
    }

    /// Load all index definitions for a space (internal helper).
    ///
    /// `pub(crate)` so `branch_ops::primitive_merge::JsonMergeHandler` can
    /// re-derive index entries for documents touched by a JSON merge.
    ///
    /// Per the B5 convergence contract
    /// (`docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`
    /// §"Surface matrix" row "JSON secondary index rows"), if `_idx`
    /// metadata cannot be trusted this call fails closed with a typed
    /// [`StrataError::PrimitiveDegraded`] rather than returning an
    /// empty or partial result set. A degradation is also recorded on
    /// the per-Database primitive-degradation registry so subsequent
    /// reads short-circuit without re-scanning the corrupt metadata.
    pub(crate) fn load_indexes(
        db: &Database,
        txn: &mut TransactionContext,
        branch_id: &BranchId,
        space: &str,
    ) -> StrataResult<Vec<index::IndexDef>> {
        // B5.4 fail-closed: if this (branch, Json, space) is already
        // marked degraded, return the typed error without re-scanning.
        if let Some(err) = crate::database::primitive_degradation::primitive_degraded_error(
            db,
            *branch_id,
            strata_core::contract::PrimitiveType::Json,
            space,
        ) {
            return Err(err);
        }

        let meta_space = index::index_meta_space_name(space);
        let meta_ns = Arc::new(Namespace::for_branch_space(*branch_id, &meta_space));
        let scan_prefix = Key::new_json(meta_ns, "");

        let entries = txn.scan_prefix(&scan_prefix)?;
        let mut indexes = Vec::new();
        for (_, value) in entries {
            if let Value::Bytes(bytes) = &value {
                match serde_json::from_slice::<index::IndexDef>(bytes) {
                    Ok(def) => indexes.push(def),
                    Err(e) => {
                        // Mark and fail closed — corrupt index metadata
                        // means the index cannot be trusted to return
                        // complete results.
                        let entry = crate::database::primitive_degradation::mark_primitive_degraded(
                            db,
                            *branch_id,
                            strata_core::contract::PrimitiveType::Json,
                            space,
                            strata_core::PrimitiveDegradedReason::IndexMetadataCorrupt,
                            format!("{e}"),
                        );
                        return Err(entry.map(|e| e.to_error()).unwrap_or_else(|| {
                            StrataError::serialization(format!("corrupt index metadata: {}", e))
                        }));
                    }
                }
            }
        }
        Ok(indexes)
    }

    /// Update index entries for a document write.
    ///
    /// If `old_value` is Some, removes old index entries first (for updates).
    /// Then writes new index entries for the new value.
    ///
    /// `pub(crate)` so `branch_ops::primitive_merge::JsonMergeHandler` can
    /// refresh secondary index entries for documents touched by a JSON
    /// merge using the same delta logic as the per-write path.
    pub(crate) fn update_index_entries(
        txn: &mut TransactionContext,
        branch_id: &BranchId,
        space: &str,
        doc_id: &str,
        old_value: Option<&JsonValue>,
        new_value: Option<&JsonValue>,
        indexes: &[index::IndexDef],
    ) -> StrataResult<()> {
        for idx_def in indexes {
            // Remove old index entry if updating/deleting
            if let Some(old_val) = old_value {
                if let Some(field_val) = index::extract_field_value(old_val, &idx_def.field_path) {
                    if let Some(encoded) = index::encode_value(&field_val, idx_def.index_type) {
                        let key = index::index_entry_key(
                            branch_id,
                            space,
                            &idx_def.name,
                            &encoded,
                            doc_id,
                        );
                        txn.delete(key)?;
                    }
                }
            }

            // Write new index entry if creating/updating
            if let Some(new_val) = new_value {
                if let Some(field_val) = index::extract_field_value(new_val, &idx_def.field_path) {
                    if let Some(encoded) = index::encode_value(&field_val, idx_def.index_type) {
                        let key = index::index_entry_key(
                            branch_id,
                            space,
                            &idx_def.name,
                            &encoded,
                            doc_id,
                        );
                        txn.put(key, Value::Bytes(vec![]))?;
                    }
                }
            }
        }
        Ok(())
    }
}

// ========== Searchable Trait Implementation ==========

impl JsonStore {
    /// BM25 search via InvertedIndex — same pattern as KV.
    fn search_bm25(
        &self,
        req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        use crate::search::{truncate_text, EntityRef, InvertedIndex, SearchHit, SearchStats};
        use std::time::Instant;

        let start = Instant::now();
        let _refresh_guard = self.db.refresh_query_guard();
        let index = self.db.extension::<InvertedIndex>()?;

        if !index.is_enabled() || index.total_docs() == 0 {
            return Ok(crate::SearchResponse::empty());
        }

        let parsed = crate::search::tokenizer::parse_query(&req.query);
        let phrase_cfg = crate::search::PhraseConfig {
            phrases: &parsed.phrases,
            boost: req.phrase_boost,
            slop: req.phrase_slop,
            filter: req.phrase_filter,
        };
        let prox_cfg = crate::search::ProximityConfig {
            enabled: req.proximity,
            window: req.proximity_window,
            weight: req.proximity_weight,
        };

        let top_k = index.score_top_k(
            &parsed.terms,
            &req.branch_id,
            req.k,
            req.bm25_k1,
            req.bm25_b,
            &phrase_cfg,
            &prox_cfg,
            Some(&req.space),
        );

        let hits: Vec<SearchHit> = top_k
            .into_iter()
            .filter_map(|scored| {
                let entity_ref = index.resolve_doc_id(scored.doc_id)?;
                // Only include Json results from this primitive
                if let EntityRef::Json {
                    ref branch_id,
                    ref space,
                    ref doc_id,
                } = entity_ref
                {
                    // Hydrate snippet from the hit's own space, not
                    // the request scope. See `event.rs` for the same
                    // Phase 0 fix and rationale.
                    let snippet = self
                        .get(branch_id, space, doc_id, &JsonPath::root())
                        .ok()
                        .flatten()
                        .map(|v| {
                            truncate_text(
                                &serde_json::to_string(v.as_inner()).unwrap_or_default(),
                                200,
                            )
                        });
                    Some(SearchHit {
                        doc_ref: entity_ref,
                        score: scored.score,
                        rank: 0,
                        snippet,
                    })
                } else {
                    None
                }
            })
            .enumerate()
            .map(|(i, mut hit)| {
                hit.rank = (i + 1) as u32;
                hit
            })
            .collect();

        let elapsed = start.elapsed().as_micros() as u64;
        let mut stats = SearchStats::new(elapsed, hits.len());
        stats = stats.with_index_used(true);

        Ok(crate::SearchResponse {
            hits,
            truncated: false,
            stats,
        })
    }
}

impl crate::search::Searchable for JsonStore {
    fn search(
        &self,
        req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        use crate::search::{EntityRef, SearchHit, SearchStats, SortDirection};
        use std::collections::HashSet;
        use std::time::Instant;

        // BM25 fallback: non-empty query with no field_filter → use InvertedIndex
        if !req.query.is_empty() && req.field_filter.is_none() && req.sort_by.is_none() {
            return self.search_bm25(req);
        }

        let start = Instant::now();

        // Need at least a field filter or sort_by to do anything
        if req.field_filter.is_none() && req.sort_by.is_none() {
            return Ok(crate::SearchResponse::empty());
        }

        self.db.transaction(req.branch_id, |txn| {
            let indexes = Self::load_indexes(self.db.as_ref(), txn, &req.branch_id, &req.space)?;

            // Resolve field filter to candidate set (if present)
            let filter_set: Option<HashSet<String>> = match &req.field_filter {
                Some(filter) => Some(index::resolve_filter(
                    self.db.as_ref(),
                    txn,
                    &req.branch_id,
                    &req.space,
                    filter,
                    &indexes,
                )?),
                None => None,
            };

            // Determine ordered doc_ids
            let ordered_doc_ids: Vec<String> = if let Some(sort) = &req.sort_by {
                // Sort by indexed field: scan the index in order
                let sort_idx = index::find_index_for_field(&sort.field, &indexes)?;
                let mut sorted = index::scan_index_ordered(
                    self.db.as_ref(),
                    txn,
                    &req.branch_id,
                    &req.space,
                    &sort_idx.name,
                )?;

                // If there's a filter, retain only matching docs (preserving sort order)
                if let Some(ref fset) = filter_set {
                    sorted.retain(|id| fset.contains(id));
                }

                if sort.direction == SortDirection::Desc {
                    sorted.reverse();
                }
                sorted
            } else {
                // No sort — use filter set, sorted by doc_id for deterministic ordering
                let mut ids: Vec<String> = filter_set.unwrap_or_default().into_iter().collect();
                ids.sort();
                ids
            };

            let total_matches = ordered_doc_ids.len();

            // Fetch documents and build hits (take top-k from ordered list)
            let mut hits: Vec<SearchHit> = Vec::new();
            for doc_id in ordered_doc_ids.iter().take(req.k) {
                let key = self.key_for(&req.branch_id, &req.space, doc_id);
                if let Some(stored) = txn.get(&key)? {
                    let doc = Self::deserialize_doc(&stored)?;
                    let snippet = crate::search::truncate_text(
                        &serde_json::to_string(&doc.value).unwrap_or_default(),
                        200,
                    );
                    hits.push(SearchHit {
                        doc_ref: EntityRef::Json {
                            branch_id: req.branch_id,
                            space: req.space.clone(),
                            doc_id: doc_id.clone(),
                        },
                        score: 1.0,
                        rank: 0,
                        snippet: Some(snippet),
                    });
                }
            }

            // Assign ranks
            for (i, hit) in hits.iter_mut().enumerate() {
                hit.rank = (i + 1) as u32;
            }

            let elapsed = start.elapsed().as_micros() as u64;
            let mut stats = SearchStats::new(elapsed, total_matches);
            stats = stats.with_index_used(true);

            Ok(crate::SearchResponse {
                hits,
                truncated: total_matches > req.k,
                stats,
            })
        })
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
    fn json_get_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
    ) -> StrataResult<Option<JsonValue>> {
        path.validate().map_err(limit_error_to_error)?;

        let key = Key::new_json(
            Arc::new(Namespace::for_branch_space(self.branch_id, space)),
            doc_id,
        );

        match self.get(&key)? {
            Some(value) => {
                let doc = JsonStore::deserialize_doc(&value)?;
                Ok(get_at_path(&doc.value, path).cloned())
            }
            None => Ok(None),
        }
    }

    fn json_set_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version> {
        path.validate().map_err(limit_error_to_error)?;
        value.validate().map_err(limit_error_to_error)?;

        let key = Key::new_json(
            Arc::new(Namespace::for_branch_space(self.branch_id, space)),
            doc_id,
        );

        let stored = self.get(&key)?.ok_or_else(|| {
            StrataError::invalid_input(format!("JSON document {} not found", doc_id))
        })?;
        let mut doc = JsonStore::deserialize_doc(&stored)?;

        set_at_path(&mut doc.value, path, value)
            .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
        doc.touch();

        let serialized = JsonStore::serialize_doc(&doc)?;
        self.put(key, serialized)?;

        Ok(Version::counter(doc.version))
    }

    fn json_create_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        value: JsonValue,
    ) -> StrataResult<Version> {
        value.validate().map_err(limit_error_to_error)?;

        let key = Key::new_json(
            Arc::new(Namespace::for_branch_space(self.branch_id, space)),
            doc_id,
        );
        let doc = JsonDoc::new(doc_id, value);

        if self.get(&key)?.is_some() {
            return Err(StrataError::invalid_input(format!(
                "JSON document {} already exists",
                doc_id
            )));
        }

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

        let key1 = store.key_for(&branch1, "default", doc_id);
        let key2 = store.key_for(&branch2, "default", doc_id);

        // Keys for different branches should be different even for same doc_id
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_key_for_same_branch() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let key1 = store.key_for(&branch_id, "default", doc_id);
        let key2 = store.key_for(&branch_id, "default", doc_id);

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
            .create(&branch_id, "default", doc_id, JsonValue::from(42i64))
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

        let version = store.create(&branch_id, "default", doc_id, value).unwrap();
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
            .create(&branch_id, "default", doc_id, JsonValue::from(1i64))
            .unwrap();

        // Second create with same ID fails
        let result = store.create(&branch_id, "default", doc_id, JsonValue::from(2i64));
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
            .create(&branch_id, "default", doc1, JsonValue::from(1i64))
            .unwrap();
        let v2 = store
            .create(&branch_id, "default", doc2, JsonValue::from(2i64))
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
            .create(&branch1, "default", doc_id, JsonValue::from(1i64))
            .unwrap();
        let v2 = store
            .create(&branch2, "default", doc_id, JsonValue::from(2i64))
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
            .create(&branch_id, "default", doc_id, JsonValue::null())
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
            .create(&branch_id, "default", doc_id, JsonValue::object())
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
            .create(&branch_id, "default", doc_id, JsonValue::array())
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
            .create(&branch_id, "default", doc_id, JsonValue::from(42i64))
            .unwrap();

        let value = store
            .get(&branch_id, "default", doc_id, &JsonPath::root())
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

        store.create(&branch_id, "default", doc_id, value).unwrap();

        let name = store
            .get(&branch_id, "default", doc_id, &"name".parse().unwrap())
            .unwrap();
        assert_eq!(
            name.and_then(|v| v.as_str().map(String::from)),
            Some("Alice".to_string())
        );

        let age = store
            .get(&branch_id, "default", doc_id, &"age".parse().unwrap())
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

        store.create(&branch_id, "default", doc_id, value).unwrap();

        let name = store
            .get(
                &branch_id,
                "default",
                doc_id,
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

        store.create(&branch_id, "default", doc_id, value).unwrap();

        let item = store
            .get(&branch_id, "default", doc_id, &"items[1]".parse().unwrap())
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
            .get(&branch_id, "default", doc_id, &JsonPath::root())
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
            .create(&branch_id, "default", doc_id, JsonValue::object())
            .unwrap();

        let result = store
            .get(
                &branch_id,
                "default",
                doc_id,
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

        assert!(!store.exists(&branch_id, "default", doc_id).unwrap());

        store
            .create(&branch_id, "default", doc_id, JsonValue::from(42i64))
            .unwrap();

        assert!(store.exists(&branch_id, "default", doc_id).unwrap());
    }

    #[test]
    fn test_exists_branch_isolation() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);

        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch1, "default", doc_id, JsonValue::from(42i64))
            .unwrap();

        // Document exists in branch1 but not in branch2
        assert!(store.exists(&branch1, "default", doc_id).unwrap());
        assert!(!store.exists(&branch2, "default", doc_id).unwrap());
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
            .create(&branch_id, "default", doc_id, JsonValue::from(42i64))
            .unwrap();

        let v2 = store
            .set(
                &branch_id,
                "default",
                doc_id,
                &JsonPath::root(),
                JsonValue::from(100i64),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let value = store
            .get(&branch_id, "default", doc_id, &JsonPath::root())
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
            .create(&branch_id, "default", doc_id, JsonValue::object())
            .unwrap();

        let v2 = store
            .set(
                &branch_id,
                "default",
                doc_id,
                &"name".parse().unwrap(),
                JsonValue::from("Alice"),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let name = store
            .get(&branch_id, "default", doc_id, &"name".parse().unwrap())
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
            .create(&branch_id, "default", doc_id, JsonValue::object())
            .unwrap();

        // Creates intermediate objects automatically
        let v2 = store
            .set(
                &branch_id,
                "default",
                doc_id,
                &"user.profile.name".parse().unwrap(),
                JsonValue::from("Bob"),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let name = store
            .get(
                &branch_id,
                "default",
                doc_id,
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
            .create(&branch_id, "default", doc_id, JsonValue::object())
            .unwrap();
        assert_eq!(v1, Version::counter(1));

        let v2 = store
            .set(
                &branch_id,
                "default",
                doc_id,
                &"a".parse().unwrap(),
                JsonValue::from(1i64),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let v3 = store
            .set(
                &branch_id,
                "default",
                doc_id,
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
            doc_id,
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
        store.create(&branch_id, "default", doc_id, value).unwrap();

        store
            .set(
                &branch_id,
                "default",
                doc_id,
                &"name".parse().unwrap(),
                JsonValue::from("Bob"),
            )
            .unwrap();

        let name = store
            .get(&branch_id, "default", doc_id, &"name".parse().unwrap())
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
        store.create(&branch_id, "default", doc_id, value).unwrap();

        store
            .set(
                &branch_id,
                "default",
                doc_id,
                &"items[1]".parse().unwrap(),
                JsonValue::from(999i64),
            )
            .unwrap();

        let item = store
            .get(&branch_id, "default", doc_id, &"items[1]".parse().unwrap())
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
        store.create(&branch_id, "default", doc_id, value).unwrap();

        // Delete the "age" field
        let v2 = store
            .delete_at_path(&branch_id, "default", doc_id, &"age".parse().unwrap())
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        // Verify "age" is gone but "name" remains
        assert!(store
            .get(&branch_id, "default", doc_id, &"age".parse().unwrap())
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .get(&branch_id, "default", doc_id, &"name".parse().unwrap())
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
        store.create(&branch_id, "default", doc_id, value).unwrap();

        // Delete nested field
        let v2 = store
            .delete_at_path(
                &branch_id,
                "default",
                doc_id,
                &"user.profile.temp".parse().unwrap(),
            )
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        // Verify "temp" is gone
        assert!(store
            .get(
                &branch_id,
                "default",
                doc_id,
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
                    doc_id,
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
        store.create(&branch_id, "default", doc_id, value).unwrap();

        // Delete middle element
        let v2 = store
            .delete_at_path(&branch_id, "default", doc_id, &"items[1]".parse().unwrap())
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        // Array should now be ["a", "c"]
        let items = store
            .get(&branch_id, "default", doc_id, &"items".parse().unwrap())
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
        let v1 = store.create(&branch_id, "default", doc_id, value).unwrap();
        assert_eq!(v1, Version::counter(1));

        let v2 = store
            .delete_at_path(&branch_id, "default", doc_id, &"a".parse().unwrap())
            .unwrap();
        assert_eq!(v2, Version::counter(2));

        let v3 = store
            .delete_at_path(&branch_id, "default", doc_id, &"b".parse().unwrap())
            .unwrap();
        assert_eq!(v3, Version::counter(3));
    }

    #[test]
    fn test_delete_at_path_missing_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let result = store.delete_at_path(&branch_id, "default", doc_id, &"field".parse().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_at_path_missing_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", doc_id, JsonValue::object())
            .unwrap();

        // Deleting a nonexistent path is idempotent (succeeds, increments version)
        let v2 = store
            .delete_at_path(
                &branch_id,
                "default",
                doc_id,
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
            .create(&branch_id, "default", doc_id, JsonValue::from(42i64))
            .unwrap();
        assert!(store.exists(&branch_id, "default", doc_id).unwrap());

        let existed = store.destroy(&branch_id, "default", doc_id).unwrap();
        assert!(existed);
        assert!(!store.exists(&branch_id, "default", doc_id).unwrap());
    }

    #[test]
    fn test_destroy_nonexistent_document() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        let existed = store.destroy(&branch_id, "default", doc_id).unwrap();
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
            .create(&branch1, "default", doc_id, JsonValue::from(1i64))
            .unwrap();
        store
            .create(&branch2, "default", doc_id, JsonValue::from(2i64))
            .unwrap();

        // Destroy in branch1
        store.destroy(&branch1, "default", doc_id).unwrap();

        // Document should be gone from branch1 but still exist in branch2
        assert!(!store.exists(&branch1, "default", doc_id).unwrap());
        assert!(store.exists(&branch2, "default", doc_id).unwrap());
    }

    #[test]
    fn test_destroy_then_recreate() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        // Create, destroy, recreate
        store
            .create(&branch_id, "default", doc_id, JsonValue::from(1i64))
            .unwrap();
        store.destroy(&branch_id, "default", doc_id).unwrap();

        // Should be able to recreate with new value
        let version = store
            .create(&branch_id, "default", doc_id, JsonValue::from(2i64))
            .unwrap();
        assert_eq!(version, Version::counter(1)); // Fresh document starts at version 1

        let value = store
            .get(&branch_id, "default", doc_id, &JsonPath::root())
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
        store.create(&branch_id, "default", doc_id, value).unwrap();

        let existed = store.destroy(&branch_id, "default", doc_id).unwrap();
        assert!(existed);
        assert!(!store.exists(&branch_id, "default", doc_id).unwrap());
    }

    #[test]
    fn test_destroy_idempotent() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();
        let doc_id = "test-doc";

        store
            .create(&branch_id, "default", doc_id, JsonValue::from(42i64))
            .unwrap();

        // First destroy returns true
        assert!(store.destroy(&branch_id, "default", doc_id).unwrap());

        // Subsequent destroys return false
        assert!(!store.destroy(&branch_id, "default", doc_id).unwrap());
        assert!(!store.destroy(&branch_id, "default", doc_id).unwrap());
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
        // Both are new documents, so version should be 1
        assert_eq!(results[0].0, Version::counter(1));
        assert_eq!(results[1].0, Version::counter(1));

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
        assert_eq!(results[0].0, Version::counter(2));

        let v = store
            .get(&branch_id, "default", "existing", &JsonPath::root())
            .unwrap();
        assert_eq!(
            v.and_then(|v| v.as_str().map(String::from)),
            Some("new".to_string())
        );
    }

    // ========== Post-mutation validation tests (#1613) ==========

    #[test]
    fn test_set_rejects_document_exceeding_depth_limit() {
        // Build a path with 101 key segments.  Each intermediate creates a
        // nested object, producing depth > MAX_NESTING_DEPTH (100).
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create a document with a simple object
        store
            .create(&branch_id, "default", "deep", JsonValue::object())
            .unwrap();

        // Build a path of depth 101 (exceeds MAX_NESTING_DEPTH=100)
        let mut path = JsonPath::root();
        for i in 0..101 {
            path = path.key(format!("k{}", i));
        }

        let result =
            store.set_or_create(&branch_id, "default", "deep", &path, JsonValue::from(1i64));
        assert!(
            result.is_err(),
            "Should reject document exceeding depth limit"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("nesting") || err_msg.contains("depth"),
            "Error should mention nesting/depth, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_incremental_sets_enforce_size_limit() {
        // Each individual value is small, but accumulated document exceeds limits.
        // Post-mutation validation must catch this.
        use strata_core::primitives::json::MAX_DOCUMENT_SIZE;

        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Start with an empty object
        store
            .create(&branch_id, "default", "big", JsonValue::object())
            .unwrap();

        // Write chunks that individually pass validation but accumulate.
        // Use 1MB chunks — after 17 writes, doc > 16MB.
        let chunk = JsonValue::from("x".repeat(1_000_000));
        let mut last_err = None;
        for i in 0..20 {
            let path = JsonPath::root().key(format!("field_{}", i));
            match store.set(&branch_id, "default", "big", &path, chunk.clone()) {
                Ok(_) => {}
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }
        assert!(
            last_err.is_some(),
            "Should have rejected document exceeding {} bytes",
            MAX_DOCUMENT_SIZE
        );
    }

    #[test]
    fn test_create_if_missing_validates_result() {
        // set_or_create on a new document with a deep path should validate
        // the resulting document, not just the input value.
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        let mut path = JsonPath::root();
        for i in 0..101 {
            path = path.key(format!("k{}", i));
        }

        let result = store.set_or_create(
            &branch_id,
            "default",
            "newdoc",
            &path,
            JsonValue::from(1i64),
        );
        assert!(
            result.is_err(),
            "Should reject new doc exceeding depth limit"
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

    // ========== Issue #1948: JSON Test Coverage Gaps ==========

    // Scenario 1: set_or_create on non-existent doc with non-root path
    #[test]
    fn test_set_or_create_nonexistent_with_nested_path() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create doc via set_or_create at a nested path — doc doesn't exist
        let path = JsonPath::root().key("user").key("name");
        let (version, full_doc) = store
            .set_or_create(
                &branch_id,
                "default",
                "doc1",
                &path,
                JsonValue::from("Alice"),
            )
            .unwrap();
        assert_eq!(version, Version::counter(1));

        // full_doc should be an object with user.name = "Alice"
        let name = get_at_path(&full_doc, &path);
        assert_eq!(name.and_then(|v| v.as_str()), Some("Alice"));

        // Verify root is an object, not the string itself
        assert!(full_doc.is_object());

        // Read back
        let result = store
            .get(&branch_id, "default", "doc1", &JsonPath::root())
            .unwrap()
            .unwrap();
        assert!(result.is_object());
        let name_val = get_at_path(&result, &path);
        assert_eq!(name_val.and_then(|v| v.as_str()), Some("Alice"));
    }

    // Scenario 2: Concurrent set on same document (with OCC retry)
    #[test]
    fn test_concurrent_set_same_document() {
        use std::sync::Arc;
        use std::thread;

        let db = Database::cache().unwrap();
        let store = Arc::new(JsonStore::new(db));
        let branch_id = BranchId::new();

        // Create a document (must be an object so field-level sets work)
        store
            .create(&branch_id, "default", "shared", JsonValue::object())
            .unwrap();

        // Spawn 10 threads, each setting a different field.
        // Retry on OCC conflict (expected under contention).
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let store = Arc::clone(&store);
                let bid = branch_id;
                thread::spawn(move || {
                    let path = JsonPath::root().key(format!("field_{}", i));
                    for _ in 0..20 {
                        match store.set_or_create(
                            &bid,
                            "default",
                            "shared",
                            &path,
                            JsonValue::from(i as i64),
                        ) {
                            Ok(_) => return,
                            Err(e) if e.to_string().contains("conflict") => continue,
                            Err(e) => panic!("Unexpected error: {}", e),
                        }
                    }
                    panic!("Failed to write field_{} after 20 retries", i);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All 10 fields should be present
        let doc = store
            .get(&branch_id, "default", "shared", &JsonPath::root())
            .unwrap()
            .unwrap();

        for i in 0..10 {
            let path = JsonPath::root().key(format!("field_{}", i));
            let val = get_at_path(&doc, &path);
            assert!(val.is_some(), "field_{} missing after concurrent writes", i);
        }
    }

    // Scenario 4: Path at exactly MAX_PATH_LENGTH (256 segments) boundary
    #[test]
    fn test_path_at_max_length_boundary() {
        use strata_core::primitives::json::MAX_PATH_LENGTH;

        // Build a path with exactly MAX_PATH_LENGTH segments — should succeed
        let mut path = JsonPath::root();
        for i in 0..MAX_PATH_LENGTH {
            path = path.key(format!("k{}", i));
        }
        assert!(
            path.validate().is_ok(),
            "Path at exactly MAX_PATH_LENGTH should be valid"
        );

        // One more segment should fail
        let over_path = path.key("extra");
        assert!(
            over_path.validate().is_err(),
            "Path exceeding MAX_PATH_LENGTH should fail"
        );
    }

    // Scenario 5: getv() returns version history with correct values
    #[test]
    fn test_getv_version_history() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create and update a document to produce version history
        store
            .create(&branch_id, "default", "doc1", JsonValue::from("v1"))
            .unwrap();
        store
            .set(
                &branch_id,
                "default",
                "doc1",
                &JsonPath::root(),
                JsonValue::from("v2"),
            )
            .unwrap();

        let history = store.getv(&branch_id, "default", "doc1").unwrap();
        assert!(history.is_some());
        let versions = history.unwrap().into_versions();
        assert!(!versions.is_empty(), "Should have at least one version");
        // Latest version should be "v2"
        assert_eq!(versions[0].value.as_str(), Some("v2"));
    }

    // Scenario 5b: getv() gracefully handles corrupt bytes (deserialization failure)
    #[test]
    fn test_getv_corrupt_deserialization() {
        // Verify that deserialize_doc on garbage bytes returns Err (not panic)
        let garbage = Value::Bytes(vec![0xFF, 0x01, 0x02, 0x03]);
        let result = JsonStore::deserialize_doc(&garbage);
        assert!(result.is_err(), "Corrupt bytes should return Err");

        // Empty bytes should also return Err
        let empty = Value::Bytes(vec![]);
        let result = JsonStore::deserialize_doc(&empty);
        assert!(result.is_err(), "Empty bytes should return Err");

        // Non-bytes value should return Err
        let wrong_type = Value::Int(42);
        let result = JsonStore::deserialize_doc(&wrong_type);
        assert!(result.is_err(), "Non-bytes value should return Err");
    }

    // Scenario 6: batch_set with mix of creates and updates
    #[test]
    fn test_batch_set_mixed_creates_and_updates() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Pre-create doc1
        store
            .create(&branch_id, "default", "doc1", JsonValue::from("original"))
            .unwrap();

        // Batch: update doc1, create doc2 and doc3
        let entries = vec![
            (
                "doc1".to_string(),
                JsonPath::root(),
                JsonValue::from("updated"),
            ),
            (
                "doc2".to_string(),
                JsonPath::root(),
                JsonValue::from("new_2"),
            ),
            (
                "doc3".to_string(),
                JsonPath::root().key("nested"),
                JsonValue::from("new_3"),
            ),
        ];

        let results = store
            .batch_set_or_create(&branch_id, "default", entries)
            .unwrap();
        assert_eq!(results.len(), 3);
        // doc1 was updated (version 2), doc2 and doc3 are new (version 1)
        assert_eq!(results[0].0, Version::counter(2));
        assert_eq!(results[1].0, Version::counter(1));
        assert_eq!(results[2].0, Version::counter(1));

        // Verify values
        let v1 = store
            .get(&branch_id, "default", "doc1", &JsonPath::root())
            .unwrap()
            .unwrap();
        assert_eq!(v1.as_str(), Some("updated"));

        let v2 = store
            .get(&branch_id, "default", "doc2", &JsonPath::root())
            .unwrap()
            .unwrap();
        assert_eq!(v2.as_str(), Some("new_2"));

        // doc3 was created with non-root path — should be an object
        let v3 = store
            .get(&branch_id, "default", "doc3", &JsonPath::root())
            .unwrap()
            .unwrap();
        assert!(v3.is_object());
    }

    // Scenario 7: list() cursor on deleted document
    #[test]
    fn test_list_cursor_after_delete() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create docs a, b, c, d, e
        for name in &["a", "b", "c", "d", "e"] {
            store
                .create(&branch_id, "default", name, JsonValue::object())
                .unwrap();
        }

        // Page 1: get first 2
        let page1 = store.list(&branch_id, "default", None, None, 2).unwrap();
        assert_eq!(page1.doc_ids.len(), 2);
        assert_eq!(page1.doc_ids, vec!["a", "b"]);
        assert!(page1.next_cursor.is_some());

        // Delete "c" (which would be next)
        store.destroy(&branch_id, "default", "c").unwrap();

        // Page 2: continue from cursor — "c" should be skipped
        let page2 = store
            .list(&branch_id, "default", None, page1.next_cursor.as_deref(), 2)
            .unwrap();
        assert_eq!(page2.doc_ids.len(), 2);
        assert_eq!(page2.doc_ids, vec!["d", "e"]);
    }

    // Scenario 8: get_at() with timestamp between two versions
    #[test]
    fn test_get_at_between_versions() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create doc
        store
            .create(&branch_id, "default", "doc1", JsonValue::from("v1"))
            .unwrap();

        // Get timestamp of v1
        let v1_versioned = store
            .get_versioned(&branch_id, "default", "doc1", &JsonPath::root())
            .unwrap()
            .unwrap();
        let ts_v1 = v1_versioned.timestamp.as_micros();

        // Sleep briefly to separate timestamps
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Compute a timestamp between v1 and v2
        let ts_between = ts_v1 + 1000; // 1ms later

        // Update to v2
        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .set(
                &branch_id,
                "default",
                "doc1",
                &JsonPath::root(),
                JsonValue::from("v2"),
            )
            .unwrap();

        // get_at with timestamp between v1 and v2 should return v1
        let result = store
            .get_at(&branch_id, "default", "doc1", &JsonPath::root(), ts_between)
            .unwrap();
        assert!(
            result.is_some(),
            "Should find v1 at timestamp between v1 and v2"
        );
        assert_eq!(result.unwrap(), JsonValue::from("v1"));
    }

    // Scenario 9: batch_destroy atomicity
    #[test]
    fn test_batch_destroy_atomicity() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create docs
        for name in &["a", "b", "c"] {
            store
                .create(&branch_id, "default", name, JsonValue::object())
                .unwrap();
        }

        // Batch destroy including one that doesn't exist
        let doc_ids: Vec<String> = vec!["a", "b", "nonexistent", "c"]
            .into_iter()
            .map(String::from)
            .collect();
        let results = store
            .batch_destroy(&branch_id, "default", &doc_ids)
            .unwrap();
        assert_eq!(results, vec![true, true, false, true]);

        // All should be gone
        assert!(!store.exists(&branch_id, "default", "a").unwrap());
        assert!(!store.exists(&branch_id, "default", "b").unwrap());
        assert!(!store.exists(&branch_id, "default", "c").unwrap());
    }

    // Scenario 10: JsonStoreExt in non-default space
    #[test]
    fn test_json_store_ext_non_default_space() {
        use crate::primitives::extensions::JsonStoreExt;

        let db = Database::cache().unwrap();
        let store = JsonStore::new(db.clone());
        let branch_id = BranchId::new();

        // Create a doc in "custom" space via the store directly
        store
            .create(&branch_id, "custom", "doc1", JsonValue::from("hello"))
            .unwrap();

        // Read it back via JsonStoreExt _in_space method
        db.transaction(branch_id, |txn| {
            let result = txn.json_get_in_space("custom", "doc1", &JsonPath::root())?;
            assert!(result.is_some(), "Should find doc in custom space via ext");
            assert_eq!(result.unwrap().as_str(), Some("hello"));

            // Verify default space doesn't have it
            let default_result = txn.json_get("doc1", &JsonPath::root())?;
            assert!(
                default_result.is_none(),
                "Default space should not have custom space doc"
            );

            Ok(())
        })
        .unwrap();
    }

    // Scenario 11: count() and sample() engine methods
    #[test]
    fn test_count_single_scan() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        assert_eq!(store.count(&branch_id, "default", None).unwrap(), 0);

        for i in 0..5 {
            store
                .create(
                    &branch_id,
                    "default",
                    &format!("doc{}", i),
                    JsonValue::from(i as i64),
                )
                .unwrap();
        }
        assert_eq!(store.count(&branch_id, "default", None).unwrap(), 5);

        // With prefix
        store
            .create(&branch_id, "default", "other", JsonValue::object())
            .unwrap();
        assert_eq!(store.count(&branch_id, "default", Some("doc")).unwrap(), 5);
        assert_eq!(store.count(&branch_id, "default", None).unwrap(), 6);
    }

    #[test]
    fn test_sample_single_scan() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        for i in 0..10 {
            store
                .create(
                    &branch_id,
                    "default",
                    &format!("doc{:02}", i),
                    JsonValue::from(i as i64),
                )
                .unwrap();
        }

        // Sample 3 of 10
        let (total, sampled) = store.sample(&branch_id, "default", None, 3).unwrap();
        assert_eq!(total, 10);
        assert_eq!(sampled.len(), 3);

        // Sample more than exist
        let (total, sampled) = store.sample(&branch_id, "default", None, 20).unwrap();
        assert_eq!(total, 10);
        assert_eq!(sampled.len(), 10);

        // Sample 0
        let (total, sampled) = store.sample(&branch_id, "default", None, 0).unwrap();
        assert_eq!(total, 10);
        assert_eq!(sampled.len(), 0);
    }

    // Scenario: batch_get transactional consistency
    #[test]
    fn test_batch_get_single_transaction() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        store
            .create(&branch_id, "default", "a", JsonValue::from("val_a"))
            .unwrap();
        store
            .create(&branch_id, "default", "b", JsonValue::from("val_b"))
            .unwrap();

        let entries = vec![
            ("a".to_string(), JsonPath::root()),
            ("nonexistent".to_string(), JsonPath::root()),
            ("b".to_string(), JsonPath::root()),
        ];

        let results = store.batch_get(&branch_id, "default", &entries).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert_eq!(results[0].as_ref().unwrap().value.as_str(), Some("val_a"));
        assert!(results[1].is_none()); // nonexistent
        assert!(results[2].is_some());
        assert_eq!(results[2].as_ref().unwrap().value.as_str(), Some("val_b"));
    }

    // Scenario: list_at with pagination
    #[test]
    fn test_list_at_with_pagination() {
        let db = Database::cache().unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        // Create 5 docs
        for i in 0..5 {
            store
                .create(
                    &branch_id,
                    "default",
                    &format!("doc{}", i),
                    JsonValue::from(i as i64),
                )
                .unwrap();
        }

        // Get timestamp after all docs exist
        let versioned = store
            .get_versioned(&branch_id, "default", "doc4", &JsonPath::root())
            .unwrap()
            .unwrap();
        let ts = versioned.timestamp.as_micros();

        // Paginated list_at: page 1
        let page1 = store
            .list_at(&branch_id, "default", None, ts, None, 2)
            .unwrap();
        assert_eq!(page1.doc_ids.len(), 2);
        assert!(page1.next_cursor.is_some());

        // Page 2
        let page2 = store
            .list_at(
                &branch_id,
                "default",
                None,
                ts,
                page1.next_cursor.as_deref(),
                2,
            )
            .unwrap();
        assert_eq!(page2.doc_ids.len(), 2);

        // Page 3 (last page)
        let page3 = store
            .list_at(
                &branch_id,
                "default",
                None,
                ts,
                page2.next_cursor.as_deref(),
                2,
            )
            .unwrap();
        assert_eq!(page3.doc_ids.len(), 1);
        assert!(page3.next_cursor.is_none());
    }

    // Scenario: format version backward compatibility
    #[test]
    fn test_format_version_backward_compat() {
        // Simulate v1 format (no version header — raw MessagePack)
        let doc = JsonDoc::new("test", JsonValue::from("hello"));
        let raw_msgpack = rmp_serde::to_vec(&doc).unwrap();
        let v1_value = Value::Bytes(raw_msgpack);

        // v1 should deserialize successfully
        let deserialized = JsonStore::deserialize_doc(&v1_value).unwrap();
        assert_eq!(deserialized.id, "test");
        assert_eq!(deserialized.value.as_str(), Some("hello"));

        // v2 format (with version byte)
        let v2_value = JsonStore::serialize_doc(&doc).unwrap();
        let deserialized2 = JsonStore::deserialize_doc(&v2_value).unwrap();
        assert_eq!(deserialized2.id, "test");
        assert_eq!(deserialized2.value.as_str(), Some("hello"));

        // Verify v2 has the version byte prefix
        if let Value::Bytes(bytes) = &v2_value {
            assert_eq!(bytes[0], JsonStore::FORMAT_VERSION);
        }
    }

    // ========== JsonStore::search() BM25 integration tests ==========

    #[test]
    fn test_json_search_bm25() {
        use crate::search::Searchable;
        use std::str::FromStr;

        let temp = tempfile::TempDir::new().unwrap();
        let db = Database::open(temp.path()).unwrap();
        let store = JsonStore::new(db.clone());
        let branch_id = BranchId::new();

        // Verify index is enabled
        let index = db.extension::<crate::search::InvertedIndex>().unwrap();
        assert!(index.is_enabled());

        store
            .create(
                &branch_id,
                "default",
                "doc1",
                JsonValue::from_str(r#"{"title":"rust programming","body":"learn rust basics"}"#)
                    .unwrap(),
            )
            .unwrap();
        store
            .create(
                &branch_id,
                "default",
                "doc2",
                JsonValue::from_str(r#"{"title":"python scripting","body":"python is popular"}"#)
                    .unwrap(),
            )
            .unwrap();
        store
            .create(
                &branch_id,
                "default",
                "doc3",
                JsonValue::from_str(
                    r#"{"title":"go concurrency","body":"goroutines and channels"}"#,
                )
                .unwrap(),
            )
            .unwrap();

        let req = crate::SearchRequest::new(branch_id, "rust");
        let response = store.search(&req).unwrap();

        assert!(
            !response.is_empty(),
            "Should find JSON docs containing 'rust'"
        );
        assert!(response.stats.index_used);
        // Only doc1 contains "rust"
        assert_eq!(response.len(), 1);
        if let crate::search::EntityRef::Json { ref doc_id, .. } = response.hits[0].doc_ref {
            assert_eq!(doc_id, "doc1");
        } else {
            panic!("Expected EntityRef::Json");
        }
    }

    #[test]
    fn test_json_search_bm25_with_set_or_create() {
        use crate::search::Searchable;
        use std::str::FromStr;

        let temp = tempfile::TempDir::new().unwrap();
        let db = Database::open(temp.path()).unwrap();
        let store = JsonStore::new(db.clone());
        let branch_id = BranchId::new();

        // Use set_or_create (creates if missing)
        store
            .set_or_create(
                &branch_id,
                "default",
                "article1",
                &JsonPath::root(),
                JsonValue::from_str(r#"{"content":"database indexing techniques"}"#).unwrap(),
            )
            .unwrap();

        let req = crate::SearchRequest::new(branch_id, "indexing");
        let response = store.search(&req).unwrap();

        assert!(
            !response.is_empty(),
            "set_or_create docs should be findable via BM25"
        );
        assert_eq!(response.len(), 1);
        if let crate::search::EntityRef::Json { ref doc_id, .. } = response.hits[0].doc_ref {
            assert_eq!(doc_id, "article1");
        } else {
            panic!("Expected EntityRef::Json");
        }
    }

    #[test]
    fn test_json_search_empty_index() {
        use crate::search::Searchable;

        let temp = tempfile::TempDir::new().unwrap();
        let db = Database::open(temp.path()).unwrap();
        let store = JsonStore::new(db);
        let branch_id = BranchId::new();

        let req = crate::SearchRequest::new(branch_id, "anything");
        let response = store.search(&req).unwrap();
        assert!(response.is_empty());
    }
}
