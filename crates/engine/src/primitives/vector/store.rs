//! VectorStore: Vector storage and search primitive
//!
//! ## Design
//!
//! VectorStore is a **stateless facade** over the Database engine for collection
//! management. Following the same pattern as KVStore, JsonStore, and other primitives:
//!
//! - VectorStore holds only `Arc<Database>` (no private state)
//! - All persistent state lives in the Database (via the extension mechanism)
//! - Multiple VectorStore instances for the same Database share state
//!
//! This ensures that concurrent access from multiple threads or instances
//! sees consistent state, avoiding the data loss bug where each VectorStore::new()
//! created a private, empty backends map.
//!
//! ## Branch Isolation
//!
//! All operations are scoped to a `BranchId`. Different branches cannot see
//! each other's collections or vectors.
//!
//! ## Thread Safety
//!
//! VectorStore is `Send + Sync` and can be safely shared across threads.
//! All VectorStore instances for the same Database share backend state
//! through `Database::extension::<VectorBackendState>()`.

use crate::database::Database;
use crate::primitives::extensions::VectorStoreExt;
use crate::primitives::vector::collection::{validate_collection_name, validate_vector_key};
use crate::primitives::vector::{
    CollectionId, CollectionInfo, CollectionRecord, IndexBackendFactory, MetadataFilter,
    VectorConfig, VectorEntry, VectorError, VectorId, VectorIndexBackend, VectorMatch,
    VectorMatchWithSource, VectorRecord, VectorResult,
};
use dashmap::DashMap;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::EntityRef;
use tracing::{debug, info, warn};

/// Statistics from vector recovery
#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    /// Number of collections created during recovery
    pub collections_created: usize,
    /// Number of collections deleted during recovery
    pub collections_deleted: usize,
    /// Number of vectors upserted during recovery (full KV path)
    pub vectors_upserted: usize,
    /// Number of vectors registered from mmap cache
    pub vectors_mmap_registered: usize,
    /// Number of vectors deleted during recovery
    pub vectors_deleted: usize,
    /// Number of lite records skipped (no mmap cache available)
    pub lite_records_skipped: usize,
}

/// Shared backend state for VectorStore
///
/// This struct is stored in the Database via the extension mechanism,
/// ensuring all VectorStore instances for the same Database share the same
/// backend state. This is critical for correct concurrent operation.
///
/// # Thread Safety
///
/// Uses `DashMap` for per-collection (per-shard) concurrency (#1624).
/// Writes to collection A no longer block reads from collection B.
pub struct VectorBackendState {
    /// In-memory index backends per collection.
    ///
    /// `DashMap` provides per-shard locking so that operations on different
    /// collections proceed concurrently without a global write lock.
    pub backends: DashMap<CollectionId, Box<dyn VectorIndexBackend>>,
}

impl Default for VectorBackendState {
    fn default() -> Self {
        Self {
            backends: DashMap::new(),
        }
    }
}

/// Validate that a query vector contains only finite f32 values.
///
/// NaN and Infinity values corrupt distance calculations and produce
/// meaningless results. Rejecting them early gives a clear error message.
fn validate_query_values(values: &[f32]) -> VectorResult<()> {
    if values.iter().any(|v| !v.is_finite()) {
        return Err(VectorError::InvalidEmbedding {
            reason: "query vector contains NaN or infinite values".into(),
        });
    }
    Ok(())
}

/// Vector storage and search primitive
///
/// Manages collections of vectors with similarity search capabilities.
/// This is a **stateless facade** - it holds only a reference to the Database.
/// All backend state is stored in the Database via `extension::<VectorBackendState>()`.
///
/// # Example
///
/// ```text
/// use strata_primitives::VectorStore;
/// use crate::database::Database;
/// use strata_core::types::BranchId;
///
/// let db = Database::open("/path/to/data")?;
/// let store = VectorStore::new(db.clone());
/// let branch_id = BranchId::new();
///
/// // Create collection
/// let config = VectorConfig::for_minilm();
/// store.create_collection(branch_id, "embeddings", config)?;
///
/// // Multiple stores share the same backend state
/// let store2 = VectorStore::new(db.clone());
/// // store2 sees the same collections as store
/// ```
#[derive(Clone)]
pub struct VectorStore {
    db: Arc<Database>,
}

impl VectorStore {
    /// Create a new VectorStore
    ///
    /// This is a stateless facade - all backend state is stored in the Database
    /// via the extension mechanism. Multiple VectorStore instances for the same
    /// Database share the same backend state.
    ///
    /// NOTE: Recovery is NOT performed automatically. Recovery is orchestrated
    /// by the Database during startup, which calls `recover()` after all
    /// primitives are registered.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Get the underlying database reference
    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Get access to the shared backend state
    ///
    /// This returns the shared `VectorBackendState` stored in the Database.
    /// All VectorStore instances for the same Database share this state.
    fn state(&self) -> Result<Arc<VectorBackendState>, VectorError> {
        self.db
            .extension::<VectorBackendState>()
            .map_err(|e| VectorError::Storage(e.to_string()))
    }

    /// Build namespace for branch+space-scoped operations
    fn namespace_for(&self, branch_id: BranchId, space: &str) -> Arc<Namespace> {
        Arc::new(Namespace::for_branch_space(branch_id, space))
    }

    /// Get the backend factory (hardcoded currently, configurable in future versions)
    fn backend_factory(&self) -> IndexBackendFactory {
        IndexBackendFactory::SegmentedHnsw(super::segmented::SegmentedHnswConfig::default())
    }

    // ========================================================================
    // Collection Management
    // ========================================================================

    /// Create a new collection
    ///
    /// Creates a collection with the specified configuration.
    /// The configuration (dimension, metric, dtype) is immutable after creation.
    ///
    /// # Errors
    /// - `CollectionAlreadyExists` if a collection with this name exists
    /// - `InvalidCollectionName` if name is invalid
    /// - `InvalidDimension` if dimension is 0
    pub fn create_collection(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
        config: VectorConfig,
    ) -> VectorResult<Versioned<CollectionInfo>> {
        // Validate name
        validate_collection_name(name)?;

        // Validate config (dimension must be > 0 and <= MAX_DIMENSION)
        const MAX_DIMENSION: usize = 65536;
        if config.dimension == 0 {
            return Err(VectorError::InvalidDimension {
                dimension: config.dimension,
            });
        }
        if config.dimension > MAX_DIMENSION {
            return Err(VectorError::InvalidDimension {
                dimension: config.dimension,
            });
        }

        let collection_id = CollectionId::new(branch_id, name);

        // Check if collection already exists
        if self.collection_exists(branch_id, space, name)? {
            return Err(VectorError::CollectionAlreadyExists {
                name: name.to_string(),
            });
        }

        let now = now_micros();

        // Create collection record
        let record = CollectionRecord::new(&config);

        // Store config in KV
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        let config_bytes = record.to_bytes()?;

        // Use transaction for atomic storage
        self.db
            .transaction(branch_id, |txn| {
                txn.put(config_key.clone(), Value::Bytes(config_bytes.clone()))
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Initialize in-memory backend
        self.init_backend(&collection_id, &config)?;

        let info = CollectionInfo {
            name: name.to_string(),
            config,
            count: 0,
            created_at: now,
        };

        info!(target: "strata::vector", collection = name, dimension = info.config.dimension, branch_id = %branch_id, "Collection created");

        Ok(Versioned::with_timestamp(
            info,
            Version::counter(1),
            Timestamp::from_micros(now),
        ))
    }

    /// Delete a collection and all its vectors
    ///
    /// This is a destructive operation that:
    /// 1. Deletes all vectors in the collection
    /// 2. Deletes the collection configuration
    /// 3. Removes the in-memory backend
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
    pub fn delete_collection(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, name);

        // Check if collection exists
        if !self.collection_exists(branch_id, space, name)? {
            return Err(VectorError::CollectionNotFound {
                name: name.to_string(),
            });
        }

        // Delete all vectors in the collection
        self.delete_all_vectors(branch_id, space, name)?;

        // Delete config from KV
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        self.db
            .transaction(branch_id, |txn| txn.delete(config_key.clone()))
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Remove in-memory backend
        {
            let state = self.state()?;
            state.backends.remove(&collection_id);
        }

        info!(target: "strata::vector", collection = name, branch_id = %branch_id, "Collection deleted");

        Ok(())
    }

    /// List all collections for a branch
    ///
    /// Returns CollectionInfo for each collection, including current vector count.
    /// Results are sorted by name for determinism (Invariant R4).
    pub fn list_collections(
        &self,
        branch_id: BranchId,
        space: &str,
    ) -> VectorResult<Vec<CollectionInfo>> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::new_vector_config_prefix(namespace);

        // Read at current version for consistency
        let version = self.db.storage().version();
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut collections = Vec::new();

        for (key, versioned_value) in entries {
            // Extract collection name from key
            let name = String::from_utf8(key.user_key.to_vec())
                .map_err(|e| VectorError::Serialization(e.to_string()))?;

            // Deserialize the record from the stored bytes
            let bytes = match &versioned_value.value {
                Value::Bytes(b) => b.clone(),
                _ => {
                    return Err(VectorError::Serialization(
                        "Expected Bytes value for collection record".to_string(),
                    ))
                }
            };
            let record = CollectionRecord::from_bytes(&bytes)?;
            let config = VectorConfig::try_from(record.config)?;

            // Get current count from backend
            let collection_id = CollectionId::new(branch_id, &name);
            let count = self.get_collection_count(&collection_id, branch_id, space, &name)?;

            collections.push(CollectionInfo {
                name,
                config,
                count,
                created_at: record.created_at,
            });
        }

        // Sort by name for determinism
        collections.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(collections)
    }

    /// Check if a collection exists (internal)
    fn collection_exists(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<bool> {
        use strata_core::traits::Storage;

        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        let version = self.db.storage().version();

        Ok(self
            .db
            .storage()
            .get_versioned(&config_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
            .is_some())
    }

    /// Get a single collection's info (internal - for snapshot/recovery)
    pub(crate) fn get_collection(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<Option<Versioned<CollectionInfo>>> {
        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);

        use strata_core::traits::Storage;
        let version = self.db.storage().version();

        let Some(versioned_value) = self
            .db
            .storage()
            .get_versioned(&config_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        let bytes = match &versioned_value.value {
            Value::Bytes(b) => b.clone(),
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for collection record".to_string(),
                ))
            }
        };
        let record = CollectionRecord::from_bytes(&bytes)?;
        let config = VectorConfig::try_from(record.config)?;

        let collection_id = CollectionId::new(branch_id, name);
        let count = self.get_collection_count(&collection_id, branch_id, space, name)?;

        let info = CollectionInfo {
            name: name.to_string(),
            config,
            count,
            created_at: record.created_at,
        };

        Ok(Some(Versioned::with_timestamp(
            info,
            versioned_value.version,
            versioned_value.timestamp,
        )))
    }

    // ========================================================================
    // Vector Operations
    // ========================================================================

    /// Insert a vector (upsert semantics)
    ///
    /// If a vector with this key already exists, it is overwritten.
    /// This follows Rule 3 (Upsert Semantics).
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
    /// - `InvalidKey` if key is invalid
    /// - `DimensionMismatch` if embedding dimension doesn't match config
    pub fn insert(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
    ) -> VectorResult<Version> {
        self.insert_inner(branch_id, space, collection, key, embedding, metadata, None)
    }

    /// Common insert implementation used by both `insert()` and `system_insert_with_source()`.
    #[allow(clippy::too_many_arguments)]
    fn insert_inner(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
        source_ref: Option<EntityRef>,
    ) -> VectorResult<Version> {
        // Validate key
        validate_vector_key(key)?;

        // Validate embedding values (reject NaN and Infinity)
        if embedding.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(VectorError::InvalidEmbedding {
                reason: "embedding contains NaN or Infinity values".to_string(),
            });
        }

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);

        // Validate dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if embedding.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: embedding.len(),
            });
        }

        // Serialize metadata to bytes for WAL storage (before it's consumed)
        let _metadata_bytes = metadata
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|e| VectorError::Serialization(e.to_string()))?;

        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        // Hold per-collection lock for the entire check-then-insert sequence to
        // prevent TOCTOU race (fixes #936). Also commit KV before updating
        // backend so a KV commit failure doesn't leave the backend in an
        // inconsistent state (fixes #937).
        let state = self.state()?;
        let mut backend = state.backends.get_mut(&collection_id).ok_or_else(|| {
            VectorError::CollectionNotFound {
                name: collection.to_string(),
            }
        })?;

        // Check existence under write lock
        let existing = self.get_vector_record_by_key(&kv_key)?;

        // Clone source_ref for inline meta before the match consumes it
        let inline_source_ref = source_ref.as_ref().cloned();

        let (vector_id, record) = if let Some(existing_record) = existing {
            // Update existing: keep the same VectorId
            let mut updated = existing_record;
            match source_ref {
                Some(sr) => updated.update_with_source(embedding.to_vec(), metadata, Some(sr)),
                None => updated.update(embedding.to_vec(), metadata),
            }
            (VectorId(updated.vector_id), updated)
        } else {
            // New vector: allocate VectorId from backend's per-collection counter
            let vector_id = backend.allocate_id();
            let record = match source_ref {
                Some(sr) => {
                    VectorRecord::new_with_source(vector_id, embedding.to_vec(), metadata, sr)
                }
                None => VectorRecord::new(vector_id, embedding.to_vec(), metadata),
            };
            (vector_id, record)
        };

        // Commit to KV FIRST (durability before in-memory update)
        let record_version = record.version;
        let record_bytes = record.to_bytes()?;
        self.db
            .transaction(branch_id, |txn| {
                txn.put(kv_key.clone(), Value::Bytes(record_bytes.clone()))
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Only update backend AFTER KV commit succeeds
        backend.insert_with_timestamp(vector_id, embedding, record.created_at)?;

        // Store inline metadata for O(1) search resolution
        backend.set_inline_meta(
            vector_id,
            super::types::InlineMeta {
                key: key.to_string(),
                source_ref: inline_source_ref,
            },
        );

        drop(backend);

        debug!(target: "strata::vector", collection, branch_id = %branch_id, "Vector upserted");

        Ok(Version::counter(record_version))
    }

    /// Get a vector by key
    ///
    /// Returns the vector entry including embedding and metadata.
    /// Returns None if vector doesn't exist.
    pub fn get(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<Option<Versioned<VectorEntry>>> {
        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);
        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        // Get record from KV with version info
        use strata_core::traits::Storage;
        let version = self.db.storage().version();
        let Some(versioned_value) = self
            .db
            .storage()
            .get_versioned(&kv_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        let bytes = match &versioned_value.value {
            Value::Bytes(b) => b,
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for vector record".to_string(),
                ))
            }
        };

        let record = VectorRecord::from_bytes(bytes)?;
        let vector_id = VectorId(record.vector_id);

        // Get embedding from backend
        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let embedding = backend
            .get(vector_id)
            .ok_or_else(|| VectorError::Internal("Embedding missing from backend".to_string()))?;

        let entry = VectorEntry {
            key: key.to_string(),
            embedding: embedding.to_vec(),
            metadata: record.metadata,
            vector_id,
            version: Version::counter(record.version),
            source_ref: record.source_ref,
        };

        Ok(Some(Versioned::with_timestamp(
            entry,
            versioned_value.version,
            versioned_value.timestamp,
        )))
    }

    /// Get a vector as of a past timestamp.
    ///
    /// Returns the vector if it existed at as_of_ts.
    /// This is a non-transactional read directly from the storage version chain.
    pub fn get_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        as_of_ts: u64,
    ) -> VectorResult<Option<VectorEntry>> {
        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        // Get historical record from storage
        let result = self
            .db
            .get_at_timestamp(&kv_key, as_of_ts)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let Some(vv) = result else {
            return Ok(None);
        };

        let bytes = match &vv.value {
            Value::Bytes(b) => b,
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for vector record".to_string(),
                ))
            }
        };

        let record = VectorRecord::from_bytes(bytes)?;

        // Use the embedding stored in the VectorRecord (historical snapshot).
        // The backend only holds the *current* embedding, which may differ if the
        // vector was re-upserted after as_of_ts.
        let embedding = if record.embedding.is_empty() {
            // Legacy records without stored embeddings: fall back to backend
            let collection_id = CollectionId::new(branch_id, collection);
            let vector_id = VectorId(record.vector_id);
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            backend
                .get(vector_id)
                .ok_or_else(|| VectorError::Internal("Embedding missing from backend".to_string()))?
                .to_vec()
        } else {
            record.embedding
        };

        Ok(Some(VectorEntry {
            key: key.to_string(),
            embedding,
            metadata: record.metadata,
            vector_id: VectorId(record.vector_id),
            version: strata_core::contract::Version::counter(record.version),
            source_ref: record.source_ref,
        }))
    }

    /// Delete a vector by key
    ///
    /// Returns true if the vector existed and was deleted.
    pub fn delete(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<bool> {
        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);
        let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, key);

        // Hold per-collection lock for entire check-then-delete (mirrors insert_inner fix #936)
        let state = self.state()?;

        let Some(record) = self.get_vector_record_by_key(&kv_key)? else {
            return Ok(false);
        };

        let vector_id = VectorId(record.vector_id);

        // KV first (mirrors insert_inner fix #937)
        self.db
            .transaction(branch_id, |txn| txn.delete(kv_key.clone()))
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Backend after KV succeeds
        if let Some(mut backend) = state.backends.get_mut(&collection_id) {
            use super::types::now_micros;
            backend.delete_with_timestamp(vector_id, now_micros())?;
            backend.remove_inline_meta(vector_id);
        }

        Ok(true)
    }

    /// Batch insert multiple vectors (upsert semantics)
    ///
    /// Acquires the write lock once, validates all entries, commits all KV writes,
    /// then updates the backend for each entry. Much more efficient than N individual inserts.
    ///
    /// # Errors
    /// - `CollectionNotFound` if collection doesn't exist
    /// - `DimensionMismatch` if any embedding has wrong dimension
    /// - `InvalidEmbedding` if any embedding contains NaN or Infinity
    /// - `InvalidKey` if any key is invalid
    pub fn batch_insert(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        entries: Vec<(String, Vec<f32>, Option<JsonValue>)>,
    ) -> VectorResult<Vec<Version>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Validate all entries before acquiring locks
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        for (key, embedding, _) in &entries {
            validate_vector_key(key)?;
            if embedding.iter().any(|v| v.is_nan() || v.is_infinite()) {
                return Err(VectorError::InvalidEmbedding {
                    reason: format!(
                        "embedding for key '{}' contains NaN or Infinity values",
                        key
                    ),
                });
            }
            if embedding.len() != config.dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: config.dimension,
                    got: embedding.len(),
                });
            }
        }

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;
        let collection_id = CollectionId::new(branch_id, collection);

        // Acquire per-collection lock once for the entire batch
        let state = self.state()?;
        let mut backend = state.backends.get_mut(&collection_id).ok_or_else(|| {
            VectorError::CollectionNotFound {
                name: collection.to_string(),
            }
        })?;

        let mut versions = Vec::with_capacity(entries.len());
        let batch_count = entries.len();

        // Prepare all records and accumulate KV writes for a single transaction
        let mut kv_writes: Vec<(Key, Value)> = Vec::with_capacity(entries.len());
        let mut backend_updates: Vec<(VectorId, String, Vec<f32>, u64)> =
            Vec::with_capacity(entries.len());

        for (key, embedding, metadata) in entries {
            let kv_key = Key::new_vector(self.namespace_for(branch_id, space), collection, &key);

            // Check existence
            let existing = self.get_vector_record_by_key(&kv_key)?;

            let (vector_id, record) = if let Some(existing_record) = existing {
                let mut updated = existing_record;
                updated.update(embedding.clone(), metadata);
                (VectorId(updated.vector_id), updated)
            } else {
                let vector_id = backend.allocate_id();
                let record = VectorRecord::new(vector_id, embedding.clone(), metadata);
                (vector_id, record)
            };

            let record_version = record.version;
            let record_bytes = record.to_bytes()?;
            kv_writes.push((kv_key, Value::Bytes(record_bytes)));
            backend_updates.push((vector_id, key, embedding, record.created_at));
            versions.push(Version::counter(record_version));
        }

        // Commit all KV writes in a single transaction
        self.db
            .transaction(branch_id, |txn| {
                for (key, value) in &kv_writes {
                    txn.put(key.clone(), value.clone())?;
                }
                Ok(())
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        // Update backend for each entry (after successful KV commit)
        for (vector_id, key, embedding, created_at) in backend_updates {
            backend.insert_with_timestamp(vector_id, &embedding, created_at)?;
            backend.set_inline_meta(
                vector_id,
                super::types::InlineMeta {
                    key,
                    source_ref: None,
                },
            );
        }

        drop(backend);

        debug!(target: "strata::vector", collection, count = batch_count, branch_id = %branch_id, "Batch upsert completed");

        Ok(versions)
    }

    /// Search for similar vectors
    ///
    /// Returns top-k vectors most similar to the query.
    /// Metadata filtering is applied as post-filter.
    ///
    /// # Invariants Satisfied
    /// - R1: Dimension validated against collection config
    /// - R2: Scores normalized to "higher = more similar"
    /// - R3: Deterministic order (backend + facade tie-breaking)
    /// - R5: Facade tie-break (score desc, key asc)
    /// - R10: Search is read-only (no mutations)
    pub fn search(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<MetadataFilter>,
    ) -> VectorResult<Vec<VectorMatch>> {
        let start = std::time::Instant::now();

        // k=0 returns empty
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);

        // Validate query dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        // Search backend with adaptive over-fetch for filtering (Issue #453)
        //
        // When a metadata filter is active, we over-fetch from the backend to account
        // for filtered-out results. If the initial fetch doesn't yield enough results,
        // we retry with a higher multiplier up to a max limit.
        //
        // Multiplier strategy: 3x -> 6x -> 12x -> all (capped at collection size)
        let mut matches = Vec::with_capacity(k);

        if filter.is_none() {
            // No filter - simple case, fetch exactly k with O(1) inline meta lookup
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let candidates = backend.search(query, k);

            for (vector_id, score) in candidates {
                if let Some(meta) = backend.get_inline_meta(vector_id) {
                    matches.push(VectorMatch {
                        key: meta.key.clone(),
                        score,
                        metadata: None,
                    });
                } else {
                    // Fallback to KV scan for vectors without inline meta
                    let (key, metadata) =
                        self.get_key_and_metadata(branch_id, space, collection, vector_id)?;
                    matches.push(VectorMatch {
                        key,
                        score,
                        metadata,
                    });
                }
            }
            drop(backend);
        } else {
            // Filter active - use adaptive over-fetch with O(1) key lookup + point-get for metadata
            let multipliers = [3, 6, 12];
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let collection_size = backend.len();
            let namespace = self.namespace_for(branch_id, space);

            for &mult in &multipliers {
                let fetch_k = (k * mult).min(collection_size);
                if fetch_k == 0 {
                    break;
                }

                let candidates = backend.search(query, fetch_k);

                matches.clear();
                for (vector_id, score) in candidates {
                    // Use inline meta for O(1) key lookup, then point-get for metadata
                    let (key, metadata) = if let Some(meta) = backend.get_inline_meta(vector_id) {
                        let kv_key = Key::new_vector(namespace.clone(), collection, &meta.key);
                        let md = self
                            .get_vector_record_by_key(&kv_key)?
                            .and_then(|r| r.metadata);
                        (meta.key.clone(), md)
                    } else {
                        self.get_key_and_metadata(branch_id, space, collection, vector_id)?
                    };

                    // Apply filter
                    if let Some(ref f) = filter {
                        if !f.matches(&metadata) {
                            continue;
                        }
                    }

                    matches.push(VectorMatch {
                        key,
                        score,
                        metadata,
                    });
                    if matches.len() >= k {
                        break;
                    }
                }

                // If we have enough results or searched all vectors, stop
                if matches.len() >= k || fetch_k >= collection_size {
                    break;
                }
            }
            drop(backend);
        }

        // Apply facade-level tie-breaking (score desc, key asc)
        // This satisfies Invariant R5
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        // Ensure we don't exceed k after sorting
        matches.truncate(k);

        debug!(target: "strata::vector", collection, k, results = matches.len(), duration_us = start.elapsed().as_micros() as u64, branch_id = %branch_id, "Vector search completed");

        Ok(matches)
    }

    /// Search for k nearest neighbors as of a given timestamp.
    ///
    /// Uses temporal filtering in the backend (HNSW nodes alive at as_of_ts)
    /// and historical metadata from the version chain.
    #[allow(clippy::too_many_arguments)]
    pub fn search_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<MetadataFilter>,
        as_of_ts: u64,
    ) -> VectorResult<Vec<VectorMatch>> {
        let start = std::time::Instant::now();

        // k=0 returns empty
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);

        // Validate dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        let mut matches = Vec::with_capacity(k);

        if filter.is_none() {
            // No filter — fetch exactly k
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let candidates = backend.search_at(query, k, as_of_ts);
            drop(backend);

            for (vector_id, score) in candidates {
                if let Some((key, metadata)) = self.find_vector_key_metadata_at(
                    branch_id, space, collection, vector_id, as_of_ts,
                )? {
                    matches.push(VectorMatch {
                        key,
                        score,
                        metadata,
                    });
                }
            }
        } else {
            // Filter active — adaptive over-fetch [3, 6, 12]
            let multipliers = [3, 6, 12];
            let state = self.state()?;
            let backend = state.backends.get(&collection_id).ok_or_else(|| {
                VectorError::CollectionNotFound {
                    name: collection.to_string(),
                }
            })?;
            let collection_size = backend.len();

            for &mult in &multipliers {
                let fetch_k = (k * mult).min(collection_size);
                if fetch_k == 0 {
                    break;
                }

                let candidates = backend.search_at(query, fetch_k, as_of_ts);

                matches.clear();
                for (vector_id, score) in candidates {
                    if let Some((key, metadata)) = self.find_vector_key_metadata_at(
                        branch_id, space, collection, vector_id, as_of_ts,
                    )? {
                        if let Some(ref f) = filter {
                            if !f.matches(&metadata) {
                                continue;
                            }
                        }
                        matches.push(VectorMatch {
                            key,
                            score,
                            metadata,
                        });
                        if matches.len() >= k {
                            break;
                        }
                    }
                }

                // If we have enough results or searched all vectors, stop
                if matches.len() >= k || fetch_k >= collection_size {
                    break;
                }
            }
            drop(backend);
        }

        // Apply facade-level tie-breaking (score desc, key asc)
        // This satisfies Invariant R5
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });
        matches.truncate(k);

        debug!(target: "strata::vector", collection, k, results = matches.len(),
               duration_us = start.elapsed().as_micros() as u64,
               as_of_ts, branch_id = %branch_id, "Temporal search completed");

        Ok(matches)
    }

    /// Find a vector's key and metadata by VectorId at a given timestamp (internal helper).
    fn find_vector_key_metadata_at(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        target_id: VectorId,
        as_of_ts: u64,
    ) -> VectorResult<Option<(String, Option<JsonValue>)>> {
        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, collection);
        let results = self
            .db
            .scan_prefix_at_timestamp(&prefix, as_of_ts)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        for (key, vv) in results {
            let bytes = match &vv.value {
                Value::Bytes(b) => b,
                _ => continue,
            };
            let record = match VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };
            if VectorId(record.vector_id) == target_id {
                let user_key = String::from_utf8(key.user_key.to_vec()).unwrap_or_default();
                // Strip the collection prefix to get just the vector key
                let vector_key = user_key
                    .strip_prefix(&format!("{}/", collection))
                    .unwrap_or(&user_key)
                    .to_string();
                return Ok(Some((vector_key, record.metadata)));
            }
        }
        Ok(None)
    }

    /// List all vector keys in a collection.
    ///
    /// Returns just the user-facing key names (without internal prefixes).
    /// Useful for introspection and sampling.
    pub fn list_keys(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
    ) -> VectorResult<Vec<String>> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, collection);

        let version = self.db.storage().version();
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut keys = Vec::new();
        for (key, _) in entries {
            let user_key = String::from_utf8(key.user_key.to_vec()).unwrap_or_default();
            // Strip the collection prefix to get just the vector key
            let vector_key = user_key
                .strip_prefix(&format!("{}/", collection))
                .unwrap_or(&user_key)
                .to_string();
            keys.push(vector_key);
        }
        Ok(keys)
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Initialize the index backend for a collection
    fn init_backend(&self, id: &CollectionId, config: &VectorConfig) -> Result<(), VectorError> {
        let backend = self.create_backend(id, config)?;
        let state = self.state()?;
        state.backends.insert(id.clone(), backend);
        Ok(())
    }

    /// Create and configure an index backend without inserting it into the map.
    fn create_backend(
        &self,
        id: &CollectionId,
        config: &VectorConfig,
    ) -> Result<Box<dyn VectorIndexBackend>, VectorError> {
        let mut backend = self.backend_factory().create(config);

        // Set flush_path so the tiered heap can flush overlays during fresh
        // indexing (not just during recovery). Without this, fresh inserts
        // stay in anonymous memory forever, causing OOM on large datasets.
        let data_dir = self.db.data_dir();
        if !data_dir.as_os_str().is_empty() {
            let vec_path = super::recovery::mmap_path(data_dir, id.branch_id, &id.name);
            if let Some(parent) = vec_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    warn!(
                        "failed to create vector data directory {}: {e}",
                        parent.display()
                    );
                }
            }
            if let Err(e) = backend.flush_heap_to_disk_if_needed(&vec_path) {
                warn!(
                    "failed to flush heap to disk at {}: {e}",
                    vec_path.display()
                );
            }
        }

        Ok(backend)
    }

    /// Get collection config (required version that errors if not found)
    fn get_collection_config_required(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<VectorConfig> {
        self.load_collection_config(branch_id, space, name)?
            .ok_or_else(|| VectorError::CollectionNotFound {
                name: name.to_string(),
            })
    }

    /// Get a vector record by KV key
    fn get_vector_record_by_key(&self, key: &Key) -> VectorResult<Option<VectorRecord>> {
        use strata_core::traits::Storage;

        let version = self.db.storage().version();
        let Some(versioned) = self
            .db
            .storage()
            .get_versioned(key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        let bytes = match &versioned.value {
            Value::Bytes(b) => b,
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for vector record".to_string(),
                ))
            }
        };

        let record = VectorRecord::from_bytes(bytes)?;
        Ok(Some(record))
    }

    /// Get key and metadata for a VectorId by scanning KV (internal)
    pub(crate) fn get_key_and_metadata(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        target_id: VectorId,
    ) -> VectorResult<(String, Option<JsonValue>)> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, collection);

        let version = self.db.storage().version();
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        for (key, versioned) in entries {
            let bytes = match &versioned.value {
                Value::Bytes(b) => b,
                _ => continue,
            };

            let record = match VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };

            if record.vector_id == target_id.0 {
                // Extract vector key from the full key
                // Key format: collection/key
                let user_key = String::from_utf8(key.user_key.to_vec())
                    .map_err(|e| VectorError::Serialization(e.to_string()))?;

                // Remove collection prefix
                let vector_key = user_key
                    .strip_prefix(&format!("{}/", collection))
                    .unwrap_or(&user_key)
                    .to_string();

                return Ok((vector_key, record.metadata));
            }
        }

        Err(VectorError::Internal(format!(
            "VectorId {:?} not found in KV",
            target_id
        )))
    }

    /// Get key, metadata, source_ref, and version for a VectorId by scanning KV (internal)
    ///
    /// Like `get_key_and_metadata()` but also returns the `source_ref` and `version`
    /// fields from the VectorRecord. Used by `search_with_sources()`.
    #[allow(dead_code)]
    fn get_key_metadata_and_source(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        target_id: VectorId,
    ) -> VectorResult<(
        String,
        Option<JsonValue>,
        Option<strata_core::EntityRef>,
        u64,
    )> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, collection);

        let version = self.db.storage().version();
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        for (key, versioned) in entries {
            let bytes = match &versioned.value {
                Value::Bytes(b) => b,
                _ => continue,
            };

            let record = match VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };

            if record.vector_id == target_id.0 {
                let user_key = String::from_utf8(key.user_key.to_vec())
                    .map_err(|e| VectorError::Serialization(e.to_string()))?;

                let vector_key = user_key
                    .strip_prefix(&format!("{}/", collection))
                    .unwrap_or(&user_key)
                    .to_string();

                return Ok((
                    vector_key,
                    record.metadata,
                    record.source_ref,
                    record.version,
                ));
            }
        }

        Err(VectorError::Internal(format!(
            "VectorId {:?} not found in KV",
            target_id
        )))
    }

    /// Get the current vector count for a collection
    fn get_collection_count(
        &self,
        id: &CollectionId,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<usize> {
        // Check in-memory backend first
        let state = self.state()?;
        if let Some(backend) = state.backends.get(id) {
            return Ok(backend.len());
        }

        // Backend not loaded - count from KV
        use strata_core::traits::Storage;
        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, name);

        let version = self.db.storage().version();
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        Ok(entries.len())
    }

    /// Delete all vectors in a collection
    fn delete_all_vectors(&self, branch_id: BranchId, space: &str, name: &str) -> VectorResult<()> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, name);

        // Scan all vector keys in this collection
        let version = self.db.storage().version();
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let keys: Vec<Key> = entries.into_iter().map(|(key, _)| key).collect();

        // Delete each vector in a transaction
        if !keys.is_empty() {
            self.db
                .transaction(branch_id, |txn| {
                    for key in &keys {
                        let k: Key = key.clone();
                        txn.delete(k)?;
                    }
                    Ok(())
                })
                .map_err(|e| VectorError::Storage(e.to_string()))?;
        }

        Ok(())
    }

    /// Load collection config from KV
    fn load_collection_config(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<Option<VectorConfig>> {
        use strata_core::traits::Storage;

        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        let version = self.db.storage().version();

        let Some(versioned_value) = self
            .db
            .storage()
            .get_versioned(&config_key, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        let bytes = match &versioned_value.value {
            Value::Bytes(b) => b.clone(),
            _ => {
                return Err(VectorError::Serialization(
                    "Expected Bytes value for collection record".to_string(),
                ))
            }
        };

        let record = CollectionRecord::from_bytes(&bytes)?;
        let config = VectorConfig::try_from(record.config)?;
        Ok(Some(config))
    }

    /// Ensure collection is loaded into memory
    ///
    /// If the collection exists in KV but not in memory (after recovery),
    /// this loads it and initializes the backend.
    ///
    /// Uses double-checked locking to avoid TOCTOU races: the read-lock fast
    /// path avoids contention in the common case, while the write-lock slow
    /// path re-checks to prevent duplicate initialization.
    pub(crate) fn ensure_collection_loaded(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, name);
        let state = self.state()?;

        // Fast path: check without entry overhead
        if state.backends.contains_key(&collection_id) {
            return Ok(());
        }

        // Slow path: load config and create backend outside lock
        let config = self
            .load_collection_config(branch_id, space, name)?
            .ok_or_else(|| VectorError::CollectionNotFound {
                name: name.to_string(),
            })?;
        let backend = self.create_backend(&collection_id, &config)?;

        // Double-check via entry API: another thread may have loaded it
        use dashmap::mapref::entry::Entry;
        match state.backends.entry(collection_id) {
            Entry::Occupied(_) => {}
            Entry::Vacant(e) => {
                e.insert(backend);
            }
        }
        Ok(())
    }

    // ========================================================================
    // WAL Replay Methods
    // ========================================================================

    /// Replay collection creation from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL - that would cause infinite loops during replay.
    ///
    /// Called by the global WAL replayer for committed VectorCollectionCreate entries.
    ///
    /// # Config Validation (Issue #452)
    ///
    /// If collection already exists, validates that the config matches.
    /// This catches WAL corruption or conflicting create entries.
    pub fn replay_create_collection(
        &self,
        branch_id: BranchId,
        name: &str,
        config: VectorConfig,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, name);

        // Check if collection already exists in backend
        let state = self.state()?;
        if let Some(existing_backend) = state.backends.get(&collection_id) {
            // Validate config matches (Issue #452)
            let existing_config = existing_backend.config();
            if existing_config.dimension != config.dimension {
                tracing::warn!(
                    target: "strata::vector",
                    collection = name,
                    existing_dim = existing_config.dimension,
                    wal_dim = config.dimension,
                    "Config mismatch during WAL replay: dimension differs"
                );
                return Err(VectorError::DimensionMismatch {
                    expected: existing_config.dimension,
                    got: config.dimension,
                });
            }
            if existing_config.metric != config.metric {
                tracing::warn!(
                    target: "strata::vector",
                    collection = name,
                    existing_metric = ?existing_config.metric,
                    wal_metric = ?config.metric,
                    "Config mismatch during WAL replay: metric differs"
                );
                return Err(VectorError::ConfigMismatch {
                    collection: name.to_string(),
                    field: "metric".to_string(),
                });
            }
            // Collection already exists with matching config - idempotent replay
            return Ok(());
        }

        // Initialize backend (no KV write - KV is replayed separately)
        let backend = self.backend_factory().create(&config);
        state.backends.insert(collection_id, backend);

        Ok(())
    }

    /// Replay collection deletion from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL.
    pub fn replay_delete_collection(&self, branch_id: BranchId, name: &str) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, name);

        // Remove in-memory backend
        let state = self.state()?;
        state.backends.remove(&collection_id);

        Ok(())
    }

    /// Replay vector upsert from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL.
    ///
    /// Uses `insert_with_id_and_timestamp` to maintain VectorId monotonicity
    /// (Invariant T4) and preserve temporal metadata for `search_at()` queries.
    ///
    /// Note: `_key`, `_metadata`, and `_source_ref` parameters are not used here because
    /// they are stored in the KV layer (via VectorRecord), which has its own WAL entries.
    /// This method only replays the embedding into the VectorHeap backend.
    #[allow(clippy::too_many_arguments)]
    pub fn replay_upsert(
        &self,
        branch_id: BranchId,
        collection: &str,
        _key: &str,
        vector_id: VectorId,
        embedding: &[f32],
        _metadata: Option<serde_json::Value>,
        _source_ref: Option<strata_core::EntityRef>,
        created_at: u64,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, collection);

        let state = self.state()?;
        let mut backend = state.backends.get_mut(&collection_id).ok_or_else(|| {
            VectorError::CollectionNotFound {
                name: collection.to_string(),
            }
        })?;

        // Use insert_with_id_and_timestamp to maintain VectorId monotonicity
        // and preserve temporal data for time-travel queries after recovery.
        backend.insert_with_id_and_timestamp(vector_id, embedding, created_at)?;

        Ok(())
    }

    /// Replay vector deletion from WAL (no WAL write)
    ///
    /// IMPORTANT: This method is for WAL replay during recovery.
    /// It does NOT write to WAL.
    ///
    /// Passes the original deletion timestamp to preserve temporal metadata
    /// for `search_at()` queries after recovery.
    pub fn replay_delete(
        &self,
        branch_id: BranchId,
        collection: &str,
        _key: &str,
        vector_id: VectorId,
        deleted_at: u64,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, collection);

        let state = self.state()?;
        if let Some(mut backend) = state.backends.get_mut(&collection_id) {
            backend.delete_with_timestamp(vector_id, deleted_at)?;
        }
        // Note: If collection doesn't exist, that's OK - it may have been deleted

        Ok(())
    }

    // ========================================================================
    // System Collection Methods (internal use only)
    // ========================================================================

    /// Create a system collection (internal use only, bypasses `_` prefix check)
    ///
    /// System collections must have names starting with `_system_`.
    pub fn create_system_collection(
        &self,
        branch_id: BranchId,
        name: &str,
        config: VectorConfig,
    ) -> VectorResult<Versioned<CollectionInfo>> {
        use crate::primitives::vector::collection::validate_system_collection_name;

        validate_system_collection_name(name)?;

        const MAX_DIMENSION: usize = 65536;
        if config.dimension == 0 || config.dimension > MAX_DIMENSION {
            return Err(VectorError::InvalidDimension {
                dimension: config.dimension,
            });
        }

        let collection_id = CollectionId::new(branch_id, name);

        if self.collection_exists(branch_id, "default", name)? {
            return Err(VectorError::CollectionAlreadyExists {
                name: name.to_string(),
            });
        }

        let now = now_micros();
        let record = CollectionRecord::new(&config);
        let config_key = Key::new_vector_config(Arc::new(Namespace::for_branch(branch_id)), name);
        let config_bytes = record.to_bytes()?;

        self.db
            .transaction(branch_id, |txn| {
                txn.put(config_key.clone(), Value::Bytes(config_bytes.clone()))
            })
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        self.init_backend(&collection_id, &config)?;

        let info = CollectionInfo {
            name: name.to_string(),
            config,
            count: 0,
            created_at: now,
        };

        Ok(Versioned::with_timestamp(
            info,
            Version::counter(1),
            Timestamp::from_micros(now),
        ))
    }

    /// Insert into a system collection (internal use only)
    pub fn system_insert(
        &self,
        branch_id: BranchId,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
    ) -> VectorResult<Version> {
        use crate::primitives::vector::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        // Delegate to the normal insert path (which doesn't re-check collection name)
        self.insert(branch_id, "default", collection, key, embedding, metadata)
    }

    /// Insert into a system collection with a source reference (internal use only)
    ///
    /// Like `system_insert` but also stores an `EntityRef` that traces the
    /// shadow embedding back to the originating record, enabling hybrid search.
    pub fn system_insert_with_source(
        &self,
        branch_id: BranchId,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<JsonValue>,
        source_ref: EntityRef,
    ) -> VectorResult<Version> {
        use crate::primitives::vector::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.insert_inner(
            branch_id,
            "default",
            collection,
            key,
            embedding,
            metadata,
            Some(source_ref),
        )
    }

    /// Get the vector count for a system collection (for filtering empty collections).
    pub fn system_collection_len(
        &self,
        branch_id: BranchId,
        collection: &str,
    ) -> VectorResult<usize> {
        let cid = CollectionId::new(branch_id, collection);
        let state = self.state()?;
        Ok(state.backends.get(&cid).map(|b| b.len()).unwrap_or(0))
    }

    /// Search a system collection (internal use only)
    pub fn system_search(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<MetadataFilter>,
    ) -> VectorResult<Vec<VectorMatch>> {
        use crate::primitives::vector::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search(branch_id, "default", collection, query, k, filter)
    }

    /// Search a system collection returning results with source references (internal use only)
    ///
    /// Like `system_search()` but returns `VectorMatchWithSource` which includes the
    /// `source_ref` and `version` from the original VectorRecord. Used by hybrid search
    /// to trace shadow vectors back to their originating records.
    pub fn system_search_with_sources(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        use crate::primitives::vector::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search_with_sources(branch_id, "default", collection, query, k)
    }

    /// Search returning results with source references (internal)
    ///
    /// Uses O(1) inline metadata lookup per candidate instead of O(n) KV prefix scans.
    fn search_with_sources(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);

        // Validate query dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        // Search backend + resolve inline metadata under a single guard
        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let candidates = backend.search(query, k);

        let mut matches: Vec<VectorMatchWithSource> = Vec::with_capacity(candidates.len());
        let mut fallback_candidates: Vec<(VectorId, f32)> = Vec::new();

        for &(vid, score) in &candidates {
            if let Some(meta) = backend.get_inline_meta(vid) {
                matches.push(VectorMatchWithSource::new(
                    meta.key.clone(),
                    score,
                    None,
                    meta.source_ref.clone(),
                    0,
                ));
            } else {
                fallback_candidates.push((vid, score));
            }
        }

        drop(backend);

        // Resolve any candidates missing inline meta via KV fallback
        for (vid, score) in fallback_candidates {
            if let Ok((key, _metadata)) =
                self.get_key_and_metadata(branch_id, space, collection, vid)
            {
                matches.push(VectorMatchWithSource::new(key, score, None, None, 0));
            }
        }

        // Facade-level tie-breaking (score desc, key asc)
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        matches.truncate(k);

        Ok(matches)
    }

    /// System search with sources, filtered by time range.
    ///
    /// Mirrors `system_search_with_sources()` but uses `search_in_range()` to
    /// restrict results to vectors created within the given timestamp range.
    pub fn system_search_with_sources_in_range(
        &self,
        branch_id: BranchId,
        collection: &str,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        use crate::primitives::vector::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.search_with_sources_in_range(
            branch_id, "default", collection, query, k, start_ts, end_ts,
        )
    }

    /// Search returning results with source references, filtered by time range.
    ///
    /// Uses O(1) inline metadata lookup per candidate instead of O(n) KV prefix scans.
    #[allow(clippy::too_many_arguments)]
    fn search_with_sources_in_range(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> VectorResult<Vec<VectorMatchWithSource>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        // Reject NaN/Infinity in query vector
        validate_query_values(query)?;

        // Ensure collection is loaded
        self.ensure_collection_loaded(branch_id, space, collection)?;

        let collection_id = CollectionId::new(branch_id, collection);

        // Validate query dimension
        let config = self.get_collection_config_required(branch_id, space, collection)?;
        if query.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: query.len(),
            });
        }

        // Search backend + resolve inline metadata under a single guard
        let state = self.state()?;
        let backend =
            state
                .backends
                .get(&collection_id)
                .ok_or_else(|| VectorError::CollectionNotFound {
                    name: collection.to_string(),
                })?;

        let candidates = backend.search_in_range(query, k, start_ts, end_ts);

        let mut matches: Vec<VectorMatchWithSource> = Vec::with_capacity(candidates.len());
        let mut fallback_candidates: Vec<(VectorId, f32)> = Vec::new();

        for &(vid, score) in &candidates {
            if let Some(meta) = backend.get_inline_meta(vid) {
                matches.push(VectorMatchWithSource::new(
                    meta.key.clone(),
                    score,
                    None,
                    meta.source_ref.clone(),
                    0,
                ));
            } else {
                fallback_candidates.push((vid, score));
            }
        }

        drop(backend);

        // Resolve any candidates missing inline meta via KV fallback
        for (vid, score) in fallback_candidates {
            if let Ok((key, _metadata)) =
                self.get_key_and_metadata(branch_id, space, collection, vid)
            {
                matches.push(VectorMatchWithSource::new(key, score, None, None, 0));
            }
        }

        // Facade-level tie-breaking (score desc, key asc)
        matches.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.cmp(&b.key))
        });

        matches.truncate(k);

        Ok(matches)
    }

    /// Delete from a system collection (internal use only)
    pub fn system_delete(
        &self,
        branch_id: BranchId,
        collection: &str,
        key: &str,
    ) -> VectorResult<bool> {
        use crate::primitives::vector::collection::validate_system_collection_name;
        validate_system_collection_name(collection)?;
        self.delete(branch_id, "default", collection, key)
    }

    /// Get index type name and memory usage for a collection
    pub fn collection_backend_stats(
        &self,
        branch_id: BranchId,
        _space: &str,
        name: &str,
    ) -> Option<(&'static str, usize)> {
        let collection_id = CollectionId::new(branch_id, name);
        let state = self.state().ok()?;
        state
            .backends
            .get(&collection_id)
            .map(|b| (b.index_type_name(), b.memory_usage()))
    }

    /// Get access to the shared backend state (for recovery/snapshot)
    pub(crate) fn backends(&self) -> Result<Arc<VectorBackendState>, VectorError> {
        self.state()
    }

    /// Get access to the database (for snapshot operations)
    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    /// Internal helper to create vector KV key
    pub(crate) fn vector_key_internal(
        &self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> Key {
        Key::new_vector(self.namespace_for(branch_id, space), collection, key)
    }

    // ========================================================================
    // Post-Merge Vector Reload (Phase 2: Branch-Aware Merge)
    // ========================================================================

    /// Reload all vector backends for a branch from KV state.
    ///
    /// Called after `merge_branches()` to ensure in-memory vector backends
    /// reflect the merged KV state. Without this, vectors merged at the KV
    /// level would be invisible to search until the next full recovery.
    ///
    /// For each vector collection in the target branch (across all spaces):
    /// 1. Creates a fresh backend from the factory
    /// 2. Scans all VectorRecords from KV (including newly merged ones)
    /// 3. Allocates new VectorIds to avoid collisions from independent branches
    /// 4. Updates KV records with the new VectorIds for consistency
    /// 5. Rebuilds the SegmentedHnswBackend (segments via `rebuild_index()`)
    /// 6. Replaces the existing in-memory backend
    ///
    /// ## VectorId Remapping
    ///
    /// When branches are created independently, each branch allocates VectorIds
    /// starting from 0. After merge, the target branch may contain VectorRecords
    /// with colliding IDs from different branches. This method allocates fresh
    /// IDs from the new backend's counter and updates KV records to match,
    /// ensuring the VectorId link between KV and backend is consistent.
    ///
    /// Collections on other branches are left untouched.
    pub fn post_merge_reload_vectors(&self, branch_id: BranchId) -> VectorResult<()> {
        self.post_merge_reload_vectors_from(branch_id, None)
    }

    /// Reload vector backends for a branch, optionally reading embeddings
    /// from a source branch's backend when KV records are lite.
    pub fn post_merge_reload_vectors_from(
        &self,
        branch_id: BranchId,
        source_branch_id: Option<BranchId>,
    ) -> VectorResult<()> {
        use strata_core::traits::Storage;

        let state = self.state()?;
        let factory = self.backend_factory();
        let version = self.db.storage().version();

        // Get all spaces for this branch (SpaceIndex.list always includes "default")
        let space_index = crate::SpaceIndex::new(self.db.clone());
        let spaces = space_index
            .list(branch_id)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let mut total_collections = 0usize;
        let mut total_vectors = 0usize;

        for space in &spaces {
            let ns = self.namespace_for(branch_id, space);

            // Scan for vector config entries in this space
            let config_prefix = Key::new_vector_config_prefix(ns.clone());
            let config_entries = match self.db.storage().scan_prefix(&config_prefix, version) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        branch_id = %branch_id,
                        space = %space,
                        error = %e,
                        "Failed to scan vector configs during post-merge reload"
                    );
                    continue;
                }
            };

            for (key, versioned) in &config_entries {
                // Parse collection config (same pattern as recovery.rs)
                let config_bytes = match &versioned.value {
                    Value::Bytes(b) => b,
                    _ => continue,
                };

                let record = match CollectionRecord::from_bytes(config_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            key = ?key,
                            error = %e,
                            "Failed to decode collection record during post-merge reload, skipping"
                        );
                        continue;
                    }
                };

                let collection_name = match key.user_key_string() {
                    Some(name) => name,
                    None => continue,
                };

                let config: VectorConfig = match record.config.try_into() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to convert collection config during post-merge reload, skipping"
                        );
                        continue;
                    }
                };

                let collection_id = CollectionId::new(branch_id, &collection_name);

                // Grab the old backend (if any) so we can read embeddings
                // from it for lite records that don't store them in KV.
                let old_backend = state.backends.remove(&collection_id).map(|(_, v)| v);

                // If we have a source branch, build a map from user_key → VectorId
                // for the source collection. This lets us determine which backend
                // to read from when VectorIds collide between source and target.
                let source_key_to_vid: BTreeMap<Vec<u8>, u64> = if let Some(src_bid) =
                    source_branch_id
                {
                    let src_ns = Arc::new(Namespace::for_branch_space(src_bid, space));
                    let src_prefix = Key::new_vector(src_ns, &collection_name, "");
                    if let Ok(src_entries) = self.db.storage().scan_prefix(&src_prefix, version) {
                        src_entries
                            .iter()
                            .filter_map(|(k, vv)| {
                                if let Value::Bytes(b) = &vv.value {
                                    VectorRecord::from_bytes(b)
                                        .ok()
                                        .map(|r| (k.user_key.to_vec(), r.vector_id))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        BTreeMap::new()
                    }
                } else {
                    BTreeMap::new()
                };

                // Create fresh backend
                let mut backend = factory.create(&config);

                // Scan all vector entries in this collection
                let vector_prefix = Key::new_vector(ns.clone(), &collection_name, "");
                let vector_entries = match self.db.storage().scan_prefix(&vector_prefix, version) {
                    Ok(entries) => entries,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to scan vectors during post-merge reload"
                        );
                        continue;
                    }
                };

                // Collect vectors that need VectorId remapping, then batch-update KV
                let mut kv_updates: Vec<(Key, Vec<u8>)> = Vec::new();
                let mut collection_vector_count = 0usize;
                let mut kv_remap_failed = false;

                for (vec_key, vec_versioned) in &vector_entries {
                    let vec_bytes = match &vec_versioned.value {
                        Value::Bytes(b) => b,
                        _ => continue,
                    };

                    let mut vec_record = match VectorRecord::from_bytes(vec_bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                target: "strata::vector",
                                error = %e,
                                "Failed to decode vector record during post-merge reload, skipping"
                            );
                            continue;
                        }
                    };

                    // Resolve the embedding: use KV record if present, else
                    // fall back to the appropriate backend based on provenance.
                    let embedding = if !vec_record.embedding.is_empty() {
                        vec_record.embedding.clone()
                    } else {
                        let old_vid = VectorId(vec_record.vector_id);

                        // Determine provenance: did this key come from the source branch?
                        // Check by matching user_key + VectorId against source KV scan.
                        let is_from_source = source_key_to_vid
                            .get(&*vec_key.user_key)
                            .is_some_and(|&src_vid| src_vid == vec_record.vector_id);

                        if is_from_source {
                            if let Some(src_bid) = source_branch_id {
                                let src_cid = CollectionId::new(src_bid, &collection_name);
                                let src_emb = state
                                    .backends
                                    .get(&src_cid)
                                    .and_then(|b| b.get(old_vid).map(|e| e.to_vec()));
                                match src_emb {
                                    Some(emb) => emb,
                                    None => match old_backend.as_ref().and_then(|b| b.get(old_vid))
                                    {
                                        Some(emb) => emb.to_vec(),
                                        None => {
                                            tracing::warn!(
                                                target: "strata::vector",
                                                vector_id = vec_record.vector_id,
                                                "Lite record: embedding not found, skipping"
                                            );
                                            continue;
                                        }
                                    },
                                }
                            } else {
                                match old_backend.as_ref().and_then(|b| b.get(old_vid)) {
                                    Some(emb) => emb.to_vec(),
                                    None => {
                                        tracing::warn!(
                                            target: "strata::vector",
                                            vector_id = vec_record.vector_id,
                                            "Lite record without backend embedding, skipping"
                                        );
                                        continue;
                                    }
                                }
                            }
                        } else {
                            // Target-originated: try old target backend first, then source
                            match old_backend.as_ref().and_then(|b| b.get(old_vid)) {
                                Some(emb) => emb.to_vec(),
                                None => {
                                    if let Some(src_bid) = source_branch_id {
                                        let src_cid = CollectionId::new(src_bid, &collection_name);
                                        let src_emb = state
                                            .backends
                                            .get(&src_cid)
                                            .and_then(|b| b.get(old_vid).map(|e| e.to_vec()));
                                        match src_emb {
                                            Some(emb) => emb,
                                            None => {
                                                tracing::warn!(
                                                    target: "strata::vector",
                                                    vector_id = vec_record.vector_id,
                                                    "Lite record: embedding not found, skipping"
                                                );
                                                continue;
                                            }
                                        }
                                    } else {
                                        tracing::warn!(
                                            target: "strata::vector",
                                            vector_id = vec_record.vector_id,
                                            "Lite record without old backend embedding, skipping"
                                        );
                                        continue;
                                    }
                                }
                            }
                        }
                    };

                    // Allocate a fresh VectorId to avoid collisions between
                    // independently-created branches that share the same ID space.
                    let new_vid = backend.allocate_id();
                    let _ =
                        backend.insert_with_timestamp(new_vid, &embedding, vec_record.created_at);

                    // If the VectorId changed, queue a KV update so that
                    // get() can resolve the correct backend entry.
                    if new_vid.as_u64() != vec_record.vector_id {
                        vec_record.vector_id = new_vid.as_u64();
                        match vec_record.to_bytes() {
                            Ok(updated_bytes) => {
                                kv_updates.push((vec_key.clone(), updated_bytes));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "strata::vector",
                                    error = %e,
                                    "Failed to serialize remapped VectorRecord, skipping collection"
                                );
                                kv_remap_failed = true;
                                break;
                            }
                        }
                    }

                    collection_vector_count += 1;
                }

                // If serialization failed mid-way, skip this collection entirely.
                // The old backend (if any) remains; vectors will be properly loaded
                // on next full recovery.
                if kv_remap_failed {
                    continue;
                }

                // Batch-write remapped VectorIds back to KV.
                // CRITICAL: If this fails, do NOT install the backend — the
                // VectorIds in KV and backend would be mismatched, causing
                // get() to return wrong embeddings silently.
                if !kv_updates.is_empty() {
                    if let Err(e) = self.db.transaction(branch_id, |txn| {
                        for (k, bytes) in &kv_updates {
                            txn.put(k.clone(), Value::Bytes(bytes.clone()))?;
                        }
                        Ok(())
                    }) {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to update VectorIds in KV after merge reload, \
                             skipping collection to prevent data inconsistency"
                        );
                        continue;
                    }
                }

                // Rebuild index (segments for SegmentedHnsw, graph for HNSW)
                backend.rebuild_index();

                // Replace existing backend atomically
                state.backends.insert(collection_id, backend);

                total_collections += 1;
                total_vectors += collection_vector_count;
            }
        }

        if total_collections > 0 {
            info!(
                target: "strata::vector",
                branch_id = %branch_id,
                total_collections,
                total_vectors,
                "Post-merge vector reload complete"
            );
        }

        Ok(())
    }
}

// ========== Searchable Trait Implementation ==========

impl crate::search::Searchable for VectorStore {
    /// Vector search via search interface
    ///
    /// NOTE: Per architecture documentation:
    /// - For SearchMode::Keyword, Vector returns empty results
    /// - Vector does not attempt to do keyword matching on metadata
    /// - For SearchMode::Vector or SearchMode::Hybrid, the caller must
    ///   provide the query embedding via VectorSearchRequest extension
    ///
    /// The hybrid search orchestrator is responsible for:
    /// 1. Embedding the text query (using an external model)
    /// 2. Calling `vector.search_by_embedding()` with the embedding
    /// 3. Fusing results via RRF
    fn search(
        &self,
        req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        use crate::search::{SearchMode, SearchResponse, SearchStats};
        use std::time::Instant;

        let start = Instant::now();

        // Vector primitive only responds to Vector or Hybrid mode
        // with an explicit query embedding provided externally.
        //
        // For Keyword mode, return empty - hybrid orchestrator handles this.
        // For Vector/Hybrid mode without embedding, return empty -
        // the hybrid orchestrator should call search_by_embedding() directly.
        match req.mode {
            SearchMode::Keyword => {
                // Vector does NOT do keyword search on metadata
                Ok(SearchResponse::new(
                    vec![],
                    false,
                    SearchStats::new(start.elapsed().as_micros() as u64, 0),
                ))
            }
            SearchMode::Vector | SearchMode::Hybrid => {
                // Requires query embedding - the orchestrator must call
                // search_by_embedding() or search_response() directly
                // with the actual embedding vector.
                Ok(SearchResponse::new(
                    vec![],
                    false,
                    SearchStats::new(start.elapsed().as_micros() as u64, 0),
                ))
            }
        }
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Vector
    }
}

/// Get current time in microseconds since Unix epoch
///
/// Returns 0 if system clock is before Unix epoch (clock went backwards).
fn now_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

// =============================================================================
// VectorStoreExt Implementation
// =============================================================================
//
// Extension trait implementation for cross-primitive transactions.
//
// LIMITATION: VectorStore operations in transactions have limited support
// because embeddings are stored in in-memory backends (VectorHeap/HNSW)
// which are not accessible through TransactionContext. Full vector operations
// require access to the VectorBackendState which is Database-scoped.
//
// Future enhancement: Could add pending_vector_ops to TransactionContext
// and apply them at commit time, but this requires significant infrastructure.

impl VectorStoreExt for TransactionContext {
    fn vector_get(
        &mut self,
        collection: &str,
        key: &str,
    ) -> strata_core::StrataResult<Option<Vec<f32>>> {
        // VectorStore embeddings are stored in VectorHeap (in-memory backend),
        // which is not accessible from TransactionContext.
        //
        // The VectorRecord in KV storage only contains vector_id (index into heap),
        // not the actual embedding data.
        //
        // To properly support this, TransactionContext would need access to
        // Database::extension::<VectorBackendState>().
        let _ = (collection, key); // Mark as intentionally unused
        Err(strata_core::StrataError::invalid_input(
            "VectorStore get operations are not supported in cross-primitive transactions. \
             Embeddings are stored in in-memory backends not accessible from TransactionContext. \
             Use VectorStore::get() directly outside of transactions."
                .to_string(),
        ))
    }

    fn vector_insert(
        &mut self,
        collection: &str,
        key: &str,
        embedding: &[f32],
    ) -> strata_core::StrataResult<Version> {
        // VectorStore inserts require:
        // 1. Adding embedding to VectorHeap (in-memory)
        // 2. Getting a VectorId from the backend's allocator
        // 3. Creating/updating VectorRecord in KV storage
        // 4. Updating the search index
        //
        // Steps 1, 2, and 4 require access to VectorBackendState which is
        // not available from TransactionContext.
        let _ = (collection, key, embedding); // Mark as intentionally unused
        Err(strata_core::StrataError::invalid_input(
            "VectorStore insert operations are not supported in cross-primitive transactions. \
             Vector operations require access to in-memory backends not accessible from \
             TransactionContext. Use VectorStore::insert() directly outside of transactions."
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::vector::{DistanceMetric, VectorConfig};
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, VectorStore) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let store = VectorStore::new(db.clone());
        (temp_dir, db, store)
    }

    // ========================================
    // Collection Lifecycle Tests (#347, #348)
    // ========================================

    #[test]
    fn test_create_collection() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();
        let versioned = store
            .create_collection(branch_id, "default", "test", config.clone())
            .unwrap();
        let info = versioned.value;

        assert_eq!(info.name, "test");
        assert_eq!(info.count, 0);
        assert_eq!(info.config.dimension, 384);
        assert_eq!(info.config.metric, DistanceMetric::Cosine);
    }

    #[test]
    fn test_collection_already_exists() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();
        store
            .create_collection(branch_id, "default", "test", config.clone())
            .unwrap();

        // Second create should fail
        let result = store.create_collection(branch_id, "default", "test", config);
        assert!(matches!(
            result,
            Err(VectorError::CollectionAlreadyExists { .. })
        ));
    }

    #[test]
    fn test_delete_collection() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();
        store
            .create_collection(branch_id, "default", "test", config.clone())
            .unwrap();

        // Delete should succeed
        store
            .delete_collection(branch_id, "default", "test")
            .unwrap();

        // Collection should no longer exist
        assert!(!store
            .collection_exists(branch_id, "default", "test")
            .unwrap());
    }

    #[test]
    fn test_delete_collection_not_found() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let result = store.delete_collection(branch_id, "default", "nonexistent");
        assert!(matches!(
            result,
            Err(VectorError::CollectionNotFound { .. })
        ));
    }

    // ========================================
    // Collection Discovery Tests (#349)
    // ========================================

    #[test]
    fn test_list_collections() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Create multiple collections
        store
            .create_collection(branch_id, "default", "zeta", VectorConfig::for_minilm())
            .unwrap();
        store
            .create_collection(branch_id, "default", "alpha", VectorConfig::for_mpnet())
            .unwrap();
        store
            .create_collection(branch_id, "default", "beta", VectorConfig::for_openai_ada())
            .unwrap();

        let collections = store.list_collections(branch_id, "default").unwrap();

        // Should be sorted by name
        assert_eq!(collections.len(), 3);
        assert_eq!(collections[0].name, "alpha");
        assert_eq!(collections[1].name, "beta");
        assert_eq!(collections[2].name, "zeta");
    }

    #[test]
    fn test_list_collections_empty() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let collections = store.list_collections(branch_id, "default").unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn test_get_collection() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(768, DistanceMetric::Euclidean).unwrap();
        store
            .create_collection(branch_id, "default", "embeddings", config)
            .unwrap();

        let info = store
            .get_collection(branch_id, "default", "embeddings")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(info.name, "embeddings");
        assert_eq!(info.config.dimension, 768);
        assert_eq!(info.config.metric, DistanceMetric::Euclidean);
    }

    #[test]
    fn test_get_collection_not_found() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let info = store
            .get_collection(branch_id, "default", "nonexistent")
            .unwrap();
        assert!(info.is_none());
    }

    // ========================================
    // Branch Isolation Tests (Rule #2)
    // ========================================

    #[test]
    fn test_branch_isolation() {
        let (_temp, _db, store) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        let config = VectorConfig::for_minilm();

        // Create same-named collection in different branches
        store
            .create_collection(branch1, "default", "shared_name", config.clone())
            .unwrap();
        store
            .create_collection(branch2, "default", "shared_name", config)
            .unwrap();

        // Each branch sees only its own collection
        let list1 = store.list_collections(branch1, "default").unwrap();
        let list2 = store.list_collections(branch2, "default").unwrap();

        assert_eq!(list1.len(), 1);
        assert_eq!(list2.len(), 1);

        // Deleting from one branch doesn't affect the other
        store
            .delete_collection(branch1, "default", "shared_name")
            .unwrap();
        assert!(store
            .get_collection(branch2, "default", "shared_name")
            .unwrap()
            .is_some());
    }

    // ========================================
    // Config Persistence Tests (#350)
    // ========================================

    #[test]
    fn test_collection_config_roundtrip() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(768, DistanceMetric::Euclidean).unwrap();
        store
            .create_collection(branch_id, "default", "test", config.clone())
            .unwrap();

        // Get collection and verify config
        let info = store
            .get_collection(branch_id, "default", "test")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(info.config.dimension, config.dimension);
        assert_eq!(info.config.metric, config.metric);
    }

    #[test]
    fn test_collection_survives_reload() {
        let temp_dir = TempDir::new().unwrap();
        let branch_id = BranchId::new();

        // Create collection
        {
            let db = Database::open(temp_dir.path()).unwrap();
            let store = VectorStore::new(db);

            let config = VectorConfig::new(512, DistanceMetric::DotProduct).unwrap();
            store
                .create_collection(branch_id, "default", "persistent", config)
                .unwrap();
        }

        // Reopen database and verify collection exists
        {
            let db = Database::open(temp_dir.path()).unwrap();
            let store = VectorStore::new(db);

            let info = store
                .get_collection(branch_id, "default", "persistent")
                .unwrap()
                .unwrap()
                .value;
            assert_eq!(info.config.dimension, 512);
            assert_eq!(info.config.metric, DistanceMetric::DotProduct);
        }
    }

    // ========================================
    // Validation Tests
    // ========================================

    #[test]
    fn test_invalid_collection_name() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();

        // Empty name
        let result = store.create_collection(branch_id, "default", "", config.clone());
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { .. })
        ));

        // Reserved name
        let result = store.create_collection(branch_id, "default", "_reserved", config.clone());
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { .. })
        ));

        // Contains slash
        let result = store.create_collection(branch_id, "default", "has/slash", config);
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { .. })
        ));
    }

    #[test]
    fn test_invalid_dimension() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Dimension 0 should fail
        let config = VectorConfig {
            dimension: 0,
            metric: DistanceMetric::Cosine,
            storage_dtype: crate::primitives::vector::StorageDtype::F32,
        };

        let result = store.create_collection(branch_id, "default", "test", config);
        assert!(matches!(
            result,
            Err(VectorError::InvalidDimension { dimension: 0 })
        ));
    }

    // ========================================
    // Thread Safety Tests
    // ========================================

    #[test]
    fn test_vector_store_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<VectorStore>();
    }

    #[test]
    fn test_vector_store_clone() {
        let (_temp, _db, store1) = setup();
        let store2 = store1.clone();

        // Both point to same database
        assert!(Arc::ptr_eq(store1.database(), store2.database()));
    }

    // ========================================
    // Vector Insert/Get/Delete Tests
    // ========================================

    #[test]
    fn test_insert_and_get_vector() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Create collection
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert vector
        let embedding = vec![1.0, 0.0, 0.0];
        store
            .insert(branch_id, "default", "test", "doc1", &embedding, None)
            .unwrap();

        // Get vector
        let entry = store
            .get(branch_id, "default", "test", "doc1")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(entry.key, "doc1");
        assert_eq!(entry.embedding, embedding);
        assert!(entry.metadata.is_none());
    }

    #[test]
    fn test_insert_with_metadata() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        let metadata = serde_json::json!({"type": "document", "author": "test"});
        store
            .insert(
                branch_id,
                "default",
                "test",
                "doc1",
                &[1.0, 0.0, 0.0],
                Some(metadata.clone()),
            )
            .unwrap();

        let entry = store
            .get(branch_id, "default", "test", "doc1")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(entry.metadata, Some(metadata));
    }

    /// Regression test: verify embeddings are stored in KV records so they
    /// survive recovery. Previously, `new_lite()` stored empty embeddings in KV
    /// which caused vectors to be silently dropped during recovery.
    #[test]
    fn test_embedding_persisted_in_kv_record() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert a vector
        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Read the raw KV record and verify the embedding is present
        let kv_key = Key::new_vector(store.namespace_for(branch_id, "default"), "test", "doc1");
        let record = store.get_vector_record_by_key(&kv_key).unwrap().unwrap();
        assert_eq!(
            record.embedding,
            vec![1.0, 0.0, 0.0],
            "Embedding must be stored in KV record for recovery"
        );

        // Upsert with new embedding and verify KV record is updated
        store
            .insert(branch_id, "default", "test", "doc1", &[0.0, 1.0, 0.0], None)
            .unwrap();

        let record = store.get_vector_record_by_key(&kv_key).unwrap().unwrap();
        assert_eq!(
            record.embedding,
            vec![0.0, 1.0, 0.0],
            "Updated embedding must be stored in KV record for recovery"
        );
    }

    #[test]
    fn test_upsert_overwrites() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert original
        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Upsert with new embedding
        store
            .insert(branch_id, "default", "test", "doc1", &[0.0, 1.0, 0.0], None)
            .unwrap();

        let entry = store
            .get(branch_id, "default", "test", "doc1")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(entry.embedding, vec![0.0, 1.0, 0.0]);
    }

    #[test]
    fn test_delete_vector() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Delete
        let deleted = store.delete(branch_id, "default", "test", "doc1").unwrap();
        assert!(deleted);

        // Should not exist
        let entry = store.get(branch_id, "default", "test", "doc1").unwrap();
        assert!(entry.is_none());

        // Delete again returns false
        let deleted = store.delete(branch_id, "default", "test", "doc1").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_delete_removes_from_kv_and_backend() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert two vectors
        store
            .insert(branch_id, "default", "test", "v1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "v2", &[0.0, 1.0, 0.0], None)
            .unwrap();

        // Verify both are searchable
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert_eq!(results.len(), 2);

        // Delete v1
        let deleted = store.delete(branch_id, "default", "test", "v1").unwrap();
        assert!(deleted);

        // KV record must be gone
        let entry = store.get(branch_id, "default", "test", "v1").unwrap();
        assert!(entry.is_none(), "KV record should be deleted");

        // Backend must exclude deleted vector from search
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "v2");

        // Re-insert with same key should work (slot is reusable)
        store
            .insert(branch_id, "default", "test", "v1", &[0.0, 0.0, 1.0], None)
            .unwrap();
        let entry = store
            .get(branch_id, "default", "test", "v1")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(entry.embedding, vec![0.0, 0.0, 1.0]);
    }

    #[test]
    fn test_dimension_mismatch() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Wrong dimension
        let result = store.insert(branch_id, "default", "test", "doc1", &[1.0, 0.0], None);
        assert!(matches!(
            result,
            Err(VectorError::DimensionMismatch {
                expected: 3,
                got: 2
            })
        ));
    }

    // ========================================
    // Vector Search Tests
    // ========================================

    #[test]
    fn test_search_basic() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert vectors
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "b", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "c", &[0.9, 0.1, 0.0], None)
            .unwrap();

        // Search
        let query = [1.0, 0.0, 0.0];
        let results = store
            .search(branch_id, "default", "test", &query, 2, None)
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "a"); // Most similar
        assert_eq!(results[1].key, "c"); // Second most similar
    }

    #[test]
    fn test_search_k_zero() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();

        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 0, None)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_with_filter() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(
                branch_id,
                "default",
                "test",
                "a",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"type": "document"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "b",
                &[0.9, 0.1, 0.0],
                Some(serde_json::json!({"type": "image"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "c",
                &[0.8, 0.2, 0.0],
                Some(serde_json::json!({"type": "document"})),
            )
            .unwrap();

        // Filter by type
        let filter = MetadataFilter::new().eq("type", "document");
        let results = store
            .search(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                10,
                Some(filter),
            )
            .unwrap();

        assert_eq!(results.len(), 2);
        for result in &results {
            let meta = result.metadata.as_ref().unwrap();
            assert_eq!(meta.get("type").unwrap().as_str().unwrap(), "document");
        }
    }

    #[test]
    fn test_search_deterministic_order() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert vectors with same similarity
        store
            .insert(branch_id, "default", "test", "b", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "c", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Search multiple times - order should be consistent
        for _ in 0..5 {
            let results = store
                .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 3, None)
                .unwrap();

            // Should be sorted by key (tie-breaker) since scores are equal
            assert_eq!(results[0].key, "a");
            assert_eq!(results[1].key, "b");
            assert_eq!(results[2].key, "c");
        }
    }

    // ========================================
    // WAL Replay Tests
    // ========================================

    #[test]
    fn test_replay_create_collection() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Replay collection creation
        store
            .replay_create_collection(branch_id, "test", config)
            .unwrap();

        // Backend should be created
        let collection_id = CollectionId::new(branch_id, "test");
        assert!(store
            .backends()
            .unwrap()
            .backends
            .contains_key(&collection_id));
    }

    #[test]
    fn test_replay_delete_collection() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .replay_create_collection(branch_id, "test", config)
            .unwrap();

        let collection_id = CollectionId::new(branch_id, "test");
        assert!(store
            .backends()
            .unwrap()
            .backends
            .contains_key(&collection_id));

        // Replay deletion
        store.replay_delete_collection(branch_id, "test").unwrap();

        assert!(!store
            .backends()
            .unwrap()
            .backends
            .contains_key(&collection_id));
    }

    #[test]
    fn test_replay_upsert() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .replay_create_collection(branch_id, "test", config)
            .unwrap();

        // Replay upsert with specific VectorId
        let vector_id = VectorId::new(42);
        store
            .replay_upsert(
                branch_id,
                "test",
                "doc1",
                vector_id,
                &[1.0, 0.0, 0.0],
                None,
                None,
                1000,
            )
            .unwrap();

        // Verify vector exists in backend
        let collection_id = CollectionId::new(branch_id, "test");
        let state = store.backends().unwrap();
        let backend = state.backends.get(&collection_id).unwrap();
        assert!(backend.contains(vector_id));
        assert_eq!(backend.len(), 1);
    }

    #[test]
    fn test_replay_upsert_maintains_id_monotonicity() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .replay_create_collection(branch_id, "test", config)
            .unwrap();

        // Replay upsert with high VectorId
        let high_id = VectorId::new(1000);
        store
            .replay_upsert(
                branch_id,
                "test",
                "doc",
                high_id,
                &[1.0, 0.0, 0.0],
                None,
                None,
                1000,
            )
            .unwrap();

        // Verify the vector exists
        let collection_id = CollectionId::new(branch_id, "test");
        let state = store.backends().unwrap();
        let backend = state.backends.get(&collection_id).unwrap();
        assert!(backend.contains(high_id));
    }

    #[test]
    fn test_replay_delete() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .replay_create_collection(branch_id, "test", config)
            .unwrap();

        // Replay upsert
        let vector_id = VectorId::new(1);
        store
            .replay_upsert(
                branch_id,
                "test",
                "doc",
                vector_id,
                &[1.0, 0.0, 0.0],
                None,
                None,
                1000,
            )
            .unwrap();

        let collection_id = CollectionId::new(branch_id, "test");
        {
            let state = store.backends().unwrap();
            assert!(state
                .backends
                .get(&collection_id)
                .unwrap()
                .contains(vector_id));
        }

        // Replay deletion
        store
            .replay_delete(branch_id, "test", "doc", vector_id, 2000)
            .unwrap();

        {
            let state = store.backends().unwrap();
            assert!(!state
                .backends
                .get(&collection_id)
                .unwrap()
                .contains(vector_id));
        }
    }

    #[test]
    fn test_replay_delete_missing_collection() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Replay delete on non-existent collection should succeed (idempotent)
        let result = store.replay_delete(branch_id, "nonexistent", "doc", VectorId::new(1), 1000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_replay_sequence() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Replay a sequence of operations
        store
            .replay_create_collection(branch_id, "col1", config.clone())
            .unwrap();

        store
            .replay_upsert(
                branch_id,
                "col1",
                "v1",
                VectorId::new(1),
                &[1.0, 0.0, 0.0],
                None,
                None,
                1000,
            )
            .unwrap();

        store
            .replay_upsert(
                branch_id,
                "col1",
                "v2",
                VectorId::new(2),
                &[0.0, 1.0, 0.0],
                None,
                None,
                2000,
            )
            .unwrap();

        store
            .replay_delete(branch_id, "col1", "v1", VectorId::new(1), 3000)
            .unwrap();

        // Verify final state
        let collection_id = CollectionId::new(branch_id, "col1");
        let state = store.backends().unwrap();
        let backend = state.backends.get(&collection_id).unwrap();

        assert!(!backend.contains(VectorId::new(1)));
        assert!(backend.contains(VectorId::new(2)));
        assert_eq!(backend.len(), 1);
    }

    #[test]
    fn test_backends_accessor() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Use backends accessor
        let state = store.backends().unwrap();
        assert_eq!(state.backends.len(), 1);
    }

    // ========================================
    // Post-Merge Reload Tests (Phase 2)
    // ========================================

    #[test]
    fn test_post_merge_reload_from_kv() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Create collection and insert vectors via normal API
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "b", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "c", &[0.0, 0.0, 1.0], None)
            .unwrap();

        let collection_id = CollectionId::new(branch_id, "test");

        // Call post_merge_reload — the old backend will be extracted
        // per-collection and used to resolve lite record embeddings.
        store.post_merge_reload_vectors(branch_id).unwrap();

        // Verify backends are restored
        {
            let state = store.state().unwrap();
            let backend = state.backends.get(&collection_id).unwrap();
            assert_eq!(backend.len(), 3, "Should have 3 vectors after reload");
        }

        // Verify search works correctly
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 3, None)
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].key, "a"); // exact match
    }

    #[test]
    fn test_post_merge_reload_empty_branch() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Reload on a branch with no vector collections should be a no-op
        let result = store.post_merge_reload_vectors(branch_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_post_merge_reload_replaces_stale_backend() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert 2 vectors
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "b", &[0.0, 1.0, 0.0], None)
            .unwrap();

        // Verify 2 vectors in backend
        let collection_id = CollectionId::new(branch_id, "test");
        {
            let state = store.state().unwrap();
            assert_eq!(state.backends.get(&collection_id).unwrap().len(), 2);
        }

        // Delete one vector (this updates both KV and backend)
        store.delete(branch_id, "default", "test", "a").unwrap();

        // Reload should rebuild from KV (which now has only "b")
        store.post_merge_reload_vectors(branch_id).unwrap();

        // Backend should reflect KV state: only "b"
        {
            let state = store.state().unwrap();
            assert_eq!(
                state.backends.get(&collection_id).unwrap().len(),
                1,
                "Should have 1 vector after reload (deleted vector not in KV)"
            );
        }

        // get() must still work for the surviving vector
        let entry = store
            .get(branch_id, "default", "test", "b")
            .unwrap()
            .expect("b should still exist")
            .value;
        assert_eq!(entry.embedding, vec![0.0, 1.0, 0.0]);

        // get() must return None for the deleted vector
        assert!(store
            .get(branch_id, "default", "test", "a")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_post_merge_reload_get_returns_correct_embeddings() {
        // Verifies VectorId remapping correctness: after reload, each
        // vector key must resolve to its own embedding, not another's.
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(
                branch_id,
                "default",
                "test",
                "x",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"label": "x"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "y",
                &[0.0, 1.0, 0.0],
                Some(serde_json::json!({"label": "y"})),
            )
            .unwrap();
        store
            .insert(branch_id, "default", "test", "z", &[0.0, 0.0, 1.0], None)
            .unwrap();

        // Reload
        store.post_merge_reload_vectors(branch_id).unwrap();

        // Verify each vector's embedding and metadata are correct
        let x = store
            .get(branch_id, "default", "test", "x")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(x.embedding, vec![1.0, 0.0, 0.0]);
        assert_eq!(
            x.metadata,
            Some(serde_json::json!({"label": "x"})),
            "x metadata must survive reload"
        );

        let y = store
            .get(branch_id, "default", "test", "y")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(y.embedding, vec![0.0, 1.0, 0.0]);
        assert_eq!(
            y.metadata,
            Some(serde_json::json!({"label": "y"})),
            "y metadata must survive reload"
        );

        let z = store
            .get(branch_id, "default", "test", "z")
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(z.embedding, vec![0.0, 0.0, 1.0]);
        assert!(z.metadata.is_none());
    }

    // ========================================
    // InlineMeta Tests
    // ========================================

    #[test]
    fn test_inline_meta_populated_on_insert() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(
                branch_id,
                "default",
                "test",
                "vec_a",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "vec_b",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();

        // Search should return keys from inline meta (no KV fallback needed)
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "vec_a"); // Most similar
        assert_eq!(results[1].key, "vec_b");
    }

    #[test]
    fn test_inline_meta_populated_on_batch_insert() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        let entries = vec![
            ("alpha".to_string(), vec![1.0, 0.0, 0.0], None),
            ("beta".to_string(), vec![0.0, 1.0, 0.0], None),
            ("gamma".to_string(), vec![0.0, 0.0, 1.0], None),
        ];
        store
            .batch_insert(branch_id, "default", "test", entries)
            .unwrap();

        // Verify inline meta works by searching
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 3, None)
            .unwrap();
        assert_eq!(results.len(), 3);
        // Most similar to [1,0,0] should be "alpha"
        assert_eq!(results[0].key, "alpha");

        // Verify all three keys appear
        let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"alpha"));
        assert!(keys.contains(&"beta"));
        assert!(keys.contains(&"gamma"));
    }

    #[test]
    fn test_inline_meta_updated_on_upsert() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert
        store
            .insert(
                branch_id,
                "default",
                "test",
                "my_key",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        // Upsert with new embedding (same key, different direction)
        store
            .insert(
                branch_id,
                "default",
                "test",
                "my_key",
                &[0.0, 0.0, 1.0],
                None,
            )
            .unwrap();

        // Search for new direction should still find "my_key"
        let results = store
            .search(branch_id, "default", "test", &[0.0, 0.0, 1.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "my_key");
    }

    #[test]
    fn test_inline_meta_removed_on_delete() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(
                branch_id,
                "default",
                "test",
                "to_delete",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "to_keep",
                &[0.9, 0.1, 0.0],
                None,
            )
            .unwrap();

        // Delete first vector
        let deleted = store
            .delete(branch_id, "default", "test", "to_delete")
            .unwrap();
        assert!(deleted);

        // Search should only return the surviving vector
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "to_keep");
    }

    #[test]
    fn test_inline_meta_with_source_ref() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_test", config)
            .unwrap();

        let source = EntityRef::Kv {
            branch_id,
            key: "original_key".to_string(),
        };
        store
            .system_insert_with_source(
                branch_id,
                "_system_test",
                "shadow_key",
                &[1.0, 0.0, 0.0],
                None,
                source.clone(),
            )
            .unwrap();

        // system_search_with_sources should return the source_ref from inline meta
        let results = store
            .system_search_with_sources(branch_id, "_system_test", &[1.0, 0.0, 0.0], 1)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "shadow_key");
        assert!(results[0].source_ref.is_some());
        // Verify it's the right EntityRef variant with the right key
        match results[0].source_ref.as_ref().unwrap() {
            EntityRef::Kv { key, .. } => assert_eq!(key, "original_key"),
            other => panic!("Expected EntityRef::Kv, got {:?}", other),
        }
    }

    #[test]
    fn test_system_collection_len_nonexistent() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Non-existent collection should return 0
        let len = store
            .system_collection_len(branch_id, "_system_missing")
            .unwrap();
        assert_eq!(len, 0);
    }

    #[test]
    fn test_system_collection_len_with_vectors() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_test", config)
            .unwrap();

        assert_eq!(
            store
                .system_collection_len(branch_id, "_system_test")
                .unwrap(),
            0
        );

        store
            .system_insert(branch_id, "_system_test", "k1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        assert_eq!(
            store
                .system_collection_len(branch_id, "_system_test")
                .unwrap(),
            1
        );

        store
            .system_insert(branch_id, "_system_test", "k2", &[0.0, 1.0, 0.0], None)
            .unwrap();
        assert_eq!(
            store
                .system_collection_len(branch_id, "_system_test")
                .unwrap(),
            2
        );
    }

    // ========================================
    // V-22: NaN/Infinity query validation
    // ========================================

    #[test]
    fn test_search_nan_query_returns_error() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();
        store
            .create_collection(branch_id, "default", "test_nan", config)
            .unwrap();

        // Insert a valid vector
        store
            .insert(
                branch_id,
                "default",
                "test_nan",
                "k1",
                &vec![1.0; 384],
                None,
            )
            .unwrap();

        // Search with NaN at first position should fail
        let mut nan_query = vec![0.0; 384];
        nan_query[0] = f32::NAN;
        let result = store.search(branch_id, "default", "test_nan", &nan_query, 5, None);
        let err = result.unwrap_err();
        assert!(
            matches!(err, VectorError::InvalidEmbedding { .. }),
            "expected InvalidEmbedding for NaN at [0], got: {err}"
        );

        // Search with NaN at last position — verify full scan
        let mut nan_last = vec![1.0; 384];
        nan_last[383] = f32::NAN;
        let result = store.search(branch_id, "default", "test_nan", &nan_last, 5, None);
        let err = result.unwrap_err();
        assert!(
            matches!(err, VectorError::InvalidEmbedding { .. }),
            "expected InvalidEmbedding for NaN at [383], got: {err}"
        );

        // Search with Infinity should also fail
        let mut inf_query = vec![0.0; 384];
        inf_query[0] = f32::INFINITY;
        let result = store.search(branch_id, "default", "test_nan", &inf_query, 5, None);
        let err = result.unwrap_err();
        assert!(
            matches!(err, VectorError::InvalidEmbedding { .. }),
            "expected InvalidEmbedding for +Inf, got: {err}"
        );

        // Search with NEG_INFINITY should also fail
        let mut neg_inf_query = vec![0.0; 384];
        neg_inf_query[0] = f32::NEG_INFINITY;
        let result = store.search(branch_id, "default", "test_nan", &neg_inf_query, 5, None);
        let err = result.unwrap_err();
        assert!(
            matches!(err, VectorError::InvalidEmbedding { .. }),
            "expected InvalidEmbedding for -Inf, got: {err}"
        );

        // All-NaN vector should fail
        let all_nan = vec![f32::NAN; 384];
        let result = store.search(branch_id, "default", "test_nan", &all_nan, 5, None);
        assert!(
            matches!(result, Err(VectorError::InvalidEmbedding { .. })),
            "expected InvalidEmbedding for all-NaN"
        );

        // Valid search should still work
        let valid_query = vec![1.0; 384];
        let result = store.search(branch_id, "default", "test_nan", &valid_query, 5, None);
        assert!(result.is_ok());

        // k=0 with NaN should return empty (short-circuit before validation)
        let result = store.search(branch_id, "default", "test_nan", &nan_query, 0, None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // ========================================
    // V-16: Concurrent ensure_collection_loaded
    // ========================================

    #[test]
    fn test_concurrent_ensure_collection_loaded() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();
        store
            .create_collection(branch_id, "default", "concurrent_test", config)
            .unwrap();

        // Insert a vector so the collection has data
        store
            .insert(
                branch_id,
                "default",
                "concurrent_test",
                "k1",
                &vec![1.0; 384],
                None,
            )
            .unwrap();

        // Evict from memory to force re-loading
        {
            let state = store.state().unwrap();
            let collection_id = CollectionId::new(branch_id, "concurrent_test");
            state.backends.remove(&collection_id);
        }

        // Spawn multiple threads calling ensure_collection_loaded simultaneously
        let store = Arc::new(store);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    store.ensure_collection_loaded(branch_id, "default", "concurrent_test")
                })
            })
            .collect();

        // All threads should succeed without panic
        for handle in handles {
            let result = handle.join().expect("thread should not panic");
            assert!(
                result.is_ok(),
                "ensure_collection_loaded failed: {:?}",
                result.err()
            );
        }

        // Verify exactly one backend exists
        let state = store.state().unwrap();
        let collection_id = CollectionId::new(branch_id, "concurrent_test");
        assert!(state.backends.contains_key(&collection_id));
    }

    // ========================================
    // V-21: Adaptive temporal search over-fetch
    // ========================================

    #[test]
    fn test_search_at_adaptive_overfetch() {
        // Test that the adaptive [3, 6, 12] retry finds matching vectors even
        // when they're ranked far from the query (beyond what a single 3x fetch
        // would return). We place 20 "common" vectors very close to the query
        // and 2 "rare" vectors far away. With k=2, the first pass fetches 6
        // candidates (all common). The retry at 12x (24, capped to 22) fetches
        // the whole collection and finds both rare vectors.
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // 20 common vectors tightly clustered near query [1, 0, 0]
        for i in 0..20 {
            let noise = (i as f32) * 0.001;
            store
                .insert(
                    branch_id,
                    "default",
                    "test",
                    &format!("common_{i:02}"),
                    &[1.0 - noise, noise, 0.0],
                    Some(serde_json::json!({"type": "common"})),
                )
                .unwrap();
        }

        // 2 rare vectors far from query (nearly orthogonal)
        store
            .insert(
                branch_id,
                "default",
                "test",
                "rare_a",
                &[0.0, 1.0, 0.0],
                Some(serde_json::json!({"type": "rare"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "rare_b",
                &[0.0, 0.0, 1.0],
                Some(serde_json::json!({"type": "rare"})),
            )
            .unwrap();

        // k=2: first pass 3x=6 fetches top-6 (all common), second pass 6x=12
        // fetches top-12 (still all common most likely), third pass 12x=24
        // capped to 22 fetches everything — finds the 2 rare vectors.
        let filter = MetadataFilter::new().eq("type", "rare");
        let results = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                2,
                Some(filter),
                u64::MAX,
            )
            .unwrap();

        assert_eq!(results.len(), 2);
        for result in &results {
            let meta = result.metadata.as_ref().unwrap();
            assert_eq!(meta.get("type").unwrap().as_str().unwrap(), "rare");
        }
        // Also verify scores: rare_a [0,1,0] has higher cosine with [1,0,0]
        // than rare_b [0,0,1] (both are 0.0 for cosine of orthogonal vectors,
        // but tie-breaking by key: "rare_a" < "rare_b")
        assert_eq!(results[0].key, "rare_a");
        assert_eq!(results[1].key, "rare_b");
    }

    #[test]
    fn test_search_at_adaptive_overfetch_filter_with_temporal() {
        // Exercise the combined filter + temporal path: insert vectors at
        // different wall-clock times, search at a timestamp that excludes
        // later vectors, with a metadata filter active.
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert first batch (will have earlier created_at timestamps)
        store
            .insert(
                branch_id,
                "default",
                "test",
                "early_match",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"category": "target"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "early_other",
                &[0.9, 0.1, 0.0],
                Some(serde_json::json!({"category": "other"})),
            )
            .unwrap();

        // Capture a timestamp between early and late inserts
        let mid_ts = now_micros();

        // Small sleep to ensure late inserts get strictly later timestamps
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Insert second batch (will have later created_at timestamps)
        store
            .insert(
                branch_id,
                "default",
                "test",
                "late_match",
                &[0.95, 0.05, 0.0],
                Some(serde_json::json!({"category": "target"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "late_other",
                &[0.8, 0.2, 0.0],
                Some(serde_json::json!({"category": "other"})),
            )
            .unwrap();

        // Search at mid_ts with filter: should only find "early_match"
        // (late_match exists but was inserted after mid_ts)
        let filter = MetadataFilter::new().eq("category", "target");
        let results = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                10,
                Some(filter),
                mid_ts,
            )
            .unwrap();

        assert_eq!(
            results.len(),
            1,
            "only early_match should be visible at mid_ts"
        );
        assert_eq!(results[0].key, "early_match");

        // Verify that searching at u64::MAX returns both matches
        let filter2 = MetadataFilter::new().eq("category", "target");
        let results_all = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                10,
                Some(filter2),
                u64::MAX,
            )
            .unwrap();
        assert_eq!(results_all.len(), 2);
    }

    #[test]
    fn test_search_at_k_zero_returns_empty() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // k=0 with no filter
        let results = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                0,
                None,
                u64::MAX,
            )
            .unwrap();
        assert!(results.is_empty());

        // k=0 with filter (should also short-circuit before filter evaluation)
        let filter = MetadataFilter::new().eq("type", "anything");
        let results = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                0,
                Some(filter),
                u64::MAX,
            )
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_at_tie_breaking() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert vectors with identical embeddings — same score when queried.
        // Insert in non-alphabetical order to verify sort isn't just insertion order.
        store
            .insert(branch_id, "default", "test", "c", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "a", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "b", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // No-filter path: R5 tie-breaking (score desc, key asc)
        let results = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                3,
                None,
                u64::MAX,
            )
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].key, "a");
        assert_eq!(results[1].key, "b");
        assert_eq!(results[2].key, "c");
    }

    #[test]
    fn test_search_at_tie_breaking_with_filter() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert vectors with identical embeddings, all matching the filter.
        // This tests R5 tie-breaking through the filter path (adaptive over-fetch).
        store
            .insert(
                branch_id,
                "default",
                "test",
                "z",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"group": "A"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "m",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"group": "A"})),
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "test",
                "a",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"group": "A"})),
            )
            .unwrap();

        let filter = MetadataFilter::new().eq("group", "A");
        let results = store
            .search_at(
                branch_id,
                "default",
                "test",
                &[1.0, 0.0, 0.0],
                3,
                Some(filter),
                u64::MAX,
            )
            .unwrap();

        assert_eq!(results.len(), 3);
        // R5: score desc (all equal), key asc — even through the filter path
        assert_eq!(results[0].key, "a");
        assert_eq!(results[1].key, "m");
        assert_eq!(results[2].key, "z");
    }
}
