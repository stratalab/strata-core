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

use crate::collection::{validate_collection_name, validate_vector_key};
use crate::{
    CollectionId, CollectionInfo, CollectionRecord, IndexBackendFactory, MetadataFilter,
    VectorConfig, VectorEntry, VectorError, VectorId, VectorIndexBackend, VectorMatch,
    VectorMatchWithSource, VectorRecord, VectorResult,
};
use dashmap::DashMap;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::EntityRef;
use strata_engine::Database;
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
/// use strata_engine::Database;
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

mod collections;
mod crud;
mod recovery;
mod search;
mod system;

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

    /// Load a backend from KV, populating it with existing vectors.
    ///
    /// Used by `ensure_collection_loaded` when a collection exists in KV but
    /// its backend was evicted from memory. This mirrors the per-collection
    /// recovery logic in `recovery::recover_from_db` to satisfy ARCH-003:
    /// losing a secondary index must not lose data.
    fn load_backend_from_kv(
        &self,
        id: &CollectionId,
        config: &VectorConfig,
        space: &str,
    ) -> Result<Box<dyn VectorIndexBackend>, VectorError> {
        use strata_core::traits::Storage;

        let mut backend = self.create_backend(id, config)?;

        let data_dir = self.db.data_dir();
        let use_mmap = !data_dir.as_os_str().is_empty();

        // Try mmap-accelerated reload (same logic as recover_from_db)
        let mut loaded_from_mmap = false;
        if use_mmap {
            let vec_path = super::recovery::mmap_path(data_dir, id.branch_id, &id.name);
            if vec_path.exists() {
                match super::heap::VectorHeap::from_mmap(&vec_path, config.clone()) {
                    Ok(heap) if !heap.is_empty() => {
                        backend.replace_heap(heap);
                        loaded_from_mmap = true;
                    }
                    Ok(_) => {} // empty mmap, fall through to KV
                    Err(e) => {
                        warn!(
                            target: "strata::vector",
                            collection = %id.name,
                            error = %e,
                            "Failed to load mmap cache during reload, falling back to KV"
                        );
                    }
                }
            }
        }

        // Scan KV for vector entries
        let ns = self.namespace_for(id.branch_id, space);
        let version = self.db.storage().version();
        let vector_prefix = Key::new_vector(ns, &id.name, "");
        let vector_entries = self
            .db
            .storage()
            .scan_prefix(&vector_prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let collection_prefix = format!("{}/", id.name);
        for (vec_key, vec_versioned) in &vector_entries {
            let vec_bytes = match &vec_versioned.value {
                Value::Bytes(b) => b,
                _ => continue,
            };

            let vec_record = match VectorRecord::from_bytes(vec_bytes) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        target: "strata::vector",
                        error = %e,
                        "Failed to decode vector record during reload, skipping"
                    );
                    continue;
                }
            };

            let vid = VectorId::new(vec_record.vector_id);

            if loaded_from_mmap && backend.get(vid).is_some() {
                backend.register_mmap_vector(vid, vec_record.created_at);
            } else if vec_record.embedding.is_empty() {
                continue; // legacy empty-embedding record — skip
            } else {
                let _ = backend.insert_with_id_and_timestamp(
                    vid,
                    &vec_record.embedding,
                    vec_record.created_at,
                );
            }

            // Populate inline metadata for O(1) search resolution
            let vector_key = String::from_utf8(vec_key.user_key.to_vec())
                .ok()
                .and_then(|uk| uk.strip_prefix(&collection_prefix).map(|s| s.to_string()))
                .unwrap_or_default();
            backend.set_inline_meta(
                vid,
                crate::types::InlineMeta {
                    key: vector_key,
                    source_ref: vec_record.source_ref.clone(),
                },
            );
        }

        // Rebuild HNSW graphs (or load from mmap cache)
        let mut graphs_loaded = false;
        if use_mmap {
            let gdir = super::graph_dir(data_dir, id.branch_id, &id.name);
            if let Ok(true) = backend.load_graphs_from_disk(&gdir) {
                graphs_loaded = true;
            }
        }
        if !graphs_loaded {
            backend.rebuild_index();
        }
        backend.seal_remaining_active();

        info!(
            target: "strata::vector",
            collection = %id.name,
            vectors = backend.len(),
            mmap = loaded_from_mmap,
            "Reloaded collection backend from KV"
        );

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

        // Slow path: load config and rebuild backend from KV (ARCH-003)
        let config = self
            .load_collection_config(branch_id, space, name)?
            .ok_or_else(|| VectorError::CollectionNotFound {
                name: name.to_string(),
            })?;
        let backend = self.load_backend_from_kv(&collection_id, &config, space)?;

        // Double-check via entry API: another thread may have loaded it,
        // or a concurrent delete may have removed the collection (#1738 / 9.5.A).
        use dashmap::mapref::entry::Entry;
        match state.backends.entry(collection_id) {
            Entry::Occupied(_) => {}
            Entry::Vacant(e) => {
                // Re-verify collection still exists in KV before inserting.
                // A concurrent delete_collection could have removed the config
                // between our initial load and this point, and inserting here
                // would resurrect a stale backend.
                if self
                    .load_collection_config(branch_id, space, name)?
                    .is_some()
                {
                    e.insert(backend);
                }
            }
        }
        Ok(())
    }
}

// ========== Searchable Trait Implementation ==========

impl strata_engine::search::Searchable for VectorStore {
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
        req: &strata_engine::SearchRequest,
    ) -> strata_core::StrataResult<strata_engine::SearchResponse> {
        use std::time::Instant;
        use strata_engine::search::{SearchMode, SearchResponse, SearchStats};

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

// NOTE: The VectorStoreExt impl for TransactionContext lives in
// strata-engine/src/primitives/extensions.rs due to orphan rules
// (neither the trait nor TransactionContext is defined in strata-vector).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DistanceMetric, VectorConfig};
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
            storage_dtype: crate::StorageDtype::F32,
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

    /// Issue #1962: Verify that lite mode constructors cannot create records that
    /// break get_at() historical access. The VectorRecord API must not expose
    /// constructors that omit embeddings, because get_at() requires the embedding
    /// to be present in the KV record for time-travel queries.
    #[test]
    fn test_issue_1962_no_lite_constructors() {
        // VectorRecord::new() must always include the embedding
        let record = VectorRecord::new(VectorId(1), vec![1.0, 2.0, 3.0], None);
        assert_eq!(record.embedding, vec![1.0, 2.0, 3.0]);

        // VectorRecord::new_with_source() must always include the embedding
        let branch_id = BranchId::new();
        let record = VectorRecord::new_with_source(
            VectorId(2),
            vec![4.0, 5.0, 6.0],
            None,
            EntityRef::json(branch_id, "test_key"),
        );
        assert_eq!(record.embedding, vec![4.0, 5.0, 6.0]);

        // update() must preserve the embedding
        let mut record = VectorRecord::new(VectorId(3), vec![1.0, 0.0, 0.0], None);
        record.update(vec![0.0, 1.0, 0.0], None);
        assert_eq!(record.embedding, vec![0.0, 1.0, 0.0]);
        assert!(
            !record.embedding.is_empty(),
            "update() must store embedding"
        );

        // update_with_source() must preserve the embedding
        let mut record = VectorRecord::new(VectorId(4), vec![1.0, 0.0, 0.0], None);
        record.update_with_source(vec![0.0, 0.0, 1.0], None, None);
        assert_eq!(record.embedding, vec![0.0, 0.0, 1.0]);
        assert!(
            !record.embedding.is_empty(),
            "update_with_source() must store embedding"
        );

        // Verify no constructor produces an empty embedding.
        // If this test ever fails, it means someone added a constructor that
        // omits the embedding, which would break get_at() and recovery.
        // (Previously, new_lite() and update_lite() did this — removed in #1962.)
    }

    /// Issue #1962: Verify get_at() returns correct historical embeddings
    /// after a vector is updated. This is the scenario that would fail if
    /// lite mode were used (empty embedding in KV → get_at() error).
    #[test]
    fn test_issue_1962_get_at_returns_historical_embedding() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert original vector
        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Record a timestamp after the first insert
        let after_insert_ts = u64::MAX;

        // Update the vector with a new embedding
        store
            .insert(branch_id, "default", "test", "doc1", &[0.0, 1.0, 0.0], None)
            .unwrap();

        // get_at with max timestamp should return the latest embedding
        let entry = store
            .get_at(branch_id, "default", "test", "doc1", after_insert_ts)
            .unwrap()
            .expect("vector should exist at this timestamp");

        assert_eq!(
            entry.embedding,
            vec![0.0, 1.0, 0.0],
            "get_at() must return the latest embedding when queried at max timestamp"
        );

        // Verify embedding is not empty (the core invariant from #1962)
        assert!(
            !entry.embedding.is_empty(),
            "get_at() must never return an empty embedding"
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
        // per-collection and used to resolve legacy empty-embedding records.
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

    // ========================================
    // Issue #1772: ensure_collection_loaded must reload vectors from KV
    // ========================================

    #[test]
    fn test_issue_1772_ensure_loaded_restores_vectors() {
        // Reproduce: create collection, insert vectors, evict backend from
        // memory, then search. Before the fix, ensure_collection_loaded
        // creates an empty backend and search returns zero results.
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::for_minilm();
        store
            .create_collection(branch_id, "default", "reload_test", config)
            .unwrap();

        // Insert 5 vectors with distinct embeddings
        let embeddings: Vec<Vec<f32>> = (0..5)
            .map(|i| {
                let mut emb = vec![0.0; 384];
                emb[i] = 1.0; // unit vectors along different axes
                emb
            })
            .collect();

        for (i, emb) in embeddings.iter().enumerate() {
            store
                .insert(
                    branch_id,
                    "default",
                    "reload_test",
                    &format!("vec_{i}"),
                    emb,
                    None,
                )
                .unwrap();
        }

        // Verify search works before eviction
        let query = &embeddings[0];
        let results = store
            .search(branch_id, "default", "reload_test", query, 5, None)
            .unwrap();
        assert_eq!(results.len(), 5, "pre-eviction search should find all 5");
        assert_eq!(results[0].key, "vec_0", "best match should be vec_0");

        // Evict the backend from memory (simulates the cold-start scenario)
        {
            let state = store.state().unwrap();
            let collection_id = CollectionId::new(branch_id, "reload_test");
            state.backends.remove(&collection_id);
        }

        // Search after eviction: ensure_collection_loaded must reload vectors
        let results = store
            .search(branch_id, "default", "reload_test", query, 5, None)
            .unwrap();
        assert_eq!(
            results.len(),
            5,
            "post-eviction search should still find all 5 vectors"
        );
        assert_eq!(
            results[0].key, "vec_0",
            "best match should still be vec_0 after reload"
        );

        // Insert a new vector after reload — verifies the backend's internal
        // ID counter was restored correctly by insert_with_id_and_timestamp.
        let mut new_emb = vec![0.0; 384];
        new_emb[0] = 0.99;
        new_emb[1] = 0.01;
        store
            .insert(branch_id, "default", "reload_test", "vec_5", &new_emb, None)
            .unwrap();

        let results = store
            .search(branch_id, "default", "reload_test", query, 6, None)
            .unwrap();
        assert_eq!(
            results.len(),
            6,
            "search after insert-on-reloaded-backend should find all 6"
        );
    }

    // ====================================================================
    // Issue #1731: Backend treated as authority, not derived cache
    // ====================================================================

    /// Test that get() falls back to KV record embedding when backend is missing it.
    /// This simulates a crash between KV commit and backend update.
    #[test]
    fn test_issue_1731_get_falls_back_to_kv_embedding() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        let embedding = vec![1.0, 0.0, 0.0];
        store
            .insert(branch_id, "default", "test", "vec1", &embedding, None)
            .unwrap();

        // Verify get() works normally
        let entry = store
            .get(branch_id, "default", "test", "vec1")
            .unwrap()
            .expect("should exist");
        assert_eq!(entry.value.embedding, vec![1.0, 0.0, 0.0]);

        // Now remove the vector from the backend only (simulating crash/failure)
        let state = store.state().unwrap();
        let collection_id = CollectionId::new(branch_id, "test");
        let mut backend = state.backends.get_mut(&collection_id).unwrap();
        let vector_id = entry.value.vector_id;
        backend.delete(vector_id).unwrap();
        drop(backend);
        drop(state);

        // get() should still work by falling back to KV record embedding
        let result = store.get(branch_id, "default", "test", "vec1");
        assert!(
            result.is_ok(),
            "get() should succeed by falling back to KV embedding, got: {:?}",
            result.err()
        );
        let entry = result.unwrap().expect("should exist in KV");
        assert_eq!(
            entry.value.embedding,
            vec![1.0, 0.0, 0.0],
            "embedding should come from KV record"
        );
    }

    /// Test that search() does not return vectors deleted from KV but still in backend.
    /// This simulates a delete where KV succeeded but backend removal failed.
    #[test]
    fn test_issue_1731_search_skips_kv_deleted_vectors() {
        let (_temp, db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Insert two vectors
        store
            .insert(branch_id, "default", "test", "vec1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "vec2", &[0.0, 1.0, 0.0], None)
            .unwrap();

        // Delete vec1 from KV only (simulating backend delete failure)
        let kv_key = Key::new_vector(
            Arc::new(Namespace::for_branch_space(branch_id, "default")),
            "test",
            "vec1",
        );
        db.transaction(branch_id, |txn| txn.delete(kv_key.clone()))
            .unwrap();

        // Search should NOT return vec1 (it's deleted from KV)
        let results = store
            .search(branch_id, "default", "test", &[1.0, 0.0, 0.0], 10, None)
            .unwrap();

        let keys: Vec<&str> = results.iter().map(|m| m.key.as_str()).collect();
        assert!(
            !keys.contains(&"vec1"),
            "search should not return KV-deleted vector, got keys: {:?}",
            keys
        );
        assert!(
            keys.contains(&"vec2"),
            "search should still return existing vector, got keys: {:?}",
            keys
        );
    }

    /// Issue #1738 / 9.5.A: ensure_collection_loaded() can resurrect a deleted
    /// backend. Thread A loads config from KV, Thread B deletes collection
    /// (KV + DashMap), Thread A inserts stale backend.
    ///
    /// This concurrent test races ensure_collection_loaded against delete_collection
    /// and verifies that after delete completes, no stale backend remains.
    #[test]
    fn test_issue_1738_ensure_collection_loaded_resurrection() {
        for _ in 0..20 {
            let db = Database::cache().unwrap();
            let store = VectorStore::new(db.clone());
            let branch_id = BranchId::new();

            let config = VectorConfig::for_minilm();
            store
                .create_collection(branch_id, "default", "resurrect", config)
                .unwrap();

            // Remove backend from DashMap to force ensure_collection_loaded to reload
            {
                let state = store.state().unwrap();
                let cid = CollectionId::new(branch_id, "resurrect");
                state.backends.remove(&cid);
            }

            let store1 = VectorStore::new(db.clone());
            let store2 = VectorStore::new(db.clone());
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

            let b1 = barrier.clone();
            let loader = std::thread::spawn(move || {
                b1.wait();
                let _ = store1.ensure_collection_loaded(branch_id, "default", "resurrect");
            });

            let b2 = barrier.clone();
            let deleter = std::thread::spawn(move || {
                b2.wait();
                let _ = store2.delete_collection(branch_id, "default", "resurrect");
            });

            loader.join().unwrap();
            deleter.join().unwrap();

            // After delete_collection completes, verify consistency:
            // If config doesn't exist in KV, backend must not exist in DashMap
            let config_exists = store
                .load_collection_config(branch_id, "default", "resurrect")
                .unwrap_or(None)
                .is_some();

            if !config_exists {
                let state = store.state().unwrap();
                let cid = CollectionId::new(branch_id, "resurrect");
                assert!(
                    !state.backends.contains_key(&cid),
                    "Backend exists in DashMap but config was deleted from KV — \
                     ensure_collection_loaded resurrected a stale backend"
                );
            }

            db.shutdown().unwrap();
        }
    }
}
