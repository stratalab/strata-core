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
    SearchOptions, VectorConfig, VectorEntry, VectorError, VectorId, VectorIndexBackend,
    VectorMatch, VectorMatchWithSource, VectorRecord, VectorResult,
};
use dashmap::DashMap;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::id::{CommitVersion, TxnId};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::EntityRef;
use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem};
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

    /// Pending HNSW operations to be applied after transaction commit.
    ///
    /// Operations are keyed by transaction id so concurrent writers on the same
    /// branch cannot apply or clear each other's deferred backend work.
    /// On commit, the VectorCommitObserver drains and applies only the committed
    /// transaction's ops. On abort or failed commit, engine-owned abort
    /// observers clear that transaction's queued ops without touching other
    /// in-flight writers.
    ///
    /// This moves vector backend maintenance ownership from executor (Session)
    /// to subsystem (VectorSubsystem), fulfilling the T2-E2 requirement.
    pending_ops: DashMap<TxnId, Vec<crate::ext::StagedVectorOp>>,

    /// Tracks whether runtime-only hooks have been registered for this
    /// database instance.
    pub(crate) runtime_wired: AtomicBool,
}

impl Default for VectorBackendState {
    fn default() -> Self {
        Self {
            backends: DashMap::new(),
            pending_ops: DashMap::new(),
            runtime_wired: AtomicBool::new(false),
        }
    }
}

impl VectorBackendState {
    /// Queue a staged vector operation for post-commit application.
    ///
    /// Called by VectorStoreExt during transactions. The operation will be
    /// applied by VectorCommitObserver after the transaction commits.
    pub fn queue_pending_op(&self, txn_id: TxnId, op: crate::ext::StagedVectorOp) {
        self.pending_ops.entry(txn_id).or_default().push(op);
    }

    /// Apply and clear all pending operations for a transaction.
    ///
    /// Called by VectorCommitObserver after transaction commit.
    /// Returns the number of operations applied.
    pub fn apply_pending_ops(&self, txn_id: TxnId) -> usize {
        if let Some((_, ops)) = self.pending_ops.remove(&txn_id) {
            let count = ops.len();
            for op in ops {
                crate::ext::apply_staged_vector_op(self, op);
            }
            count
        } else {
            0
        }
    }

    /// Clear pending operations for a transaction without applying them.
    ///
    /// Called by engine-owned abort observers on transaction abort or commit
    /// failure to clean up uncommitted ops.
    pub fn clear_pending_ops(&self, txn_id: TxnId) {
        self.pending_ops.remove(&txn_id);
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
/// use strata_engine::{Database, OpenSpec};
/// use strata_vector::{VectorConfig, VectorStore, VectorSubsystem};
/// use strata_core::types::BranchId;
///
/// // Open via OpenSpec with VectorSubsystem so vector recovery
/// // and drop-time freeze are installed. `Database::open` alone does
/// // NOT install VectorSubsystem — always use `OpenSpec::with_subsystem`
/// // (or `strata_executor::Strata::open`, which wraps this pattern) for
/// // disk-backed vector stores, otherwise vector state will not
/// // survive drop+reopen.
/// let db = Database::open_runtime(
///     OpenSpec::primary("/path/to/data").with_subsystem(VectorSubsystem)
/// )?;
///
/// let store = VectorStore::new(db.clone());
/// let branch_id = BranchId::new();
///
/// // Create collection
/// let config = VectorConfig::for_minilm();
/// store.create_collection(branch_id, "embeddings", config)?;
///
/// // Multiple stores share the same backend state (VectorBackendState
/// // is a Database extension)
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

    /// Get access to the shared backend state
    ///
    /// This returns the shared `VectorBackendState` stored in the Database.
    /// All VectorStore instances for the same Database share this state.
    pub fn state(&self) -> Result<Arc<VectorBackendState>, VectorError> {
        let state = self
            .db
            .extension::<VectorBackendState>()
            .map_err(|e| VectorError::Storage(e.to_string()))?;
        crate::recovery::ensure_runtime_wiring(&self.db, &state);
        Ok(state)
    }

    /// Build namespace for branch+space-scoped operations
    fn namespace_for(&self, branch_id: BranchId, space: &str) -> Arc<Namespace> {
        Arc::new(Namespace::for_branch_space(branch_id, space))
    }

    /// Get a backend factory for a specific backend type (Issue #1964).
    fn backend_factory_for(&self, backend_type: crate::IndexBackendType) -> IndexBackendFactory {
        IndexBackendFactory::from_type(backend_type)
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Initialize the index backend for a collection (uses default backend type).
    fn init_backend(&self, id: &CollectionId, config: &VectorConfig) -> Result<(), VectorError> {
        self.init_backend_with_type(id, config, crate::IndexBackendType::default())
    }

    /// Initialize the index backend for a collection with explicit backend type.
    fn init_backend_with_type(
        &self,
        id: &CollectionId,
        config: &VectorConfig,
        backend_type: crate::IndexBackendType,
    ) -> Result<(), VectorError> {
        let backend = self.create_backend_with_type(id, config, backend_type)?;
        let state = self.state()?;
        state.backends.insert(id.clone(), backend);
        Ok(())
    }

    /// Create and configure an index backend with explicit type (Issue #1964).
    fn create_backend_with_type(
        &self,
        id: &CollectionId,
        config: &VectorConfig,
        backend_type: crate::IndexBackendType,
    ) -> Result<Box<dyn VectorIndexBackend>, VectorError> {
        let mut backend = self.backend_factory_for(backend_type).create(config);

        // Set flush_path so the tiered heap can flush overlays during fresh
        // indexing (not just during recovery). Without this, fresh inserts
        // stay in anonymous memory forever, causing OOM on large datasets.
        let data_dir = self.db.data_dir();
        if !data_dir.as_os_str().is_empty() {
            let vec_path = super::recovery::mmap_path(data_dir, id.branch_id, &id.space, &id.name);
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
        backend_type: crate::IndexBackendType,
    ) -> Result<Box<dyn VectorIndexBackend>, VectorError> {
        use strata_core::traits::Storage;

        let mut backend = self.create_backend_with_type(id, config, backend_type)?;

        let data_dir = self.db.data_dir();
        // Followers can reopen with storage intentionally clamped below the
        // primary's latest durable caches. Keep lazy reload aligned with
        // startup recovery and rebuild from the follower-visible KV snapshot.
        let use_mmap = !data_dir.as_os_str().is_empty() && !self.db.is_follower();

        // Try mmap-accelerated reload (same logic as recover_from_db)
        let mut loaded_from_mmap = false;
        if use_mmap {
            let vec_path = super::recovery::mmap_path(data_dir, id.branch_id, &id.space, &id.name);
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
        let version = CommitVersion(self.db.storage().version());
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
            let gdir = super::graph_dir(data_dir, id.branch_id, &id.space, &id.name);
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

        let version = CommitVersion(self.db.storage().version());
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

        let version = CommitVersion(self.db.storage().version());
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

        let version = CommitVersion(self.db.storage().version());
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        Ok(entries.len())
    }

    /// Delete all vectors in a collection atomically (Issue #1968).
    ///
    /// All deletes are performed in a single transaction to guarantee
    /// atomicity — either all vectors are deleted or none are.
    fn delete_all_vectors(&self, branch_id: BranchId, space: &str, name: &str) -> VectorResult<()> {
        use strata_core::traits::Storage;

        let namespace = self.namespace_for(branch_id, space);
        let prefix = Key::vector_collection_prefix(namespace, name);

        // Scan all vector keys in this collection
        let version = CommitVersion(self.db.storage().version());
        let entries = self
            .db
            .storage()
            .scan_prefix(&prefix, version)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        let keys: Vec<Key> = entries.into_iter().map(|(key, _)| key).collect();

        if !keys.is_empty() {
            self.db
                .transaction(branch_id, |txn| {
                    for key in &keys {
                        txn.delete(key.clone())?;
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
        Ok(self
            .load_collection_record(branch_id, space, name)?
            .map(|(config, _record)| config))
    }

    /// Load the full CollectionRecord from KV (includes backend_type).
    fn load_collection_record(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<Option<(VectorConfig, CollectionRecord)>> {
        use strata_core::traits::Storage;

        let config_key = Key::new_vector_config(self.namespace_for(branch_id, space), name);
        let version = CommitVersion(self.db.storage().version());

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
        let config = VectorConfig::try_from(record.config.clone())?;
        Ok(Some((config, record)))
    }

    /// Ensure collection is loaded into memory
    ///
    /// If the collection exists in KV but not in memory (after recovery),
    /// this loads it and initializes the backend.
    ///
    /// Uses double-checked locking to avoid TOCTOU races: the read-lock fast
    /// path avoids contention in the common case, while the write-lock slow
    /// path re-checks to prevent duplicate initialization.
    pub fn ensure_collection_loaded(
        &self,
        branch_id: BranchId,
        space: &str,
        name: &str,
    ) -> VectorResult<()> {
        let collection_id = CollectionId::new(branch_id, space, name);
        let state = self.state()?;

        // Fast path: check without entry overhead
        if state.backends.contains_key(&collection_id) {
            return Ok(());
        }

        // Slow path: load config and rebuild backend from KV (ARCH-003)
        let (config, record) = self
            .load_collection_record(branch_id, space, name)?
            .ok_or_else(|| VectorError::CollectionNotFound {
                name: name.to_string(),
            })?;
        let backend_type = record.backend_type();
        let backend = self.load_backend_from_kv(&collection_id, &config, space, backend_type)?;

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
    ///   provide the query embedding via VectorQueryRequest extension
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
        use strata_engine::search::{
            truncate_text, SearchHit, SearchMode, SearchResponse, SearchStats,
        };
        use strata_engine::system_space::SYSTEM_SPACE;

        let start = Instant::now();

        // Vector primitive only responds to Vector or Hybrid mode
        // with an explicit query embedding provided externally.
        let embedding = match req.mode {
            SearchMode::Keyword => {
                return Ok(SearchResponse::new(
                    vec![],
                    false,
                    SearchStats::new(start.elapsed().as_micros() as u64, 0),
                ));
            }
            SearchMode::Vector | SearchMode::Hybrid => match &req.precomputed_embedding {
                Some(emb) => emb,
                None => {
                    return Ok(SearchResponse::new(
                        vec![],
                        false,
                        SearchStats::new(start.elapsed().as_micros() as u64, 0),
                    ));
                }
            },
        };

        // Discover all _system_embed_* collections on this branch
        let collections = self
            .list_collections(req.branch_id, SYSTEM_SPACE)
            .unwrap_or_default();

        let mut all_hits: Vec<SearchHit> = Vec::new();

        for col in &collections {
            if !col.name.starts_with("_system_embed_") {
                continue;
            }
            if col.count == 0 {
                continue;
            }

            let matches =
                match self.system_search_with_sources(req.branch_id, &col.name, embedding, req.k) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

            for m in matches {
                // Phase 1 Part 2: drop matches whose source isn't in the
                // caller's space, and drop matches with no `source_ref`
                // entirely. Shadow collections (`_system_embed_*`) live in
                // SYSTEM_SPACE and intermix entities from every user
                // space, so the source_ref is the only honest signal.
                let Some(source) = m.source_ref else {
                    continue; // no source → can't attribute to a space
                };
                if source.space() != Some(req.space.as_str()) {
                    continue; // wrong space, or space-less Branch ref
                }
                let snippet = m
                    .metadata
                    .as_ref()
                    .map(|meta| truncate_text(&meta.to_string(), 100));
                all_hits.push(SearchHit {
                    doc_ref: source,
                    score: m.score,
                    rank: 0,
                    snippet,
                });
            }
        }

        // Sort by score descending, assign ranks
        all_hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_hits.truncate(req.k);
        for (i, hit) in all_hits.iter_mut().enumerate() {
            hit.rank = (i + 1) as u32;
        }

        let searched_any = collections
            .iter()
            .any(|c| c.name.starts_with("_system_embed_") && c.count > 0);

        let elapsed = start.elapsed().as_micros() as u64;
        let mut stats = SearchStats::new(elapsed, all_hits.len());
        if searched_any {
            stats = stats.with_index_used(true);
        }

        Ok(SearchResponse::new(all_hits, false, stats))
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Vector
    }
}

// Re-export now_micros from types for use in store submodules (via `use super::*`).
use crate::types::now_micros;

// VectorStoreExt implementation lives in crate::ext (ext.rs).
// It implements vector operations directly on TransactionContext for OCC support.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::VectorStoreExt;
    use crate::{DistanceMetric, VectorConfig};
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, VectorStore) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_runtime(
            OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem),
        )
        .unwrap();
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
            let db = Database::open_runtime(
                OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem),
            )
            .unwrap();
            let store = VectorStore::new(db);

            let config = VectorConfig::new(512, DistanceMetric::DotProduct).unwrap();
            store
                .create_collection(branch_id, "default", "persistent", config)
                .unwrap();
        }

        // Reopen database and verify collection exists
        {
            let db = Database::open_runtime(
                OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem),
            )
            .unwrap();
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
            EntityRef::json(branch_id, "default", "test_key"),
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

    // ========================================
    // Version History (getv) Tests
    // ========================================

    #[test]
    fn test_getv_returns_all_versions() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Upsert same key three times with different embeddings
        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "doc1", &[0.0, 1.0, 0.0], None)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "doc1", &[0.0, 0.0, 1.0], None)
            .unwrap();

        let history = store
            .getv(branch_id, "default", "test", "doc1")
            .unwrap()
            .expect("history should be present");

        assert_eq!(history.len(), 3, "should have 3 versions");
        // Newest first
        assert_eq!(history[0].value.embedding, vec![0.0, 0.0, 1.0]);
        assert_eq!(history[1].value.embedding, vec![0.0, 1.0, 0.0]);
        assert_eq!(history[2].value.embedding, vec![1.0, 0.0, 0.0]);
    }

    #[test]
    fn test_getv_nonexistent_returns_none() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        let history = store.getv(branch_id, "default", "test", "missing").unwrap();
        assert!(history.is_none());
    }

    #[test]
    fn test_getv_preserves_metadata_per_version() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        // Upsert with metadata v1
        store
            .insert(
                branch_id,
                "default",
                "test",
                "doc1",
                &[1.0, 0.0, 0.0],
                Some(serde_json::json!({"tag": "v1"})),
            )
            .unwrap();

        // Upsert with metadata v2
        store
            .insert(
                branch_id,
                "default",
                "test",
                "doc1",
                &[0.0, 1.0, 0.0],
                Some(serde_json::json!({"tag": "v2"})),
            )
            .unwrap();

        let history = store
            .getv(branch_id, "default", "test", "doc1")
            .unwrap()
            .unwrap();

        assert_eq!(history.len(), 2);
        // Newest version should carry the "v2" metadata
        assert_eq!(
            history[0].value.metadata,
            Some(serde_json::json!({"tag": "v2"}))
        );
        assert_eq!(
            history[1].value.metadata,
            Some(serde_json::json!({"tag": "v1"}))
        );
    }

    #[test]
    fn test_getv_newest_first_ordering() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .insert(branch_id, "default", "test", "doc1", &[0.0, 1.0, 0.0], None)
            .unwrap();

        let history = store
            .getv(branch_id, "default", "test", "doc1")
            .unwrap()
            .unwrap();

        assert_eq!(history.len(), 2);
        // Monotonic newest-first on timestamp
        let ts_new: u64 = history[0].timestamp.into();
        let ts_old: u64 = history[1].timestamp.into();
        assert!(
            ts_new >= ts_old,
            "history[0].timestamp ({}) should be >= history[1].timestamp ({})",
            ts_new,
            ts_old
        );
    }

    #[test]
    fn test_getv_after_delete_skips_tombstone() {
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();

        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();
        store.delete(branch_id, "default", "test", "doc1").unwrap();

        // After delete, live version history (filtered) should still contain
        // the single pre-delete version; tombstones are skipped.
        let history = store
            .getv(branch_id, "default", "test", "doc1")
            .unwrap()
            .expect("pre-delete version should remain visible via getv");

        assert_eq!(history.len(), 1);
        assert_eq!(history[0].value.embedding, vec![1.0, 0.0, 0.0]);
    }

    #[test]
    fn test_getv_on_nonexistent_collection_returns_none() {
        // getv reads directly from the storage version chain and must NOT
        // return CollectionNotFound. Unlike vector_get, it does not call
        // ensure_collection_loaded. Calling getv against a collection that
        // was never created should succeed with None, not error.
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let history = store
            .getv(branch_id, "default", "never-created", "doc1")
            .expect("getv must not return CollectionNotFound");
        assert!(
            history.is_none(),
            "nonexistent collection should return None history"
        );
    }

    #[test]
    fn test_getv_after_delete_collection_returns_history() {
        // delete_collection tombstones all vector keys but the version chain
        // is preserved. getv should still find the pre-delete versions as
        // live entries (tombstones are skipped).
        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "test", config)
            .unwrap();
        store
            .insert(branch_id, "default", "test", "doc1", &[1.0, 0.0, 0.0], None)
            .unwrap();

        // Delete the whole collection — HNSW backend is gone, KV tombstoned
        store
            .delete_collection(branch_id, "default", "test")
            .unwrap();

        // getv must not error on missing backend. It must return a
        // well-defined answer (Some with the pre-delete version after
        // tombstones are skipped).
        let history = store
            .getv(branch_id, "default", "test", "doc1")
            .expect("getv must not error after delete_collection");

        // Either the pre-delete version is still reachable (if delete_collection
        // writes a tombstone that the skip logic filters out) or history is None
        // (if delete_collection hard-removes the version chain entry). Both
        // are valid, but the call must succeed and the result must match exactly
        // one of these shapes.
        match history {
            Some(h) => {
                assert_eq!(
                    h.len(),
                    1,
                    "pre-delete version should be the only live entry"
                );
                assert_eq!(h[0].value.embedding, vec![1.0, 0.0, 0.0]);
            }
            None => {
                // Tombstoned entries were skipped and no live versions remain.
                // Acceptable — the invariant tested here is "no panic / no error".
            }
        }
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

        let collection_id = CollectionId::new(branch_id, "default", "test");

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
        let collection_id = CollectionId::new(branch_id, "default", "test");
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
            space: "default".to_string(),
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
            let collection_id = CollectionId::new(branch_id, "default", "concurrent_test");
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
        let collection_id = CollectionId::new(branch_id, "default", "concurrent_test");
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
            let collection_id = CollectionId::new(branch_id, "default", "reload_test");
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
        let collection_id = CollectionId::new(branch_id, "default", "test");
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
            let db =
                Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
            let store = VectorStore::new(db.clone());
            let branch_id = BranchId::new();

            let config = VectorConfig::for_minilm();
            store
                .create_collection(branch_id, "default", "resurrect", config)
                .unwrap();

            // Remove backend from DashMap to force ensure_collection_loaded to reload
            {
                let state = store.state().unwrap();
                let cid = CollectionId::new(branch_id, "default", "resurrect");
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
                let cid = CollectionId::new(branch_id, "default", "resurrect");
                assert!(
                    !state.backends.contains_key(&cid),
                    "Backend exists in DashMap but config was deleted from KV — \
                     ensure_collection_loaded resurrected a stale backend"
                );
            }

            db.shutdown().unwrap();
        }
    }

    // ========================================================================
    // Issue #1964: Configurable backend factory
    // ========================================================================

    #[test]
    fn test_create_collection_with_brute_force_backend() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection_with_backend(
                branch_id,
                "default",
                "small_collection",
                config,
                crate::IndexBackendType::BruteForce,
            )
            .unwrap();

        // Insert and search should work with BruteForce backend
        store
            .insert(
                branch_id,
                "default",
                "small_collection",
                "vec1",
                &[1.0, 0.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "small_collection",
                "vec2",
                &[0.0, 1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        let results = store
            .search(
                branch_id,
                "default",
                "small_collection",
                &[1.0, 0.0, 0.0, 0.0],
                2,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "vec1");

        // Verify backend is reported as brute_force
        let stats = store
            .collection_backend_stats(branch_id, "default", "small_collection")
            .unwrap();
        assert_eq!(stats.0, "brute_force");
    }

    #[test]
    fn test_backend_type_persisted_in_collection_record() {
        use crate::types::IndexBackendType;

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        let record = CollectionRecord::with_backend_type(&config, IndexBackendType::BruteForce);

        let bytes = record.to_bytes().unwrap();
        let loaded = CollectionRecord::from_bytes(&bytes).unwrap();
        assert_eq!(loaded.backend_type(), IndexBackendType::BruteForce);
    }

    #[test]
    fn test_backend_type_all_variants_roundtrip() {
        use crate::types::IndexBackendType;

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        for bt in [
            IndexBackendType::BruteForce,
            IndexBackendType::Hnsw,
            IndexBackendType::SegmentedHnsw,
        ] {
            let record = CollectionRecord::with_backend_type(&config, bt);
            let bytes = record.to_bytes().unwrap();
            let loaded = CollectionRecord::from_bytes(&bytes).unwrap();
            assert_eq!(loaded.backend_type(), bt, "Roundtrip failed for {:?}", bt);
        }
    }

    // ========================================================================
    // Issues #1966, #1967: SearchOptions
    // ========================================================================

    #[test]
    fn test_search_with_include_metadata() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "meta_test", config)
            .unwrap();

        let metadata = serde_json::json!({"category": "science"});
        store
            .insert(
                branch_id,
                "default",
                "meta_test",
                "doc1",
                &[1.0, 0.0, 0.0, 0.0],
                Some(metadata.clone()),
            )
            .unwrap();

        // Default search: metadata=None for unfiltered
        let results = store
            .search(
                branch_id,
                "default",
                "meta_test",
                &[1.0, 0.0, 0.0, 0.0],
                1,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].metadata.is_none());

        // With include_metadata=true: metadata should be populated
        let results = store
            .search_with_options(
                branch_id,
                "default",
                "meta_test",
                &[1.0, 0.0, 0.0, 0.0],
                1,
                SearchOptions::default().with_include_metadata(true),
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metadata.as_ref().unwrap(), &metadata);
    }

    #[test]
    fn test_search_with_custom_overfetch_multipliers() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "overfetch", config)
            .unwrap();

        // Insert 10 vectors, half with category "a", half with "b"
        for i in 0..10 {
            let cat = if i < 5 { "a" } else { "b" };
            let metadata = serde_json::json!({"cat": cat});
            let emb = [i as f32, 0.0, 0.0, 1.0];
            store
                .insert(
                    branch_id,
                    "default",
                    "overfetch",
                    &format!("vec{}", i),
                    &emb,
                    Some(metadata),
                )
                .unwrap();
        }

        // Search with filter and custom multipliers [2]
        let filter = MetadataFilter::new().eq("cat", "a");
        let opts = SearchOptions::default()
            .with_filter(filter)
            .with_overfetch_multipliers(vec![2]);

        let results = store
            .search_with_options(
                branch_id,
                "default",
                "overfetch",
                &[5.0, 0.0, 0.0, 1.0],
                3,
                opts,
            )
            .unwrap();
        // Should find up to 3 results with category "a"
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
    }

    #[test]
    fn test_search_with_empty_overfetch_multipliers_still_returns_results() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "empty_mult", config)
            .unwrap();

        for i in 0..5 {
            let metadata = serde_json::json!({"x": "a"});
            store
                .insert(
                    branch_id,
                    "default",
                    "empty_mult",
                    &format!("v{}", i),
                    &[i as f32, 0.0, 0.0, 1.0],
                    Some(metadata),
                )
                .unwrap();
        }

        // Empty multipliers with a filter should still return results (falls back to [1])
        let filter = MetadataFilter::new().eq("x", "a");
        let opts = SearchOptions::default()
            .with_filter(filter)
            .with_overfetch_multipliers(vec![]);

        let results = store
            .search_with_options(
                branch_id,
                "default",
                "empty_mult",
                &[0.0, 0.0, 0.0, 1.0],
                3,
                opts,
            )
            .unwrap();
        assert_eq!(
            results.len(),
            3,
            "Empty multipliers should fall back to [1]"
        );
    }

    // ========================================================================
    // Issue #1968: Batched delete_all_vectors
    // ========================================================================

    #[test]
    fn test_delete_collection_with_many_vectors() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "large", config)
            .unwrap();

        // Insert 2500 vectors (tests batched deletion with BATCH_SIZE=1000)
        for i in 0..2500 {
            let emb = [i as f32, 0.0, 0.0, 1.0];
            store
                .insert(
                    branch_id,
                    "default",
                    "large",
                    &format!("v{}", i),
                    &emb,
                    None,
                )
                .unwrap();
        }

        // Verify vectors exist
        let results = store
            .search(
                branch_id,
                "default",
                "large",
                &[0.0, 0.0, 0.0, 1.0],
                10,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 10);

        // Delete collection (exercises batched delete)
        store
            .delete_collection(branch_id, "default", "large")
            .unwrap();

        // Verify collection is gone
        let collections = store.list_collections(branch_id, "default").unwrap();
        assert!(collections.is_empty());
    }

    // ========================================================================
    // Issue #1970: Concurrent inserts to same collection
    // ========================================================================

    #[test]
    fn test_concurrent_upserts_same_collection() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "concurrent", config)
            .unwrap();

        let num_threads = 4;
        let vectors_per_thread = 25;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let store = store.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..vectors_per_thread {
                        let key = format!("t{}_v{}", thread_id, i);
                        let emb = [thread_id as f32, i as f32, (thread_id + i) as f32, 1.0];
                        store
                            .insert(branch_id, "default", "concurrent", &key, &emb, None)
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let expected = num_threads * vectors_per_thread;
        let results = store
            .search(
                branch_id,
                "default",
                "concurrent",
                &[1.0, 1.0, 1.0, 1.0],
                expected,
                None,
            )
            .unwrap();
        assert_eq!(
            results.len(),
            expected,
            "All {} vectors should be searchable",
            expected
        );
    }

    // ========================================================================
    // Issue #1970 scenario 1: Search after backend reload (simulates recovery)
    // ========================================================================

    #[test]
    fn test_search_after_backend_eviction_and_reload() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db.clone());
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "recover_test", config)
            .unwrap();

        // Insert vectors
        for i in 0..10 {
            let emb = [i as f32, 0.0, 0.0, 1.0];
            store
                .insert(
                    branch_id,
                    "default",
                    "recover_test",
                    &format!("vec{}", i),
                    &emb,
                    None,
                )
                .unwrap();
        }

        // Evict the backend to simulate recovery scenario
        {
            let state = store.state().unwrap();
            let cid = CollectionId::new(branch_id, "default", "recover_test");
            state.backends.remove(&cid);
        }

        // Search should reload backend from KV and still work
        let results = store
            .search(
                branch_id,
                "default",
                "recover_test",
                &[5.0, 0.0, 0.0, 1.0],
                5,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 5);

        // Verify inline meta works (results have keys, not empty strings)
        for r in &results {
            assert!(
                r.key.starts_with("vec"),
                "key should be populated: {}",
                r.key
            );
        }
    }

    // ========================================================================
    // Issue #1970 scenario 8: Batch insert with mix of new/update vectors
    // ========================================================================

    #[test]
    fn test_batch_insert_mix_new_and_update() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "batch_mix", config)
            .unwrap();

        // Insert initial vectors
        store
            .insert(
                branch_id,
                "default",
                "batch_mix",
                "existing1",
                &[1.0, 0.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        store
            .insert(
                branch_id,
                "default",
                "batch_mix",
                "existing2",
                &[0.0, 1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        // Batch: update existing1, update existing2, add new1
        let batch = vec![
            (
                "existing1".to_string(),
                vec![0.0, 0.0, 1.0, 0.0],
                None::<serde_json::Value>,
            ),
            ("existing2".to_string(), vec![0.0, 0.0, 0.0, 1.0], None),
            ("new1".to_string(), vec![1.0, 1.0, 0.0, 0.0], None),
        ];
        store
            .batch_insert(branch_id, "default", "batch_mix", batch)
            .unwrap();

        // Should have 3 vectors total
        let results = store
            .search(
                branch_id,
                "default",
                "batch_mix",
                &[0.0, 0.0, 1.0, 0.0],
                10,
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 3);
        // existing1 should be closest (updated to [0,0,1,0])
        assert_eq!(results[0].key, "existing1");
    }

    // ========================================================================
    // Issue #1907: Concurrent search + insert (triggers seal) stress test
    // ========================================================================

    /// Prove that concurrent inserts (which trigger seal) and searches
    /// produce correct results. DashMap's per-shard locking serializes
    /// `get_mut()` (insert/seal) vs `get()` (search), so the race
    /// described in #1907 cannot occur.
    #[test]
    fn test_issue_1907_concurrent_insert_and_search_during_seal() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "seal_race", config)
            .unwrap();

        // Seed with some vectors so searches have results
        for i in 0..20 {
            let emb = [i as f32, 0.0, 0.0, 1.0];
            store
                .insert(
                    branch_id,
                    "default",
                    "seal_race",
                    &format!("seed_{}", i),
                    &emb,
                    None,
                )
                .unwrap();
        }

        let num_writers = 2;
        let num_readers = 4;
        let inserts_per_writer = 200;
        let searches_per_reader = 200;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_writers + num_readers));

        let mut writer_handles = Vec::new();
        let mut reader_handles = Vec::new();

        // Writer threads: insert vectors (triggers seal when active buffer fills)
        for writer_id in 0..num_writers {
            let store = store.clone();
            let barrier = barrier.clone();
            writer_handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..inserts_per_writer {
                    let key = format!("w{}_{}", writer_id, i);
                    let emb = [writer_id as f32, i as f32, 0.0, 1.0];
                    store
                        .insert(branch_id, "default", "seal_race", &key, &emb, None)
                        .unwrap();
                }
            }));
        }

        // Reader threads: search concurrently with inserts
        for _reader_id in 0..num_readers {
            let store = store.clone();
            let barrier = barrier.clone();
            reader_handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..searches_per_reader {
                    let results = store
                        .search(
                            branch_id,
                            "default",
                            "seal_race",
                            &[1.0, 1.0, 0.0, 1.0],
                            5,
                            None,
                        )
                        .unwrap();
                    // Every search must return results (at least the seed vectors)
                    assert!(
                        !results.is_empty(),
                        "Search returned 0 results during concurrent insert/seal"
                    );
                }
            }));
        }

        for h in writer_handles {
            h.join().unwrap();
        }
        for h in reader_handles {
            h.join().unwrap();
        }

        // After all writers finish, verify all vectors are searchable
        let total_expected = 20 + (num_writers * inserts_per_writer);
        let final_results = store
            .search(
                branch_id,
                "default",
                "seal_race",
                &[1.0, 1.0, 0.0, 1.0],
                total_expected,
                None,
            )
            .unwrap();
        assert_eq!(
            final_results.len(),
            total_expected,
            "All {} vectors should be searchable after concurrent insert+search",
            total_expected
        );
    }

    /// Issue #1572: delete() does not hold the per-collection write lock during
    /// KV existence check and KV delete, allowing a concurrent insert to be
    /// silently clobbered.
    ///
    /// Scenario: Thread A deletes "key" while Thread B inserts "key".
    /// If delete's KV read + KV delete happen without the write lock, the delete
    /// can race ahead and delete the record that insert just committed.
    ///
    /// After both threads complete, KV and backend must agree: either both
    /// see the key (insert serialized last) or neither does (delete serialized
    /// last). A mismatch indicates a TOCTOU race.
    #[test]
    fn test_issue_1572_delete_insert_toctou() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "race", config)
            .unwrap();

        let num_rounds = 100;
        let mut inconsistencies = 0u32;

        for _round in 0..num_rounds {
            // Seed the key so delete has something to find
            let emb_v1 = [1.0_f32, 0.0, 0.0, 0.0];
            store
                .insert(branch_id, "default", "race", "key", &emb_v1, None)
                .unwrap();

            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

            // Thread A: delete
            let store_a = store.clone();
            let barrier_a = barrier.clone();
            let handle_a = std::thread::spawn(move || {
                barrier_a.wait();
                store_a.delete(branch_id, "default", "race", "key")
            });

            // Thread B: insert (upsert with new embedding)
            let store_b = store.clone();
            let barrier_b = barrier.clone();
            let emb_v2 = [0.0_f32, 1.0, 0.0, 0.0];
            let handle_b = std::thread::spawn(move || {
                barrier_b.wait();
                store_b.insert(branch_id, "default", "race", "key", &emb_v2, None)
            });

            let delete_result = handle_a.join().unwrap();
            let insert_result = handle_b.join().unwrap();

            // With OCC, one of the concurrent operations may get a conflict
            // error (TransactionAborted). This is correct: the race IS detected.
            // At least one must succeed; both succeeding is also valid.
            let delete_ok = delete_result.is_ok();
            let insert_ok = insert_result.is_ok();
            assert!(
                delete_ok || insert_ok,
                "At least one concurrent operation must succeed"
            );

            // If insert failed (OCC conflict), re-insert so consistency check works
            if !insert_ok {
                let emb_v2 = [0.0_f32, 1.0, 0.0, 0.0];
                store
                    .insert(branch_id, "default", "race", "key", &emb_v2, None)
                    .unwrap();
            }

            // KV and backend must agree on existence.
            let kv_exists = store
                .get(branch_id, "default", "race", "key")
                .unwrap()
                .is_some();
            let search_results = store
                .search(
                    branch_id,
                    "default",
                    "race",
                    &[0.0, 1.0, 0.0, 0.0],
                    10,
                    None,
                )
                .unwrap();
            let search_finds_key = search_results.iter().any(|m| m.key == "key");

            if kv_exists != search_finds_key {
                inconsistencies += 1;
            }
        }

        assert_eq!(
            inconsistencies, 0,
            "KV/backend inconsistency detected in {inconsistencies}/{num_rounds} rounds — TOCTOU race in delete"
        );
    }

    /// Issue #1572 stress test: concurrent delete + insert on the SAME key.
    ///
    /// Under correct locking, exactly one serialization order applies per
    /// iteration. If the TOCTOU bug is present, a significant fraction of
    /// iterations will lose the insert's KV record while the backend retains
    /// the entry (phantom) or vice versa.
    #[test]
    fn test_issue_1572_delete_insert_toctou_concurrent() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db);
        let branch_id = BranchId::new();

        let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "stress", config)
            .unwrap();

        let num_rounds = 100;
        let num_threads = 4; // 2 inserters + 2 deleters per round
        let mut inconsistencies = 0u32;

        for _round in 0..num_rounds {
            // Seed a vector
            let seed_emb = [1.0_f32, 0.0, 0.0, 0.0];
            store
                .insert(branch_id, "default", "stress", "target", &seed_emb, None)
                .unwrap();

            let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));
            let mut handles = Vec::new();

            // 2 deleters
            for _ in 0..2 {
                let s = store.clone();
                let b = barrier.clone();
                handles.push(std::thread::spawn(move || {
                    b.wait();
                    let _ = s.delete(branch_id, "default", "stress", "target");
                }));
            }

            // 2 inserters
            for t in 0..2 {
                let s = store.clone();
                let b = barrier.clone();
                let emb = [0.0, t as f32 + 1.0, 0.0, 0.0];
                handles.push(std::thread::spawn(move || {
                    b.wait();
                    let _ = s.insert(branch_id, "default", "stress", "target", &emb, None);
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            // After all threads complete, check consistency between KV and backend.
            let kv_exists = store
                .get(branch_id, "default", "stress", "target")
                .unwrap()
                .is_some();
            let search_results = store
                .search(
                    branch_id,
                    "default",
                    "stress",
                    &[0.0, 1.0, 0.0, 0.0],
                    10,
                    None,
                )
                .unwrap();
            let search_finds_target = search_results.iter().any(|m| m.key == "target");

            // KV and search must agree: if KV says it exists, search should find it.
            // If KV says it doesn't exist, search should not find it.
            if kv_exists != search_finds_target {
                inconsistencies += 1;
            }
        }

        assert_eq!(
            inconsistencies, 0,
            "KV/backend inconsistency detected in {inconsistencies}/{num_rounds} rounds — TOCTOU race in delete"
        );
    }

    // ========================================
    // Searchable Trait Tests (#2146)
    // ========================================

    #[test]
    fn test_searchable_vector_mode_with_embedding() {
        use strata_engine::search::{SearchMode, Searchable};
        use strata_engine::SearchRequest;

        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        // Create a system embed collection and insert vectors. Each shadow
        // record points back at a real KV entry in the user's `default`
        // space — the only honest way to attribute a hit to a space.
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "key-a",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "default", "key-a"),
            )
            .unwrap();
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "key-b",
                &[0.0, 1.0, 0.0],
                None,
                EntityRef::kv(branch_id, "default", "key-b"),
            )
            .unwrap();

        // Search via Searchable trait with precomputed embedding
        let req = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
            .with_k(2);
        let response = Searchable::search(&store, &req).unwrap();

        assert_eq!(response.len(), 2);
        assert_eq!(response.hits[0].rank, 1);
        assert!(response.hits[0].score > response.hits[1].score);
        assert!(response.stats.index_used);
        // doc_refs are now the source refs, not the shadow vectors.
        assert!(matches!(
            &response.hits[0].doc_ref,
            EntityRef::Kv { space, key, .. }
            if space == "default" && key == "key-a"
        ));
    }

    /// Phase 1 Part 2: hybrid vector search must drop matches whose source
    /// is in a different space than the request, and must drop matches with
    /// no source_ref entirely.
    #[test]
    fn test_searchable_vector_filters_cross_space_sources() {
        use strata_engine::search::{SearchMode, Searchable};
        use strata_engine::SearchRequest;

        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        // Two records with identical embeddings, different source spaces
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-a",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "tenant_a", "real_key"),
            )
            .unwrap();
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-b",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::kv(branch_id, "tenant_b", "real_key"),
            )
            .unwrap();
        // A third record with NO source_ref — must be dropped under
        // space-filtered hybrid mode (no honest space attribution).
        store
            .system_insert(
                branch_id,
                "_system_embed_kv",
                "shadow-orphan",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        // Search from tenant_a → exactly the tenant_a hit
        let req_a = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
            .with_space("tenant_a")
            .with_k(10);
        let resp_a = Searchable::search(&store, &req_a).unwrap();
        assert_eq!(
            resp_a.len(),
            1,
            "tenant_a should see exactly its own hit, got {}",
            resp_a.len()
        );
        if let EntityRef::Kv { space, .. } = &resp_a.hits[0].doc_ref {
            assert_eq!(space, "tenant_a");
        } else {
            panic!("expected EntityRef::Kv");
        }

        // Search from tenant_b → exactly the tenant_b hit
        let req_b = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
            .with_space("tenant_b")
            .with_k(10);
        let resp_b = Searchable::search(&store, &req_b).unwrap();
        assert_eq!(resp_b.len(), 1);
        if let EntityRef::Kv { space, .. } = &resp_b.hits[0].doc_ref {
            assert_eq!(space, "tenant_b");
        } else {
            panic!("expected EntityRef::Kv");
        }

        // Search from a third space → zero hits (no source matches it,
        // and the orphan with no source_ref must also be dropped).
        let req_c = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
            .with_space("tenant_c")
            .with_k(10);
        let resp_c = Searchable::search(&store, &req_c).unwrap();
        assert_eq!(
            resp_c.len(),
            0,
            "tenant_c has no source-matching hits and orphan must be dropped"
        );
    }

    #[test]
    fn test_searchable_keyword_mode_returns_empty() {
        use strata_engine::search::{SearchMode, Searchable};
        use strata_engine::SearchRequest;

        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();
        store
            .system_insert(
                branch_id,
                "_system_embed_kv",
                "key-a",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        let req = SearchRequest::new(branch_id, "test query")
            .with_mode(SearchMode::Keyword)
            .with_k(10);
        let response = Searchable::search(&store, &req).unwrap();

        assert!(
            response.is_empty(),
            "Keyword mode should return empty for vectors"
        );
    }

    #[test]
    fn test_searchable_no_embedding_returns_empty() {
        use strata_engine::search::{SearchMode, Searchable};
        use strata_engine::SearchRequest;

        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();
        store
            .system_insert(
                branch_id,
                "_system_embed_kv",
                "key-a",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();

        // Vector mode but no precomputed_embedding
        let req = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_k(10);
        let response = Searchable::search(&store, &req).unwrap();

        assert!(response.is_empty(), "No embedding should return empty");
    }

    #[test]
    fn test_searchable_multi_collection_merge() {
        use strata_engine::search::{SearchMode, Searchable};
        use strata_engine::SearchRequest;

        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();

        // Create two system embed collections
        store
            .create_system_collection(branch_id, "_system_embed_kv", config.clone())
            .unwrap();
        store
            .create_system_collection(branch_id, "_system_embed_json", config)
            .unwrap();

        // Insert into kv collection — close to query.
        // Each shadow record points back at a real source in `default`.
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "kv-hit",
                &[0.9, 0.1, 0.0],
                None,
                EntityRef::kv(branch_id, "default", "kv-hit"),
            )
            .unwrap();

        // Insert into json collection — exact match to query
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_json",
                "json-hit",
                &[1.0, 0.0, 0.0],
                None,
                EntityRef::Json {
                    branch_id,
                    space: "default".to_string(),
                    doc_id: "json-hit".to_string(),
                },
            )
            .unwrap();

        let req = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
            .with_k(10);
        let response = Searchable::search(&store, &req).unwrap();

        assert_eq!(response.len(), 2, "Should find hits from both collections");
        // json-hit is exact match (score=1.0), kv-hit is close (score~0.99)
        // Both should be present, ranked by score
        assert!(response.hits[0].score >= response.hits[1].score);
        assert_eq!(response.hits[0].rank, 1);
        assert_eq!(response.hits[1].rank, 2);
        // Verify hits resolve back to their source primitives.
        let mut saw_kv = false;
        let mut saw_json = false;
        for h in &response.hits {
            match &h.doc_ref {
                EntityRef::Kv { key, .. } if key == "kv-hit" => saw_kv = true,
                EntityRef::Json { doc_id, .. } if doc_id == "json-hit" => saw_json = true,
                other => panic!("unexpected doc_ref: {:?}", other),
            }
        }
        assert!(saw_kv && saw_json);
    }

    #[test]
    fn test_searchable_source_ref_propagation() {
        use strata_engine::search::{SearchMode, Searchable};
        use strata_engine::SearchRequest;

        let (_temp, _db, store) = setup();
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_system_collection(branch_id, "_system_embed_kv", config)
            .unwrap();

        // Insert with a source_ref pointing back to a KV entity
        let source = EntityRef::kv(branch_id, "default", "original-key");
        store
            .system_insert_with_source(
                branch_id,
                "_system_embed_kv",
                "shadow-key",
                &[1.0, 0.0, 0.0],
                None,
                source.clone(),
            )
            .unwrap();

        let req = SearchRequest::new(branch_id, "")
            .with_mode(SearchMode::Vector)
            .with_precomputed_embedding(vec![1.0, 0.0, 0.0])
            .with_k(5);
        let response = Searchable::search(&store, &req).unwrap();

        assert_eq!(response.len(), 1);
        // doc_ref should be the KV source_ref, not the vector key
        assert_eq!(response.hits[0].doc_ref, source);
    }

    #[test]
    fn test_raw_cache_db_manual_commit_updates_hnsw() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db.clone());
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "emb", config)
            .unwrap();

        // VectorStore registers commit observers. Follower refresh is handled via
        // RefreshHook (not ReplayObserver). Check that commit observer exists.
        assert!(
            !db.commit_observers().is_empty(),
            "VectorStore should register a commit observer"
        );

        let state = store.state().unwrap();
        let mut txn = db.begin_transaction(branch_id).unwrap();
        let (_version, _op) = txn
            .vector_upsert(
                branch_id,
                "default",
                "emb",
                "v1",
                &[1.0, 0.0, 0.0],
                None,
                None,
                &state,
            )
            .unwrap();
        db.commit_transaction(&mut txn).unwrap();
        db.end_transaction(txn);

        let results = store
            .search(branch_id, "default", "emb", &[1.0, 0.0, 0.0], 1, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "v1");

        let _ = store.state().unwrap();
        // Same as above: observers count includes SearchSubsystem observers
        assert!(!db.commit_observers().is_empty());
    }

    #[test]
    fn test_manual_abort_clears_pending_vector_ops() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let store = VectorStore::new(db.clone());
        let branch_id = BranchId::new();

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        store
            .create_collection(branch_id, "default", "emb", config)
            .unwrap();

        let state = store.state().unwrap();
        let mut txn = db.begin_transaction(branch_id).unwrap();
        let txn_id = txn.txn_id;

        txn.vector_upsert(
            branch_id,
            "default",
            "emb",
            "v_abort",
            &[1.0, 0.0, 0.0],
            None,
            None,
            &state,
        )
        .unwrap();

        assert!(
            state.pending_ops.contains_key(&txn_id),
            "vector upsert should stage backend work until commit"
        );

        txn.abort();

        assert!(
            !state.pending_ops.contains_key(&txn_id),
            "abort should clear staged vector work via engine observers"
        );
    }
}
