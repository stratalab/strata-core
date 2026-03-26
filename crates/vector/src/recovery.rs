//! Vector Recovery Participant
//!
//! Registers VectorStore as a recovery participant so that vector state
//! (in-memory backends with embeddings) is restored when the Database reopens.
//!
//! ## How It Works
//!
//! 1. `register_vector_recovery()` registers a recovery function with the engine
//! 2. When `Database::open()` runs, it calls all registered recovery participants
//! 3. The vector recovery function scans KV store for vector config and data entries
//! 4. For each collection config found, it creates a backend and loads embeddings
//! 5. The Database is ready with all vector embeddings restored
//!
//! ## mmap Acceleration
//!
//! After KV-based recovery, the global VectorHeap is frozen to a `.vec` mmap
//! file under `{data_dir}/vectors/{branch_hex}/{collection}.vec`.
//!
//! On subsequent opens the recovery path tries to load the heap from the mmap
//! cache first.  If the file is present and valid, the KV scan for vector
//! entries is replaced by a lightweight timestamp-only scan: the heap is
//! already populated from the mmap, and we only register each vector's
//! ID + timestamp so that `rebuild_index()` can rebuild the HNSW graph.
//!
//! Sealed segment graphs are also frozen to `.hgr` files under
//! `{data_dir}/vectors/{branch_hex}/{collection}_graphs/seg_{id}.hgr`.
//! On subsequent opens, `load_graphs_from_disk()` loads the pre-built graphs
//! (with neighbor data mmap-backed), skipping the expensive `rebuild_index()`.
//!
//! All mmap files are **caches** — if missing, corrupt, or with a dimension
//! mismatch, recovery falls back transparently to full KV-based rebuild
//! with no data loss.

use strata_core::StrataResult;
use strata_engine::recovery::{register_recovery_participant, RecoveryParticipant};
use strata_engine::Database;
use tracing::info;

/// Recovery function for VectorStore
///
/// Called by Database during startup to restore vector state from KV store.
fn recover_vector_state(db: &Database) -> StrataResult<()> {
    recover_from_db(db)?;

    // Register the vector refresh hook for follower refresh support
    register_vector_refresh_hook(db);

    Ok(())
}

/// Compute the `.vec` mmap cache path for a given collection.
pub(crate) fn mmap_path(
    data_dir: &std::path::Path,
    branch_id: strata_core::types::BranchId,
    collection_name: &str,
) -> std::path::PathBuf {
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    data_dir
        .join("vectors")
        .join(branch_hex)
        .join(format!("{}.vec", collection_name))
}

/// Internal recovery implementation that works with &Database
fn recover_from_db(db: &Database) -> StrataResult<()> {
    use super::{CollectionId, IndexBackendFactory, VectorBackendState, VectorConfig, VectorId};
    use crate::heap::VectorHeap;
    use std::sync::Arc;
    use strata_core::traits::Storage;
    use strata_core::types::{Key, Namespace};
    use strata_core::value::Value;

    // Skip recovery for cache databases
    if db.is_cache() {
        return Ok(());
    }

    // Get access to the shared backend state
    let state = db.extension::<VectorBackendState>()?;
    let factory = IndexBackendFactory::default();

    let snapshot_version = db.storage().version();
    let mut stats = super::RecoveryStats::default();
    let data_dir = db.data_dir();
    let use_mmap = !data_dir.as_os_str().is_empty();
    // Followers must never write to the primary's data directory.
    let can_write_disk = use_mmap && !db.is_follower();

    // Iterate all branch_ids in storage
    for branch_id in db.storage().branch_ids() {
        let ns = Arc::new(Namespace::for_branch_space(branch_id, "default"));

        // Scan for vector config entries in this run
        let config_prefix = Key::new_vector_config_prefix(ns.clone());
        let config_entries = match db.storage().scan_prefix(&config_prefix, snapshot_version) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    target: "strata::vector",
                    branch_id = ?branch_id,
                    error = %e,
                    "Failed to scan vector configs during recovery"
                );
                continue;
            }
        };

        for (key, versioned) in &config_entries {
            // Parse the collection config from the KV value
            let config_bytes = match &versioned.value {
                Value::Bytes(b) => b,
                _ => continue,
            };

            // Decode the CollectionRecord
            let record = match super::CollectionRecord::from_bytes(config_bytes) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        key = ?key,
                        error = %e,
                        "Failed to decode collection record during recovery, skipping"
                    );
                    continue;
                }
            };

            // Extract collection name from the key's user_key
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
                        "Failed to convert collection config during recovery, skipping"
                    );
                    continue;
                }
            };
            let collection_id = CollectionId::new(branch_id, &collection_name);

            // Create backend for this collection
            let mut backend = factory.create(&config);

            // -----------------------------------------------------------
            // Try mmap-accelerated recovery: load heap from disk cache.
            // -----------------------------------------------------------
            let mut loaded_from_mmap = false;
            if use_mmap {
                let vec_path = mmap_path(data_dir, branch_id, &collection_name);
                if vec_path.exists() {
                    match VectorHeap::from_mmap(&vec_path, config.clone()) {
                        Ok(heap) => {
                            if heap.is_empty() {
                                tracing::debug!(
                                    target: "strata::vector",
                                    collection = %collection_name,
                                    "Mmap cache is empty, falling back to KV"
                                );
                            } else {
                                backend.replace_heap(heap);
                                loaded_from_mmap = true;
                                tracing::debug!(
                                    target: "strata::vector",
                                    collection = %collection_name,
                                    "Loaded heap from mmap cache"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: "strata::vector",
                                collection = %collection_name,
                                error = %e,
                                "Failed to load mmap cache, falling back to KV"
                            );
                        }
                    }
                }
            }

            // -----------------------------------------------------------
            // Scan KV for vector entries
            // -----------------------------------------------------------
            let vector_prefix = Key::new_vector(ns.clone(), &collection_name, "");
            let vector_entries = match db.storage().scan_prefix(&vector_prefix, snapshot_version) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        collection = %collection_name,
                        error = %e,
                        "Failed to scan vectors during recovery"
                    );
                    // If mmap loaded, the backend has embeddings but no timestamps;
                    // proceed anyway so rebuild_index() at least builds the graph.
                    state.backends.insert(collection_id.clone(), backend);
                    stats.collections_created += 1;
                    continue;
                }
            };

            let collection_prefix = format!("{}/", collection_name);
            for (vec_key, vec_versioned) in &vector_entries {
                let vec_bytes = match &vec_versioned.value {
                    Value::Bytes(b) => b,
                    _ => continue,
                };

                // Decode the VectorRecord
                let vec_record = match super::VectorRecord::from_bytes(vec_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            error = %e,
                            "Failed to decode vector record during recovery, skipping"
                        );
                        continue;
                    }
                };

                let vid = VectorId::new(vec_record.vector_id);

                if loaded_from_mmap && backend.get(vid).is_some() {
                    // Heap already has the embedding — just register ID + timestamp
                    backend.register_mmap_vector(vid, vec_record.created_at);
                    stats.vectors_mmap_registered += 1;
                } else if loaded_from_mmap && !vec_record.embedding.is_empty() {
                    // Vector in KV but not in mmap (added after last freeze) — insert from KV
                    let _ = backend.insert_with_id_and_timestamp(
                        vid,
                        &vec_record.embedding,
                        vec_record.created_at,
                    );
                    stats.vectors_upserted += 1;
                } else if vec_record.embedding.is_empty() {
                    // Legacy record with no embedding (pre-#1962 lite mode or
                    // old records deserialized via #[serde(default)]). Cannot
                    // recover without the embedding — skip.
                    tracing::warn!(
                        target: "strata::vector",
                        vector_id = vec_record.vector_id,
                        "Skipping record with empty embedding during recovery"
                    );
                    continue;
                } else {
                    // Full KV-based recovery: insert embedding + timestamp
                    let _ = backend.insert_with_id_and_timestamp(
                        vid,
                        &vec_record.embedding,
                        vec_record.created_at,
                    );
                    stats.vectors_upserted += 1;
                }

                // Populate inline metadata for O(1) search resolution
                let vector_key = String::from_utf8(vec_key.user_key.to_vec())
                    .ok()
                    .and_then(|uk| uk.strip_prefix(&collection_prefix).map(|s| s.to_string()))
                    .unwrap_or_default();
                backend.set_inline_meta(
                    vid,
                    super::types::InlineMeta {
                        key: vector_key,
                        source_ref: vec_record.source_ref.clone(),
                    },
                );
            }

            state.backends.insert(collection_id.clone(), backend);
            stats.collections_created += 1;
        }
    }

    // -----------------------------------------------------------
    // Rebuild HNSW graphs (or load from mmap cache)
    // -----------------------------------------------------------
    {
        for mut entry in state.backends.iter_mut() {
            let mut loaded = false;
            if use_mmap {
                let gdir = super::graph_dir(data_dir, entry.key().branch_id, &entry.key().name);
                if let Ok(true) = entry.value_mut().load_graphs_from_disk(&gdir) {
                    loaded = true;
                }
            }
            if !loaded {
                entry.value_mut().rebuild_index();
            }

            // Seal any remaining active buffer entries into HNSW segments.
            // After graph loading, partial chunks may remain in the active
            // buffer for O(n) brute-force search. Sealing them into HNSW
            // segments ensures all vectors benefit from O(log n) search.
            entry.value_mut().seal_remaining_active();

            if can_write_disk {
                let gdir = super::graph_dir(data_dir, entry.key().branch_id, &entry.key().name);
                let _ = entry.value_mut().freeze_graphs_to_disk(&gdir);
            }
        }
    }

    // -----------------------------------------------------------
    // Freeze heaps to mmap cache & configure flush paths
    // -----------------------------------------------------------
    if can_write_disk {
        for mut entry in state.backends.iter_mut() {
            let vec_path = mmap_path(data_dir, entry.key().branch_id, &entry.key().name);
            if !entry.value().is_heap_mmap() {
                let name = entry.key().name.clone();
                if let Err(e) = entry.value_mut().freeze_heap_to_disk(&vec_path) {
                    tracing::warn!(
                        target: "strata::vector",
                        collection = %name,
                        error = %e,
                        "Failed to freeze heap to mmap cache"
                    );
                }
            }
            // Configure periodic flush path so the backend can flush
            // its overlay during long-running indexing operations.
            let _ = entry.value_mut().flush_heap_to_disk_if_needed(&vec_path);
        }
    }

    if stats.collections_created > 0
        || stats.vectors_upserted > 0
        || stats.vectors_mmap_registered > 0
    {
        info!(
            target: "strata::vector",
            collections_created = stats.collections_created,
            vectors_upserted = stats.vectors_upserted,
            vectors_mmap_registered = stats.vectors_mmap_registered,
            mmap_cache = use_mmap,
            "Vector recovery complete"
        );
    }

    Ok(())
}

/// Register VectorStore as a recovery participant
///
/// Call this once during application startup, before opening any Database.
/// This ensures that vector state (in-memory backends with embeddings) is
/// automatically restored when a Database is reopened.
pub fn register_vector_recovery() {
    register_recovery_participant(RecoveryParticipant::new("vector", recover_vector_state));
}

/// Subsystem implementation for vector recovery and shutdown hooks.
///
/// Used with `DatabaseBuilder` for explicit subsystem registration.
pub struct VectorSubsystem;

impl strata_engine::recovery::Subsystem for VectorSubsystem {
    fn name(&self) -> &'static str {
        "vector"
    }

    fn recover(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
    ) -> strata_core::StrataResult<()> {
        recover_vector_state(db)
    }

    fn freeze(&self, db: &strata_engine::Database) -> strata_core::StrataResult<()> {
        db.freeze_vector_heaps()
    }
}

// =============================================================================
// Refresh Hook Implementation
// =============================================================================

/// Register the vector refresh hook with the database.
///
/// This allows the engine's follower refresh to incrementally update
/// vector backends without knowing the concrete vector types.
fn register_vector_refresh_hook(db: &Database) {
    use std::sync::Arc;

    let state = match db.extension::<super::VectorBackendState>() {
        Ok(s) => s,
        Err(_) => return,
    };

    let hook = Arc::new(VectorRefreshHook { state });

    if let Ok(hooks) = db.extension::<strata_engine::RefreshHooks>() {
        hooks.register(hook);
    }
}

/// Refresh hook implementation for vector backends.
struct VectorRefreshHook {
    state: std::sync::Arc<super::VectorBackendState>,
}

// Safety: VectorBackendState uses DashMap which is Send + Sync
unsafe impl Send for VectorRefreshHook {}
unsafe impl Sync for VectorRefreshHook {}

impl strata_engine::RefreshHook for VectorRefreshHook {
    fn pre_delete_read(
        &self,
        db: &strata_engine::Database,
        deletes: &[strata_core::types::Key],
    ) -> Vec<(strata_core::types::Key, Vec<u8>)> {
        use strata_core::traits::Storage;
        use strata_core::types::TypeTag;

        let mut pre_reads = Vec::new();
        for key in deletes {
            if key.type_tag == TypeTag::Vector {
                if let Ok(Some(vv)) = db.storage().get_versioned(key, u64::MAX) {
                    if let strata_core::value::Value::Bytes(ref bytes) = vv.value {
                        pre_reads.push((key.clone(), bytes.clone()));
                    }
                }
            }
        }
        pre_reads
    }

    fn apply_refresh(
        &self,
        puts: &[(strata_core::types::Key, strata_core::value::Value)],
        pre_read_deletes: &[(strata_core::types::Key, Vec<u8>)],
    ) {
        use strata_core::primitives::vector::{CollectionId, VectorId};
        use strata_core::types::TypeTag;

        // Vector puts: insert into backend
        for (key, value) in puts {
            if key.type_tag != TypeTag::Vector {
                continue;
            }
            let bytes = match value {
                strata_core::value::Value::Bytes(b) => b,
                _ => continue,
            };
            let record = match super::VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let user_key_str = match key.user_key_string() {
                Some(s) => s,
                None => continue,
            };
            let (collection, vector_key) = match user_key_str.split_once('/') {
                Some(pair) => pair,
                None => continue,
            };
            let branch_id = key.namespace.branch_id;
            let cid = CollectionId::new(branch_id, collection);
            let vid = VectorId::new(record.vector_id);

            if let Some(mut backend) = self.state.backends.get_mut(&cid) {
                if let Err(e) =
                    backend.insert_with_timestamp(vid, &record.embedding, record.created_at)
                {
                    tracing::warn!(
                        target: "strata::refresh",
                        collection = collection,
                        vector_key = vector_key,
                        error = %e,
                        "Vector insert failed during refresh"
                    );
                }
                backend.set_inline_meta(
                    vid,
                    super::types::InlineMeta {
                        key: vector_key.to_string(),
                        source_ref: record.source_ref.clone(),
                    },
                );
            } else {
                tracing::debug!(
                    target: "strata::refresh",
                    collection = collection,
                    "Skipping vector insert for unknown collection (will be picked up on restart)"
                );
            }
        }

        // Vector deletes: remove from backend using pre-reads
        for (key, bytes) in pre_read_deletes {
            let record = match super::VectorRecord::from_bytes(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let user_key_str = match key.user_key_string() {
                Some(s) => s,
                None => continue,
            };
            let collection = match user_key_str.split_once('/') {
                Some((coll, _)) => coll,
                None => continue,
            };
            let branch_id = key.namespace.branch_id;
            let cid = CollectionId::new(branch_id, collection);
            let vid = VectorId::new(record.vector_id);

            if let Some(mut backend) = self.state.backends.get_mut(&cid) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0);
                if let Err(e) = backend.delete_with_timestamp(vid, now) {
                    tracing::warn!(
                        target: "strata::refresh",
                        collection = collection,
                        error = %e,
                        "Vector delete failed during refresh"
                    );
                }
                backend.remove_inline_meta(vid);
            }
        }
    }

    fn freeze_to_disk(&self, db: &strata_engine::Database) -> strata_core::StrataResult<()> {
        let data_dir = db.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no mmap
        }

        for entry in self.state.backends.iter() {
            let (cid, backend) = (entry.key(), entry.value());
            let branch_hex = format!("{:032x}", u128::from_be_bytes(*cid.branch_id.as_bytes()));
            let vec_path = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}.vec", cid.name));
            backend.freeze_heap_to_disk(&vec_path)?;

            // Also freeze graphs
            let gdir = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}_graphs", cid.name));
            backend.freeze_graphs_to_disk(&gdir)?;
        }
        Ok(())
    }

    fn post_merge_reload(
        &self,
        db: &strata_engine::Database,
        target_branch: strata_core::types::BranchId,
        source_branch: Option<strata_core::types::BranchId>,
    ) -> strata_core::StrataResult<()> {
        // Reload vector backends from KV for the target branch.
        // This is a simplified inline version that doesn't require Arc<Database>.
        use strata_core::traits::Storage;
        use strata_core::types::{Key, Namespace};
        use strata_core::value::Value;

        let factory = super::IndexBackendFactory::default();
        let version = db.storage().version();
        let ns = std::sync::Arc::new(Namespace::for_branch_space(target_branch, "default"));

        let config_prefix = Key::new_vector_config_prefix(ns.clone());
        let config_entries = db
            .storage()
            .scan_prefix(&config_prefix, version)
            .map_err(|e| strata_core::StrataError::storage(e.to_string()))?;

        for (key, versioned) in &config_entries {
            let config_bytes = match &versioned.value {
                Value::Bytes(b) => b,
                _ => continue,
            };
            let record = match super::CollectionRecord::from_bytes(config_bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let collection_name = match key.user_key_string() {
                Some(name) => name,
                None => continue,
            };
            let config: super::VectorConfig = match record.config.try_into() {
                Ok(c) => c,
                Err(_) => continue,
            };
            let cid = super::CollectionId::new(target_branch, &collection_name);

            let mut backend = factory.create(&config);
            let vector_prefix = Key::new_vector(ns.clone(), &collection_name, "");
            let vector_entries = match db.storage().scan_prefix(&vector_prefix, version) {
                Ok(e) => e,
                Err(_) => continue,
            };

            let collection_prefix = format!("{}/", collection_name);
            for (vec_key, vec_versioned) in &vector_entries {
                let vec_bytes = match &vec_versioned.value {
                    Value::Bytes(b) => b,
                    _ => continue,
                };
                let vec_record = match super::VectorRecord::from_bytes(vec_bytes) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let vid = super::VectorId::new(vec_record.vector_id);

                if vec_record.embedding.is_empty() {
                    // Legacy empty-embedding record — try to copy from source branch backend
                    if let Some(src) = source_branch {
                        let src_cid = super::CollectionId::new(src, &collection_name);
                        if let Some(src_backend) = self.state.backends.get(&src_cid) {
                            if let Some(emb) = src_backend.get(vid) {
                                let _ = backend.insert_with_id_and_timestamp(
                                    vid,
                                    emb,
                                    vec_record.created_at,
                                );
                            }
                        }
                    }
                } else {
                    let _ = backend.insert_with_id_and_timestamp(
                        vid,
                        &vec_record.embedding,
                        vec_record.created_at,
                    );
                }

                let vector_key = String::from_utf8(vec_key.user_key.to_vec())
                    .ok()
                    .and_then(|uk| uk.strip_prefix(&collection_prefix).map(|s| s.to_string()))
                    .unwrap_or_default();
                backend.set_inline_meta(
                    vid,
                    super::types::InlineMeta {
                        key: vector_key,
                        source_ref: vec_record.source_ref.clone(),
                    },
                );
            }

            backend.rebuild_index();
            backend.seal_remaining_active();
            self.state.backends.insert(cid, backend);
        }
        Ok(())
    }
}
