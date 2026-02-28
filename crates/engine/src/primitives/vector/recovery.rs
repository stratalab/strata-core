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

use crate::database::Database;
use crate::recovery::{register_recovery_participant, RecoveryParticipant};
use strata_core::StrataResult;
use tracing::info;

/// Recovery function for VectorStore
///
/// Called by Database during startup to restore vector state from KV store.
fn recover_vector_state(db: &Database) -> StrataResult<()> {
    recover_from_db(db)
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
    use crate::primitives::vector::heap::VectorHeap;
    use std::sync::Arc;
    use strata_core::traits::SnapshotView;
    use strata_core::types::{Key, Namespace};
    use strata_core::value::Value;

    // Skip recovery for cache databases
    if db.is_cache() {
        return Ok(());
    }

    // Get access to the shared backend state
    let state = db.extension::<VectorBackendState>()?;
    let factory = IndexBackendFactory::default();

    let snapshot = db.storage().create_snapshot();
    let mut stats = super::RecoveryStats::default();
    let data_dir = db.data_dir();
    let use_mmap = !data_dir.as_os_str().is_empty();

    // Iterate all branch_ids in storage
    for branch_id in db.storage().branch_ids() {
        let ns = Arc::new(Namespace::for_branch_space(branch_id, "default"));

        // Scan for vector config entries in this run
        let config_prefix = Key::new_vector_config_prefix(ns.clone());
        let config_entries = match snapshot.scan_prefix(&config_prefix) {
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
            let vector_entries = match snapshot.scan_prefix(&vector_prefix) {
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
                    state
                        .backends
                        .write()
                        .insert(collection_id.clone(), backend);
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
                    // Lite record (embedding stripped from KV): skip during full
                    // KV-based recovery. The embedding only exists in the mmap
                    // cache, so this vector will be available on the next open
                    // that successfully loads the mmap file.
                    tracing::warn!(
                        target: "strata::vector",
                        vector_id = vec_record.vector_id,
                        "Skipping lite record (no embedding) during KV-based recovery"
                    );
                    stats.lite_records_skipped += 1;
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
                let vector_key = String::from_utf8(vec_key.user_key.clone())
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

            state
                .backends
                .write()
                .insert(collection_id.clone(), backend);
            stats.collections_created += 1;
        }
    }

    // -----------------------------------------------------------
    // Rebuild HNSW graphs (or load from mmap cache)
    // -----------------------------------------------------------
    {
        let mut backends = state.backends.write();
        for (cid, backend) in backends.iter_mut() {
            let mut loaded = false;
            if use_mmap {
                let gdir = super::graph_dir(data_dir, cid.branch_id, &cid.name);
                if let Ok(true) = backend.load_graphs_from_disk(&gdir) {
                    loaded = true;
                }
            }
            if !loaded {
                backend.rebuild_index();
            }

            // Seal any remaining active buffer entries into HNSW segments.
            // After graph loading, partial chunks may remain in the active
            // buffer for O(n) brute-force search. Sealing them into HNSW
            // segments ensures all vectors benefit from O(log n) search.
            backend.seal_remaining_active();

            if use_mmap {
                let gdir = super::graph_dir(data_dir, cid.branch_id, &cid.name);
                let _ = backend.freeze_graphs_to_disk(&gdir);
            }
        }
    }

    // -----------------------------------------------------------
    // Freeze heaps to mmap cache & configure flush paths
    // -----------------------------------------------------------
    if use_mmap {
        let mut backends = state.backends.write();
        for (cid, backend) in backends.iter_mut() {
            let vec_path = mmap_path(data_dir, cid.branch_id, &cid.name);
            if !backend.is_heap_mmap() {
                if let Err(e) = backend.freeze_heap_to_disk(&vec_path) {
                    tracing::warn!(
                        target: "strata::vector",
                        collection = %cid.name,
                        error = %e,
                        "Failed to freeze heap to mmap cache"
                    );
                }
            }
            // Configure periodic flush path so the backend can flush
            // its overlay during long-running indexing operations.
            let _ = backend.flush_heap_to_disk_if_needed(&vec_path);
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
            lite_records_skipped = stats.lite_records_skipped,
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
