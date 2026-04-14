//! Vector Recovery
//!
//! `VectorSubsystem` restores vector state (in-memory backends with embeddings)
//! when the Database reopens.
//!
//! ## How It Works
//!
//! 1. `OpenSpec::with_subsystem(VectorSubsystem)` installs the subsystem at open time
//! 2. `VectorSubsystem::recover` scans KV store for vector config and data entries
//! 3. For each collection config found, it creates a backend and loads embeddings
//! 4. The Database is ready with all vector embeddings restored
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

use strata_core::id::CommitVersion;
use strata_core::StrataResult;
use strata_engine::database::observers::{AbortInfo, AbortObserver};
use strata_engine::Database;
use tracing::info;

/// Recovery function for VectorStore
///
/// Called by Database during startup to restore vector state from KV store.
fn recover_vector_state(db: &Database) -> StrataResult<()> {
    recover_from_db(db)?;

    // Register the lifecycle hook used for freeze-to-disk and merge reloads.
    register_vector_lifecycle_hook(db);

    Ok(())
}

/// Compute the `.vec` mmap cache path for a given collection.
/// On-disk path for the heap mmap of a collection.
///
/// Layout: `{data_dir}/vectors/{branch_hex}/{space}/{collection_name}.vec`.
/// Including `space` as a subdirectory ensures two collections with the
/// same `(branch_id, name)` in different spaces never collide on disk.
/// Space names are validated to `[a-z0-9_-]` (`crates/core/src/types.rs`)
/// so they are always filesystem-safe — no escaping needed. The reserved
/// `_system_` space is also safe.
pub(crate) fn mmap_path(
    data_dir: &std::path::Path,
    branch_id: strata_core::types::BranchId,
    space: &str,
    collection_name: &str,
) -> std::path::PathBuf {
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    data_dir
        .join("vectors")
        .join(branch_hex)
        .join(space)
        .join(format!("{}.vec", collection_name))
}

/// Wipe the on-disk vector caches (`.vec` heap mmap and `_graphs/` graph
/// dir) for a single collection.
///
/// Best-effort: missing files are ignored, I/O failures are logged as
/// warnings but never returned. Used by `VectorStore::delete_collection`
/// (single-collection delete) and `VectorStore::purge_collections_in_space`
/// (force-delete cleanup) so that a deleted collection cannot resurrect
/// itself from a stale on-disk cache after the next recovery pass.
pub(crate) fn purge_collection_disk_cache(data_dir: &std::path::Path, cid: &super::CollectionId) {
    if data_dir.as_os_str().is_empty() {
        return;
    }
    let vec_path = mmap_path(data_dir, cid.branch_id, &cid.space, &cid.name);
    if vec_path.exists() {
        if let Err(e) = std::fs::remove_file(&vec_path) {
            tracing::warn!(
                target: "strata::vector",
                path = ?vec_path,
                error = %e,
                "Failed to remove vector mmap cache during purge"
            );
        }
    }
    let gdir = super::graph_dir(data_dir, cid.branch_id, &cid.space, &cid.name);
    if gdir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&gdir) {
            tracing::warn!(
                target: "strata::vector",
                path = ?gdir,
                error = %e,
                "Failed to remove vector graph dir during purge"
            );
        }
    }
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

    let snapshot_version = CommitVersion(db.storage().version());
    let mut stats = super::RecoveryStats::default();
    let data_dir = db.data_dir();
    let use_mmap = !data_dir.as_os_str().is_empty();
    // Followers must never write to the primary's data directory.
    let can_write_disk = use_mmap && !db.is_follower();

    // Iterate all branch_ids in storage
    for branch_id in db.storage().branch_ids() {
        // Enumerate every space registered in this branch's metadata, plus
        // "default" (always implicit) and `_system_` (shadow collections live
        // there but `SpaceIndex::list` strips it). Without enumerating all
        // spaces, vector collections in non-default user spaces would be
        // silently invisible to startup recovery and only resurface via the
        // lazy reload fallback — see space-correctness-fix-plan §4.5.
        //
        // We scan space metadata directly via the storage layer rather than
        // calling `SpaceIndex::list`, because that helper requires
        // `Arc<Database>` and recovery only has `&Database`.
        //
        // Phase 3 cross-reference: this metadata scan now matches the
        // semantics of `SpaceIndex::discover_used_spaces` + `list` (union
        // of metadata and discovery) because Phase 3's startup repair runs
        // BEFORE `VectorSubsystem::recover`. By the time we get here, every
        // space with real data has a metadata entry, so the metadata-only
        // scan below is functionally equivalent to a full data scan. If
        // that ordering ever changes, switch this loop to use
        // `SpaceIndex::discover_used_spaces` directly (see
        // `crates/engine/src/primitives/space.rs`).
        let space_prefix = Key::new_space_prefix(branch_id);
        let mut spaces: Vec<String> = match db
            .storage()
            .scan_prefix(&space_prefix, snapshot_version)
        {
            Ok(entries) => entries
                .into_iter()
                .filter_map(|(k, _)| String::from_utf8(k.user_key.to_vec()).ok())
                .filter(|s| s != strata_engine::system_space::SYSTEM_SPACE)
                .collect(),
            Err(e) => {
                tracing::warn!(
                    target: "strata::vector",
                    branch_id = ?branch_id,
                    error = %e,
                    "Failed to list spaces during vector recovery; falling back to default + system",
                );
                Vec::new()
            }
        };
        if !spaces.iter().any(|s| s == "default") {
            spaces.insert(0, "default".to_string());
        }
        spaces.push(strata_engine::system_space::SYSTEM_SPACE.to_string());
        for space in &spaces {
            let space = space.as_str();
            let ns = Arc::new(Namespace::for_branch_space(branch_id, space));

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

                // Extract collection name from the key's user_key ("__config__/{name}")
                let collection_name = match key.user_key_string() {
                    Some(raw) => raw.strip_prefix("__config__/").unwrap_or(&raw).to_string(),
                    None => continue,
                };

                // Read backend type before consuming record.config (Issue #1964)
                let backend_type = record.backend_type();

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
                let collection_id = CollectionId::new(branch_id, space, &collection_name);

                let factory = IndexBackendFactory::from_type(backend_type);
                let mut backend = factory.create(&config);

                // -----------------------------------------------------------
                // Try mmap-accelerated recovery: load heap from disk cache.
                // -----------------------------------------------------------
                let mut loaded_from_mmap = false;
                if use_mmap {
                    let vec_path = mmap_path(data_dir, branch_id, space, &collection_name);
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
                let vector_entries =
                    match db.storage().scan_prefix(&vector_prefix, snapshot_version) {
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
        } // for space in spaces
    }

    // -----------------------------------------------------------
    // Rebuild HNSW graphs (or load from mmap cache)
    // -----------------------------------------------------------
    {
        for mut entry in state.backends.iter_mut() {
            let mut loaded = false;
            if use_mmap {
                let gdir = super::graph_dir(
                    data_dir,
                    entry.key().branch_id,
                    &entry.key().space,
                    &entry.key().name,
                );
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
                let gdir = super::graph_dir(
                    data_dir,
                    entry.key().branch_id,
                    &entry.key().space,
                    &entry.key().name,
                );
                let _ = entry.value_mut().freeze_graphs_to_disk(&gdir);
            }
        }
    }

    // -----------------------------------------------------------
    // Freeze heaps to mmap cache & configure flush paths
    // -----------------------------------------------------------
    if can_write_disk {
        for mut entry in state.backends.iter_mut() {
            let vec_path = mmap_path(
                data_dir,
                entry.key().branch_id,
                &entry.key().space,
                &entry.key().name,
            );
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

/// Subsystem implementation for vector recovery and shutdown hooks.
///
/// Used with `OpenSpec::with_subsystem(VectorSubsystem)` for explicit subsystem registration.
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

    fn initialize(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
    ) -> strata_core::StrataResult<()> {
        let state = db.extension::<super::VectorBackendState>()?;
        ensure_runtime_wiring(db, &state);
        Ok(())
    }

    fn freeze(&self, db: &strata_engine::Database) -> strata_core::StrataResult<()> {
        db.freeze_vector_heaps()
    }
}

// =============================================================================
// Vector Commit Observer
// =============================================================================

use strata_engine::database::observers::{
    CommitInfo, CommitObserver, ObserverError, ReplayInfo, ReplayObserver,
};

pub(crate) fn ensure_runtime_wiring(
    db: &std::sync::Arc<strata_engine::Database>,
    state: &std::sync::Arc<super::VectorBackendState>,
) {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    if state.runtime_wired.swap(true, Ordering::AcqRel) {
        return;
    }

    db.merge_registry().register_vector(
        crate::merge_handler::vector_precheck_fn,
        crate::merge_handler::vector_post_commit_fn,
    );

    let commit_observer = Arc::new(VectorCommitObserver {
        db: Arc::downgrade(db),
    });
    db.commit_observers().register(commit_observer);

    let abort_observer = Arc::new(VectorAbortObserver {
        db: Arc::downgrade(db),
    });
    db.abort_observers().register(abort_observer);

    let replay_observer = Arc::new(VectorReplayObserver {
        db: Arc::downgrade(db),
    });
    db.replay_observers().register(replay_observer);
}

/// Observer that applies pending HNSW backend operations after commit.
///
/// Vector write methods queue `StagedVectorOp`s in `VectorBackendState` during
/// transactions. This observer applies them after successful commit, updating
/// the in-memory HNSW indices.
///
/// This moves vector backend maintenance ownership from executor-local deferred
/// work to subsystem-owned observers (VectorSubsystem), fulfilling the T2-E2
/// requirement.
struct VectorCommitObserver {
    db: std::sync::Weak<strata_engine::Database>,
}

impl CommitObserver for VectorCommitObserver {
    fn name(&self) -> &'static str {
        "vector"
    }

    fn on_commit(&self, info: &CommitInfo) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(()); // Database dropped
        };

        // Get vector backend state and apply pending ops for this commit only.
        if let Ok(state) = db.extension::<super::VectorBackendState>() {
            let applied = state.apply_pending_ops(info.txn_id);
            if applied > 0 {
                tracing::debug!(
                    target: "strata::vector",
                    txn_id = info.txn_id.0,
                    branch_id = ?info.branch_id,
                    commit_version = info.commit_version.0,
                    ops_applied = applied,
                    "Applied pending HNSW operations"
                );
            }
        }

        Ok(())
    }
}

struct VectorAbortObserver {
    db: std::sync::Weak<strata_engine::Database>,
}

impl AbortObserver for VectorAbortObserver {
    fn name(&self) -> &'static str {
        "vector-abort"
    }

    fn on_abort(&self, info: &AbortInfo) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };

        if let Ok(state) = db.extension::<super::VectorBackendState>() {
            state.clear_pending_ops(info.txn_id);
        }

        Ok(())
    }
}

struct VectorReplayObserver {
    db: std::sync::Weak<strata_engine::Database>,
}

impl ReplayObserver for VectorReplayObserver {
    fn name(&self) -> &'static str {
        "vector"
    }

    fn on_replay(&self, info: &ReplayInfo) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };

        if let Ok(state) = db.extension::<super::VectorBackendState>() {
            apply_replayed_vector_changes(&state, &info.puts, &info.deleted_values);
        }

        Ok(())
    }
}

// =============================================================================
// Lifecycle Hook Implementation
// =============================================================================

fn apply_replayed_vector_changes(
    state: &super::VectorBackendState,
    puts: &[(strata_core::types::Key, strata_core::value::Value)],
    deleted_values: &[(strata_core::types::Key, strata_core::value::Value)],
) {
    use strata_core::primitives::vector::{CollectionId, VectorId};
    use strata_core::types::TypeTag;

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
        let cid = CollectionId::new(branch_id, key.namespace.space.as_str(), collection);
        let vid = VectorId::new(record.vector_id);

        if let Some(mut backend) = state.backends.get_mut(&cid) {
            if let Err(e) = backend.insert_with_timestamp(vid, &record.embedding, record.created_at)
            {
                tracing::warn!(
                    target: "strata::refresh",
                    collection = collection,
                    vector_key = vector_key,
                    error = %e,
                    "Vector insert failed during replay"
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

    for (key, value) in deleted_values {
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
        let collection = match user_key_str.split_once('/') {
            Some((coll, _)) => coll,
            None => continue,
        };
        let branch_id = key.namespace.branch_id;
        let cid = CollectionId::new(branch_id, key.namespace.space.as_str(), collection);
        let vid = VectorId::new(record.vector_id);

        if let Some(mut backend) = state.backends.get_mut(&cid) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            if let Err(e) = backend.delete_with_timestamp(vid, now) {
                tracing::warn!(
                    target: "strata::refresh",
                    collection = collection,
                    error = %e,
                    "Vector delete failed during replay"
                );
            }
            backend.remove_inline_meta(vid);
        }
    }
}

/// Register the vector lifecycle hook with the database.
///
/// This hook only owns freeze-to-disk and post-merge reload behaviors.
fn register_vector_lifecycle_hook(db: &Database) {
    use std::sync::Arc;

    let state = match db.extension::<super::VectorBackendState>() {
        Ok(s) => s,
        Err(_) => return,
    };

    let hook = Arc::new(VectorLifecycleHook { state });

    if let Ok(hooks) = db.extension::<strata_engine::RefreshHooks>() {
        hooks.register(hook);
    }
}

/// Lifecycle hook implementation for vector backends.
struct VectorLifecycleHook {
    state: std::sync::Arc<super::VectorBackendState>,
}

// Safety: VectorBackendState uses DashMap which is Send + Sync
unsafe impl Send for VectorLifecycleHook {}
unsafe impl Sync for VectorLifecycleHook {}

impl strata_engine::RefreshHook for VectorLifecycleHook {
    fn pre_delete_read(
        &self,
        _db: &strata_engine::Database,
        _deletes: &[strata_core::types::Key],
    ) -> Vec<(strata_core::types::Key, Vec<u8>)> {
        Vec::new()
    }

    fn apply_refresh(
        &self,
        _puts: &[(strata_core::types::Key, strata_core::value::Value)],
        _pre_read_deletes: &[(strata_core::types::Key, Vec<u8>)],
    ) {
        // Follower-side vector maintenance now lives in VectorReplayObserver.
    }

    fn freeze_to_disk(&self, db: &strata_engine::Database) -> strata_core::StrataResult<()> {
        let data_dir = db.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no mmap
        }

        for entry in self.state.backends.iter() {
            let (cid, backend) = (entry.key(), entry.value());
            // Use the space-aware path helpers so freeze writes to the same
            // location recovery reads from. Without `cid.space` in the path,
            // two collections sharing `(branch_id, name)` across different
            // spaces would clobber each other's `.vec` and `_graphs/` files.
            let vec_path = mmap_path(data_dir, cid.branch_id, &cid.space, &cid.name);
            backend.freeze_heap_to_disk(&vec_path)?;

            let gdir = super::graph_dir(data_dir, cid.branch_id, &cid.space, &cid.name);
            backend.freeze_graphs_to_disk(&gdir)?;
        }
        Ok(())
    }

    // post_merge_reload uses the trait default (no-op).
    //
    // The vector merge post-commit lifecycle moved to
    // `VectorMergeHandler::post_commit` (via `register_vector_semantic_merge`)
    // — see crates/vector/src/merge_handler.rs. The handler does a
    // *per-collection* rebuild, scoped to the collections the merge
    // actually touched, instead of the full-branch rebuild this method
    // used to do. The full-branch path also had a real bug — it only
    // scanned the "default" namespace and silently lost vectors in user
    // spaces. The handler iterates affected (space, collection) pairs
    // explicitly and avoids both problems.
    //
    // `freeze_to_disk` remains lifecycle-owned here. Follower replay moved
    // to `VectorReplayObserver`.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two collections with the same `(branch, name)` in different
    /// spaces must produce distinct on-disk paths. Without this, the
    /// `.vec` mmap heap of one tenant would clobber another's after
    /// Phase 0 — see `docs/design/space-correctness-fix-plan.md`
    /// Part 4. The `space` subdirectory enforces isolation.
    #[test]
    fn test_mmap_path_includes_space() {
        let bid = strata_core::types::BranchId::new();
        let root = std::path::Path::new("/tmp/strata-vec-test");

        let a = mmap_path(root, bid, "tenant_a", "embeddings");
        let b = mmap_path(root, bid, "tenant_b", "embeddings");
        assert_ne!(a, b, "different spaces must produce different paths");
        assert!(a.to_string_lossy().contains("tenant_a"));
        assert!(b.to_string_lossy().contains("tenant_b"));

        // graph_dir under the same constraint
        let ga = super::super::graph_dir(root, bid, "tenant_a", "embeddings");
        let gb = super::super::graph_dir(root, bid, "tenant_b", "embeddings");
        assert_ne!(ga, gb);
        assert!(ga.to_string_lossy().contains("tenant_a"));
        assert!(gb.to_string_lossy().contains("tenant_b"));
    }
}
