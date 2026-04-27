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

#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::io;

use strata_core::id::CommitVersion;
use strata_core::{BranchId, StrataResult};
use strata_engine::database::observers::{AbortInfo, AbortObserver};
use strata_engine::Database;
use strata_storage::{Key, Storage, TypeTag};
use tracing::info;

#[cfg(test)]
thread_local! {
    static VECTOR_SCAN_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
}

#[cfg(test)]
fn inject_vector_scan_failure_for_test(kind: io::ErrorKind) {
    VECTOR_SCAN_FAILURE.with(|slot| slot.set(Some(kind)));
}

#[cfg(test)]
fn clear_vector_scan_failure_for_test() {
    VECTOR_SCAN_FAILURE.with(|slot| slot.set(None));
}

#[cfg(test)]
fn maybe_inject_vector_scan_failure_for_test() -> Option<io::Error> {
    VECTOR_SCAN_FAILURE.with(|slot| {
        slot.take()
            .map(|kind| io::Error::new(kind, "injected vector recovery scan failure"))
    })
}

/// Recovery function for VectorStore
///
/// Called by Database during startup to restore vector state from KV store.
fn recover_vector_state(db: &std::sync::Arc<Database>) -> StrataResult<()> {
    recover_from_db(db.as_ref())?;

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
    branch_id: BranchId,
    space: &str,
    collection_name: &str,
) -> std::path::PathBuf {
    branch_cache_dir(data_dir, branch_id)
        .join(space)
        .join(format!("{}.vec", collection_name))
}

/// Compute the on-disk cache directory for a branch.
///
/// Layout: `{data_dir}/vectors/{branch_hex}/`.
pub(crate) fn branch_cache_dir(
    data_dir: &std::path::Path,
    branch_id: BranchId,
) -> std::path::PathBuf {
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*branch_id.as_bytes()));
    data_dir.join("vectors").join(branch_hex)
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

/// Wipe the entire on-disk vector cache tree for a branch.
///
/// This is stronger than per-collection purge and is used during branch delete
/// to remove orphan sidecar files that no longer have a loaded backend.
pub(crate) fn purge_branch_disk_cache(data_dir: &std::path::Path, branch_id: BranchId) {
    if data_dir.as_os_str().is_empty() {
        return;
    }

    let branch_dir = branch_cache_dir(data_dir, branch_id);
    if branch_dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(&branch_dir) {
            tracing::warn!(
                target: "strata::vector",
                path = ?branch_dir,
                error = %e,
                "Failed to remove vector branch cache dir during purge"
            );
        }
    }
}

/// Internal recovery implementation that works with &Database
fn recover_from_db(db: &Database) -> StrataResult<()> {
    use super::{CollectionId, IndexBackendFactory, VectorBackendState, VectorConfig, VectorId};
    use crate::heap::VectorHeap;
    use std::sync::Arc;
    use strata_core::value::Value;
    use strata_storage::{Key, Namespace, Storage};

    // Skip recovery for cache databases
    if db.is_cache() {
        return Ok(());
    }

    // Get access to the shared backend state
    let state = db.extension::<VectorBackendState>()?;

    let snapshot_version = CommitVersion(db.storage().version());
    let mut stats = super::RecoveryStats::default();
    let data_dir = db.data_dir();
    // Followers can intentionally lag the primary's visible version when a
    // blocked record survives restart. Do not trust the primary's persisted
    // vector caches in that case; rebuild from the follower-visible KV snapshot.
    let use_mmap = !data_dir.as_os_str().is_empty() && !db.is_follower();
    let can_write_disk = use_mmap;

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

                // Extract collection name from the key's user_key ("__config__/{name}")
                // before decoding so we can attribute a degradation to a
                // named primitive even if decode fails (B5.4).
                let collection_name = match key.user_key_string() {
                    Some(raw) => raw.strip_prefix("__config__/").unwrap_or(&raw).to_string(),
                    None => continue,
                };

                // Decode the CollectionRecord. On failure this collection
                // must fail closed for subsequent reads rather than be
                // silently dropped from recovery — see
                // `docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`
                // §"Surface matrix" row "vector in-memory / HNSW state".
                let record = match super::CollectionRecord::from_bytes(config_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            key = ?key,
                            error = %e,
                            "Failed to decode collection record during recovery; marking collection degraded"
                        );
                        strata_engine::database::primitive_degradation::mark_primitive_degraded(
                            db,
                            branch_id,
                            strata_core::contract::PrimitiveType::Vector,
                            &collection_name,
                            strata_core::PrimitiveDegradedReason::ConfigDecodeFailure,
                            format!("{e}"),
                        );
                        continue;
                    }
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
                            "Failed to convert collection config during recovery; marking collection degraded"
                        );
                        strata_engine::database::primitive_degradation::mark_primitive_degraded(
                            db,
                            branch_id,
                            strata_core::contract::PrimitiveType::Vector,
                            &collection_name,
                            strata_core::PrimitiveDegradedReason::ConfigShapeConversion,
                            format!("{e}"),
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
                #[cfg(test)]
                let scan_result = if let Some(err) = maybe_inject_vector_scan_failure_for_test() {
                    Err(err.into())
                } else {
                    db.storage().scan_prefix(&vector_prefix, snapshot_version)
                };
                #[cfg(not(test))]
                let scan_result = db.storage().scan_prefix(&vector_prefix, snapshot_version);

                let vector_entries = match scan_result {
                    Ok(entries) => entries,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            collection = %collection_name,
                            error = %e,
                            "Failed to scan vectors during recovery; marking collection degraded"
                        );
                        // B5.4 — vector rebuild failure must fail closed. A
                        // partial mmap-only or empty backend is not trusted
                        // enough to serve branch-visible reads.
                        strata_engine::database::primitive_degradation::mark_primitive_degraded(
                            db,
                            branch_id,
                            strata_core::contract::PrimitiveType::Vector,
                            &collection_name,
                            strata_core::PrimitiveDegradedReason::ConfigMismatch,
                            format!("failed to scan vectors during recovery: {e}"),
                        );
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
                                "Failed to decode vector record during recovery; marking collection degraded"
                            );
                            strata_engine::database::primitive_degradation::mark_primitive_degraded(
                                db,
                                branch_id,
                                strata_core::contract::PrimitiveType::Vector,
                                &collection_name,
                                strata_core::PrimitiveDegradedReason::ConfigMismatch,
                                format!("{e}"),
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
                        if let Err(e) = backend.insert_with_id_and_timestamp(
                            vid,
                            &vec_record.embedding,
                            vec_record.created_at,
                        ) {
                            tracing::warn!(
                                target: "strata::vector",
                                collection = %collection_name,
                                vector_id = vec_record.vector_id,
                                error = %e,
                                "Failed to insert vector during recovery; marking collection degraded"
                            );
                            strata_engine::database::primitive_degradation::mark_primitive_degraded(
                                db,
                                branch_id,
                                strata_core::contract::PrimitiveType::Vector,
                                &collection_name,
                                strata_core::PrimitiveDegradedReason::ConfigMismatch,
                                format!("{e}"),
                            );
                            continue;
                        }
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
                        if let Err(e) = backend.insert_with_id_and_timestamp(
                            vid,
                            &vec_record.embedding,
                            vec_record.created_at,
                        ) {
                            tracing::warn!(
                                target: "strata::vector",
                                collection = %collection_name,
                                vector_id = vec_record.vector_id,
                                error = %e,
                                "Failed to insert vector during recovery; marking collection degraded"
                            );
                            strata_engine::database::primitive_degradation::mark_primitive_degraded(
                                db,
                                branch_id,
                                strata_core::contract::PrimitiveType::Vector,
                                &collection_name,
                                strata_core::PrimitiveDegradedReason::ConfigMismatch,
                                format!("{e}"),
                            );
                            continue;
                        }
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

    fn cleanup_deleted_branch(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
        branch_id: &BranchId,
        _branch_name: &str,
    ) -> strata_core::StrataResult<()> {
        let store = crate::VectorStore::new(db.clone());
        store
            .purge_collections_in_branch(*branch_id)
            .map_err(|e| strata_core::StrataError::internal(e.to_string()))?;
        purge_branch_disk_cache(db.data_dir(), *branch_id);
        Ok(())
    }

    fn freeze(&self, db: &strata_engine::Database) -> strata_core::StrataResult<()> {
        db.freeze_vector_heaps()
    }
}

// =============================================================================
// Vector Commit Observer
// =============================================================================

use strata_engine::database::observers::{CommitInfo, CommitObserver, ObserverError};

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

// =============================================================================
// Lifecycle Hook Implementation
// =============================================================================

/// Register the vector lifecycle hook with the database.
fn register_vector_lifecycle_hook(db: &std::sync::Arc<Database>) {
    use std::sync::Arc;

    let state = match db.extension::<super::VectorBackendState>() {
        Ok(s) => s,
        Err(_) => return,
    };

    let hook = Arc::new(VectorLifecycleHook {
        state,
        db: Arc::downgrade(db),
    });

    if let Ok(hooks) = db.extension::<strata_engine::RefreshHooks>() {
        hooks.register(hook);
    }
}

/// Lifecycle hook implementation for vector backends.
struct VectorLifecycleHook {
    state: std::sync::Arc<super::VectorBackendState>,
    db: std::sync::Weak<strata_engine::Database>,
}

enum PendingVectorOp {
    CreateCollection {
        collection_id: crate::CollectionId,
        backend_type: crate::IndexBackendType,
        config: crate::VectorConfig,
    },
    UpsertVector {
        collection_id: crate::CollectionId,
        vector_id: crate::VectorId,
        vector_key: String,
        embedding: Vec<f32>,
        created_at: u64,
        source_ref: Option<strata_core::EntityRef>,
    },
    DeleteVector {
        collection_id: crate::CollectionId,
        vector_id: crate::VectorId,
        deleted_at: u64,
    },
    DropCollection {
        collection_id: crate::CollectionId,
    },
}

struct PendingVectorRefresh {
    state: std::sync::Arc<super::VectorBackendState>,
    ops: Vec<PendingVectorOp>,
}

impl strata_engine::PreparedRefresh for PendingVectorRefresh {
    fn publish(self: Box<Self>) {
        use dashmap::mapref::entry::Entry;

        for op in self.ops {
            match op {
                PendingVectorOp::CreateCollection {
                    collection_id,
                    backend_type,
                    config,
                } => match self.state.backends.entry(collection_id) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) => {
                        entry.insert(
                            crate::IndexBackendFactory::from_type(backend_type).create(&config),
                        );
                    }
                },
                PendingVectorOp::UpsertVector {
                    collection_id,
                    vector_id,
                    vector_key,
                    embedding,
                    created_at,
                    source_ref,
                } => {
                    let mut backend =
                        self.state.backends.get_mut(&collection_id).expect(
                            "staged vector refresh missing collection backend during publish",
                        );
                    backend
                        .insert_with_timestamp(vector_id, &embedding, created_at)
                        .expect("validated staged vector refresh insert failed during publish");
                    backend.set_inline_meta(
                        vector_id,
                        crate::types::InlineMeta {
                            key: vector_key,
                            source_ref,
                        },
                    );
                }
                PendingVectorOp::DeleteVector {
                    collection_id,
                    vector_id,
                    deleted_at,
                } => {
                    if let Some(mut backend) = self.state.backends.get_mut(&collection_id) {
                        backend
                            .delete_with_timestamp(vector_id, deleted_at)
                            .expect("validated staged vector refresh delete failed during publish");
                        backend.remove_inline_meta(vector_id);
                    }
                }
                PendingVectorOp::DropCollection { collection_id } => {
                    self.state.backends.remove(&collection_id);
                }
            }
        }
    }
}

// Safety: VectorBackendState uses DashMap which is Send + Sync
unsafe impl Send for VectorLifecycleHook {}
unsafe impl Sync for VectorLifecycleHook {}

impl strata_engine::RefreshHook for VectorLifecycleHook {
    fn name(&self) -> &'static str {
        "vector"
    }

    fn pre_delete_read(
        &self,
        db: &strata_engine::Database,
        deletes: &[Key],
    ) -> Vec<(Key, Vec<u8>)> {
        deletes
            .iter()
            .filter(|key| key.type_tag == TypeTag::Vector)
            .map(|key| {
                let bytes = db
                    .storage()
                    .get_versioned(key, CommitVersion::MAX)
                    .ok()
                    .and_then(|value| value)
                    .and_then(|versioned| match versioned.value {
                        strata_core::value::Value::Bytes(bytes) => Some(bytes),
                        _ => None,
                    })
                    .unwrap_or_default();
                (key.clone(), bytes)
            })
            .collect()
    }

    fn apply_refresh(
        &self,
        puts: &[(Key, strata_core::value::Value)],
        pre_read_deletes: &[(Key, Vec<u8>)],
    ) -> Result<Box<dyn strata_engine::PreparedRefresh>, strata_engine::RefreshHookError> {
        use crate::{CollectionId, VectorId};

        let Some(db) = self.db.upgrade() else {
            return Ok(Box::new(strata_engine::NoopPreparedRefresh));
        };
        let store = crate::VectorStore::new(db.clone());
        let mut ops = Vec::with_capacity(puts.len() + pre_read_deletes.len());

        for (key, value) in puts
            .iter()
            .filter(|(key, _)| key.type_tag == TypeTag::Vector)
        {
            let Some(user_key) = key.user_key_string() else {
                return Err(strata_engine::RefreshHookError::new(
                    "vector",
                    "vector key is not valid UTF-8",
                ));
            };

            if let Some(collection_name) = user_key.strip_prefix("__config__/") {
                let bytes = match value {
                    strata_core::value::Value::Bytes(bytes) => bytes,
                    _ => {
                        return Err(strata_engine::RefreshHookError::new(
                            "vector",
                            format!(
                                "collection config {} stored non-bytes payload during refresh",
                                collection_name
                            ),
                        ))
                    }
                };
                let record = crate::CollectionRecord::from_bytes(bytes).map_err(|e| {
                    strata_engine::RefreshHookError::new(
                        "vector",
                        format!(
                            "failed to decode collection config {}: {}",
                            collection_name, e
                        ),
                    )
                })?;
                let config = crate::VectorConfig::try_from(record.config.clone()).map_err(|e| {
                    strata_engine::RefreshHookError::new(
                        "vector",
                        format!("failed to decode config {}: {}", collection_name, e),
                    )
                })?;
                let backend_type = record.backend_type();
                let collection_id = CollectionId::new(
                    key.namespace.branch_id,
                    key.namespace.space.as_str(),
                    collection_name,
                );

                ops.push(PendingVectorOp::CreateCollection {
                    collection_id,
                    backend_type,
                    config,
                });
                continue;
            }
        }

        for (key, value) in puts
            .iter()
            .filter(|(key, _)| key.type_tag == TypeTag::Vector)
        {
            let Some(user_key) = key.user_key_string() else {
                return Err(strata_engine::RefreshHookError::new(
                    "vector",
                    "vector key is not valid UTF-8",
                ));
            };
            if user_key.starts_with("__config__/") {
                continue;
            }

            let (collection_name, vector_key) = user_key.split_once('/').ok_or_else(|| {
                strata_engine::RefreshHookError::new(
                    "vector",
                    format!("invalid vector key format during refresh: {}", user_key),
                )
            })?;

            store
                .ensure_collection_loaded(
                    key.namespace.branch_id,
                    key.namespace.space.as_str(),
                    collection_name,
                )
                .map_err(|e| {
                    strata_engine::RefreshHookError::new(
                        "vector",
                        format!(
                            "failed to load collection {} in space {}: {}",
                            collection_name, key.namespace.space, e
                        ),
                    )
                })?;

            let bytes = match value {
                strata_core::value::Value::Bytes(bytes) => bytes,
                _ => {
                    return Err(strata_engine::RefreshHookError::new(
                        "vector",
                        format!(
                            "vector payload {} stored non-bytes value during refresh",
                            user_key
                        ),
                    ))
                }
            };
            let record = crate::VectorRecord::from_bytes(bytes).map_err(|e| {
                strata_engine::RefreshHookError::new(
                    "vector",
                    format!("failed to decode vector {}: {}", user_key, e),
                )
            })?;

            if record.embedding.is_empty() {
                continue;
            }

            let collection_id = CollectionId::new(
                key.namespace.branch_id,
                key.namespace.space.as_str(),
                collection_name,
            );
            let vid = VectorId::new(record.vector_id);
            let backend = self.state.backends.get(&collection_id).ok_or_else(|| {
                strata_engine::RefreshHookError::new(
                    "vector",
                    format!(
                        "collection {} in space {} is not loaded during refresh",
                        collection_name, key.namespace.space
                    ),
                )
            })?;
            let expected_dim = backend.config().dimension;
            drop(backend);
            if record.embedding.len() != expected_dim {
                return Err(strata_engine::RefreshHookError::new(
                    "vector",
                    format!(
                        "failed to insert vector {} into {}: dimension mismatch (expected {}, got {})",
                        vector_key,
                        collection_name,
                        expected_dim,
                        record.embedding.len()
                    ),
                ));
            }
            ops.push(PendingVectorOp::UpsertVector {
                collection_id,
                vector_id: vid,
                vector_key: vector_key.to_string(),
                embedding: record.embedding.clone(),
                created_at: record.created_at,
                source_ref: record.source_ref.clone(),
            });
        }

        for (key, bytes) in pre_read_deletes
            .iter()
            .filter(|(key, _)| key.type_tag == TypeTag::Vector)
        {
            let Some(user_key) = key.user_key_string() else {
                return Err(strata_engine::RefreshHookError::new(
                    "vector",
                    "deleted vector key is not valid UTF-8",
                ));
            };
            if user_key.starts_with("__config__/") || bytes.is_empty() {
                continue;
            }

            let (collection_name, _) = user_key.split_once('/').ok_or_else(|| {
                strata_engine::RefreshHookError::new(
                    "vector",
                    format!(
                        "invalid deleted vector key format during refresh: {}",
                        user_key
                    ),
                )
            })?;

            store
                .ensure_collection_loaded(
                    key.namespace.branch_id,
                    key.namespace.space.as_str(),
                    collection_name,
                )
                .map_err(|e| {
                    strata_engine::RefreshHookError::new(
                        "vector",
                        format!(
                            "failed to load collection {} in space {}: {}",
                            collection_name, key.namespace.space, e
                        ),
                    )
                })?;

            let record = crate::VectorRecord::from_bytes(bytes).map_err(|e| {
                strata_engine::RefreshHookError::new(
                    "vector",
                    format!("failed to decode deleted vector {}: {}", user_key, e),
                )
            })?;
            let collection_id = CollectionId::new(
                key.namespace.branch_id,
                key.namespace.space.as_str(),
                collection_name,
            );
            let vid = VectorId::new(record.vector_id);
            self.state.backends.get(&collection_id).ok_or_else(|| {
                strata_engine::RefreshHookError::new(
                    "vector",
                    format!(
                        "collection {} in space {} is not loaded during refresh delete",
                        collection_name, key.namespace.space
                    ),
                )
            })?;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            ops.push(PendingVectorOp::DeleteVector {
                collection_id,
                vector_id: vid,
                deleted_at: now,
            });
        }

        for (key, _) in pre_read_deletes
            .iter()
            .filter(|(key, _)| key.type_tag == TypeTag::Vector)
        {
            let Some(user_key) = key.user_key_string() else {
                return Err(strata_engine::RefreshHookError::new(
                    "vector",
                    "deleted vector key is not valid UTF-8",
                ));
            };
            let Some(collection_name) = user_key.strip_prefix("__config__/") else {
                continue;
            };

            let collection_id = CollectionId::new(
                key.namespace.branch_id,
                key.namespace.space.as_str(),
                collection_name,
            );
            ops.push(PendingVectorOp::DropCollection { collection_id });
        }

        Ok(Box::new(PendingVectorRefresh {
            state: self.state.clone(),
            ops,
        }))
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
    // `freeze_to_disk` remains lifecycle-owned here. Follower replay now runs
    // through the fallible refresh hook so refresh only advances after HNSW
    // state is consistent.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DistanceMetric, VectorStore};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use strata_engine::database::OpenSpec;
    use strata_engine::{
        Database, NoopPreparedRefresh, PreparedRefresh, RefreshHook, RefreshHookError,
        RefreshHooks, Subsystem,
    };

    #[derive(Clone)]
    struct FailOnceRefreshSubsystem {
        fail_once: Arc<AtomicBool>,
    }

    impl FailOnceRefreshSubsystem {
        fn new(fail_once: Arc<AtomicBool>) -> Self {
            Self { fail_once }
        }
    }

    struct FailOnceRefreshHook {
        fail_once: Arc<AtomicBool>,
    }

    impl RefreshHook for FailOnceRefreshHook {
        fn name(&self) -> &'static str {
            "vector-test-fail-once"
        }

        fn pre_delete_read(&self, _db: &Database, _deletes: &[Key]) -> Vec<(Key, Vec<u8>)> {
            Vec::new()
        }

        fn apply_refresh(
            &self,
            puts: &[(Key, strata_core::value::Value)],
            _pre_read_deletes: &[(Key, Vec<u8>)],
        ) -> Result<Box<dyn PreparedRefresh>, RefreshHookError> {
            if !puts.is_empty() && self.fail_once.swap(false, Ordering::SeqCst) {
                return Err(RefreshHookError::new(
                    "vector-test-fail-once",
                    "injected refresh hook failure",
                ));
            }
            Ok(Box::new(NoopPreparedRefresh))
        }

        fn freeze_to_disk(&self, _db: &Database) -> strata_core::StrataResult<()> {
            Ok(())
        }
    }

    impl Subsystem for FailOnceRefreshSubsystem {
        fn name(&self) -> &'static str {
            "vector-test-fail-once"
        }

        fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
            Ok(())
        }

        fn initialize(&self, db: &Arc<Database>) -> strata_core::StrataResult<()> {
            let hooks = db.extension::<RefreshHooks>()?;
            hooks.register(Arc::new(FailOnceRefreshHook {
                fail_once: self.fail_once.clone(),
            }));
            Ok(())
        }
    }

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

    #[test]
    fn test_recovery_vector_scan_failure_marks_collection_degraded_and_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_name = "main";
        let branch_id = strata_engine::primitives::branch::resolve_branch_name(branch_name);

        {
            let db = Database::open_runtime(
                OpenSpec::primary(temp.path())
                    .with_subsystem(VectorSubsystem)
                    .with_subsystem(strata_engine::SearchSubsystem),
            )
            .unwrap();
            let store = VectorStore::new(db.clone());

            db.branches().create(branch_name).unwrap();
            store
                .create_collection(
                    branch_id,
                    "default",
                    "embeddings",
                    crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
                )
                .unwrap();
            store
                .insert(
                    branch_id,
                    "default",
                    "embeddings",
                    "v1",
                    &[1.0, 0.0, 0.0],
                    None,
                )
                .unwrap();
        }

        clear_vector_scan_failure_for_test();
        inject_vector_scan_failure_for_test(std::io::ErrorKind::Other);

        let db = Database::open_runtime(
            OpenSpec::primary(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(strata_engine::SearchSubsystem),
        )
        .unwrap();
        let store = VectorStore::new(db.clone());

        let registry = db
            .extension::<strata_engine::PrimitiveDegradationRegistry>()
            .unwrap();
        let entry = registry
            .lookup(
                branch_id,
                strata_core::contract::PrimitiveType::Vector,
                "embeddings",
            )
            .expect("scan failure must mark the collection degraded");
        assert_eq!(
            entry.reason,
            strata_core::PrimitiveDegradedReason::ConfigMismatch
        );

        let err = store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[1.0, 0.0, 0.0],
                5,
                None,
            )
            .expect_err("degraded collection must fail closed after reopen");
        match strata_core::StrataError::from(err) {
            strata_core::StrataError::PrimitiveDegraded {
                primitive, name, ..
            } => {
                assert_eq!(primitive, strata_core::contract::PrimitiveType::Vector);
                assert_eq!(name, "embeddings");
            }
            other => panic!("expected PrimitiveDegraded, got {other:?}"),
        }

        clear_vector_scan_failure_for_test();
    }

    #[test]
    fn test_branch_delete_purges_vector_caches_and_prevents_recovery_resurrection() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_name = "feature";
        let branch_id = strata_engine::primitives::branch::resolve_branch_name(branch_name);
        let vec_path = mmap_path(temp.path(), branch_id, "default", "embeddings");

        {
            let db = Database::open_runtime(
                OpenSpec::primary(temp.path())
                    .with_subsystem(VectorSubsystem)
                    .with_subsystem(strata_engine::SearchSubsystem),
            )
            .unwrap();
            let store = VectorStore::new(db.clone());

            db.branches().create(branch_name).unwrap();
            store
                .create_collection(
                    branch_id,
                    "default",
                    "embeddings",
                    crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
                )
                .unwrap();
            store
                .insert(
                    branch_id,
                    "default",
                    "embeddings",
                    "v1",
                    &[1.0, 0.0, 0.0],
                    None,
                )
                .unwrap();

            db.flush().unwrap();
            db.freeze_vector_heaps().unwrap();
            assert!(
                vec_path.exists(),
                "expected branch collection mmap cache to exist before delete"
            );

            db.branches().delete(branch_name).unwrap();
            assert!(
                !vec_path.exists(),
                "branch delete must purge stale vector mmap caches"
            );
        }

        {
            let db = Database::open_runtime(
                OpenSpec::primary(temp.path())
                    .with_subsystem(VectorSubsystem)
                    .with_subsystem(strata_engine::SearchSubsystem),
            )
            .unwrap();
            let store = VectorStore::new(db.clone());

            db.branches().create(branch_name).unwrap();
            store
                .create_collection(
                    branch_id,
                    "default",
                    "embeddings",
                    crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
                )
                .unwrap();
            db.flush().unwrap();
        }

        let reopened = Database::open_runtime(
            OpenSpec::primary(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(strata_engine::SearchSubsystem),
        )
        .unwrap();
        let store = VectorStore::new(reopened);
        let matches = store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[1.0, 0.0, 0.0],
                10,
                None,
            )
            .unwrap();
        assert!(
            matches.is_empty(),
            "recreated branch collection must not recover stale vectors after restart"
        );
    }

    #[test]
    fn test_branch_delete_purges_orphan_branch_cache_dir() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_name = "feature";
        let branch_id = strata_engine::primitives::branch::resolve_branch_name(branch_name);
        let branch_dir = branch_cache_dir(temp.path(), branch_id);
        let orphan_file = branch_dir.join("default").join("orphan.vec");
        let orphan_graph = branch_dir
            .join("default")
            .join("embeddings_graphs")
            .join("seg_0.hgr");

        let db = Database::open_runtime(
            OpenSpec::primary(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(strata_engine::SearchSubsystem),
        )
        .unwrap();

        db.branches().create(branch_name).unwrap();

        std::fs::create_dir_all(orphan_file.parent().unwrap()).unwrap();
        std::fs::write(&orphan_file, b"stale mmap cache").unwrap();
        std::fs::create_dir_all(orphan_graph.parent().unwrap()).unwrap();
        std::fs::write(&orphan_graph, b"stale graph cache").unwrap();
        assert!(branch_dir.exists(), "expected synthetic branch cache dir");

        db.branches().delete(branch_name).unwrap();

        assert!(
            !branch_dir.exists(),
            "branch delete must purge the entire branch cache tree, including orphan files"
        );
    }

    #[test]
    fn test_follower_refresh_replays_collection_create_insert_and_delete() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_id = strata_core::types::BranchId::default();

        let primary =
            Database::open_runtime(OpenSpec::primary(temp.path()).with_subsystem(VectorSubsystem))
                .unwrap();
        let primary_store = VectorStore::new(primary.clone());

        let follower =
            Database::open_runtime(OpenSpec::follower(temp.path()).with_subsystem(VectorSubsystem))
                .unwrap();
        let follower_store = VectorStore::new(follower.clone());

        primary_store
            .create_collection(
                branch_id,
                "default",
                "embeddings",
                crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .unwrap();
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "v1",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        let outcome = follower.refresh();
        assert!(
            outcome.is_caught_up(),
            "follower refresh should apply collection creation and vector insert"
        );

        let matches = follower_store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[1.0, 0.0, 0.0],
                5,
                None,
            )
            .unwrap();
        assert_eq!(
            matches.len(),
            1,
            "inserted vector must be searchable after refresh"
        );
        assert_eq!(matches[0].key, "v1");
        assert!(
            follower_store
                .get_collection(branch_id, "default", "embeddings")
                .unwrap()
                .is_some(),
            "collection create must load a backend on the follower"
        );

        primary_store
            .delete(branch_id, "default", "embeddings", "v1")
            .unwrap();
        primary.flush().unwrap();

        let outcome = follower.refresh();
        assert!(
            outcome.is_caught_up(),
            "follower refresh should apply vector delete"
        );

        let matches = follower_store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[1.0, 0.0, 0.0],
                5,
                None,
            )
            .unwrap();
        assert!(
            matches.is_empty(),
            "deleted vector must be removed from follower search results"
        );
    }

    #[test]
    fn test_vector_refresh_does_not_perturb_visible_results_before_visibility_advance() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_id = strata_core::types::BranchId::default();
        let fail_once = Arc::new(AtomicBool::new(false));

        let primary = Database::open_runtime(
            OpenSpec::primary(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        let primary_store = VectorStore::new(primary.clone());

        primary_store
            .create_collection(
                branch_id,
                "default",
                "embeddings",
                crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .unwrap();
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "visible-a",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        let follower = Database::open_runtime(
            OpenSpec::follower(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        let follower_store = VectorStore::new(follower.clone());

        let baseline = follower_store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[0.0, 1.0, 0.0],
                1,
                None,
            )
            .unwrap();
        assert_eq!(baseline.len(), 1);
        assert_eq!(baseline[0].key, "visible-a");

        fail_once.store(true, Ordering::SeqCst);
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "blocked-b",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        let outcome = follower.refresh();
        assert!(
            matches!(outcome, strata_engine::RefreshOutcome::Stuck { .. }),
            "vector refresh should block after staging backend updates"
        );

        let after_block = follower_store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[0.0, 1.0, 0.0],
                1,
                None,
            )
            .unwrap();
        assert_eq!(
            after_block.len(),
            1,
            "blocked staged vectors must not displace visible results"
        );
        assert_eq!(
            after_block[0].key, "visible-a",
            "blocked staged vectors must remain invisible to readers"
        );
    }

    #[test]
    fn test_blocked_follower_restart_does_not_load_newer_vector_caches() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_id = strata_core::types::BranchId::default();
        let fail_once = Arc::new(AtomicBool::new(false));

        let primary = Database::open_runtime(
            OpenSpec::primary(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        let primary_store = VectorStore::new(primary.clone());

        primary_store
            .create_collection(
                branch_id,
                "default",
                "embeddings",
                crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .unwrap();
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "visible-a",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        let follower = Database::open_runtime(
            OpenSpec::follower(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();

        fail_once.store(true, Ordering::SeqCst);
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "blocked-b",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        assert!(
            matches!(
                follower.refresh(),
                strata_engine::RefreshOutcome::Stuck { .. }
            ),
            "refresh should block before making the new vector visible"
        );

        drop(follower);
        primary.shutdown().unwrap();
        drop(primary);

        let reopened = Database::open_runtime(
            OpenSpec::follower(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(
                    false,
                )))),
        )
        .unwrap();
        let reopened_store = VectorStore::new(reopened);

        let matches = reopened_store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[0.0, 1.0, 0.0],
                1,
                None,
            )
            .unwrap();
        assert_eq!(
            matches.len(),
            1,
            "follower reopen must not let blocked mmap-only vectors displace visible results"
        );
        assert_eq!(matches[0].key, "visible-a");
    }

    #[test]
    fn test_blocked_follower_lazy_load_does_not_load_newer_vector_caches() {
        let temp = tempfile::TempDir::new().unwrap();
        let branch_id = strata_core::types::BranchId::default();
        let fail_once = Arc::new(AtomicBool::new(false));

        let primary = Database::open_runtime(
            OpenSpec::primary(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        let primary_store = VectorStore::new(primary.clone());

        primary_store
            .create_collection(
                branch_id,
                "default",
                "embeddings",
                crate::VectorConfig::new(3, DistanceMetric::Cosine).unwrap(),
            )
            .unwrap();
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "visible-a",
                &[1.0, 0.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        let follower = Database::open_runtime(
            OpenSpec::follower(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();

        fail_once.store(true, Ordering::SeqCst);
        primary_store
            .insert(
                branch_id,
                "default",
                "embeddings",
                "blocked-b",
                &[0.0, 1.0, 0.0],
                None,
            )
            .unwrap();
        primary.flush().unwrap();

        assert!(
            matches!(
                follower.refresh(),
                strata_engine::RefreshOutcome::Stuck { .. }
            ),
            "refresh should block before making the new vector visible"
        );

        drop(follower);
        primary.shutdown().unwrap();
        drop(primary);

        let reopened = Database::open_runtime(
            OpenSpec::follower(temp.path())
                .with_subsystem(VectorSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(
                    false,
                )))),
        )
        .unwrap();
        let reopened_store = VectorStore::new(reopened.clone());
        let state = reopened.extension::<crate::VectorBackendState>().unwrap();
        state.backends.clear();
        assert!(
            state.backends.is_empty(),
            "test setup must force the lazy-load path instead of using recovered backends"
        );

        let matches = reopened_store
            .search(
                branch_id,
                "default",
                "embeddings",
                &[0.0, 1.0, 0.0],
                1,
                None,
            )
            .unwrap();
        assert_eq!(
            matches.len(),
            1,
            "lazy-loaded follower collections must rebuild from visible KV state"
        );
        assert_eq!(
            matches[0].key, "visible-a",
            "lazy load must not pull blocked vectors from primary mmap caches"
        );
    }
}
