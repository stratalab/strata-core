//! Search Recovery
//!
//! `SearchSubsystem` restores the InvertedIndex when the Database reopens.
//!
//! ## How It Works
//!
//! 1. `OpenSpec::with_subsystem(SearchSubsystem)` installs the subsystem at open time
//! 2. `SearchSubsystem::recover` calls `recover_search_state`, which either:
//!    - **Fast path**: Loads manifest + mmap'd sealed segments (sub-second)
//!    - **Slow path**: Scans all KV/Event entries, indexes them, and
//!      freezes to disk for the next open
//! 3. Enables the index for search operations
//!
//! ## mmap Acceleration
//!
//! After the first KV-scan recovery, the index is frozen to `{data_dir}/search/`
//! (manifest + `.sidx` segment files). Subsequent opens load these files via mmap,
//! skipping the expensive KV scan entirely.
//!
//! All mmap files are **caches** — if missing, corrupt, or version-mismatched,
//! recovery falls back transparently to full KV-based rebuild with no data loss.

use crate::database::Database;
use crate::primitives::branch::resolve_branch_name;
use crate::search::InvertedIndex;
use strata_core::branch_dag::SYSTEM_BRANCH;
use strata_core::types::TypeTag;
use strata_core::value::Value;
use strata_core::StrataResult;
use tracing::info;

/// Extract indexable text from a Value.
///
/// Returns `Some(text)` for String and JSON-serializable values.
/// Returns `None` for Null, Bool, Bytes (not searchable).
pub fn extract_indexable_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Null | Value::Bool(_) | Value::Bytes(_) => None,
        other => serde_json::to_string(other).ok(),
    }
}

/// True if `space` is a JSON-internal space (secondary index storage).
///
/// `JsonStore::create_index` writes index metadata to `_idx_meta/{space}` and
/// index entries to `_idx/{space}/{name}`, both stored under `TypeTag::Json`.
/// Recovery scans of `TypeTag::Json` see them across every space in the
/// branch — these are not user documents and must not feed BM25.
fn is_json_internal_space(space: &str) -> bool {
    space.starts_with("_idx/") || space.starts_with("_idx_meta/")
}

/// Recovery function for the InvertedIndex.
///
/// Called by Database during startup to restore search index state.
fn recover_search_state(db: &Database) -> StrataResult<()> {
    recover_from_db(db)
}

/// Internal recovery implementation.
fn recover_from_db(db: &Database) -> StrataResult<()> {
    // Skip recovery for cache (ephemeral) databases.
    // Cache databases have their index enabled directly by Database::cache().
    if db.is_cache() {
        return Ok(());
    }

    let index = db.extension::<InvertedIndex>()?;
    let data_dir = db.data_dir();
    let use_disk = !data_dir.as_os_str().is_empty();

    // Configure data directory for persistence
    if use_disk {
        index.set_data_dir(data_dir.to_path_buf());
    }

    // ---------------------------------------------------------------
    // Fast path: load from manifest + mmap'd sealed segments
    // ---------------------------------------------------------------
    if use_disk {
        match index.load_from_disk() {
            Ok(true) => {
                info!(
                    target: "strata::search",
                    total_docs = index.total_docs(),
                    "Search index loaded from mmap cache (fast path)"
                );
                index.enable();

                // Reconcile: re-index any KV/Event entries committed after the
                // last freeze but before a crash (issue #1908).
                let reconciled = reconcile_index(db, &index)?;
                if reconciled > 0 {
                    info!(
                        target: "strata::search",
                        reconciled,
                        "Reconciled missing entries after fast-path load"
                    );
                    if !db.is_follower() {
                        if let Err(e) = index.freeze_to_disk() {
                            tracing::warn!(
                                target: "strata::search",
                                error = %e,
                                "Failed to freeze search index after reconciliation"
                            );
                        }
                    }
                }

                return Ok(());
            }
            Ok(false) => {
                // No manifest found — fall through to slow path
            }
            Err(e) => {
                tracing::warn!(
                    target: "strata::search",
                    error = %e,
                    "Failed to load search index from mmap cache, falling back to KV scan"
                );
            }
        }
    }

    // ---------------------------------------------------------------
    // Slow path: rebuild from KV/Event entries
    // ---------------------------------------------------------------
    // Enable the index before scanning so that index_document() calls
    // are not silently dropped by the is_enabled() guard.
    index.enable();

    let mut docs_indexed: u64 = 0;
    let mut branches_scanned: u64 = 0;

    // Skip the _system_ branch — its KV entries are internal graph
    // infrastructure (DAG metadata, catalog, node data), not user content.
    let system_branch_id = resolve_branch_name(SYSTEM_BRANCH);

    for branch_id in db.storage().branch_ids() {
        if branch_id == system_branch_id {
            continue;
        }
        branches_scanned += 1;

        // --- KV entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::KV) {
            let text = match extract_indexable_text(&vv.value) {
                Some(t) => t,
                None => continue,
            };

            let user_key = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::Kv {
                branch_id,
                space: key.namespace.space.to_string(),
                key: user_key,
            };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }

        // --- Event entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Event) {
            // Skip metadata keys (same as checkpoint logic)
            if *key.user_key == *b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                continue;
            }

            let text = match extract_indexable_text(&vv.value) {
                Some(t) => t,
                None => continue,
            };

            // Parse sequence from the key (8-byte big-endian u64)
            let sequence = if key.user_key.len() == 8 {
                u64::from_be_bytes((*key.user_key).try_into().unwrap_or([0; 8]))
            } else {
                continue; // Skip non-sequence keys
            };

            let entity_ref = crate::search::EntityRef::Event {
                branch_id,
                space: key.namespace.space.to_string(),
                sequence,
            };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }

        // --- Graph entries (node data only) ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Graph) {
            // Skip internal metadata keys (catalog, meta, type defs,
            // edge counts, indexes). Covers both top-level (__catalog__)
            // and graph-scoped ({graph}/__meta__, {graph}/__types__/, etc.)
            if key.user_key.starts_with(b"__") || key.user_key.windows(3).any(|w| w == b"/__") {
                continue;
            }

            let text = match extract_indexable_text(&vv.value) {
                Some(t) => t,
                None => continue,
            };

            let user_key = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::Graph {
                branch_id,
                space: key.namespace.space.to_string(),
                key: user_key,
            };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }

        // --- JSON entries ---
        // After Phase 0 bumped the BM25 manifest version, the slow path runs
        // on every existing DB. Without this loop, JSON documents silently
        // disappear from BM25 until the next user-triggered re-put.
        //
        // `list_by_type` returns JSON entries across ALL spaces in this
        // branch, including secondary-index storage (`_idx/{space}/{name}`)
        // and index metadata (`_idx_meta/{space}`). Skip those by space
        // prefix — they are not user documents.
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Json) {
            if is_json_internal_space(&key.namespace.space) {
                continue;
            }

            let doc_id = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let doc = match crate::primitives::json::JsonStore::deserialize_doc(&vv.value) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let text = serde_json::to_string(doc.value.as_inner()).unwrap_or_default();

            let entity_ref = crate::search::EntityRef::Json {
                branch_id,
                space: key.namespace.space.to_string(),
                doc_id,
            };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }
    }

    // Freeze to disk for next startup (fast path).
    // Followers must never write to the primary's data directory.
    if use_disk && docs_indexed > 0 && !db.is_follower() {
        if let Err(e) = index.freeze_to_disk() {
            tracing::warn!(
                target: "strata::search",
                error = %e,
                "Failed to freeze search index after rebuild"
            );
        }
    }

    index.enable();

    if docs_indexed > 0 || branches_scanned > 0 {
        info!(
            target: "strata::search",
            docs_indexed = docs_indexed,
            branches_scanned = branches_scanned,
            "Search index recovery complete (slow path)"
        );
    }

    Ok(())
}

/// Scan KV/Event entries and re-index any missing from the search index.
///
/// Returns the number of entries that were re-indexed. Entries already
/// present in the DocIdMap are skipped (O(1) DashMap lookup per entry).
fn reconcile_index(db: &Database, index: &InvertedIndex) -> StrataResult<u64> {
    let system_branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let mut reconciled: u64 = 0;

    for branch_id in db.storage().branch_ids() {
        if branch_id == system_branch_id {
            continue;
        }

        // --- KV entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::KV) {
            let user_key = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::Kv {
                branch_id,
                space: key.namespace.space.to_string(),
                key: user_key,
            };

            if index.has_document(&entity_ref) {
                continue;
            }

            let text = match extract_indexable_text(&vv.value) {
                Some(t) => t,
                None => continue,
            };

            index.index_document(&entity_ref, &text, None);
            reconciled += 1;
        }

        // --- Event entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Event) {
            if *key.user_key == *b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                continue;
            }

            let sequence = if key.user_key.len() == 8 {
                u64::from_be_bytes((*key.user_key).try_into().unwrap_or([0; 8]))
            } else {
                continue;
            };

            let entity_ref = crate::search::EntityRef::Event {
                branch_id,
                space: key.namespace.space.to_string(),
                sequence,
            };

            if index.has_document(&entity_ref) {
                continue;
            }

            let text = match extract_indexable_text(&vv.value) {
                Some(t) => t,
                None => continue,
            };

            index.index_document(&entity_ref, &text, None);
            reconciled += 1;
        }

        // --- Graph entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Graph) {
            if key.user_key.starts_with(b"__") {
                continue;
            }

            let user_key = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::Graph {
                branch_id,
                space: key.namespace.space.to_string(),
                key: user_key,
            };

            if index.has_document(&entity_ref) {
                continue;
            }

            let text = match extract_indexable_text(&vv.value) {
                Some(t) => t,
                None => continue,
            };

            index.index_document(&entity_ref, &text, None);
            reconciled += 1;
        }

        // --- JSON entries ---
        // Mirror the slow-path skip: never reconcile JSON-internal spaces
        // (`_idx/...`, `_idx_meta/...`) — these are secondary-index storage,
        // not user documents.
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Json) {
            if is_json_internal_space(&key.namespace.space) {
                continue;
            }

            let doc_id = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::Json {
                branch_id,
                space: key.namespace.space.to_string(),
                doc_id,
            };

            if index.has_document(&entity_ref) {
                continue;
            }

            let doc = match crate::primitives::json::JsonStore::deserialize_doc(&vv.value) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let text = serde_json::to_string(doc.value.as_inner()).unwrap_or_default();
            index.index_document(&entity_ref, &text, None);
            reconciled += 1;
        }
    }

    Ok(reconciled)
}

/// Subsystem implementation for search index recovery and shutdown hooks.
///
/// Used with `OpenSpec::with_subsystem(SearchSubsystem)` for explicit subsystem registration.
pub struct SearchSubsystem;

impl crate::recovery::Subsystem for SearchSubsystem {
    fn name(&self) -> &'static str {
        "search"
    }

    fn recover(
        &self,
        db: &std::sync::Arc<crate::database::Database>,
    ) -> strata_core::StrataResult<()> {
        recover_search_state(db)
    }

    fn initialize(
        &self,
        db: &std::sync::Arc<crate::database::Database>,
    ) -> strata_core::StrataResult<()> {
        use std::sync::Arc;

        // Register commit observer for search index maintenance.
        // Index updates happen inline during primitive operations; this observer
        // handles periodic seal operations for durability.
        let commit_observer = Arc::new(SearchCommitObserver {
            db: Arc::downgrade(db),
        });
        db.commit_observers().register(commit_observer);

        // Register replay observer for follower index maintenance.
        // Followers don't execute primitive operations (data arrives via WAL),
        // so this observer ensures the index stays consistent after replays.
        let replay_observer = Arc::new(SearchReplayObserver {
            db: Arc::downgrade(db),
        });
        db.replay_observers().register(replay_observer);

        Ok(())
    }

    fn freeze(&self, db: &crate::database::Database) -> strata_core::StrataResult<()> {
        db.freeze_search_index()
    }
}

// =============================================================================
// Search Observers
// =============================================================================

use crate::database::observers::{
    CommitInfo, CommitObserver, ObserverError, ReplayInfo, ReplayObserver,
};
use std::sync::{atomic::AtomicU64, Weak};

/// Commit observer for search index maintenance.
///
/// ## Architectural Note: Why Search Differs from Vector
///
/// Unlike vector (where HNSW can't participate in OCC and needs post-commit
/// updates via VectorCommitObserver), search index updates happen **inline**
/// during primitive operations (KV, Event, JSON put). This is intentional:
///
/// - **Search index is transactional**: Updates can be rolled back by simply
///   not committing. The inline updates participate in MVCC.
/// - **Immediate visibility**: Search results reflect writes within the same
///   transaction (read-your-writes).
/// - **No staged ops needed**: Unlike embeddings, search tokens don't require
///   separate storage - they're derived from committed data on recovery.
///
/// Moving to observer-based indexing would change semantics (search results
/// not visible until commit) and complicate rollback. The current inline
/// approach is the correct architecture for search.
///
/// This observer handles secondary concerns (metrics, periodic seal) rather
/// than primary indexing.
struct SearchCommitObserver {
    db: Weak<Database>,
}

impl CommitObserver for SearchCommitObserver {
    fn name(&self) -> &'static str {
        "search"
    }

    fn on_commit(&self, _info: &CommitInfo) -> Result<(), ObserverError> {
        // Primary index updates happen inline during primitive operations.
        // This observer handles secondary concerns:
        // - Could trigger periodic seal (currently handled by freeze/shutdown)
        // - Could track commit metrics (future enhancement)
        Ok(())
    }
}

/// Replay observer for follower search index maintenance.
///
/// ## Architectural Note: Follower Search Reconciliation
///
/// Followers receive data via WAL replay, not primitive operations. Unlike
/// vector (which uses RefreshHook to capture embeddings during WAL apply),
/// search doesn't need real-time incremental updates because:
///
/// - **Recovery rebuilds**: Search index is rebuildable from committed data,
///   so recovery can fully reconcile the index.
/// - **Derived state**: Search tokens are derived from document content, not
///   stored separately like embeddings.
/// - **Cost tradeoff**: Real-time reconciliation would require re-parsing all
///   replayed documents to extract tokens. Recovery-based reconciliation is
///   more efficient for read replicas.
///
/// For workloads requiring real-time follower search, a RefreshHook-based
/// approach (like vector) could be implemented as a future enhancement.
struct SearchReplayObserver {
    db: Weak<Database>,
}

impl ReplayObserver for SearchReplayObserver {
    fn name(&self) -> &'static str {
        "search"
    }

    fn on_replay(&self, _info: &ReplayInfo) -> Result<(), ObserverError> {
        // Follower search index reconciliation is deferred to recovery.
        // See architectural note above for rationale.
        Ok(())
    }
}
