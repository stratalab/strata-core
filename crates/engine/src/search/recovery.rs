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
use crate::search::{extract_search_text, InvertedIndex};
use crate::StrataResult;
use std::collections::HashSet;
use strata_core::branch_dag::SYSTEM_BRANCH;
use strata_core::id::CommitVersion;
use strata_core::traits::Storage;
use strata_core::types::{Key, TypeTag};
use strata_core::value::Value;
use tracing::info;

/// Extract indexable text from a Value.
///
/// Returns `Some(text)` for String and JSON-serializable values.
/// Returns `None` for Null, Bool, Bytes (not searchable).
pub fn extract_indexable_text(value: &Value) -> Option<String> {
    extract_search_text(value)
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

fn parse_graph_node_index_entry(
    key: &Key,
    value: &Value,
) -> Option<(crate::search::EntityRef, String)> {
    if key.type_tag != TypeTag::Graph {
        return None;
    }

    if key.user_key.starts_with(b"__") || key.user_key.windows(3).any(|w| w == b"/__") {
        return None;
    }

    let user_key = key.user_key_string()?;
    let parts: Vec<&str> = user_key.splitn(3, '/').collect();
    if parts.len() != 3 || parts[1] != "n" {
        return None;
    }

    let node_id = parts[2];
    let Value::String(json) = value else {
        return None;
    };
    let data: GraphNodeData = serde_json::from_str(json).ok()?;

    // Keep this in sync with strata-graph's GraphStore::build_node_search_text().
    let mut text = String::new();
    text.push_str(node_id);
    if let Some(ref ot) = data.object_type {
        text.push(' ');
        text.push_str(ot);
    }
    if let Some(ref props) = data.properties {
        text.push(' ');
        text.push_str(&serde_json::to_string(props).unwrap_or_default());
    }
    if let Some(ref uri) = data.entity_ref {
        text.push(' ');
        text.push_str(uri);
    }

    Some((
        crate::search::EntityRef::Graph {
            branch_id: key.namespace.branch_id,
            space: key.namespace.space.to_string(),
            key: user_key,
        },
        text,
    ))
}

#[derive(serde::Deserialize)]
struct GraphNodeData {
    #[serde(default)]
    entity_ref: Option<String>,
    #[serde(default)]
    properties: Option<serde_json::Value>,
    #[serde(default)]
    object_type: Option<String>,
}

fn visible_entries_by_type(
    db: &Database,
    branch_id: strata_core::types::BranchId,
    type_tag: TypeTag,
) -> Vec<(Key, strata_core::VersionedValue)> {
    let snapshot_version = CommitVersion(db.storage().version());
    db.storage()
        .list_by_type_at_version(&branch_id, type_tag, snapshot_version)
        .into_iter()
        .filter(|entry| !entry.is_tombstone)
        .filter_map(|entry| {
            db.storage()
                .get_versioned(&entry.key, snapshot_version)
                .ok()
                .flatten()
                .map(|vv| (entry.key, vv))
        })
        .collect()
}

fn visible_search_documents_for_branch(
    db: &Database,
    branch_id: strata_core::types::BranchId,
) -> Vec<(crate::search::EntityRef, String)> {
    let mut docs = Vec::new();

    for (key, vv) in visible_entries_by_type(db, branch_id, TypeTag::KV) {
        if let Some(SearchRefreshOp::Index { entity_ref, text }) =
            index_replayed_document(&key, &vv.value)
        {
            docs.push((entity_ref, text));
        }
    }

    for (key, vv) in visible_entries_by_type(db, branch_id, TypeTag::Event) {
        if let Some(SearchRefreshOp::Index { entity_ref, text }) =
            index_replayed_document(&key, &vv.value)
        {
            docs.push((entity_ref, text));
        }
    }

    for (key, vv) in visible_entries_by_type(db, branch_id, TypeTag::Graph) {
        if let Some(doc) = parse_graph_node_index_entry(&key, &vv.value) {
            docs.push(doc);
        }
    }

    for (key, vv) in visible_entries_by_type(db, branch_id, TypeTag::Json) {
        if let Some(SearchRefreshOp::Index { entity_ref, text }) =
            index_replayed_document(&key, &vv.value)
        {
            docs.push((entity_ref, text));
        }
    }

    docs
}

fn is_reconciled_search_entity_ref(
    entity_ref: &crate::search::EntityRef,
    branch_id: strata_core::types::BranchId,
) -> bool {
    match entity_ref {
        crate::search::EntityRef::Kv {
            branch_id: ref_bid, ..
        }
        | crate::search::EntityRef::Event {
            branch_id: ref_bid, ..
        }
        | crate::search::EntityRef::Json {
            branch_id: ref_bid, ..
        }
        | crate::search::EntityRef::Graph {
            branch_id: ref_bid, ..
        } => *ref_bid == branch_id,
        crate::search::EntityRef::Vector { .. } | crate::search::EntityRef::Branch { .. } => false,
    }
}

fn rebuild_search_entries_for_branch(
    db: &Database,
    index: &InvertedIndex,
    branch_id: strata_core::types::BranchId,
) -> u64 {
    let current_docs = visible_search_documents_for_branch(db, branch_id);
    for (entity_ref, text) in &current_docs {
        index.index_document(entity_ref, text, None);
    }
    current_docs.len() as u64
}

fn reconcile_search_entries_for_branch(
    db: &Database,
    index: &InvertedIndex,
    branch_id: strata_core::types::BranchId,
) -> u64 {
    let current_docs = visible_search_documents_for_branch(db, branch_id);
    let current_refs: HashSet<_> = current_docs
        .iter()
        .map(|(entity_ref, _)| entity_ref.clone())
        .collect();

    let mut reconciled = 0u64;

    for entity_ref in index.entity_refs_snapshot() {
        if is_reconciled_search_entity_ref(&entity_ref, branch_id)
            && index.has_document(&entity_ref)
            && !current_refs.contains(&entity_ref)
        {
            index.remove_document(&entity_ref);
            reconciled += 1;
        }
    }

    for (entity_ref, text) in &current_docs {
        if index.document_matches_text(entity_ref, text) {
            continue;
        }
        index.index_document(entity_ref, text, None);
        reconciled += 1;
    }

    reconciled
}

enum SearchRefreshOp {
    Index {
        entity_ref: crate::search::EntityRef,
        text: String,
    },
    Remove {
        entity_ref: crate::search::EntityRef,
    },
}

fn index_replayed_document(key: &Key, value: &Value) -> Option<SearchRefreshOp> {
    let branch_id = key.namespace.branch_id;
    match key.type_tag {
        TypeTag::KV => {
            if let Some(text) = extract_indexable_text(value) {
                if let Some(user_key) = key.user_key_string() {
                    return Some(SearchRefreshOp::Index {
                        entity_ref: crate::search::EntityRef::Kv {
                            branch_id,
                            space: key.namespace.space.to_string(),
                            key: user_key,
                        },
                        text,
                    });
                }
            }
        }
        TypeTag::Event => {
            if *key.user_key == *b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                return None;
            }
            if let Some(text) = extract_indexable_text(value) {
                if key.user_key.len() == 8 {
                    let sequence = u64::from_be_bytes((*key.user_key).try_into().unwrap_or([0; 8]));
                    return Some(SearchRefreshOp::Index {
                        entity_ref: crate::search::EntityRef::Event {
                            branch_id,
                            space: key.namespace.space.to_string(),
                            sequence,
                        },
                        text,
                    });
                }
            }
        }
        TypeTag::Json => {
            if is_json_internal_space(&key.namespace.space) {
                return None;
            }
            let doc_id = key.user_key_string()?;
            let Ok(doc) = crate::primitives::json::JsonStore::deserialize_doc(value) else {
                return None;
            };
            let text = serde_json::to_string(doc.value.as_inner()).unwrap_or_default();
            return Some(SearchRefreshOp::Index {
                entity_ref: crate::search::EntityRef::Json {
                    branch_id,
                    space: key.namespace.space.to_string(),
                    doc_id,
                },
                text,
            });
        }
        _ => {}
    }
    None
}

fn remove_replayed_document(key: &Key) -> Option<SearchRefreshOp> {
    let branch_id = key.namespace.branch_id;
    match key.type_tag {
        TypeTag::KV => {
            if let Some(user_key) = key.user_key_string() {
                return Some(SearchRefreshOp::Remove {
                    entity_ref: crate::search::EntityRef::Kv {
                        branch_id,
                        space: key.namespace.space.to_string(),
                        key: user_key,
                    },
                });
            }
        }
        TypeTag::Event => {
            if *key.user_key == *b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                return None;
            }
            if key.user_key.len() == 8 {
                let sequence = u64::from_be_bytes((*key.user_key).try_into().unwrap_or([0; 8]));
                return Some(SearchRefreshOp::Remove {
                    entity_ref: crate::search::EntityRef::Event {
                        branch_id,
                        space: key.namespace.space.to_string(),
                        sequence,
                    },
                });
            }
        }
        TypeTag::Json => {
            if is_json_internal_space(&key.namespace.space) {
                return None;
            }
            if let Some(doc_id) = key.user_key_string() {
                return Some(SearchRefreshOp::Remove {
                    entity_ref: crate::search::EntityRef::Json {
                        branch_id,
                        space: key.namespace.space.to_string(),
                        doc_id,
                    },
                });
            }
        }
        _ => {}
    }
    None
}

struct PendingSearchRefresh {
    index: std::sync::Arc<InvertedIndex>,
    ops: Vec<SearchRefreshOp>,
}

impl crate::database::refresh::PreparedRefresh for PendingSearchRefresh {
    fn publish(self: Box<Self>) {
        for op in self.ops {
            match op {
                SearchRefreshOp::Index { entity_ref, text } => {
                    self.index.index_document(&entity_ref, &text, None);
                }
                SearchRefreshOp::Remove { entity_ref } => {
                    self.index.remove_document(&entity_ref);
                }
            }
        }
    }
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
    if use_disk && !db.is_follower() {
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

        docs_indexed += rebuild_search_entries_for_branch(db, &index, branch_id);
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

        reconciled += reconcile_search_entries_for_branch(db, index, branch_id);
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

    fn recover(&self, db: &std::sync::Arc<crate::database::Database>) -> StrataResult<()> {
        recover_search_state(db)
    }

    fn initialize(&self, db: &std::sync::Arc<crate::database::Database>) -> StrataResult<()> {
        use std::sync::Arc;

        if let Ok(index) = db.extension::<InvertedIndex>() {
            let refresh_hook = Arc::new(SearchRefreshHook { index });
            if let Ok(hooks) = db.extension::<crate::RefreshHooks>() {
                hooks.register(refresh_hook);
            }
        }

        // Register commit observer for search index maintenance.
        // Index updates happen inline during primitive operations; this observer
        // handles periodic seal operations for durability.
        let commit_observer = Arc::new(SearchCommitObserver {
            _db: Arc::downgrade(db),
        });
        db.commit_observers().register(commit_observer);

        Ok(())
    }

    fn freeze(&self, db: &crate::database::Database) -> StrataResult<()> {
        db.freeze_search_index()
    }

    /// Production owner for search-cache deletion on branch drop (closes
    /// DG-017). Two-phase, best-effort:
    ///
    ///   1. In-memory: remove all documents in the deleted branch from the
    ///      shared `InvertedIndex` so subsequent searches don't return stale
    ///      hits.
    ///   2. On-disk: re-freeze the index so the persisted `search.manifest`
    ///      no longer references the deleted branch's `doc_id_map` entries.
    ///      Without this, a restart would resurrect the stale documents.
    ///
    /// Search uses a single global manifest (not per-branch directories),
    /// so the cleanup is surgical rather than directory-wipe (contrast with
    /// `VectorSubsystem::cleanup_deleted_branch`). Sealed `.sidx` segment
    /// files retain tombstones for the removed docs and self-clean on the
    /// next seal/compaction cycle — DG-017 only requires the deletion to
    /// have an owner, not a stronger physical compaction.
    ///
    /// Errors are logged at `warn!` and swallowed: the branch deletion is
    /// already committed when this hook runs, per the trait contract at
    /// `crates/engine/src/recovery/subsystem.rs`.
    ///
    /// **Concurrency note:** `freeze_search_index` writes the global
    /// `search.manifest` via a fixed `search.manifest.tmp` path with no
    /// inter-call serialization. Two concurrent branch deletes (or a
    /// concurrent freeze on shutdown) can race on that temp file. The race
    /// is pre-existing — `InvertedIndex::freeze_to_disk` is documented as a
    /// single-writer operation and is not introduced by this hook. If the
    /// race surfaces in practice, an internal mutex inside `freeze_to_disk`
    /// is the right place to fix it.
    fn cleanup_deleted_branch(
        &self,
        db: &std::sync::Arc<crate::database::Database>,
        branch_id: &strata_core::types::BranchId,
        branch_name: &str,
    ) -> StrataResult<()> {
        let Ok(index) = db.extension::<InvertedIndex>() else {
            return Ok(());
        };

        let removed = index.remove_documents_in_branch(*branch_id);

        if removed > 0 {
            if let Err(e) = db.freeze_search_index() {
                tracing::warn!(
                    target: "strata::search",
                    branch = branch_name,
                    error = %e,
                    "Failed to persist search index after branch cleanup"
                );
            } else {
                tracing::info!(
                    target: "strata::search",
                    branch = branch_name,
                    removed,
                    "Search index purged for deleted branch"
                );
            }
        }

        Ok(())
    }
}

// =============================================================================
// Search Observers
// =============================================================================

use crate::database::observers::{CommitInfo, CommitObserver, ObserverError};
use std::sync::Weak;

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
    _db: Weak<Database>,
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

struct SearchRefreshHook {
    index: std::sync::Arc<InvertedIndex>,
}

impl crate::RefreshHook for SearchRefreshHook {
    fn name(&self) -> &'static str {
        "search"
    }

    fn pre_delete_read(&self, _db: &Database, deletes: &[Key]) -> Vec<(Key, Vec<u8>)> {
        deletes
            .iter()
            .cloned()
            .map(|key| (key, Vec::new()))
            .collect()
    }

    fn apply_refresh(
        &self,
        puts: &[(Key, Value)],
        pre_read_deletes: &[(Key, Vec<u8>)],
    ) -> Result<Box<dyn crate::database::refresh::PreparedRefresh>, crate::database::RefreshHookError>
    {
        if !self.index.is_enabled() {
            return Ok(Box::new(crate::database::refresh::NoopPreparedRefresh));
        }

        let mut ops = Vec::with_capacity(puts.len() + pre_read_deletes.len());
        for (key, value) in puts {
            if let Some(op) = index_replayed_document(key, value) {
                ops.push(op);
            }
        }
        for (key, _) in pre_read_deletes {
            if let Some(op) = remove_replayed_document(key) {
                ops.push(op);
            }
        }

        Ok(Box::new(PendingSearchRefresh {
            index: self.index.clone(),
            ops,
        }))
    }

    fn freeze_to_disk(&self, _db: &Database) -> StrataResult<()> {
        Ok(())
    }
}
