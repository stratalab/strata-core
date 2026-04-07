//! Search Recovery Participant
//!
//! Registers the InvertedIndex as a recovery participant so that search
//! index state is restored when the Database reopens.
//!
//! ## How It Works
//!
//! 1. `register_search_recovery()` registers a recovery function with the engine
//! 2. When `Database::open()` runs, it calls all registered recovery participants
//! 3. The search recovery function either:
//!    - **Fast path**: Loads manifest + mmap'd sealed segments (sub-second)
//!    - **Slow path**: Scans all KV/Event entries, indexes them, and
//!      freezes to disk for the next open
//! 4. Enables the index for search operations
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
use crate::recovery::{register_recovery_participant, RecoveryParticipant};
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
    }

    Ok(reconciled)
}

/// Register the InvertedIndex as a recovery participant.
///
/// Call this once during application startup, before opening any Database.
/// This ensures that the search index is automatically restored when a
/// Database is reopened.
pub fn register_search_recovery() {
    register_recovery_participant(RecoveryParticipant::new("search", recover_search_state));
}

/// Subsystem implementation for search index recovery and shutdown hooks.
///
/// Used with `DatabaseBuilder` for explicit subsystem registration.
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

    fn freeze(&self, db: &crate::database::Database) -> strata_core::StrataResult<()> {
        db.freeze_search_index()
    }
}
