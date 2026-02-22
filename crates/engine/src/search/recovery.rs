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
//!    - **Slow path**: Scans all KV/State/Event entries, indexes them, and
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
use crate::recovery::{register_recovery_participant, RecoveryParticipant};
use crate::search::InvertedIndex;
use strata_core::types::TypeTag;
use strata_core::value::Value;
use strata_core::StrataResult;
use tracing::info;

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
    // Slow path: rebuild from KV/State/Event entries
    // ---------------------------------------------------------------
    let mut docs_indexed: u64 = 0;
    let mut branches_scanned: u64 = 0;

    for branch_id in db.storage().branch_ids() {
        branches_scanned += 1;

        // --- KV entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::KV) {
            let text = match &vv.value {
                Value::String(s) => s.clone(),
                Value::Null | Value::Bool(_) | Value::Bytes(_) => continue,
                other => match serde_json::to_string(other) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
            };

            let user_key = match key.user_key_string() {
                Some(k) => k,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::Kv {
                branch_id,
                key: user_key,
            };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }

        // --- State entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::State) {
            let text = match &vv.value {
                Value::String(s) => s.clone(),
                Value::Null | Value::Bool(_) | Value::Bytes(_) => continue,
                other => match serde_json::to_string(other) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
            };

            let name = match key.user_key_string() {
                Some(n) => n,
                None => continue,
            };

            let entity_ref = crate::search::EntityRef::State { branch_id, name };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }

        // --- Event entries ---
        for (key, vv) in db.storage().list_by_type(&branch_id, TypeTag::Event) {
            // Skip metadata keys (same as checkpoint logic)
            if key.user_key == b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                continue;
            }

            let text = match &vv.value {
                Value::String(s) => s.clone(),
                Value::Null | Value::Bool(_) | Value::Bytes(_) => continue,
                other => match serde_json::to_string(other) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
            };

            // Parse sequence from the key (8-byte big-endian u64)
            let sequence = if key.user_key.len() == 8 {
                u64::from_be_bytes(key.user_key.as_slice().try_into().unwrap_or([0; 8]))
            } else {
                continue; // Skip non-sequence keys
            };

            let entity_ref = crate::search::EntityRef::Event {
                branch_id,
                sequence,
            };
            index.index_document(&entity_ref, &text, None);
            docs_indexed += 1;
        }
    }

    // Freeze to disk for next startup (fast path)
    if use_disk && docs_indexed > 0 {
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

/// Register the InvertedIndex as a recovery participant.
///
/// Call this once during application startup, before opening any Database.
/// This ensures that the search index is automatically restored when a
/// Database is reopened.
pub fn register_search_recovery() {
    register_recovery_participant(RecoveryParticipant::new("search", recover_search_state));
}
