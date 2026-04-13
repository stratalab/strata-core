//! Branch command handlers (MVP)
//!
//! This module implements handlers for MVP Branch commands by dispatching
//! to the engine's `BranchService` via `db.branches()`.

use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_engine::{ForkOptions, MergeOptions};
use strata_engine::BranchMetadata;

use crate::bridge::{extract_version, from_engine_branch_status, Primitives};
use crate::convert::convert_result;
use crate::types::{BranchId, BranchInfo, VersionedBranchInfo};
use crate::{Error, Output, Result};

// =============================================================================
// Audit Log Helper
// =============================================================================

/// Emit an audit event on the `_system_` branch. Best-effort — failures are
/// logged, never propagated.
fn emit_audit_event(p: &Arc<Primitives>, event_type: &str, payload: serde_json::Value) {
    let system_branch_id = strata_engine::primitives::branch::resolve_branch_name("_system_");
    // Convert serde_json::Value (Object) to strata_core::Value (Object).
    // Event payload must be a Value::Object for event log validation.
    let core_value: strata_core::value::Value = payload.into();
    if let Err(e) = p
        .event
        .append(&system_branch_id, "default", event_type, core_value)
    {
        tracing::warn!(
            target: "strata::audit",
            event_type,
            error = %e,
            "Failed to emit audit event"
        );
    }
}

// =============================================================================
// Conversion Helpers
// =============================================================================

/// Convert engine BranchMetadata to executor BranchInfo.
fn metadata_to_branch_info(m: &BranchMetadata) -> BranchInfo {
    BranchInfo {
        id: BranchId::from(m.name.clone()),
        status: from_engine_branch_status(m.status),
        created_at: m.created_at.as_micros(),
        updated_at: m.updated_at.as_micros(),
        parent_id: None,
    }
}

/// Convert engine Versioned<BranchMetadata> to executor VersionedBranchInfo.
fn versioned_to_branch_info(v: strata_core::Versioned<BranchMetadata>) -> VersionedBranchInfo {
    let info = metadata_to_branch_info(&v.value);
    VersionedBranchInfo {
        info,
        version: extract_version(&v.version),
        timestamp: v.timestamp.into(),
    }
}

/// Validate a branch name.
///
/// Rejects empty names, whitespace-only names, and names containing
/// control characters or NUL bytes.
fn validate_branch_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidInput {
            reason: "Branch name must not be empty".to_string(),
            hint: None,
        });
    }
    if name.trim().is_empty() {
        return Err(Error::InvalidInput {
            reason: "Branch name must not be whitespace-only".to_string(),
            hint: None,
        });
    }
    if name.contains('\0') {
        return Err(Error::InvalidInput {
            reason: "Branch name must not contain NUL bytes".to_string(),
            hint: None,
        });
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(Error::InvalidInput {
            reason: "Branch name must not contain control characters".to_string(),
            hint: None,
        });
    }
    if name.len() > 255 {
        return Err(Error::InvalidInput {
            reason: "Branch name must not exceed 255 bytes".to_string(),
            hint: None,
        });
    }
    if name.starts_with("_system") {
        return Err(Error::InvalidInput {
            reason: "Branch names starting with '_system' are reserved".to_string(),
            hint: Some("Choose a different branch name.".to_string()),
        });
    }
    Ok(())
}

/// Guard: reject operations on the default branch that would delete it.
fn reject_default_branch(branch: &BranchId, operation: &str) -> Result<()> {
    if branch.is_default() {
        return Err(Error::ConstraintViolation {
            reason: format!("Cannot {} the default branch", operation),
        });
    }
    Ok(())
}

// =============================================================================
// MVP Handlers
// =============================================================================

/// Handle BranchCreate command.
pub fn branch_create(
    p: &Arc<Primitives>,
    branch_id: Option<String>,
    _metadata: Option<strata_core::Value>,
) -> Result<Output> {
    // Users can provide any string as a branch name (like git branch names).
    // If not provided, generate a UUID for anonymous branches.
    let branch_str = match &branch_id {
        Some(s) => {
            validate_branch_name(s)?;
            s.clone()
        }
        None => uuid::Uuid::new_v4().to_string(),
    };

    // Use BranchService for canonical path with DAG integration.
    let metadata = p.db.branches().create(&branch_str).map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
    })?;

    emit_audit_event(
        p,
        "branch.create",
        serde_json::json!({
            "branch": branch_str,
        }),
    );

    Ok(Output::BranchWithVersion {
        info: metadata_to_branch_info(&metadata),
        version: metadata.version,
    })
}

/// Handle BranchGet command.
pub fn branch_get(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    if branch.as_str().starts_with("_system") {
        return Ok(Output::MaybeBranchInfo(None));
    }
    let result = convert_result(p.branch.get_branch(branch.as_str()))?;
    match result {
        Some(v) => Ok(Output::MaybeBranchInfo(Some(versioned_to_branch_info(v)))),
        None => Ok(Output::MaybeBranchInfo(None)),
    }
}

/// Handle BranchList command.
pub fn branch_list(
    p: &Arc<Primitives>,
    _state: Option<crate::types::BranchStatus>,
    limit: Option<u64>,
    _offset: Option<u64>,
) -> Result<Output> {
    // MVP: ignore status filter, list all branches
    let ids = convert_result(p.branch.list_branches())?;

    let mut all = Vec::new();
    for id in ids {
        if id.starts_with("_system") {
            continue;
        }
        if let Some(versioned) = convert_result(p.branch.get_branch(&id))? {
            all.push(versioned_to_branch_info(versioned));
        }
    }

    // Apply limit if specified
    let limited: Vec<VersionedBranchInfo> = match limit {
        Some(l) => all.into_iter().take(l as usize).collect(),
        None => all,
    };

    Ok(Output::BranchInfoList(limited))
}

/// Handle BranchExists command.
pub fn branch_exists(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    if branch.as_str().starts_with("_system") {
        return Ok(Output::Bool(false));
    }
    let exists = convert_result(p.branch.exists(branch.as_str()))?;
    Ok(Output::Bool(exists))
}

/// Handle BranchDelete command.
///
/// After deleting the branch metadata, performs cleanup:
/// - Removes the per-branch commit lock to prevent unbounded growth (#944)
/// - Deletes all vector collections for the branch to free memory (#946)
pub fn branch_delete(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    reject_default_branch(&branch, "delete")?;
    crate::handlers::reject_system_branch(&branch)?;

    // Use BranchService for canonical path with DAG integration.
    p.db.branches().delete(branch.as_str()).map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
    })?;

    // Cleanup: remove per-branch commit lock (#944)
    // Convert the executor BranchId to core BranchId for the lock cleanup
    if let Ok(core_branch_id) = crate::bridge::to_core_branch_id(&branch) {
        // Clean up storage-layer segments (segment files, memtables, refcounts) (#1702).
        // Must happen after logical deletion so in-progress reads see the deletion first.
        p.db.clear_branch_storage(&core_branch_id);

        // Best-effort: if a concurrent commit holds the lock, skip removal.
        // The stale entry is harmless and will be cleaned up on next attempt.
        let _ = p.db.remove_branch_lock(&core_branch_id);

        // Cleanup: delete all vector collections for this branch (#946)
        // Best-effort: silently continue if vector cleanup fails, since the
        // branch metadata is already deleted and data will be orphaned but harmless.
        if let Ok(collections) = p.vector.list_collections(core_branch_id, "default") {
            for collection in collections {
                let _ = p
                    .vector
                    .delete_collection(core_branch_id, "default", &collection.name);
            }
        }
    }

    emit_audit_event(
        p,
        "branch.delete",
        serde_json::json!({
            "branch": branch.as_str(),
        }),
    );

    Ok(Output::Unit)
}

// =============================================================================
// Branch Operations (fork, diff, merge)
// =============================================================================

/// Handle BranchFork command.
pub fn branch_fork(
    p: &Arc<Primitives>,
    source: String,
    destination: String,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    if source.starts_with("_system") {
        return Err(Error::InvalidInput {
            reason: format!("Cannot fork from reserved branch '{}'", source),
            hint: Some("Branches starting with '_system' are internal.".to_string()),
        });
    }
    validate_branch_name(&destination)?;

    // Use BranchService for canonical path with DAG integration.
    let options = ForkOptions::default()
        .with_message(message.clone().unwrap_or_default())
        .with_creator(creator.clone().unwrap_or_default());
    // Only set message/creator if they were provided
    let options = match (&message, &creator) {
        (Some(m), Some(c)) => ForkOptions::default().with_message(m.clone()).with_creator(c.clone()),
        (Some(m), None) => ForkOptions::default().with_message(m.clone()),
        (None, Some(c)) => ForkOptions::default().with_creator(c.clone()),
        (None, None) => ForkOptions::default(),
    };
    let info = p.db.branches().fork_with_options(&source, &destination, options)
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

    emit_audit_event(
        p,
        "branch.fork",
        serde_json::json!({
            "parent": source,
            "child": destination,
            "fork_version": info.fork_version,
        }),
    );

    Ok(Output::BranchForked(info))
}

/// Handle BranchDiff command.
pub fn branch_diff(
    p: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
    filter_primitives: Option<Vec<strata_core::PrimitiveType>>,
    filter_spaces: Option<Vec<String>>,
    as_of: Option<u64>,
) -> Result<Output> {
    let has_filter = filter_primitives.is_some() || filter_spaces.is_some();
    let options = strata_engine::branch_ops::DiffOptions {
        filter: if has_filter {
            Some(strata_engine::branch_ops::DiffFilter {
                primitives: filter_primitives,
                spaces: filter_spaces,
            })
        } else {
            None
        },
        as_of,
    };
    let result =
        strata_engine::branch_ops::diff_branches_with_options(&p.db, &branch_a, &branch_b, options)
            .map_err(|e| Error::Internal {
                reason: e.to_string(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            })?;
    Ok(Output::BranchDiff(result))
}

/// Handle BranchMerge command.
pub fn branch_merge(
    p: &Arc<Primitives>,
    source: String,
    target: String,
    strategy: strata_engine::MergeStrategy,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    if source.starts_with("_system") || target.starts_with("_system") {
        return Err(Error::InvalidInput {
            reason: "Cannot merge to or from reserved '_system' branches".to_string(),
            hint: Some("Branches starting with '_system' are internal.".to_string()),
        });
    }

    // Query merge base from DAG via BranchService (for repeated merges and materialized branches).
    // Failures are OK — engine falls back to storage-level fork info.
    let merge_base_override = p.db.branches().merge_base(&source, &target).ok().flatten();

    // Use BranchService for canonical path with DAG integration.
    let mut options = MergeOptions::with_strategy(strategy);
    if let Some(mb) = merge_base_override {
        options = options.with_merge_base(mb.branch_id, mb.commit_version.0);
    }
    if let Some(m) = &message {
        options = options.with_message(m.clone());
    }
    if let Some(c) = &creator {
        options = options.with_creator(c.clone());
    }
    let info = p.db.branches().merge_with_options(&source, &target, options)
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

    let strategy_str = match strategy {
        strata_engine::MergeStrategy::LastWriterWins => "last_writer_wins",
        strata_engine::MergeStrategy::Strict => "strict",
    };

    emit_audit_event(
        p,
        "branch.merge",
        serde_json::json!({
            "source": source,
            "target": target,
            "strategy": strategy_str,
            "keys_applied": info.keys_applied,
            "keys_deleted": info.keys_deleted,
            "merge_version": info.merge_version,
        }),
    );

    Ok(Output::BranchMerged(info))
}

/// Handle BranchDiffThreeWay command.
pub fn branch_diff_three_way(
    p: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
) -> Result<Output> {
    // Route through BranchService — it queries merge_base from DAG internally.
    // Pass None for merge_base; if the caller wants an override, BranchService
    // already handles querying the DAG hook.
    let result = p.db.branches().diff3(&branch_a, &branch_b, None)
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

    Ok(Output::ThreeWayDiff(result))
}

/// Handle BranchMergeBase command.
pub fn branch_merge_base(
    p: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
) -> Result<Output> {
    // Route through BranchService — it queries merge_base from DAG internally.
    let merge_base = p.db.branches().merge_base(&branch_a, &branch_b)
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

    // Convert MergeBaseResult to MergeBaseInfo for output compatibility
    let result = merge_base.map(|mb| strata_engine::branch_ops::MergeBaseInfo {
        branch: mb.branch_name,
        version: mb.commit_version,
    });

    Ok(Output::MergeBaseInfo(result))
}

// =============================================================================
// Revert Handlers
// =============================================================================

/// Handle BranchRevert command.
pub fn branch_revert(
    p: &Arc<Primitives>,
    branch: String,
    from_version: u64,
    to_version: u64,
) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;

    // Use BranchService for canonical path with DAG integration.
    let info = p.db.branches().revert(&branch, CommitVersion(from_version), CommitVersion(to_version))
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: None,
        })?;

    emit_audit_event(
        p,
        "branch.revert",
        serde_json::json!({
            "branch": branch,
            "from_version": from_version,
            "to_version": to_version,
            "keys_reverted": info.keys_reverted,
        }),
    );

    Ok(Output::BranchReverted(info))
}

// =============================================================================
// Cherry-Pick Handlers
// =============================================================================

/// Handle BranchCherryPick command.
pub fn branch_cherry_pick(
    p: &Arc<Primitives>,
    source: String,
    target: String,
    keys: Option<Vec<(String, String)>>,
    filter_spaces: Option<Vec<String>>,
    filter_keys: Option<Vec<String>>,
    filter_primitives: Option<Vec<strata_core::PrimitiveType>>,
) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(source.as_str()))?;
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(target.as_str()))?;

    // Use BranchService for canonical path with DAG integration.
    let info = if let Some(keys) = keys {
        // Direct key pick
        p.db.branches().cherry_pick(&source, &target, &keys)
    } else {
        // Diff-based pick with filter — query merge base from DAG via BranchService.
        // Failures are OK — engine falls back to storage-level fork info.
        let merge_base_override = p.db.branches()
            .merge_base(&source, &target)
            .ok()
            .flatten()
            .map(|mb| (mb.branch_id, mb.commit_version.0));
        let filter = strata_engine::branch_ops::CherryPickFilter {
            spaces: filter_spaces,
            keys: filter_keys,
            primitives: filter_primitives,
        };
        p.db.branches().cherry_pick_from_diff(&source, &target, filter, merge_base_override)
    }
    .map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: None,
    })?;

    emit_audit_event(
        p,
        "branch.cherry_pick",
        serde_json::json!({
            "source": source,
            "target": target,
            "keys_applied": info.keys_applied,
            "keys_deleted": info.keys_deleted,
        }),
    );

    Ok(Output::BranchCherryPicked(info))
}

// =============================================================================
// Bundle Handlers
// =============================================================================

/// Handle BranchExport command.
pub fn branch_export(p: &Arc<Primitives>, branch_id: String, path: String) -> Result<Output> {
    let export_path = std::path::Path::new(&path);
    let info =
        strata_engine::bundle::export_branch(&p.db, &branch_id, export_path).map_err(|e| {
            Error::Io {
                reason: format!("Export failed: {}", e),
                hint: None,
            }
        })?;

    Ok(Output::BranchExported(crate::types::BranchExportResult {
        branch_id: info.branch_id,
        path: info.path.to_string_lossy().to_string(),
        entry_count: info.entry_count,
        bundle_size: info.bundle_size,
    }))
}

/// Handle BranchImport command.
pub fn branch_import(p: &Arc<Primitives>, path: String) -> Result<Output> {
    let import_path = std::path::Path::new(&path);
    // The DAG `on_create` hook fires automatically from inside
    // `bundle::import_branch` after the branch is created.
    let info = strata_engine::bundle::import_branch(&p.db, import_path).map_err(|e| Error::Io {
        reason: format!("Import failed: {}", e),
        hint: None,
    })?;

    Ok(Output::BranchImported(crate::types::BranchImportResult {
        branch_id: info.branch_id,
        transactions_applied: info.transactions_applied,
        keys_written: info.keys_written,
    }))
}

/// Handle BranchBundleValidate command.
pub fn branch_bundle_validate(path: String) -> Result<Output> {
    let validate_path = std::path::Path::new(&path);
    let info = strata_engine::bundle::validate_bundle(validate_path).map_err(|e| Error::Io {
        reason: format!("Validation failed: {}", e),
        hint: None,
    })?;

    Ok(Output::BundleValidated(
        crate::types::BundleValidateResult {
            branch_id: info.branch_id,
            format_version: info.format_version,
            entry_count: info.entry_count,
            checksums_valid: info.checksums_valid,
        },
    ))
}

// =============================================================================
// Tag Handlers
// =============================================================================

/// Handle TagCreate command.
pub fn tag_create(
    p: &Arc<Primitives>,
    branch: String,
    name: String,
    version: Option<u64>,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let info = strata_engine::branch_ops::create_tag(
        &p.db,
        &branch,
        &name,
        version,
        message.as_deref(),
        creator.as_deref(),
    )
    .map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: None,
    })?;

    emit_audit_event(
        p,
        "branch.tag",
        serde_json::json!({
            "branch": branch,
            "tag": name,
            "version": info.version,
            "message": message,
        }),
    );

    Ok(Output::TagCreated(info))
}

/// Handle TagDelete command.
pub fn tag_delete(p: &Arc<Primitives>, branch: String, name: String) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let deleted = strata_engine::branch_ops::delete_tag(&p.db, &branch, &name).map_err(|e| {
        Error::Internal {
            reason: e.to_string(),
            hint: None,
        }
    })?;
    Ok(Output::Bool(deleted))
}

/// Handle TagList command.
pub fn tag_list(p: &Arc<Primitives>, branch: String) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let tags =
        strata_engine::branch_ops::list_tags(&p.db, &branch).map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: None,
        })?;
    Ok(Output::TagList(tags))
}

/// Handle TagResolve command.
pub fn tag_resolve(p: &Arc<Primitives>, branch: String, name: String) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let tag = strata_engine::branch_ops::resolve_tag(&p.db, &branch, &name).map_err(|e| {
        Error::Internal {
            reason: e.to_string(),
            hint: None,
        }
    })?;
    Ok(Output::MaybeTag(tag))
}

// =============================================================================
// Note Handlers
// =============================================================================

/// Handle NoteAdd command.
pub fn note_add(
    p: &Arc<Primitives>,
    branch: String,
    version: u64,
    message: String,
    author: Option<String>,
    metadata: Option<strata_core::Value>,
) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let note = strata_engine::branch_ops::add_note(
        &p.db,
        &branch,
        CommitVersion(version),
        &message,
        author.as_deref(),
        metadata,
    )
    .map_err(|e| Error::Internal {
        reason: e.to_string(),
        hint: None,
    })?;

    emit_audit_event(
        p,
        "branch.note",
        serde_json::json!({
            "branch": branch,
            "version": version,
            "message": message,
        }),
    );

    Ok(Output::NoteAdded(note))
}

/// Handle NoteGet command.
pub fn note_get(p: &Arc<Primitives>, branch: String, version: Option<u64>) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let notes = strata_engine::branch_ops::get_notes(&p.db, &branch, version).map_err(|e| {
        Error::Internal {
            reason: e.to_string(),
            hint: None,
        }
    })?;
    Ok(Output::NoteList(notes))
}

/// Handle NoteDelete command.
pub fn note_delete(p: &Arc<Primitives>, branch: String, version: u64) -> Result<Output> {
    crate::handlers::reject_system_branch(&crate::types::BranchId::from(branch.as_str()))?;
    let deleted = strata_engine::branch_ops::delete_note(&p.db, &branch, CommitVersion(version))
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
            hint: None,
        })?;
    Ok(Output::Bool(deleted))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reject_default_branch() {
        let branch = BranchId::from("default");
        assert!(reject_default_branch(&branch, "delete").is_err());

        let branch = BranchId::from("f47ac10b-58cc-4372-a567-0e02b2c3d479");
        assert!(reject_default_branch(&branch, "delete").is_ok());
    }

    #[test]
    fn test_metadata_to_branch_info() {
        let m = BranchMetadata {
            name: "test-branch".to_string(),
            branch_id: "some-uuid".to_string(),
            parent_branch: None,
            status: strata_engine::BranchStatus::Active,
            created_at: strata_core::contract::Timestamp::from(1000000u64),
            updated_at: strata_core::contract::Timestamp::from(2000000u64),
            completed_at: None,
            error: None,
            version: 1,
        };
        let info = metadata_to_branch_info(&m);
        assert_eq!(info.id.as_str(), "test-branch");
        assert_eq!(info.status, crate::types::BranchStatus::Active);
    }

    #[test]
    fn reject_create_system_branch() {
        assert!(validate_branch_name("_system_foo").is_err());
        assert!(validate_branch_name("_system_").is_err());
        assert!(validate_branch_name("_system").is_err());
    }

    #[test]
    fn reject_system_branch_variants() {
        // All _system* prefixes must be rejected
        for name in &["_system_", "_system", "_system_foo", "_system_bar"] {
            let branch = BranchId::from(*name);
            assert!(
                crate::handlers::reject_system_branch(&branch).is_err(),
                "expected rejection for '{name}'"
            );
        }
        // Normal branches must be allowed
        for name in &["default", "my-branch", "system_not_prefixed"] {
            let branch = BranchId::from(*name);
            assert!(
                crate::handlers::reject_system_branch(&branch).is_ok(),
                "expected OK for '{name}'"
            );
        }
    }

    #[test]
    fn validate_normal_branch_names() {
        assert!(validate_branch_name("my-branch").is_ok());
        assert!(validate_branch_name("feature/test").is_ok());
        assert!(validate_branch_name("default").is_ok());
    }
}
