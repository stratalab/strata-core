//! Branch command handlers (MVP)
//!
//! This module implements handlers for MVP Branch commands by dispatching
//! to the engine's `BranchService` via `db.branches()`.

use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_engine::BranchMetadata;
use strata_engine::{
    CherryPickFilter, DiffFilter, DiffOptions, ForkOptions, MergeBaseInfo, MergeOptions,
};

use crate::bridge::{extract_version, from_engine_branch_status, Primitives};
use crate::convert::convert_result;
use crate::types::{BranchId, BranchInfo, VersionedBranchInfo};
use crate::{Error, Output, Result};

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

// =============================================================================
// MVP Handlers
// =============================================================================

/// Handle BranchCreate command.
pub fn branch_create(
    p: &Arc<Primitives>,
    branch_id: Option<String>,
    metadata: Option<strata_core::Value>,
) -> Result<Output> {
    // BranchCreate still accepts metadata for command/API parity, but the
    // current executor MVP path does not persist it yet.
    let _ = metadata;

    // Users can provide any string as a branch name (like git branch names).
    // If not provided, generate a UUID for anonymous branches.
    let branch_str = branch_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Use BranchService for canonical path with DAG integration.
    let metadata = convert_result(p.db.branches().create(&branch_str))?;

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
    let result = convert_result(p.db.branches().info_versioned(branch.as_str()))?;
    match result {
        Some(v) => Ok(Output::MaybeBranchInfo(Some(versioned_to_branch_info(v)))),
        None => Ok(Output::MaybeBranchInfo(None)),
    }
}

/// Handle BranchList command.
pub fn branch_list(
    p: &Arc<Primitives>,
    state: Option<crate::types::BranchStatus>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Output> {
    // The executor currently exposes only the Active branch status, so the
    // status filter is a no-op until additional user-visible states exist.
    let _ = state;

    let branches = p.db.branches();
    let ids = convert_result(branches.list())?;

    let mut all = Vec::new();
    for id in ids {
        if id.starts_with("_system") {
            continue;
        }
        if let Some(versioned) = convert_result(branches.info_versioned(&id))? {
            all.push(versioned_to_branch_info(versioned));
        }
    }

    let offset = offset.unwrap_or(0) as usize;
    let branches = all.into_iter().skip(offset);

    let limited: Vec<VersionedBranchInfo> = match limit {
        Some(l) => branches.take(l as usize).collect(),
        None => branches.collect(),
    };

    Ok(Output::BranchInfoList(limited))
}

/// Handle BranchExists command.
pub fn branch_exists(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    if branch.as_str().starts_with("_system") {
        return Ok(Output::Bool(false));
    }
    let exists = convert_result(p.db.branches().exists(branch.as_str()))?;
    Ok(Output::Bool(exists))
}

/// Handle BranchDelete command.
///
/// BranchService owns the canonical delete path, including DAG integration and
/// subsystem teardown for branch-owned runtime state.
pub fn branch_delete(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    // Use BranchService for the canonical path.
    convert_result(p.db.branches().delete(branch.as_str()))?;
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
    // Use BranchService for canonical path with DAG integration.
    let options = match (&message, &creator) {
        (Some(m), Some(c)) => ForkOptions::default()
            .with_message(m.clone())
            .with_creator(c.clone()),
        (Some(m), None) => ForkOptions::default().with_message(m.clone()),
        (None, Some(c)) => ForkOptions::default().with_creator(c.clone()),
        (None, None) => ForkOptions::default(),
    };
    let info = convert_result(
        p.db.branches()
            .fork_with_options(&source, &destination, options),
    )?;

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
    let options = DiffOptions {
        filter: if has_filter {
            Some(DiffFilter {
                primitives: filter_primitives,
                spaces: filter_spaces,
            })
        } else {
            None
        },
        as_of,
    };
    let result = convert_result(
        p.db.branches()
            .diff_with_options(&branch_a, &branch_b, options),
    )?;
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
    // Use BranchService for canonical path with DAG integration.
    let mut options = MergeOptions::with_strategy(strategy);
    if let Some(m) = &message {
        options = options.with_message(m.clone());
    }
    if let Some(c) = &creator {
        options = options.with_creator(c.clone());
    }
    let info = convert_result(
        p.db.branches()
            .merge_with_options(&source, &target, options),
    )?;

    Ok(Output::BranchMerged(info))
}

/// Handle BranchDiffThreeWay command.
pub fn branch_diff_three_way(
    p: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
) -> Result<Output> {
    // Route through BranchService — it resolves DAG-backed merge bases when
    // the graph subsystem is installed and falls back to storage-level fork
    // info otherwise.
    let result = convert_result(p.db.branches().diff3(&branch_a, &branch_b, None))?;

    Ok(Output::ThreeWayDiff(result))
}

/// Handle BranchMergeBase command.
pub fn branch_merge_base(
    p: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
) -> Result<Output> {
    // Route through BranchService — it queries merge_base from DAG internally.
    let merge_base = convert_result(p.db.branches().merge_base(&branch_a, &branch_b))?;

    // Convert MergeBaseResult to MergeBaseInfo for output compatibility
    let result = merge_base.map(|mb| MergeBaseInfo {
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
    // Use BranchService for canonical path with DAG integration.
    let info = convert_result(p.db.branches().revert(
        &branch,
        CommitVersion(from_version),
        CommitVersion(to_version),
    ))?;

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
    // Use BranchService for canonical path with DAG integration.
    let info = convert_result(if let Some(keys) = keys {
        // Direct key pick
        p.db.branches().cherry_pick(&source, &target, &keys)
    } else {
        let filter = CherryPickFilter {
            spaces: filter_spaces,
            keys: filter_keys,
            primitives: filter_primitives,
        };
        p.db.branches()
            .cherry_pick_from_diff(&source, &target, filter, None)
    })?;

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
///
/// Routes through `db.branches().tag()` for observer notification.
pub fn tag_create(
    p: &Arc<Primitives>,
    branch: String,
    name: String,
    version: Option<u64>,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    // Use BranchService for canonical path with observer notification.
    let info = convert_result(p.db.branches().tag(
        &branch,
        &name,
        version,
        message.as_deref(),
        creator.as_deref(),
    ))?;

    Ok(Output::TagCreated(info))
}

/// Handle TagDelete command.
///
/// Routes through `db.branches().untag()` for observer notification.
pub fn tag_delete(p: &Arc<Primitives>, branch: String, name: String) -> Result<Output> {
    // Use BranchService for canonical path with observer notification.
    let deleted = convert_result(p.db.branches().untag(&branch, &name))?;
    Ok(Output::Bool(deleted))
}

/// Handle TagList command.
pub fn tag_list(p: &Arc<Primitives>, branch: String) -> Result<Output> {
    let tags = convert_result(p.db.branches().list_tags(&branch))?;
    Ok(Output::TagList(tags))
}

/// Handle TagResolve command.
pub fn tag_resolve(p: &Arc<Primitives>, branch: String, name: String) -> Result<Output> {
    let tag = convert_result(p.db.branches().resolve_tag(&branch, &name))?;
    Ok(Output::MaybeTag(tag))
}

// =============================================================================
// Note Handlers
// =============================================================================

/// Handle NoteAdd command.
///
/// Routes through `db.branches().add_note()`.
/// Notes are metadata annotations — they don't trigger the BranchOpObserver
/// pipeline since they're not structural branch operations.
pub fn note_add(
    p: &Arc<Primitives>,
    branch: String,
    version: u64,
    message: String,
    author: Option<String>,
    metadata: Option<strata_core::Value>,
) -> Result<Output> {
    // Use BranchService for canonical path.
    let note = convert_result(p.db.branches().add_note(
        &branch,
        CommitVersion(version),
        &message,
        author.as_deref(),
        metadata,
    ))?;

    Ok(Output::NoteAdded(note))
}

/// Handle NoteGet command.
pub fn note_get(p: &Arc<Primitives>, branch: String, version: Option<u64>) -> Result<Output> {
    let notes = convert_result(p.db.branches().get_notes(&branch, version))?;
    Ok(Output::NoteList(notes))
}

/// Handle NoteDelete command.
pub fn note_delete(p: &Arc<Primitives>, branch: String, version: u64) -> Result<Output> {
    let deleted = convert_result(p.db.branches().delete_note(&branch, CommitVersion(version)))?;
    Ok(Output::Bool(deleted))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
