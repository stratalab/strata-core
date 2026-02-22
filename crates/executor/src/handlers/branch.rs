//! Branch command handlers (MVP)
//!
//! This module implements handlers for MVP Branch commands by dispatching
//! directly to engine primitives via `bridge::Primitives`.

use std::sync::Arc;

use strata_engine::BranchMetadata;

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
        created_at: m.created_at,
        updated_at: m.updated_at,
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
        });
    }
    if name.trim().is_empty() {
        return Err(Error::InvalidInput {
            reason: "Branch name must not be whitespace-only".to_string(),
        });
    }
    if name.contains('\0') {
        return Err(Error::InvalidInput {
            reason: "Branch name must not contain NUL bytes".to_string(),
        });
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(Error::InvalidInput {
            reason: "Branch name must not contain control characters".to_string(),
        });
    }
    if name.len() > 255 {
        return Err(Error::InvalidInput {
            reason: "Branch name must not exceed 255 bytes".to_string(),
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

    // MVP: ignore metadata, use simple create_branch
    let versioned = convert_result(p.branch.create_branch(&branch_str))?;

    Ok(Output::BranchWithVersion {
        info: metadata_to_branch_info(&versioned.value),
        version: extract_version(&versioned.version),
    })
}

/// Handle BranchGet command.
pub fn branch_get(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
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
    convert_result(p.branch.delete_branch(branch.as_str()))?;

    // Cleanup: remove per-branch commit lock (#944)
    // Convert the executor BranchId to core BranchId for the lock cleanup
    if let Ok(core_branch_id) = crate::bridge::to_core_branch_id(&branch) {
        p.db.remove_branch_lock(&core_branch_id);

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

    Ok(Output::Unit)
}

// =============================================================================
// Branch Operations (fork, diff, merge)
// =============================================================================

/// Handle BranchFork command.
pub fn branch_fork(p: &Arc<Primitives>, source: String, destination: String) -> Result<Output> {
    let info =
        strata_engine::branch_ops::fork_branch(&p.db, &source, &destination).map_err(|e| {
            Error::Internal {
                reason: e.to_string(),
            }
        })?;
    Ok(Output::BranchForked(info))
}

/// Handle BranchDiff command.
pub fn branch_diff(p: &Arc<Primitives>, branch_a: String, branch_b: String) -> Result<Output> {
    let result =
        strata_engine::branch_ops::diff_branches(&p.db, &branch_a, &branch_b).map_err(|e| {
            Error::Internal {
                reason: e.to_string(),
            }
        })?;
    Ok(Output::BranchDiff(result))
}

/// Handle BranchMerge command.
pub fn branch_merge(
    p: &Arc<Primitives>,
    source: String,
    target: String,
    strategy: strata_engine::MergeStrategy,
) -> Result<Output> {
    let info = strata_engine::branch_ops::merge_branches(&p.db, &source, &target, strategy)
        .map_err(|e| Error::Internal {
            reason: e.to_string(),
        })?;
    Ok(Output::BranchMerged(info))
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
    let info = strata_engine::bundle::import_branch(&p.db, import_path).map_err(|e| Error::Io {
        reason: format!("Import failed: {}", e),
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
            created_at: 1000000,
            updated_at: 2000000,
            completed_at: None,
            error: None,
            version: 1,
        };
        let info = metadata_to_branch_info(&m);
        assert_eq!(info.id.as_str(), "test-branch");
        assert_eq!(info.status, crate::types::BranchStatus::Active);
    }
}
