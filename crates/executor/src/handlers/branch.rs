use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_engine::BranchMetadata;
use strata_engine::{
    CherryPickFilter, DiffFilter, DiffOptions, ForkOptions, MergeBaseInfo, MergeOptions,
};

use crate::bridge::{
    branch_is_reserved, extract_version, from_engine_branch_status, reject_reserved_branch_name,
    Primitives,
};
use crate::convert::convert_result;
use crate::{
    BranchExportResult, BranchId, BranchImportResult, BranchInfo, BranchStatus,
    BundleValidateResult, Output, Result, VersionedBranchInfo,
};

fn metadata_to_branch_info(metadata: &BranchMetadata) -> BranchInfo {
    BranchInfo {
        id: BranchId::from(metadata.name.clone()),
        status: from_engine_branch_status(metadata.status),
        created_at: metadata.created_at.as_micros(),
        updated_at: metadata.updated_at.as_micros(),
        parent_id: None,
    }
}

fn versioned_to_branch_info(value: strata_core::Versioned<BranchMetadata>) -> VersionedBranchInfo {
    VersionedBranchInfo {
        info: metadata_to_branch_info(&value.value),
        version: extract_version(&value.version),
        timestamp: value.timestamp.into(),
    }
}

pub(crate) fn create(
    db: &Arc<strata_engine::Database>,
    branch_id: Option<String>,
    metadata: Option<strata_core::Value>,
) -> Result<Output> {
    let _ = metadata;
    let branch_name = branch_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let metadata = convert_result(db.branches().create(&branch_name))?;

    Ok(Output::BranchWithVersion {
        info: metadata_to_branch_info(&metadata),
        version: metadata.version,
    })
}

pub(crate) fn get(db: &Arc<strata_engine::Database>, branch: BranchId) -> Result<Output> {
    if branch_is_reserved(branch.as_str()) {
        return Ok(Output::MaybeBranchInfo(None));
    }
    let value = convert_result(db.branches().info_versioned(branch.as_str()))?;
    Ok(Output::MaybeBranchInfo(value.map(versioned_to_branch_info)))
}

pub(crate) fn list(
    db: &Arc<strata_engine::Database>,
    state: Option<BranchStatus>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<Output> {
    let _ = state;

    let ids = convert_result(db.branches().list())?;
    let branches = ids
        .into_iter()
        .filter(|id| !branch_is_reserved(id))
        .filter_map(|id| db.branches().info_versioned(&id).ok().flatten())
        .map(versioned_to_branch_info)
        .skip(offset.unwrap_or(0) as usize);

    let limited = match limit {
        Some(limit) => branches.take(limit as usize).collect(),
        None => branches.collect(),
    };

    Ok(Output::BranchInfoList(limited))
}

pub(crate) fn exists(db: &Arc<strata_engine::Database>, branch: BranchId) -> Result<Output> {
    if branch_is_reserved(branch.as_str()) {
        return Ok(Output::Bool(false));
    }
    let exists = convert_result(db.branches().exists(branch.as_str()))?;
    Ok(Output::Bool(exists))
}

pub(crate) fn delete(db: &Arc<strata_engine::Database>, branch: BranchId) -> Result<Output> {
    convert_result(db.branches().delete(branch.as_str()))?;
    Ok(Output::Unit)
}

pub(crate) fn fork(
    primitives: &Arc<Primitives>,
    source: String,
    destination: String,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    reject_reserved_branch_name(&source)?;
    reject_reserved_branch_name(&destination)?;

    let options = match (&message, &creator) {
        (Some(message), Some(creator)) => ForkOptions::default()
            .with_message(message.clone())
            .with_creator(creator.clone()),
        (Some(message), None) => ForkOptions::default().with_message(message.clone()),
        (None, Some(creator)) => ForkOptions::default().with_creator(creator.clone()),
        (None, None) => ForkOptions::default(),
    };

    let info = convert_result(primitives.db.branches().fork_with_options(
        &source,
        &destination,
        options,
    ))?;
    Ok(Output::BranchForked(info))
}

pub(crate) fn diff(
    primitives: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
    filter_primitives: Option<Vec<strata_core::PrimitiveType>>,
    filter_spaces: Option<Vec<String>>,
    as_of: Option<u64>,
) -> Result<Output> {
    reject_reserved_branch_name(&branch_a)?;
    reject_reserved_branch_name(&branch_b)?;

    let has_filter = filter_primitives.is_some() || filter_spaces.is_some();
    let options = DiffOptions {
        filter: has_filter.then_some(DiffFilter {
            primitives: filter_primitives,
            spaces: filter_spaces,
        }),
        as_of,
    };
    let result = convert_result(
        primitives
            .db
            .branches()
            .diff_with_options(&branch_a, &branch_b, options),
    )?;
    Ok(Output::BranchDiff(result))
}

pub(crate) fn merge(
    primitives: &Arc<Primitives>,
    source: String,
    target: String,
    strategy: strata_engine::MergeStrategy,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    reject_reserved_branch_name(&source)?;
    reject_reserved_branch_name(&target)?;

    let mut options = MergeOptions::with_strategy(strategy);
    if let Some(message) = &message {
        options = options.with_message(message.clone());
    }
    if let Some(creator) = &creator {
        options = options.with_creator(creator.clone());
    }
    let info = convert_result(
        primitives
            .db
            .branches()
            .merge_with_options(&source, &target, options),
    )?;
    Ok(Output::BranchMerged(info))
}

pub(crate) fn diff_three_way(
    primitives: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
) -> Result<Output> {
    reject_reserved_branch_name(&branch_a)?;
    reject_reserved_branch_name(&branch_b)?;

    let result = convert_result(primitives.db.branches().diff3(&branch_a, &branch_b))?;
    Ok(Output::ThreeWayDiff(result))
}

pub(crate) fn merge_base(
    primitives: &Arc<Primitives>,
    branch_a: String,
    branch_b: String,
) -> Result<Output> {
    reject_reserved_branch_name(&branch_a)?;
    reject_reserved_branch_name(&branch_b)?;

    let merge_base = convert_result(primitives.db.branches().merge_base(&branch_a, &branch_b))?;
    let info = merge_base.map(|base| MergeBaseInfo {
        branch: base.branch_name,
        version: base.commit_version,
    });
    Ok(Output::MergeBaseInfo(info))
}

pub(crate) fn revert(
    primitives: &Arc<Primitives>,
    branch: String,
    from_version: u64,
    to_version: u64,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let info = convert_result(primitives.db.branches().revert(
        &branch,
        CommitVersion(from_version),
        CommitVersion(to_version),
    ))?;
    Ok(Output::BranchReverted(info))
}

pub(crate) fn cherry_pick(
    primitives: &Arc<Primitives>,
    source: String,
    target: String,
    keys: Option<Vec<(String, String)>>,
    filter_spaces: Option<Vec<String>>,
    filter_keys: Option<Vec<String>>,
    filter_primitives: Option<Vec<strata_core::PrimitiveType>>,
) -> Result<Output> {
    reject_reserved_branch_name(&source)?;
    reject_reserved_branch_name(&target)?;

    let info = convert_result(if let Some(keys) = keys {
        primitives
            .db
            .branches()
            .cherry_pick(&source, &target, &keys)
    } else {
        let filter = CherryPickFilter {
            spaces: filter_spaces,
            keys: filter_keys,
            primitives: filter_primitives,
        };
        primitives
            .db
            .branches()
            .cherry_pick_from_diff(&source, &target, filter)
    })?;
    Ok(Output::BranchCherryPicked(info))
}

pub(crate) fn export_bundle(
    primitives: &Arc<Primitives>,
    branch_id: String,
    path: String,
) -> Result<Output> {
    reject_reserved_branch_name(&branch_id)?;

    let export_path = std::path::Path::new(&path);
    let info = strata_engine::bundle::export_branch(&primitives.db, &branch_id, export_path)
        .map_err(|error| crate::Error::Io {
            reason: format!("Export failed: {error}"),
            hint: None,
        })?;

    Ok(Output::BranchExported(BranchExportResult {
        branch_id: info.branch_id,
        path: info.path.to_string_lossy().to_string(),
        entry_count: info.entry_count,
        bundle_size: info.bundle_size,
    }))
}

pub(crate) fn import_bundle(primitives: &Arc<Primitives>, path: String) -> Result<Output> {
    let import_path = std::path::Path::new(&path);
    let info =
        strata_engine::bundle::import_branch(&primitives.db, import_path).map_err(|error| {
            crate::Error::Io {
                reason: format!("Import failed: {error}"),
                hint: None,
            }
        })?;

    Ok(Output::BranchImported(BranchImportResult {
        branch_id: info.branch_id,
        transactions_applied: info.transactions_applied,
        keys_written: info.keys_written,
    }))
}

pub(crate) fn validate_bundle(path: String) -> Result<Output> {
    let validate_path = std::path::Path::new(&path);
    let info = strata_engine::bundle::validate_bundle(validate_path).map_err(|error| {
        crate::Error::Io {
            reason: format!("Validation failed: {error}"),
            hint: None,
        }
    })?;

    Ok(Output::BundleValidated(BundleValidateResult {
        branch_id: info.branch_name,
        format_version: info.format_version,
        entry_count: info.entry_count,
        checksums_valid: info.checksums_valid,
    }))
}

pub(crate) fn tag_create(
    primitives: &Arc<Primitives>,
    branch: String,
    name: String,
    version: Option<u64>,
    message: Option<String>,
    creator: Option<String>,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let info = convert_result(primitives.db.branches().tag(
        &branch,
        &name,
        version,
        message.as_deref(),
        creator.as_deref(),
    ))?;
    Ok(Output::TagCreated(info))
}

pub(crate) fn tag_delete(
    primitives: &Arc<Primitives>,
    branch: String,
    name: String,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let deleted = convert_result(primitives.db.branches().untag(&branch, &name))?;
    Ok(Output::Bool(deleted))
}

pub(crate) fn tag_list(primitives: &Arc<Primitives>, branch: String) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let tags = convert_result(primitives.db.branches().list_tags(&branch))?;
    Ok(Output::TagList(tags))
}

pub(crate) fn tag_resolve(
    primitives: &Arc<Primitives>,
    branch: String,
    name: String,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let tag = convert_result(primitives.db.branches().resolve_tag(&branch, &name))?;
    Ok(Output::MaybeTag(tag))
}

pub(crate) fn note_add(
    primitives: &Arc<Primitives>,
    branch: String,
    version: u64,
    message: String,
    author: Option<String>,
    metadata: Option<strata_core::Value>,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let note = convert_result(primitives.db.branches().add_note(
        &branch,
        CommitVersion(version),
        &message,
        author.as_deref(),
        metadata,
    ))?;
    Ok(Output::NoteAdded(note))
}

pub(crate) fn note_get(
    primitives: &Arc<Primitives>,
    branch: String,
    version: Option<u64>,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let notes = convert_result(primitives.db.branches().get_notes(&branch, version))?;
    Ok(Output::NoteList(notes))
}

pub(crate) fn note_delete(
    primitives: &Arc<Primitives>,
    branch: String,
    version: u64,
) -> Result<Output> {
    reject_reserved_branch_name(&branch)?;

    let deleted = convert_result(
        primitives
            .db
            .branches()
            .delete_note(&branch, CommitVersion(version)),
    )?;
    Ok(Output::Bool(deleted))
}
