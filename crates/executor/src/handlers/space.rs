//! Space management command handlers.

use std::sync::Arc;

use strata_core::types::{Key, Namespace, TypeTag};
use strata_core::validate_space_name;

use crate::bridge::{to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::BranchId;
use crate::{Error, Output, Result};

/// Handle SpaceList command.
pub fn space_list(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;
    let spaces = convert_result(p.space.list(core_branch_id))?;
    Ok(Output::SpaceList(spaces))
}

/// Handle SpaceCreate command.
pub fn space_create(p: &Arc<Primitives>, branch: BranchId, space: String) -> Result<Output> {
    validate_space_name(&space).map_err(|reason| Error::InvalidInput { reason, hint: None })?;
    let core_branch_id = to_core_branch_id(&branch)?;
    convert_result(p.space.register(core_branch_id, &space))?;
    Ok(Output::Unit)
}

/// Handle SpaceDelete command.
pub fn space_delete(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    force: bool,
) -> Result<Output> {
    // Cannot delete the default or _system_ space
    if space == "default" || space == strata_engine::system_space::SYSTEM_SPACE {
        return Err(Error::ConstraintViolation {
            reason: format!("Cannot delete the '{}' space", space),
        });
    }

    let core_branch_id = to_core_branch_id(&branch)?;

    // If not forcing, check that the space is empty
    if !force {
        let empty = convert_result(p.space.is_empty(core_branch_id, &space))?;
        if !empty {
            return Err(Error::ConstraintViolation {
                reason: format!(
                    "Space '{}' is not empty. Use force=true to delete anyway.",
                    space
                ),
            });
        }
    }

    // Delete all data in the space by scanning+deleting all TypeTag prefixes
    let ns = std::sync::Arc::new(Namespace::for_branch_space(core_branch_id, &space));
    convert_result(p.db.transaction(core_branch_id, |txn| {
        for type_tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::Json,
            TypeTag::Vector,
            TypeTag::Graph,
        ] {
            let prefix = Key::new(ns.clone(), type_tag, vec![]);
            let entries = txn.scan_prefix(&prefix)?;
            for (key, _) in entries {
                txn.delete(key)?;
            }
        }
        Ok(())
    }))?;

    // Post-commit secondary cleanup. Each step is best-effort: a failure
    // logs a warning but never rolls back the primary delete (which has
    // already committed above). Without this block, force-deleting a
    // space leaves stale BM25 docs, ghost vector backends, dangling
    // on-disk vector caches, and orphan shadow embeddings — see Phase 4
    // of `docs/design/space-correctness-fix-plan.md`.

    // 1) BM25 inverted index — drop every doc whose EntityRef lives in
    //    (branch, space). Skipped if the search index is not registered.
    if let Ok(index) = p.db.extension::<strata_engine::search::InvertedIndex>() {
        let removed = index.remove_documents_in_space(core_branch_id, &space);
        if removed > 0 {
            tracing::info!(
                target: "strata::space",
                branch_id = %core_branch_id,
                space = space.as_str(),
                removed,
                "Removed BM25 documents during space delete"
            );
        }
    }

    // 2) Vector backends + on-disk caches — purge every collection in
    //    (branch, space) from the in-memory state and wipe its `.vec`
    //    and `_graphs/` cache files.
    if let Err(e) = p.vector.purge_collections_in_space(core_branch_id, &space) {
        tracing::warn!(
            target: "strata::space",
            branch_id = %core_branch_id,
            space = space.as_str(),
            error = %e,
            "Failed to purge vector collections during space delete"
        );
    }

    // 3) Shadow embeddings — delete every entry in `_system_embed_*`
    //    whose source-key prefix matches the deleted space, and drain
    //    the in-memory embed buffer for the same (branch, space).
    //    No-op when the `embed` feature is not compiled in.
    let _ =
        crate::handlers::embed_hook::delete_shadow_embeddings_for_space(p, core_branch_id, &space);

    // Delete the space metadata key (last — keeps the metadata key as
    // the externally observable "space exists" signal until all
    // secondary cleanup has had a chance to run).
    convert_result(p.space.delete(core_branch_id, &space))?;

    Ok(Output::Unit)
}

/// Handle SpaceExists command.
pub fn space_exists(p: &Arc<Primitives>, branch: BranchId, space: String) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;
    let exists = convert_result(p.space.exists(core_branch_id, &space))?;
    Ok(Output::Bool(exists))
}
