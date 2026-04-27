use std::sync::Arc;

use strata_storage::{Key, Namespace, TypeTag};

use crate::bridge::{require_branch_exists, to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::handlers::embed_runtime;
use crate::{BranchId, Error, Output, Result};

pub(crate) fn delete(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    force: bool,
) -> Result<Output> {
    if space == "default" || space == strata_engine::system_space::SYSTEM_SPACE {
        return Err(Error::ConstraintViolation {
            reason: format!("Cannot delete the '{space}' space"),
        });
    }

    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    if !force {
        let empty = convert_result(primitives.space.is_empty(branch_id, &space))?;
        if !empty {
            return Err(Error::ConstraintViolation {
                reason: format!("Space '{space}' is not empty. Use force=true to delete anyway."),
            });
        }
    }

    let namespace = Arc::new(Namespace::for_branch_space(branch_id, &space));
    convert_result(primitives.db.transaction(branch_id, |txn| {
        for type_tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::Json,
            TypeTag::Vector,
            TypeTag::Graph,
        ] {
            let prefix = Key::new(namespace.clone(), type_tag, vec![]);
            let entries = txn.scan_prefix(&prefix)?;
            for (key, _) in entries {
                txn.delete(key)?;
            }
        }
        Ok(())
    }))?;

    if let Ok(index) = primitives
        .db
        .extension::<strata_engine::search::InvertedIndex>()
    {
        let removed = index.remove_documents_in_space(branch_id, &space);
        if removed > 0 {
            tracing::info!(
                target: "strata::space",
                branch_id = %branch_id,
                space = space.as_str(),
                removed,
                "Removed search documents during space delete"
            );
        }
    }

    if let Err(error) = primitives
        .vector
        .purge_collections_in_space(branch_id, &space)
    {
        tracing::warn!(
            target: "strata::space",
            branch_id = %branch_id,
            space = space.as_str(),
            error = %error,
            "Failed to purge vector collections during space delete"
        );
    }

    embed_runtime::delete_shadow_embeddings_for_space(primitives, branch_id, &space);

    convert_result(primitives.space.delete(branch_id, &space))?;
    Ok(Output::Unit)
}
