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
    // Cannot delete the default space
    if space == "default" {
        return Err(Error::ConstraintViolation {
            reason: "Cannot delete the default space".into(),
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
        #[allow(deprecated)]
        for type_tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::State,
            TypeTag::Trace,
            TypeTag::Json,
            TypeTag::Vector,
            TypeTag::VectorConfig,
        ] {
            let prefix = Key::new(ns.clone(), type_tag, vec![]);
            let entries = txn.scan_prefix(&prefix)?;
            for (key, _) in entries {
                txn.delete(key)?;
            }
        }
        Ok(())
    }))?;

    // Delete the space metadata key
    convert_result(p.space.delete(core_branch_id, &space))?;

    Ok(Output::Unit)
}

/// Handle SpaceExists command.
pub fn space_exists(p: &Arc<Primitives>, branch: BranchId, space: String) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;
    let exists = convert_result(p.space.exists(core_branch_id, &space))?;
    Ok(Output::Bool(exists))
}
