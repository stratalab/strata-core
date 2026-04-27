use std::sync::Arc;

use crate::bridge::{to_core_branch_id, validate_session_space, Primitives};
use crate::convert::convert_result;
use crate::{BranchId, Output, Result};

pub(crate) fn list(primitives: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let spaces = convert_result(primitives.space.list(branch_id))?;
    Ok(Output::SpaceList(spaces))
}

pub(crate) fn create(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
) -> Result<Output> {
    validate_session_space(&space)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(primitives.space.register(branch_id, &space))?;
    Ok(Output::Unit)
}

pub(crate) fn exists(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let exists = convert_result(primitives.space.exists(branch_id, &space))?;
    Ok(Output::Bool(exists))
}
