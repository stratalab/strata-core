use std::sync::Arc;

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::BranchId;
use crate::{Error, Output, Result};

pub(crate) fn retention_apply(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let current = p.db.current_version();
    let _pruned = p.db.gc_versions_before(branch_id, current);
    Ok(Output::Unit)
}

pub(crate) fn retention_unavailable(command: &str) -> Result<Output> {
    Err(Error::Internal {
        reason: format!("{command} is not implemented"),
        hint: Some(
            "The retention report surface is not available through this command yet.".to_string(),
        ),
    })
}

pub(crate) fn time_range(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    match p.db.time_range(branch_id).map_err(crate::Error::from)? {
        Some((oldest_ts, latest_ts)) => Ok(Output::TimeRange {
            oldest_ts: Some(oldest_ts),
            latest_ts: Some(latest_ts),
        }),
        None => Ok(Output::TimeRange {
            oldest_ts: None,
            latest_ts: None,
        }),
    }
}
