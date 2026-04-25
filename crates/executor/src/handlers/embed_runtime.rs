use std::sync::Arc;

use crate::bridge::{remap, remap_error, Primitives};
use crate::output::EmbedStatusInfo;
use crate::{BranchId, Command, Error, Output, Result};

fn execute_compat(p: &Arc<Primitives>, command: Command) -> Result<Output> {
    let backend = strata_executor_legacy::Executor::new(p.db.clone());
    let command = remap(command, "command")?;
    let output = backend.execute(command).map_err(remap_error)?;
    remap(output, "command output")
}

pub(crate) fn embed_status(p: &Arc<Primitives>) -> Result<EmbedStatusInfo> {
    match execute_compat(p, Command::EmbedStatus)? {
        Output::EmbedStatus(info) => Ok(info),
        other => Err(Error::Internal {
            reason: format!("unexpected output for EmbedStatus: {other:?}"),
            hint: None,
        }),
    }
}

pub(crate) fn reindex_embeddings(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    execute_compat(
        p,
        Command::ReindexEmbeddings {
            branch: Some(branch),
        },
    )
}
