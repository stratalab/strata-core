use super::{
    branch_name, product_space, EventService, Executor, GraphService, JsonService, VectorService,
};
use crate::ExecutorError;
use strata_core::Timestamp;

/// #3112 S3a: `as_of` and `as_of_time` ask the same question in two clocks.
fn as_of_conflict() -> ExecutorError {
    ExecutorError::new(
        "invalid_argument.executor.as_of_conflict",
        "as_of and as_of_time are mutually exclusive: pass a commit timestamp or \
         a wall-clock instant, not both",
    )
}

impl Executor {
    /// #3112 S3a: collapses the `as_of` / `as_of_time` pair into the single
    /// logical timestamp every read path already understands.
    ///
    /// Accepting both would be ambiguous, so it is refused rather than given a
    /// precedence rule — a caller that sets both has a bug, and silently
    /// honouring one of them hides it. Resolution itself is engine semantics
    /// (hard rule 7); this is the transport-level glue that picks which
    /// question was asked.
    pub(super) fn resolve_as_of(
        &mut self,
        branch: Option<&str>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Option<Timestamp>, ExecutorError> {
        match (as_of, as_of_time) {
            (Some(_), Some(_)) => Err(as_of_conflict()),
            (Some(as_of), None) => Ok(Some(Timestamp::from_micros(as_of))),
            (None, Some(instant)) => {
                let branch = branch_name(branch, &self.default_branch)?;
                Ok(Some(self.database.resolve_wall_clock(
                    &branch,
                    Timestamp::from_micros(instant),
                )?))
            }
            (None, None) => Ok(None),
        }
    }

    pub(super) fn kv_service(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<strata_engine::KvService<'_>, ExecutorError> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(space, &self.default_space)?;
        Ok(self.database.kv(branch, space)?)
    }

    pub(super) fn json_service(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<JsonService<'_>, ExecutorError> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(space, &self.default_space)?;
        Ok(self.database.json(branch, space)?)
    }

    pub(super) fn vector_service(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<VectorService<'_>, ExecutorError> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(space, &self.default_space)?;
        Ok(self.database.vector(branch, space)?)
    }

    pub(super) fn event_service(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<EventService<'_>, ExecutorError> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(space, &self.default_space)?;
        Ok(self.database.event(branch, space)?)
    }

    pub(super) fn graph_service(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<GraphService<'_>, ExecutorError> {
        let branch = branch_name(branch, &self.default_branch)?;
        let space = product_space(space, &self.default_space)?;
        Ok(self.database.graph(branch, space)?)
    }
}
