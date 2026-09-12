use super::super::{
    BatchJsonDeleteEntry, BatchJsonEntry, BatchJsonGetEntry, Command, Executor, ExecutorError,
    Output,
};

impl Executor {
    /// Executes a default-branch JSON set command.
    pub fn json_set(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::JsonSet {
            branch: None,
            space: None,
            key: key.into(),
            path: path.into(),
            value,
        })
    }

    /// Executes a default-branch JSON get command.
    pub fn json_get(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::JsonGet {
            branch: None,
            space: None,
            key: key.into(),
            path: path.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch JSON delete command.
    pub fn json_delete(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::JsonDelete {
            branch: None,
            space: None,
            key: key.into(),
            path: path.into(),
        })
    }

    /// Executes a default-branch JSON batch set command.
    pub fn json_batch_set(
        &mut self,
        entries: Vec<BatchJsonEntry>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::JsonBatchSet {
            branch: None,
            space: None,
            entries,
        })
    }

    /// Executes a default-branch JSON batch get command.
    pub fn json_batch_get(
        &mut self,
        entries: Vec<BatchJsonGetEntry>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::JsonBatchGet {
            branch: None,
            space: None,
            entries,
        })
    }

    /// Executes a default-branch JSON batch delete command.
    pub fn json_batch_delete(
        &mut self,
        entries: Vec<BatchJsonDeleteEntry>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::JsonBatchDelete {
            branch: None,
            space: None,
            entries,
        })
    }
}
