use super::super::{Command, Executor, ExecutorError, Output};

impl Executor {
    /// Executes a branch-list command.
    pub fn branch_list(&mut self) -> Result<Output, ExecutorError> {
        self.execute(Command::BranchList {})
    }

    /// Executes a branch-get command.
    pub fn branch_get(&mut self, branch: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::BranchGet {
            branch: branch.into(),
        })
    }

    /// Executes a branch-create command.
    pub fn branch_create(&mut self, branch: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::BranchCreate {
            branch: branch.into(),
        })
    }

    /// Executes a branch-fork-current command.
    pub fn branch_fork_current(
        &mut self,
        source: impl Into<String>,
        branch: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::BranchForkCurrent {
            source: source.into(),
            branch: branch.into(),
        })
    }

    /// Executes a branch-delete command.
    pub fn branch_delete(&mut self, branch: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::BranchDelete {
            branch: branch.into(),
        })
    }
}
