use strata_executor::{Command, Output};

/// Shell-local branch, space, and transaction prompt state.
pub(crate) struct Context {
    default_branch: String,
    branch: String,
    space: String,
    in_transaction: bool,
}

impl Context {
    /// Create a new shell context.
    pub(crate) fn new(default_branch: String, branch: String, space: String) -> Self {
        Self {
            default_branch,
            branch,
            space,
            in_transaction: false,
        }
    }

    /// Current branch name.
    pub(crate) fn branch(&self) -> &str {
        &self.branch
    }

    /// Current space name.
    pub(crate) fn space(&self) -> &str {
        &self.space
    }

    /// Set the current branch.
    pub(crate) fn set_branch(&mut self, branch: String) {
        self.branch = branch;
    }

    /// Set the current space.
    pub(crate) fn set_space(&mut self, space: String) {
        self.space = space;
    }

    /// Prompt string for interactive mode.
    pub(crate) fn prompt(&self) -> String {
        if self.in_transaction {
            format!("strata:{}/{}(txn)> ", self.branch, self.space)
        } else {
            format!("strata:{}/{}> ", self.branch, self.space)
        }
    }

    /// Update local state from a successful command result.
    pub(crate) fn note_success(&mut self, command: &Command, output: &Output) {
        match output {
            Output::TxnBegun => self.in_transaction = true,
            Output::TxnCommitted { .. } | Output::TxnAborted => self.in_transaction = false,
            _ => {}
        }

        if let Command::BranchDelete { branch } = command {
            if branch.as_str() == self.branch {
                self.branch = self.default_branch.clone();
                self.space = "default".to_string();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Context;
    use strata_executor::{Command, Output};

    #[test]
    fn prompt_tracks_transaction_state() {
        let mut context = Context::new(
            "default".to_string(),
            "main".to_string(),
            "analytics".to_string(),
        );

        assert_eq!(context.prompt(), "strata:main/analytics> ");

        context.note_success(&Command::TxnInfo, &Output::TxnBegun);
        assert_eq!(context.prompt(), "strata:main/analytics(txn)> ");

        context.note_success(&Command::TxnCommit, &Output::TxnCommitted { version: 7 });
        assert_eq!(context.prompt(), "strata:main/analytics> ");
    }

    #[test]
    fn deleting_current_branch_resets_to_default_branch_and_space() {
        let mut context = Context::new(
            "default".to_string(),
            "feature".to_string(),
            "analytics".to_string(),
        );

        context.note_success(
            &Command::BranchDelete {
                branch: "feature".into(),
            },
            &Output::Bool(true),
        );

        assert_eq!(context.branch(), "default");
        assert_eq!(context.space(), "default");
    }
}
