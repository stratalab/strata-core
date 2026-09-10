//! CLI session context.

/// Branch and space selected for a command.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scope {
    pub(crate) branch: Option<String>,
    pub(crate) space: Option<String>,
}

/// Mutable shell context shared by REPL and pipe execution.
#[cfg(feature = "native")]
#[derive(Clone, Debug, Default)]
pub(crate) struct CommandContext {
    branch: Option<String>,
    space: Option<String>,
}

#[cfg(feature = "native")]
impl CommandContext {
    pub(crate) fn new(branch: Option<String>, space: Option<String>) -> Self {
        Self { branch, space }
    }

    pub(crate) fn scope_with_overrides(
        &self,
        branch: Option<String>,
        space: Option<String>,
    ) -> Scope {
        Scope {
            branch: branch.or_else(|| self.branch.clone()),
            space: space.or_else(|| self.space.clone()),
        }
    }

    pub(crate) fn set_branch(&mut self, branch: String) {
        self.branch = Some(branch);
    }

    pub(crate) fn set_space(&mut self, space: Option<String>) {
        self.space = space;
    }

    pub(crate) fn branch_or_default<'a>(&'a self, executor_default: &'a str) -> &'a str {
        self.branch.as_deref().unwrap_or(executor_default)
    }

    pub(crate) fn space_or_default(&self) -> &str {
        self.space
            .as_deref()
            .unwrap_or(strata_executor::DEFAULT_SPACE)
    }

    pub(crate) fn prompt(&self, executor_default: &str) -> String {
        let branch = self.branch_or_default(executor_default);
        format!("strata:{branch}/{}> ", self.space_or_default())
    }
}
