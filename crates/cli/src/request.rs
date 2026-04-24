use strata_executor::Command;

/// A parsed CLI request.
pub(crate) enum CliRequest {
    /// Execute an executor command.
    Execute(Command),
    /// Run a REPL-local meta command.
    Meta(MetaCommand),
}

/// REPL-local commands that never reach the executor.
pub(crate) enum MetaCommand {
    /// Switch branch and optional space context.
    Use {
        branch: String,
        space: Option<String>,
    },
    /// Print help for a command group or the top-level command tree.
    Help { command: Option<String> },
    /// Clear the screen.
    Clear,
    /// Exit the REPL.
    Quit,
}
