//! Session wrapper with branch/space context.
//!
//! Holds both a `Strata` handle (for branch power API) and a `Session`
//! (for transactional command execution). Both share the same underlying
//! `Arc<Database>`.

use strata_executor::{
    BranchDiffResult, Branches, Command, Error, ForkInfo, MergeInfo, MergeStrategy, Output, Result,
    Session, Strata,
};

/// Wraps the database handles and tracks current context.
pub struct SessionState {
    db: Strata,
    session: Session,
    branch: String,
    space: String,
    in_transaction: bool,
}

impl SessionState {
    /// Create a new SessionState from a Strata handle.
    pub fn new(db: Strata, branch: String, space: String) -> Self {
        let session = db.session();
        Self {
            db,
            session,
            branch,
            space,
            in_transaction: false,
        }
    }

    /// Execute a command via the session.
    pub fn execute(&mut self, cmd: Command) -> Result<Output> {
        let output = self.session.execute(cmd)?;
        // Track transaction state changes
        match &output {
            Output::TxnBegun => self.in_transaction = true,
            Output::TxnCommitted { .. } | Output::TxnAborted => self.in_transaction = false,
            _ => {}
        }
        Ok(output)
    }

    /// Get a Branches handle for fork/diff/merge.
    #[allow(dead_code)]
    pub fn branches(&self) -> Branches<'_> {
        self.db.branches()
    }

    /// Fork the current branch.
    pub fn fork_branch(&self, destination: &str) -> Result<ForkInfo> {
        self.db.branches().fork(&self.branch, destination)
    }

    /// Diff two branches.
    pub fn diff_branches(&self, branch_a: &str, branch_b: &str) -> Result<BranchDiffResult> {
        self.db.branches().diff(branch_a, branch_b)
    }

    /// Merge a source branch into the current branch.
    pub fn merge_branch(&self, source: &str, strategy: MergeStrategy) -> Result<MergeInfo> {
        self.db.branches().merge(source, &self.branch, strategy)
    }

    /// Current branch name.
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Current space name.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// Switch branch context.
    pub fn set_branch(&mut self, name: &str) -> Result<()> {
        // Verify branch exists
        let exists = match self.session.execute(Command::BranchExists {
            branch: name.into(),
        })? {
            Output::Bool(b) => b,
            _ => {
                return Err(Error::Internal {
                    reason: "Unexpected output".into(),
                })
            }
        };
        if !exists {
            return Err(Error::BranchNotFound {
                branch: name.to_string(),
                hint: None,
            });
        }
        self.branch = name.to_string();
        Ok(())
    }

    /// Switch space context.
    pub fn set_space(&mut self, name: &str) {
        self.space = name.to_string();
    }

    /// Whether a transaction is currently active.
    #[allow(dead_code)]
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Generate the REPL prompt string.
    pub fn prompt(&self) -> String {
        if self.in_transaction {
            format!("strata:{}/{}(txn)> ", self.branch, self.space)
        } else {
            format!("strata:{}/{}> ", self.branch, self.space)
        }
    }
}
