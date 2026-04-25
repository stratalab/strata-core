use std::sync::Arc;

use strata_engine::Database;
use strata_security::AccessMode;

use crate::bridge::{
    access_denied, is_local_executor_command, is_read_only, remap, remap_error,
    runtime_default_branch, transaction_already_active, transaction_branch_mismatch,
    transaction_not_active, validate_branch_exists, validate_session_space,
};
use crate::handlers::reject_system_branch;
use crate::ipc::IpcClient;
use crate::{BranchId, Command, Error, Executor, Output, Result};

/// Stateful command session with executor-managed defaults and transactions.
pub struct Session {
    backend: SessionBackend,
    executor: Option<Executor>,
    access_mode: AccessMode,
    current_branch: BranchId,
    current_space: String,
    txn_branch: Option<BranchId>,
}

enum SessionBackend {
    Local(strata_executor_legacy::Session),
    Ipc(IpcClient),
}

impl Session {
    /// Create a session over a database handle.
    pub fn new(db: Arc<Database>) -> Self {
        Self::new_with_mode(db, AccessMode::ReadWrite)
    }

    /// Create a session with an explicit access mode.
    pub fn new_with_mode(db: Arc<Database>, access_mode: AccessMode) -> Self {
        let current_branch = runtime_default_branch(&db);
        Self {
            backend: SessionBackend::Local(strata_executor_legacy::Session::new_with_mode(
                db.clone(),
                access_mode,
            )),
            executor: Some(Executor::new_with_mode(db, access_mode)),
            access_mode,
            current_branch,
            current_space: "default".to_string(),
            txn_branch: None,
        }
    }

    pub(crate) fn with_context(
        db: Arc<Database>,
        access_mode: AccessMode,
        current_branch: impl Into<BranchId>,
        current_space: impl Into<String>,
    ) -> Self {
        Self {
            backend: SessionBackend::Local(strata_executor_legacy::Session::new_with_mode(
                db.clone(),
                access_mode,
            )),
            executor: Some(Executor::new_with_mode(db, access_mode)),
            access_mode,
            current_branch: current_branch.into(),
            current_space: current_space.into(),
            txn_branch: None,
        }
    }

    pub(crate) fn new_ipc(
        client: IpcClient,
        access_mode: AccessMode,
        current_branch: impl Into<BranchId>,
        current_space: impl Into<String>,
    ) -> Self {
        Self {
            backend: SessionBackend::Ipc(client),
            executor: None,
            access_mode,
            current_branch: current_branch.into(),
            current_space: current_space.into(),
            txn_branch: None,
        }
    }

    /// Execute a command within the session.
    pub fn execute(&mut self, mut command: Command) -> Result<Output> {
        if is_read_only(self.access_mode) && command.is_write() {
            if let Some(executor) = self.executor.as_ref() {
                return Err(access_denied(executor.database(), command.name()));
            }
            return Err(Error::AccessDenied {
                command: command.name().to_string(),
                hint: Some("Database is in read-only mode.".to_string()),
            });
        }

        if let Command::TxnBegin { branch, .. } = &mut command {
            if branch.is_none() {
                *branch = Some(self.current_branch.clone());
            }
        }

        command.resolve_space_defaults_with(&self.current_space);
        command.resolve_defaults_with(self.effective_branch());
        if let Some(branch) = command.resolved_branch() {
            reject_system_branch(branch)?;
        }

        let txn_command = match &command {
            Command::TxnBegin { branch, .. } => {
                Some(TxnCommand::Begin(branch.clone().expect(
                    "TxnBegin branch should be resolved before execution",
                )))
            }
            Command::TxnCommit => Some(TxnCommand::Commit),
            Command::TxnRollback => Some(TxnCommand::Rollback),
            Command::TxnInfo => Some(TxnCommand::Info),
            Command::TxnIsActive => Some(TxnCommand::IsActive),
            _ => None,
        };

        if let Some(txn_branch) = self.txn_branch.as_ref() {
            if let Some(command_branch) = command.resolved_branch() {
                if command_branch != txn_branch {
                    return Err(transaction_branch_mismatch(command_branch, txn_branch));
                }
            }
        }

        match &command {
            Command::TxnIsActive => return Ok(Output::Bool(self.in_transaction())),
            Command::TxnInfo if !self.in_transaction() => return Ok(Output::TxnInfo(None)),
            Command::TxnBegin { .. } if self.in_transaction() => {
                return Err(transaction_already_active())
            }
            Command::TxnCommit | Command::TxnRollback if !self.in_transaction() => {
                return Err(transaction_not_active())
            }
            _ => {}
        }

        let txn_active = self.in_transaction();
        match &mut self.backend {
            SessionBackend::Local(backend) => {
                if txn_command.is_some() {
                    let command = remap(command, "command")?;
                    let output = backend.execute(command).map_err(remap_error)?;
                    let output = remap(output, "command output")?;
                    self.update_tx_state(txn_command, &output);
                    return Ok(output);
                }

                let executor = self
                    .executor
                    .as_ref()
                    .expect("local sessions should always have an executor");
                if is_local_executor_command(&command) {
                    return executor.execute_local(command);
                }

                if !txn_active {
                    return executor.execute(command);
                }

                let command = remap(command, "command")?;
                let output = backend.execute(command).map_err(remap_error)?;
                let output = remap(output, "command output")?;
                self.update_tx_state(txn_command, &output);
                Ok(output)
            }
            SessionBackend::Ipc(client) => {
                let output = client.execute(command)?;
                self.update_tx_state(txn_command, &output);
                Ok(output)
            }
        }
    }

    /// Return whether the session currently has an open transaction.
    pub fn in_transaction(&self) -> bool {
        self.txn_branch.is_some()
    }

    /// Return the current branch used for commands that omit a branch.
    pub fn current_branch(&self) -> &str {
        self.effective_branch().as_str()
    }

    /// Update the current branch used for commands that omit a branch.
    pub fn set_branch(&mut self, branch: impl Into<BranchId>) -> Result<()> {
        if self.in_transaction() {
            return Err(Error::InvalidInput {
                reason: "cannot change branch while a transaction is active".to_string(),
                hint: Some("Commit or roll back the active transaction first.".to_string()),
            });
        }
        let branch = branch.into();
        if let Some(executor) = self.executor.as_ref() {
            validate_branch_exists(executor.database(), &branch)?;
        } else {
            match self.execute(Command::BranchExists {
                branch: branch.clone(),
            })? {
                Output::Bool(true) => {}
                Output::Bool(false) => {
                    return Err(Error::BranchNotFound {
                        branch: branch.to_string(),
                        hint: None,
                    });
                }
                other => {
                    return Err(Error::Internal {
                        reason: format!("Unexpected output for BranchExists: {other:?}"),
                        hint: None,
                    });
                }
            }
        }
        self.current_branch = branch;
        Ok(())
    }

    /// Return the current space used for commands that omit a space.
    pub fn current_space(&self) -> &str {
        &self.current_space
    }

    /// Update the current space used for commands that omit a space.
    pub fn set_space(&mut self, space: impl Into<String>) -> Result<()> {
        if self.in_transaction() {
            return Err(Error::InvalidInput {
                reason: "cannot change space while a transaction is active".to_string(),
                hint: Some("Commit or roll back the active transaction first.".to_string()),
            });
        }
        let space = space.into();
        validate_session_space(&space)?;
        self.current_space = space;
        Ok(())
    }

    /// Return the access mode used by this session.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    fn effective_branch(&self) -> &BranchId {
        self.txn_branch.as_ref().unwrap_or(&self.current_branch)
    }

    fn update_tx_state(&mut self, txn_command: Option<TxnCommand>, output: &Output) {
        match (txn_command, output) {
            (Some(TxnCommand::Begin(branch)), Output::TxnBegun) => {
                self.txn_branch = Some(branch);
            }
            (Some(TxnCommand::Commit), Output::TxnCommitted { .. })
            | (Some(TxnCommand::Rollback), Output::TxnAborted)
            | (Some(TxnCommand::Info), Output::TxnInfo(None))
            | (Some(TxnCommand::IsActive), Output::Bool(false)) => {
                self.txn_branch = None;
            }
            (Some(TxnCommand::Info), Output::TxnInfo(Some(_)))
            | (Some(TxnCommand::IsActive), Output::Bool(true)) => {
                if self.txn_branch.is_none() {
                    self.txn_branch = Some(self.current_branch.clone());
                }
            }
            _ => {}
        }
    }
}

#[derive(Clone)]
enum TxnCommand {
    Begin(BranchId),
    Commit,
    Rollback,
    Info,
    IsActive,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use strata_core::Value;
    use strata_security::AccessMode;

    use crate::{Command, Error, Output, Session, Strata};

    fn test_db() -> Arc<strata_engine::Database> {
        Strata::cache()
            .expect("cache database should open")
            .database()
    }

    #[test]
    fn omitted_space_uses_session_space() {
        let db = test_db();
        let mut session = Session::new(db.clone());
        session
            .set_space("analytics")
            .expect("space should update outside a transaction");

        session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "shared-key".into(),
                value: Value::Int(7),
            })
            .expect("kv put should use the session space");

        let mut default_space = Session::new(db.clone());
        let default_value = default_space
            .execute(Command::KvGet {
                branch: None,
                space: Some("default".into()),
                key: "shared-key".into(),
                as_of: None,
            })
            .expect("default-space read should succeed");
        assert!(matches!(
            default_value,
            Output::Maybe(None) | Output::MaybeVersioned(None)
        ));

        let analytics_value = default_space
            .execute(Command::KvGet {
                branch: None,
                space: Some("analytics".into()),
                key: "shared-key".into(),
                as_of: None,
            })
            .expect("analytics-space read should succeed");
        assert!(matches!(
            analytics_value,
            Output::Maybe(Some(_)) | Output::MaybeVersioned(Some(_))
        ));
    }

    #[test]
    fn transaction_commands_validate_local_state() {
        let mut session = Session::new(test_db());

        let error = session
            .execute(Command::TxnCommit)
            .expect_err("commit without a transaction should be rejected");
        assert!(matches!(error, Error::TransactionNotActive { .. }));

        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect("txn begin should succeed");

        let error = session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect_err("begin with an active transaction should be rejected");
        assert!(matches!(error, Error::TransactionAlreadyActive { .. }));
    }

    #[test]
    fn context_cannot_change_while_transaction_active() {
        let mut session = Session::new(test_db());
        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect("txn begin should succeed");

        let branch_error = session
            .set_branch("feature")
            .expect_err("branch changes should be blocked while a transaction is active");
        assert!(matches!(branch_error, Error::InvalidInput { .. }));

        let space_error = session
            .set_space("analytics")
            .expect_err("space changes should be blocked while a transaction is active");
        assert!(matches!(space_error, Error::InvalidInput { .. }));
    }

    #[test]
    fn set_branch_requires_existing_branch() {
        let mut session = Session::new(test_db());
        let error = session
            .set_branch("missing")
            .expect_err("branch changes should validate existence");

        match error {
            Error::BranchNotFound { branch, .. } => assert_eq!(branch, "missing"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn set_space_rejects_invalid_name() {
        let mut session = Session::new(test_db());
        let error = session
            .set_space("not allowed")
            .expect_err("space changes should validate the name");
        assert!(matches!(error, Error::InvalidInput { .. }));
    }

    #[test]
    fn explicit_transaction_branch_becomes_effective_context() {
        let db = test_db();
        db.branches()
            .create("feature")
            .expect("feature branch should be created");

        let mut session = Session::new(db);
        session
            .execute(Command::TxnBegin {
                branch: Some("feature".into()),
                options: None,
            })
            .expect("txn begin should succeed");

        assert_eq!(session.current_branch(), "feature");
    }

    #[test]
    fn active_transaction_rejects_branch_mismatch() {
        let db = test_db();
        db.branches()
            .create("feature")
            .expect("feature branch should be created");
        db.branches()
            .create("other")
            .expect("other branch should be created");

        let mut session = Session::new(db);
        session
            .execute(Command::TxnBegin {
                branch: Some("feature".into()),
                options: None,
            })
            .expect("txn begin should succeed");

        let error = session
            .execute(Command::KvPut {
                branch: Some("other".into()),
                space: None,
                key: "key".into(),
                value: Value::Int(1),
            })
            .expect_err("active transactions should reject mismatched branches");
        assert!(matches!(error, Error::InvalidInput { .. }));
    }

    #[test]
    fn read_only_session_rejects_transaction_begin() {
        let mut session = Session::new_with_mode(test_db(), AccessMode::ReadOnly);
        let error = session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect_err("read-only session should reject transaction begin");

        assert!(matches!(error, Error::AccessDenied { .. }));
    }

    #[test]
    fn transaction_begin_rejects_system_branch() {
        let mut session = Session::new(test_db());
        let error = session
            .execute(Command::TxnBegin {
                branch: Some("_system".into()),
                options: None,
            })
            .expect_err("system branch transactions should be rejected");

        assert!(matches!(error, Error::InvalidInput { .. }));
    }
}
