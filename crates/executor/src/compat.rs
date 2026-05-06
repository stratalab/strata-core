use std::path::Path;
use std::sync::{Arc, Mutex};

use strata_engine::{
    open_product_cache, open_product_database, AccessMode, Database, HealthReport, OpenOptions,
    ProductOpenError, ProductOpenOutcome, SystemMetrics,
};
use strata_storage::validate_space_name;

use crate::ipc::IpcClient;
use crate::{BranchId, Command, DatabaseInfo, Error, Executor, Output, Result, Session, Value};

/// High-level database handle for opening databases and creating sessions.
pub struct Strata {
    backend: Backend,
    current_branch: BranchId,
    current_space: String,
    access_mode: AccessMode,
}

enum Backend {
    Local { executor: Executor },
    Ipc { client: Mutex<IpcClient> },
}

impl Strata {
    /// Open a database with the default product options.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with(path, OpenOptions::default())
    }

    /// Open a database with explicit options.
    pub fn open_with<P: AsRef<Path>>(path: P, options: OpenOptions) -> Result<Self> {
        let data_dir = path.as_ref().to_path_buf();
        let outcome = open_product_database(
            &data_dir,
            options,
            transitional_product_subsystems_until_vector_absorption(),
        )
        .map_err(product_open_error)?;

        match outcome {
            ProductOpenOutcome::Local {
                db, access_mode, ..
            } => Self::from_local_executor(Executor::new_with_mode(db, access_mode), access_mode),
            ProductOpenOutcome::Ipc {
                socket_path,
                access_mode,
                ..
            } => {
                let client = IpcClient::connect(&socket_path).map_err(|error| Error::Io {
                    reason: format!("Failed to connect to IPC server: {error}"),
                    hint: None,
                })?;
                Self::from_ipc_client(client, access_mode)
            }
            _ => Err(Error::Internal {
                reason: "engine returned an unknown product open outcome".to_string(),
                hint: None,
            }),
        }
    }

    /// Open an ephemeral in-memory database.
    pub fn cache() -> Result<Self> {
        let outcome = open_product_cache(transitional_product_subsystems_until_vector_absorption())
            .map_err(product_open_error)?;

        match outcome {
            ProductOpenOutcome::Local {
                db, access_mode, ..
            } => Self::from_local_executor(Executor::new_with_mode(db, access_mode), access_mode),
            ProductOpenOutcome::Ipc { .. } => Err(Error::Internal {
                reason: "cache product open unexpectedly returned an IPC fallback".to_string(),
                hint: None,
            }),
            _ => Err(Error::Internal {
                reason: "engine returned an unknown product cache outcome".to_string(),
                hint: None,
            }),
        }
    }

    /// Create an independent handle to the same database.
    pub fn new_handle(&self) -> Result<Self> {
        match &self.backend {
            Backend::Local { executor } => {
                let db = executor.database().clone();
                let executor = Executor::new_with_mode(db, self.access_mode);
                if self.access_mode == AccessMode::ReadWrite {
                    Self::ensure_default_branch(&executor)?;
                }
                Self::from_local_executor(executor, self.access_mode)
            }
            Backend::Ipc { client } => {
                let guard = client.lock().unwrap_or_else(|e| e.into_inner());
                let client = guard.connect_new().map_err(|error| Error::Io {
                    reason: format!("Failed to open new IPC connection: {error}"),
                    hint: None,
                })?;
                Self::from_ipc_client(client, self.access_mode)
            }
        }
    }

    /// Check connectivity to the underlying database handle.
    pub fn ping(&self) -> Result<String> {
        match self.execute_cmd(Command::Ping)? {
            Output::Pong { version } => Ok(version),
            other => Err(unexpected_output("Ping", other)),
        }
    }

    /// Return database information for this handle.
    pub fn info(&self) -> Result<DatabaseInfo> {
        match self.execute_cmd(Command::Info)? {
            Output::DatabaseInfo(info) => Ok(info),
            other => Err(unexpected_output("Info", other)),
        }
    }

    /// Return the current health report for this handle.
    pub fn health(&self) -> Result<HealthReport> {
        match self.execute_cmd(Command::Health)? {
            Output::Health(report) => Ok(report),
            other => Err(unexpected_output("Health", other)),
        }
    }

    /// Return the current metrics snapshot for this handle.
    pub fn metrics(&self) -> Result<SystemMetrics> {
        match self.execute_cmd(Command::Metrics)? {
            Output::Metrics(metrics) => Ok(metrics),
            other => Err(unexpected_output("Metrics", other)),
        }
    }

    /// Flush buffered state to durable storage.
    pub fn flush(&self) -> Result<()> {
        match self.execute_cmd(Command::Flush)? {
            Output::Unit => Ok(()),
            other => Err(unexpected_output("Flush", other)),
        }
    }

    /// Run a storage compaction cycle.
    pub fn compact(&self) -> Result<()> {
        match self.execute_cmd(Command::Compact)? {
            Output::Unit => Ok(()),
            other => Err(unexpected_output("Compact", other)),
        }
    }

    /// Close the database handle.
    pub fn close(self) -> Result<()> {
        match self.backend {
            Backend::Local { executor } => {
                let db = executor.database().clone();
                db.shutdown().map_err(Error::from)?;
                Ok(())
            }
            Backend::Ipc { .. } => Ok(()),
        }
    }

    /// Create a session bound to this handle's current context.
    pub fn session(&self) -> Result<Session> {
        match &self.backend {
            Backend::Local { executor } => Ok(Session::with_context(
                executor.database().clone(),
                self.access_mode,
                self.current_branch.clone(),
                self.current_space.clone(),
            )),
            Backend::Ipc { client } => {
                let guard = client.lock().unwrap_or_else(|e| e.into_inner());
                let client = guard.connect_new().map_err(|error| Error::Io {
                    reason: format!("Failed to create new IPC connection for session: {error}"),
                    hint: None,
                })?;
                Ok(Session::new_ipc(
                    client,
                    self.access_mode,
                    self.current_branch.clone(),
                    self.current_space.clone(),
                ))
            }
        }
    }

    /// Return the underlying database handle for local callers.
    pub fn database(&self) -> Arc<Database> {
        match &self.backend {
            Backend::Local { executor } => executor.database().clone(),
            Backend::Ipc { .. } => {
                panic!("database() not available on IPC-backed Strata handles")
            }
        }
    }

    /// Return the access mode for this handle.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    /// Return true if this handle is connected to a remote IPC server.
    pub fn is_ipc(&self) -> bool {
        matches!(self.backend, Backend::Ipc { .. })
    }

    /// Return the current branch name for this handle.
    pub fn current_branch(&self) -> &str {
        self.current_branch.as_str()
    }

    /// Switch the current branch for this handle.
    pub fn set_branch(&mut self, branch_name: &str) -> Result<()> {
        match self.execute_cmd(Command::BranchExists {
            branch: branch_name.into(),
        })? {
            Output::Bool(true) => {
                self.current_branch = BranchId::from(branch_name);
                Ok(())
            }
            Output::Bool(false) => Err(Error::BranchNotFound {
                branch: branch_name.to_string(),
                hint: None,
            }),
            other => Err(unexpected_output("BranchExists", other)),
        }
    }

    /// Return the current space name for this handle.
    pub fn current_space(&self) -> &str {
        &self.current_space
    }

    /// Switch the current space for this handle.
    pub fn set_space(&mut self, space: &str) -> Result<()> {
        validate_space_name(space).map_err(|reason| Error::InvalidInput { reason, hint: None })?;
        self.current_space = space.to_string();
        Ok(())
    }

    /// Put a value in the current branch and space.
    pub fn kv_put(&self, key: &str, value: impl Into<Value>) -> Result<u64> {
        extract_version(
            "KvPut",
            self.execute_cmd(Command::KvPut {
                branch: Some(self.current_branch.clone()),
                space: Some(self.current_space.clone()),
                key: key.to_string(),
                value: value.into(),
            })?,
        )
    }

    /// Get a value from the current branch and space.
    pub fn kv_get(&self, key: &str) -> Result<Option<Value>> {
        match self.execute_cmd(Command::KvGet {
            branch: Some(self.current_branch.clone()),
            space: Some(self.current_space.clone()),
            key: key.to_string(),
            as_of: None,
        })? {
            Output::Maybe(value) => Ok(value),
            Output::MaybeVersioned(value) => Ok(value.map(|versioned| versioned.value)),
            other => Err(unexpected_output("KvGet", other)),
        }
    }

    /// Write a JSON value at the given path in the current branch and space.
    pub fn json_set(&self, key: &str, path: &str, value: impl Into<Value>) -> Result<u64> {
        extract_version(
            "JsonSet",
            self.execute_cmd(Command::JsonSet {
                branch: Some(self.current_branch.clone()),
                space: Some(self.current_space.clone()),
                key: key.to_string(),
                path: path.to_string(),
                value: value.into(),
            })?,
        )
    }

    /// Append an event in the current branch and space.
    pub fn event_append(&self, event_type: &str, payload: Value) -> Result<u64> {
        extract_version(
            "EventAppend",
            self.execute_cmd(Command::EventAppend {
                branch: Some(self.current_branch.clone()),
                space: Some(self.current_space.clone()),
                event_type: event_type.to_string(),
                payload,
            })?,
        )
    }

    fn execute_cmd(&self, command: Command) -> Result<Output> {
        if matches!(self.backend, Backend::Ipc { .. })
            && self.access_mode == AccessMode::ReadOnly
            && command.is_write()
        {
            return Err(Error::AccessDenied {
                command: command.name().to_string(),
                hint: Some("Database is in read-only mode.".to_string()),
            });
        }

        match &self.backend {
            Backend::Local { executor } => executor.execute(command),
            Backend::Ipc { client } => client
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .execute(command),
        }
    }

    fn from_local_executor(executor: Executor, access_mode: AccessMode) -> Result<Self> {
        let default_branch = Self::load_default_branch_from_executor(&executor)?;
        Ok(Self {
            backend: Backend::Local { executor },
            current_branch: default_branch.clone(),
            current_space: "default".to_string(),
            access_mode,
        })
    }

    fn from_ipc_client(client: IpcClient, access_mode: AccessMode) -> Result<Self> {
        let default_branch = Self::load_default_branch_from_ipc(&client)?;
        Ok(Self {
            backend: Backend::Ipc {
                client: Mutex::new(client),
            },
            current_branch: default_branch.clone(),
            current_space: "default".to_string(),
            access_mode,
        })
    }

    fn load_default_branch_from_executor(executor: &Executor) -> Result<BranchId> {
        match executor.execute(Command::Info)? {
            Output::DatabaseInfo(info) => Ok(BranchId::from(info.default_branch)),
            other => Err(unexpected_output("Info", other)),
        }
    }

    fn load_default_branch_from_ipc(client: &IpcClient) -> Result<BranchId> {
        let mut client = client.connect_new().map_err(|error| Error::Io {
            reason: format!("Failed to open IPC branch probe connection: {error}"),
            hint: None,
        })?;
        match client.execute(Command::Info)? {
            Output::DatabaseInfo(info) => Ok(BranchId::from(info.default_branch)),
            other => Err(unexpected_output("Info", other)),
        }
    }

    fn ensure_default_branch(executor: &Executor) -> Result<()> {
        let default_branch = executor.default_branch().clone();
        match executor.execute(Command::BranchExists {
            branch: default_branch.clone(),
        })? {
            Output::Bool(true) => Ok(()),
            Output::Bool(false) => match executor.execute(Command::BranchCreate {
                branch_id: Some(default_branch.to_string()),
                metadata: None,
            })? {
                Output::BranchWithVersion { .. } => Ok(()),
                other => Err(unexpected_output("BranchCreate", other)),
            },
            other => Err(unexpected_output("BranchExists", other)),
        }
    }
}

fn transitional_product_subsystems_until_vector_absorption(
) -> Vec<Box<dyn strata_engine::Subsystem>> {
    vec![Box::new(strata_vector::VectorSubsystem)]
}

fn product_open_error(error: ProductOpenError) -> Error {
    Error::Internal {
        reason: error.to_string(),
        hint: None,
    }
}

fn extract_version(command: &str, output: Output) -> Result<u64> {
    match output {
        Output::Version(version) => Ok(version),
        Output::WriteResult { version, .. } => Ok(version),
        Output::EventAppendResult { sequence, .. } => Ok(sequence),
        other => Err(unexpected_output(command, other)),
    }
}

fn unexpected_output(command: &str, output: Output) -> Error {
    Error::Internal {
        reason: format!("Unexpected output for {command}: {output:?}"),
        hint: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use strata_core::Value;
    use strata_engine::{Database, OpenOptions, ProductOpenOutcome};

    use crate::{
        AccessMode, BranchId, Command, Error, Executor, IpcServer, Output, Session, Strata,
    };

    use super::Backend;

    fn test_db() -> Arc<Database> {
        Strata::cache()
            .expect("cache database should open")
            .database()
    }

    fn disk_db_with_default_branch(default_branch: &str) -> (tempfile::TempDir, Arc<Database>) {
        let dir = tempfile::tempdir().expect("tempdir should succeed");
        let outcome = strata_engine::open_product_database(
            dir.path(),
            OpenOptions::default().default_branch(default_branch),
            super::transitional_product_subsystems_until_vector_absorption(),
        )
        .expect("disk database should open through product open");
        let db = match outcome {
            ProductOpenOutcome::Local { db, .. } => db,
            other => panic!("expected local database, got {other:?}"),
        };
        (dir, db)
    }

    #[test]
    fn executor_execute_returns_new_output() {
        let executor = Executor::new(test_db());
        let output = executor
            .execute(Command::Ping)
            .expect("ping should succeed");

        match output {
            Output::Pong { version } => assert!(!version.is_empty()),
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[test]
    fn session_transaction_state_tracks_commands() {
        let db = test_db();
        let mut session = Session::new(db);

        assert!(!session.in_transaction());

        let output = session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect("txn begin should succeed");
        assert_eq!(output, Output::TxnBegun);
        assert!(session.in_transaction());

        let output = session
            .execute(Command::TxnRollback)
            .expect("txn rollback should succeed");
        assert_eq!(output, Output::TxnAborted);
        assert!(!session.in_transaction());
    }

    #[test]
    fn read_only_session_rejects_writes() {
        let db = test_db();
        let mut session = Session::new_with_mode(db, AccessMode::ReadOnly);

        let error = session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "k".into(),
                value: Value::Int(1),
            })
            .expect_err("write should be rejected");

        assert!(matches!(error, Error::AccessDenied { .. }));
    }

    #[test]
    fn strata_methods_use_new_error_and_value_types() {
        let db = Strata::cache().expect("cache database should open");
        assert_eq!(db.access_mode(), AccessMode::ReadWrite);
        assert_eq!(db.current_branch(), "default");
        assert_eq!(db.current_space(), "default");

        let version = db
            .kv_put("greeting", Value::String("hello".into()))
            .expect("kv put should succeed");
        assert!(version > 0);

        let value = db.kv_get("greeting").expect("kv get should succeed");
        assert_eq!(value, Some(Value::String("hello".into())));

        db.json_set("doc:1", "$", Value::Bool(true))
            .expect("json set should succeed");

        let mut event = HashMap::new();
        event.insert("ok".to_string(), Value::Bool(true));
        db.event_append("system.init", Value::Object(Box::new(event)))
            .expect("event append should succeed");

        db.close().expect("close should succeed");
    }

    #[test]
    fn new_handle_refreshes_default_branch_for_local_handles() {
        let (_dir, db) = disk_db_with_default_branch("main");
        let stale = Strata {
            backend: Backend::Local {
                executor: Executor::new_with_mode(db, AccessMode::ReadWrite),
            },
            current_branch: BranchId::from("default"),
            current_space: "default".to_string(),
            access_mode: AccessMode::ReadWrite,
        };

        let fresh = stale.new_handle().expect("new handle should open");
        assert_eq!(fresh.current_branch(), "main");
    }

    #[test]
    fn new_handle_refreshes_default_branch_for_ipc_handles() {
        let (dir, db) = disk_db_with_default_branch("main");
        let mut server =
            IpcServer::start(dir.path(), db, AccessMode::ReadWrite).expect("server should start");
        let client = crate::ipc::IpcClient::connect(&dir.path().join("strata.sock"))
            .expect("client should connect");
        let stale = Strata {
            backend: Backend::Ipc {
                client: Mutex::new(client),
            },
            current_branch: BranchId::from("default"),
            current_space: "default".to_string(),
            access_mode: AccessMode::ReadWrite,
        };

        let fresh = stale.new_handle().expect("new IPC handle should open");
        assert_eq!(fresh.current_branch(), "main");
        assert!(fresh.is_ipc());

        server.shutdown();
    }
}
