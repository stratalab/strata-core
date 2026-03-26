//! Stateful session for transaction support.
//!
//! The [`Session`] wraps an [`Executor`] and manages an optional open
//! transaction, providing read-your-writes semantics across multiple
//! commands within a transaction boundary.
//!
//! # Usage
//!
//! ```text
//! use strata_executor::Session;
//!
//! let mut session = Session::new(db.clone());
//!
//! // Begin a transaction
//! session.execute(Command::TxnBegin { branch: None, options: None })?;
//!
//! // Data commands route through the transaction
//! session.execute(Command::KvPut { branch: None, key: "k".into(), value: Value::Int(1) })?;
//! let out = session.execute(Command::KvGet { branch: None, key: "k".into() })?;
//!
//! // Commit
//! session.execute(Command::TxnCommit)?;
//! ```

use std::sync::Arc;

use strata_core::types::{Key, Namespace, TypeTag};
use strata_engine::{Database, Transaction, TransactionContext, TransactionOps};
use strata_security::AccessMode;

use crate::bridge::{
    extract_version, json_to_value, parse_path, to_core_branch_id, to_versioned_value,
    value_to_json,
};
use crate::convert::convert_result;
use crate::ipc::IpcClient;
use crate::types::BranchId;
use crate::{Command, Error, Executor, Output, Result};

/// A stateful session that wraps an [`Executor`] and manages an optional
/// open transaction with read-your-writes semantics.
///
/// When no transaction is active, commands delegate to the inner `Executor`.
/// When a transaction is active, data commands (KV, Event, JSON)
/// route through the engine's `Transaction<'a>` / `TransactionOps` trait,
/// while non-transactional commands (Branch, Vector, DB) still delegate to
/// the `Executor`.
///
/// For IPC mode, the session delegates all commands (including transactions)
/// to the server via a dedicated IPC connection. The server creates a
/// per-connection Session, so transaction state is managed server-side.
pub struct Session {
    executor: Executor,
    db: Arc<Database>,
    txn_ctx: Option<TransactionContext>,
    txn_branch_id: Option<strata_core::types::BranchId>,
    /// Optional IPC client for remote sessions.
    ipc_client: Option<IpcClient>,
}

impl Session {
    /// Create a new session.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            executor: Executor::new(db.clone()),
            db,
            txn_ctx: None,
            txn_branch_id: None,
            ipc_client: None,
        }
    }

    /// Create a new session with an explicit access mode.
    pub fn new_with_mode(db: Arc<Database>, access_mode: AccessMode) -> Self {
        Self {
            executor: Executor::new_with_mode(db.clone(), access_mode),
            db,
            txn_ctx: None,
            txn_branch_id: None,
            ipc_client: None,
        }
    }

    /// Create a new IPC-backed session.
    ///
    /// All commands (including transaction lifecycle) are sent over the IPC
    /// connection. The server creates a per-connection Session, so transaction
    /// state is managed server-side.
    pub fn new_ipc(client: IpcClient, access_mode: AccessMode) -> Self {
        // We need a dummy Database for the executor, but for IPC sessions
        // the executor is never used — all commands go through the IPC client.
        // Database::cache() is a lightweight in-memory DB used only as a placeholder.
        let db =
            Database::cache().expect("failed to create in-memory placeholder DB (out of memory?)");
        Self {
            executor: Executor::new_with_mode(db.clone(), access_mode),
            db,
            txn_ctx: None,
            txn_branch_id: None,
            ipc_client: Some(client),
        }
    }

    /// Returns whether a transaction is currently active.
    pub fn in_transaction(&self) -> bool {
        self.txn_ctx.is_some()
    }

    /// Execute a command, routing through the active transaction when appropriate.
    pub fn execute(&mut self, mut cmd: Command) -> Result<Output> {
        // IPC sessions delegate everything to the server
        if let Some(ref mut client) = self.ipc_client {
            return client.execute(cmd);
        }

        if self.executor.access_mode() == AccessMode::ReadOnly && cmd.is_write() {
            let hint = if self.db.is_follower() {
                Some("This database is a read-only follower. Writes must go through the primary instance.".to_string())
            } else {
                Some("Database is in read-only mode.".to_string())
            };
            return Err(Error::AccessDenied {
                command: cmd.name().to_string(),
                hint,
            });
        }

        cmd.resolve_defaults();

        match &cmd {
            // Transaction lifecycle commands
            Command::TxnBegin { .. } => self.handle_begin(&cmd),
            Command::TxnCommit => self.handle_commit(),
            Command::TxnRollback => self.handle_abort(),
            Command::TxnInfo => self.handle_txn_info(),
            Command::TxnIsActive => Ok(Output::Bool(self.in_transaction())),

            // Vector write commands are not supported inside a transaction
            // because the engine's vector store is not transactional.
            Command::VectorUpsert { .. }
            | Command::VectorDelete { .. }
            | Command::VectorCreateCollection { .. }
            | Command::VectorDeleteCollection { .. }
                if self.txn_ctx.is_some() =>
            {
                Err(Error::InvalidInput {
                    reason: "Vector write operations are not supported inside a transaction"
                        .to_string(),
                    hint: None,
                })
            }

            // Branch create/delete modify global state outside the transaction
            // scope and are not supported inside a transaction.
            Command::BranchCreate { .. } | Command::BranchDelete { .. }
                if self.txn_ctx.is_some() =>
            {
                Err(Error::InvalidInput {
                    reason: "Branch create/delete operations are not supported inside a transaction"
                        .to_string(),
                    hint: None,
                })
            }

            // Non-transactional commands always go to executor.
            // Branch read commands (Get, List, Exists) are safe to delegate
            // regardless of transaction state since they only read metadata.
            Command::BranchCreate { .. }
            | Command::BranchGet { .. }
            | Command::BranchList { .. }
            | Command::BranchExists { .. }
            | Command::BranchDelete { .. }
            // Vector commands: writes delegate to executor outside txn,
            // reads are always safe to delegate.
            | Command::VectorUpsert { .. }
            | Command::VectorGet { .. }
            | Command::VectorDelete { .. }
            | Command::VectorSearch { .. }
            | Command::VectorCreateCollection { .. }
            | Command::VectorDeleteCollection { .. }
            | Command::VectorListCollections { .. }
            | Command::Ping
            | Command::Info
            | Command::Flush
            | Command::Compact
            | Command::EmbedStatus
            | Command::RetentionApply { .. }
            | Command::RetentionStats { .. }
            | Command::RetentionPreview { .. }
            | Command::BranchExport { .. }
            | Command::BranchImport { .. }
            | Command::BranchBundleValidate { .. }
            | Command::Search { .. }
            // Space commands: manage spaces at the branch level,
            // not transactional.
            | Command::SpaceList { .. }
            | Command::SpaceCreate { .. }
            | Command::SpaceDelete { .. }
            | Command::SpaceExists { .. }
            // Version history commands (KvGetv, JsonGetv) require
            // storage-layer version chains which are not available through the
            // transaction context. These always read from the committed store,
            // even during an active transaction.
            | Command::KvGetv { .. }
            | Command::JsonGetv { .. }
            // JsonList enumerates keys via storage-layer scan. Making it
            // txn-aware would require merging the write-set with a committed
            // prefix scan, which is non-trivial. It reads from the committed
            // store even during an active transaction.
            | Command::JsonList { .. }
            // EventGetByType uses scan_prefix on per-type index keys.
            // The transaction write-set has these keys (#1972), but
            // scan_prefix is only available on the committed store, so
            // this always reads from the committed store even during an
            // active transaction.
            | Command::EventGetByType { .. } => self.executor.execute(cmd),

            // Data commands: route through txn if active, else delegate
            _ => {
                if self.txn_ctx.is_some() {
                    self.execute_in_txn(cmd)
                } else {
                    self.executor.execute(cmd)
                }
            }
        }
    }

    /// Get a reference to the underlying executor.
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    // =========================================================================
    // Transaction lifecycle handlers
    // =========================================================================

    fn handle_begin(&mut self, cmd: &Command) -> Result<Output> {
        if self.txn_ctx.is_some() {
            return Err(Error::TransactionAlreadyActive {
                hint: Some("Commit or rollback before starting a new one.".to_string()),
            });
        }

        let branch = match cmd {
            Command::TxnBegin { branch, .. } => branch.clone().unwrap_or_else(BranchId::default),
            _ => unreachable!(),
        };

        let core_branch_id = to_core_branch_id(&branch)?;
        let ctx = self.db.begin_transaction(core_branch_id)?;
        self.txn_ctx = Some(ctx);
        self.txn_branch_id = Some(core_branch_id);

        Ok(Output::TxnBegun)
    }

    fn handle_commit(&mut self) -> Result<Output> {
        let mut ctx = self.txn_ctx.take().ok_or(Error::TransactionNotActive {
            hint: Some("Start one with: begin".to_string()),
        })?;
        self.txn_branch_id = None;

        match self.db.commit_transaction(&mut ctx) {
            Ok(version) => {
                self.db.end_transaction(ctx);
                Ok(Output::TxnCommitted { version })
            }
            Err(e) => {
                // Return context to pool even on failure
                self.db.end_transaction(ctx);
                // Discriminate error types: only OCC validation failures
                // become TransactionConflict; storage/WAL errors become Io;
                // other errors become Internal.
                match &e {
                    strata_core::StrataError::TransactionAborted { .. }
                    | strata_core::StrataError::Conflict { .. }
                    | strata_core::StrataError::VersionConflict { .. }
                    | strata_core::StrataError::WriteConflict { .. } => {
                        Err(Error::TransactionConflict {
                            reason: e.to_string(),
                            hint: Some(
                                "Another write modified this key. Retry your transaction."
                                    .to_string(),
                            ),
                        })
                    }
                    strata_core::StrataError::Storage { .. }
                    | strata_core::StrataError::Corruption { .. } => Err(Error::Io {
                        reason: e.to_string(),
                        hint: None,
                    }),
                    _ => Err(Error::Internal {
                        reason: e.to_string(),
                        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                    }),
                }
            }
        }
    }

    fn handle_abort(&mut self) -> Result<Output> {
        let ctx = self.txn_ctx.take().ok_or(Error::TransactionNotActive {
            hint: Some("Start one with: begin".to_string()),
        })?;
        self.txn_branch_id = None;
        self.db.end_transaction(ctx);
        Ok(Output::TxnAborted)
    }

    fn handle_txn_info(&self) -> Result<Output> {
        if let Some(ctx) = &self.txn_ctx {
            Ok(Output::TxnInfo(Some(crate::types::TransactionInfo {
                id: ctx.txn_id.to_string(),
                status: crate::types::TxnStatus::Active,
                started_at: 0,
            })))
        } else {
            Ok(Output::TxnInfo(None))
        }
    }

    // =========================================================================
    // In-transaction command execution
    // =========================================================================

    fn execute_in_txn(&mut self, cmd: Command) -> Result<Output> {
        let branch_id = self
            .txn_branch_id
            .expect("txn_branch_id set when txn_ctx is Some");

        // Extract space from the command being executed
        let space = match &cmd {
            Command::KvPut { space, .. }
            | Command::KvGet { space, .. }
            | Command::KvDelete { space, .. }
            | Command::KvList { space, .. }
            | Command::KvGetv { space, .. }
            | Command::EventAppend { space, .. }
            | Command::EventGet { space, .. }
            | Command::EventGetByType { space, .. }
            | Command::EventLen { space, .. }
            | Command::JsonSet { space, .. }
            | Command::JsonGet { space, .. }
            | Command::JsonGetv { space, .. }
            | Command::JsonDelete { space, .. }
            | Command::JsonList { space, .. } => {
                space.clone().unwrap_or_else(|| "default".to_string())
            }
            _ => "default".to_string(),
        };
        let ns = Arc::new(Namespace::for_branch_space(branch_id, &space));

        // Temporarily take the context to create a Transaction
        let mut ctx = self.txn_ctx.take().unwrap();
        let result = Self::dispatch_in_txn(&self.executor, &mut ctx, ns, cmd);
        self.txn_ctx = Some(ctx);

        result
    }

    fn dispatch_in_txn(
        executor: &Executor,
        ctx: &mut TransactionContext,
        ns: Arc<Namespace>,
        cmd: Command,
    ) -> Result<Output> {
        // Read commands use ctx.get() / ctx.scan_prefix() directly so they
        // fall through to the snapshot when the key isn't in the write-set.
        // Write commands create a Transaction which handles event sequencing
        // and other write-specific logic.
        match cmd {
            // === KV reads — via ctx for snapshot fallback ===
            Command::KvGet { key, .. } => {
                let full_key = Key::new_kv(ns, &key);
                let result = ctx.get(&full_key).map_err(Error::from)?;
                Ok(Output::Maybe(result))
            }
            Command::KvList {
                prefix,
                cursor,
                limit,
                ..
            } => {
                let prefix_key = match prefix {
                    Some(ref p) => Key::new_kv(ns.clone(), p),
                    None => Key::new(ns.clone(), TypeTag::KV, vec![]),
                };
                let entries = ctx.scan_prefix(&prefix_key).map_err(Error::from)?;
                let keys: Vec<String> = entries
                    .into_iter()
                    .filter_map(|(k, _)| k.user_key_string())
                    .collect();
                if let Some(lim) = limit {
                    let start_idx = if let Some(ref cur) = cursor {
                        keys.iter().position(|k| k > cur).unwrap_or(keys.len())
                    } else {
                        0
                    };
                    let end_idx = std::cmp::min(start_idx + lim as usize, keys.len());
                    Ok(Output::Keys(keys[start_idx..end_idx].to_vec()))
                } else {
                    Ok(Output::Keys(keys))
                }
            }

            // === JSON reads — via ctx for snapshot fallback ===
            Command::JsonGet { key, path, .. } => {
                let full_key = Key::new_json(ns.clone(), &key);
                if path == "$" || path.is_empty() {
                    let result = ctx.get(&full_key).map_err(Error::from)?;
                    match result {
                        Some(strata_core::value::Value::String(s)) => {
                            let jv: strata_core::JsonValue =
                                serde_json::from_str(&s).map_err(|e| Error::Serialization {
                                    reason: e.to_string(),
                                })?;
                            let val = convert_result(json_to_value(jv))?;
                            Ok(Output::Maybe(Some(val)))
                        }
                        Some(strata_core::value::Value::Bytes(b)) => {
                            // JSON documents are stored as MessagePack-encoded JsonDoc structs
                            let doc: strata_engine::JsonDoc =
                                rmp_serde::from_slice(&b).map_err(|e| Error::Serialization {
                                    reason: format!("Failed to deserialize JSON document: {}", e),
                                })?;
                            let val = convert_result(json_to_value(doc.value))?;
                            Ok(Output::Maybe(Some(val)))
                        }
                        Some(other) => Ok(Output::Maybe(Some(other))),
                        None => Ok(Output::Maybe(None)),
                    }
                } else {
                    // Path-based get still needs Transaction for JSON patch logic
                    let mut txn = Transaction::new(ctx, ns);
                    let json_path = convert_result(parse_path(&path))?;
                    let result = txn.json_get_path(&key, &json_path).map_err(Error::from)?;
                    match result {
                        Some(jv) => {
                            let val = convert_result(json_to_value(jv))?;
                            Ok(Output::Maybe(Some(val)))
                        }
                        None => Ok(Output::Maybe(None)),
                    }
                }
            }

            // === Write commands — use Transaction ===
            Command::KvPut { key, value, .. } => {
                let mut txn = Transaction::new(ctx, ns);
                let version = txn.kv_put(&key, value).map_err(Error::from)?;
                Ok(Output::WriteResult {
                    key,
                    version: extract_version(&version),
                })
            }
            Command::KvDelete { key, .. } => {
                let full_key = Key::new_kv(ns, &key);
                let existed = ctx.exists(&full_key).map_err(Error::from)?;
                ctx.delete(full_key).map_err(Error::from)?;
                Ok(Output::DeleteResult {
                    key,
                    deleted: existed,
                })
            }

            // === Event operations — use Transaction for hash chaining ===
            Command::EventAppend {
                event_type,
                payload,
                ..
            } => {
                let mut txn = Transaction::new(ctx, ns);
                let version = txn
                    .event_append(&event_type, payload)
                    .map_err(Error::from)?;
                Ok(Output::EventAppendResult {
                    sequence: extract_version(&version),
                    event_type,
                })
            }
            Command::EventGet { sequence, .. } => {
                let mut txn = Transaction::new(ctx, ns);
                let result = txn.event_get(sequence).map_err(Error::from)?;
                Ok(Output::MaybeVersioned(result.map(|v| {
                    to_versioned_value(strata_core::Versioned::new(
                        v.value.payload.clone(),
                        v.version,
                    ))
                })))
            }
            Command::EventLen { .. } => {
                let mut txn = Transaction::new(ctx, ns);
                let len = txn.event_len().map_err(Error::from)?;
                Ok(Output::Uint(len))
            }

            // === JSON writes — use Transaction ===
            Command::JsonSet {
                key, path, value, ..
            } => {
                let mut txn = Transaction::new(ctx, ns);
                let json_path = convert_result(parse_path(&path))?;
                let json_value = convert_result(value_to_json(value))?;
                let version = txn
                    .json_set(&key, &json_path, json_value)
                    .map_err(Error::from)?;
                Ok(Output::WriteResult {
                    key,
                    version: extract_version(&version),
                })
            }
            Command::JsonDelete { key, .. } => {
                let mut txn = Transaction::new(ctx, ns);
                let deleted = txn.json_delete(&key).map_err(Error::from)?;
                Ok(Output::DeleteResult { key, deleted })
            }

            // Commands not directly mapped to TransactionOps — delegate to executor.
            // This includes batch operations, history, CAS, scan, incr, etc.
            other => executor.execute(other),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(ctx) = self.txn_ctx.take() {
            self.db.end_transaction(ctx);
        }
    }
}
