use std::collections::BTreeSet;
use std::sync::Arc;

use strata_core::types::{Key, Namespace, TypeTag};
use strata_engine::transaction::context::Transaction as ScopedTransaction;
use strata_engine::transaction_ops::TransactionOps as _;
use strata_engine::{Database, Transaction, TransactionContext};
use strata_graph::ext::GraphStoreExt;
use strata_graph::types::{Direction, EdgeData, GraphMeta, NodeData};
use strata_security::AccessMode;
use strata_vector::ext::VectorStoreExt;

use crate::bridge::{
    access_denied, bypasses_active_transaction, extract_version, is_read_only, json_to_value,
    parse_path, runtime_default_branch, serde_json_to_value_public, to_core_branch_id,
    to_versioned_value, transaction_already_active, transaction_branch_mismatch,
    transaction_not_active, validate_branch_exists, validate_key, validate_not_internal_collection,
    validate_session_space, validate_value, value_to_json, value_to_serde_json_public, Primitives,
};
use crate::convert::convert_result;
use crate::handlers::embed_runtime;
use crate::handlers::reject_system_branch;
use crate::ipc::IpcClient;
use crate::{
    BatchGetItemResult, BatchItemResult, BranchId, Command, Error, Executor, GraphNeighborHit,
    Output, Result, TransactionInfo, TxnStatus, VectorData, VersionedValue, VersionedVectorData,
};

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
    Local(LocalSession),
    Ipc(IpcClient),
}

struct LocalSession {
    db: Arc<Database>,
    txn: Option<Transaction>,
    effects: TxnSideEffects,
}

impl LocalSession {
    fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            txn: None,
            effects: TxnSideEffects::default(),
        }
    }

    fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    fn begin(&mut self, branch: &BranchId) -> Result<Output> {
        let branch = to_core_branch_id(branch)?;
        let txn = self.db.begin_transaction(branch)?;
        self.txn = Some(txn);
        self.effects = TxnSideEffects::default();
        Ok(Output::TxnBegun)
    }

    fn commit(&mut self, executor: &Executor) -> Result<Output> {
        let mut txn = self.txn.take().ok_or_else(transaction_not_active)?;
        let branch_id = txn.branch_id();
        let effects = std::mem::take(&mut self.effects);
        match txn.commit() {
            Ok(version) => {
                apply_txn_side_effects(executor.primitives(), branch_id, effects)?;
                Ok(Output::TxnCommitted { version })
            }
            Err(error) => match &error {
                strata_core::StrataError::TransactionAborted { .. }
                | strata_core::StrataError::Conflict { .. }
                | strata_core::StrataError::VersionConflict { .. }
                | strata_core::StrataError::WriteConflict { .. } => {
                    Err(Error::TransactionConflict {
                        reason: error.to_string(),
                        hint: Some(
                            "Another write modified this key. Retry your transaction."
                                .to_string(),
                        ),
                    })
                }
                strata_core::StrataError::Storage { .. }
                | strata_core::StrataError::Corruption { .. } => Err(Error::Io {
                    reason: error.to_string(),
                    hint: None,
                }),
                _ => Err(Error::Internal {
                    reason: error.to_string(),
                    hint: Some(
                        "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                            .to_string(),
                    ),
                }),
            },
        }
    }

    fn rollback(&mut self) -> Result<Output> {
        let mut txn = self.txn.take().ok_or_else(transaction_not_active)?;
        txn.abort();
        self.effects = TxnSideEffects::default();
        Ok(Output::TxnAborted)
    }

    fn txn_info(&self) -> Output {
        Output::TxnInfo(self.txn.as_ref().map(|txn| TransactionInfo {
            id: txn.txn_id.to_string(),
            status: TxnStatus::Active,
            started_at: 0,
        }))
    }

    fn execute_in_txn(&mut self, executor: &Executor, command: Command) -> Result<Output> {
        match &command {
            Command::VectorCreateCollection { .. } | Command::VectorDeleteCollection { .. } => {
                return Err(Error::InvalidInput {
                    reason:
                        "Collection create/delete operations are not supported inside a transaction"
                            .to_string(),
                    hint: None,
                });
            }
            Command::BranchCreate { .. } | Command::BranchDelete { .. } => {
                return Err(Error::InvalidInput {
                    reason:
                        "Branch create/delete operations are not supported inside a transaction"
                            .to_string(),
                    hint: None,
                });
            }
            Command::GraphDelete { .. }
            | Command::GraphDefineObjectType { .. }
            | Command::GraphDeleteObjectType { .. }
            | Command::GraphDefineLinkType { .. }
            | Command::GraphDeleteLinkType { .. }
            | Command::GraphFreezeOntology { .. }
            | Command::GraphBulkInsert { .. } => {
                return Err(Error::InvalidInput {
                    reason: "Graph delete, bulk insert, and ontology operations are not supported inside a transaction"
                        .to_string(),
                    hint: None,
                });
            }
            _ => {}
        }

        let branch_id = self
            .txn
            .as_ref()
            .expect("transaction should be active while dispatching in-transaction commands")
            .branch_id();
        let space = command_space(&command);
        let namespace = Arc::new(Namespace::for_branch_space(branch_id, &space));
        let txn = self
            .txn
            .as_mut()
            .expect("transaction should be active while dispatching in-transaction commands");
        let ctx = txn
            .context_mut()
            .expect("transaction context should be available until commit or rollback");

        dispatch_in_txn(
            executor,
            ctx,
            namespace,
            branch_id,
            &space,
            command,
            &mut self.effects,
        )
    }
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
            backend: SessionBackend::Local(LocalSession::new(db.clone())),
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
            backend: SessionBackend::Local(LocalSession::new(db.clone())),
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
        let bypasses_txn = bypasses_active_transaction(&command);

        if !bypasses_txn {
            if let Some(txn_branch) = self.txn_branch.as_ref() {
                if let Some(command_branch) = command.resolved_branch() {
                    if command_branch != txn_branch {
                        return Err(transaction_branch_mismatch(command_branch, txn_branch));
                    }
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

        match &mut self.backend {
            SessionBackend::Local(backend) => {
                let executor = self
                    .executor
                    .as_ref()
                    .expect("local sessions should always have an executor");

                match command {
                    Command::TxnBegin {
                        branch: Some(branch),
                        options: _,
                    } => {
                        let output = backend.begin(&branch)?;
                        self.txn_branch = Some(branch);
                        Ok(output)
                    }
                    Command::TxnCommit => {
                        let output = backend.commit(executor);
                        self.txn_branch = None;
                        output
                    }
                    Command::TxnRollback => {
                        let output = backend.rollback();
                        self.txn_branch = None;
                        output
                    }
                    Command::TxnInfo => Ok(backend.txn_info()),
                    other => {
                        if bypasses_active_transaction(&other) {
                            return executor.execute(other);
                        }
                        if !backend.in_transaction() {
                            return executor.execute(other);
                        }
                        backend.execute_in_txn(executor, other)
                    }
                }
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
        match &self.backend {
            SessionBackend::Local(backend) => backend.in_transaction(),
            SessionBackend::Ipc(_) => self.txn_branch.is_some(),
        }
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

fn command_space(command: &Command) -> String {
    match command {
        Command::KvPut { space, .. }
        | Command::KvGet { space, .. }
        | Command::KvDelete { space, .. }
        | Command::KvList { space, .. }
        | Command::KvGetv { space, .. }
        | Command::KvScan { space, .. }
        | Command::KvBatchPut { space, .. }
        | Command::KvBatchGet { space, .. }
        | Command::KvBatchDelete { space, .. }
        | Command::KvBatchExists { space, .. }
        | Command::JsonSet { space, .. }
        | Command::JsonGet { space, .. }
        | Command::JsonGetv { space, .. }
        | Command::JsonDelete { space, .. }
        | Command::JsonBatchSet { space, .. }
        | Command::JsonBatchGet { space, .. }
        | Command::JsonBatchDelete { space, .. }
        | Command::JsonList { space, .. }
        | Command::EventAppend { space, .. }
        | Command::EventBatchAppend { space, .. }
        | Command::EventGet { space, .. }
        | Command::EventGetByType { space, .. }
        | Command::EventLen { space, .. }
        | Command::EventRange { space, .. }
        | Command::EventRangeByTime { space, .. }
        | Command::EventListTypes { space, .. }
        | Command::EventList { space, .. }
        | Command::GraphCreate { space, .. }
        | Command::GraphDelete { space, .. }
        | Command::GraphList { space, .. }
        | Command::GraphGetMeta { space, .. }
        | Command::GraphAddNode { space, .. }
        | Command::GraphGetNode { space, .. }
        | Command::GraphRemoveNode { space, .. }
        | Command::GraphListNodes { space, .. }
        | Command::GraphListNodesPaginated { space, .. }
        | Command::GraphAddEdge { space, .. }
        | Command::GraphRemoveEdge { space, .. }
        | Command::GraphNeighbors { space, .. }
        | Command::GraphBulkInsert { space, .. }
        | Command::GraphBfs { space, .. }
        | Command::GraphDefineObjectType { space, .. }
        | Command::GraphGetObjectType { space, .. }
        | Command::GraphListObjectTypes { space, .. }
        | Command::GraphDeleteObjectType { space, .. }
        | Command::GraphDefineLinkType { space, .. }
        | Command::GraphGetLinkType { space, .. }
        | Command::GraphListLinkTypes { space, .. }
        | Command::GraphDeleteLinkType { space, .. }
        | Command::GraphFreezeOntology { space, .. }
        | Command::GraphOntologyStatus { space, .. }
        | Command::GraphOntologySummary { space, .. }
        | Command::GraphListOntologyTypes { space, .. }
        | Command::GraphNodesByType { space, .. }
        | Command::GraphWcc { space, .. }
        | Command::GraphCdlp { space, .. }
        | Command::GraphPagerank { space, .. }
        | Command::GraphLcc { space, .. }
        | Command::GraphSssp { space, .. }
        | Command::VectorUpsert { space, .. }
        | Command::VectorGet { space, .. }
        | Command::VectorGetv { space, .. }
        | Command::VectorDelete { space, .. }
        | Command::VectorQuery { space, .. }
        | Command::VectorCreateCollection { space, .. }
        | Command::VectorDeleteCollection { space, .. }
        | Command::VectorListCollections { space, .. }
        | Command::VectorCollectionStats { space, .. }
        | Command::VectorBatchUpsert { space, .. }
        | Command::VectorBatchGet { space, .. }
        | Command::VectorBatchDelete { space, .. }
        | Command::VectorSample { space, .. } => {
            space.clone().unwrap_or_else(|| "default".to_string())
        }
        _ => "default".to_string(),
    }
}

fn parse_direction(s: Option<&str>) -> Result<Direction> {
    match s {
        None | Some("outgoing") => Ok(Direction::Outgoing),
        Some("incoming") => Ok(Direction::Incoming),
        Some("both") => Ok(Direction::Both),
        Some(other) => Err(Error::InvalidInput {
            reason: format!(
                "Invalid direction '{}'. Must be 'outgoing', 'incoming', or 'both'.",
                other
            ),
            hint: None,
        }),
    }
}

fn parse_cascade_policy(s: Option<&str>) -> Result<strata_graph::types::CascadePolicy> {
    match s {
        None | Some("ignore") => Ok(strata_graph::types::CascadePolicy::Ignore),
        Some("cascade") => Ok(strata_graph::types::CascadePolicy::Cascade),
        Some("detach") => Ok(strata_graph::types::CascadePolicy::Detach),
        Some(other) => Err(Error::InvalidInput {
            reason: format!(
                "Invalid cascade_policy '{}'. Must be 'cascade', 'detach', or 'ignore'.",
                other
            ),
            hint: None,
        }),
    }
}

fn dispatch_in_txn(
    executor: &Executor,
    ctx: &mut TransactionContext,
    namespace: Arc<Namespace>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    command: Command,
    effects: &mut TxnSideEffects,
) -> Result<Output> {
    match command {
        Command::KvGet { as_of: Some(_), .. }
        | Command::KvList { as_of: Some(_), .. }
        | Command::JsonGet { as_of: Some(_), .. }
        | Command::JsonList { as_of: Some(_), .. }
        | Command::EventGet { as_of: Some(_), .. }
        | Command::EventGetByType { as_of: Some(_), .. }
        | Command::EventLen { as_of: Some(_), .. }
        | Command::GraphGetNode { as_of: Some(_), .. }
        | Command::GraphListNodes { as_of: Some(_), .. }
        | Command::GraphNeighbors { as_of: Some(_), .. }
        | Command::VectorGet { as_of: Some(_), .. } => executor.execute(command),

        Command::KvGet { key, .. } => {
            let full_key = Key::new_kv(namespace, &key);
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
                Some(ref prefix) => Key::new_kv(namespace.clone(), prefix),
                None => Key::new(namespace.clone(), TypeTag::KV, vec![]),
            };
            let entries = ctx.scan_prefix(&prefix_key).map_err(Error::from)?;
            let keys: Vec<String> = entries
                .into_iter()
                .filter_map(|(key, _)| key.user_key_string())
                .collect();
            if let Some(limit) = limit {
                let start = if let Some(ref cursor) = cursor {
                    keys.iter()
                        .position(|key| key > cursor)
                        .unwrap_or(keys.len())
                } else {
                    0
                };
                let end = std::cmp::min(start + limit as usize, keys.len());
                Ok(Output::Keys(keys[start..end].to_vec()))
            } else {
                Ok(Output::Keys(keys))
            }
        }
        Command::KvScan { start, limit, .. } => {
            let prefix_key = Key::new(namespace, TypeTag::KV, vec![]);
            let entries = ctx.scan_prefix(&prefix_key).map_err(Error::from)?;
            let iter = entries
                .into_iter()
                .filter_map(|(key, value)| key.user_key_string().map(|key| (key, value)));
            let iter: Box<dyn Iterator<Item = (String, strata_core::Value)>> =
                if let Some(start) = start {
                    Box::new(iter.skip_while(move |(key, _)| key.as_str() < start.as_str()))
                } else {
                    Box::new(iter)
                };
            let pairs: Vec<(String, strata_core::Value)> = if let Some(limit) = limit {
                iter.take(limit as usize).collect()
            } else {
                iter.collect()
            };
            Ok(Output::KvScanResult(pairs))
        }
        Command::KvPut { key, value, .. } => {
            convert_result(validate_key(&key))?;
            convert_result(validate_value(&value, &executor.primitives().limits))?;
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let version = txn.kv_put(&key, value).map_err(Error::from)?;
            effects.record_kv(space, &key);
            Ok(Output::WriteResult {
                key,
                version: extract_version(&version),
            })
        }
        Command::KvDelete { key, .. } => {
            convert_result(validate_key(&key))?;
            let full_key = Key::new_kv(namespace, &key);
            let existed = ctx.exists(&full_key).map_err(Error::from)?;
            ctx.delete(full_key).map_err(Error::from)?;
            if existed {
                effects.record_kv(space, &key);
            }
            Ok(Output::DeleteResult {
                key,
                deleted: existed,
            })
        }
        Command::KvBatchPut { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            let mut txn = ScopedTransaction::new(ctx, namespace);
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = validate_value(&entry.value, &executor.primitives().limits) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = txn.kv_put(&entry.key, entry.value) {
                    results[index].error = Some(error.to_string());
                } else {
                    effects.record_kv(space, &entry.key);
                }
            }
            Ok(Output::BatchResults(results))
        }
        Command::KvBatchGet { keys, .. } => {
            let mut results = vec![
                BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: None,
                };
                keys.len()
            ];
            for (index, key) in keys.into_iter().enumerate() {
                if let Err(error) = validate_key(&key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let full_key = Key::new_kv(namespace.clone(), &key);
                let value = ctx.get(&full_key).map_err(Error::from)?;
                results[index].value = value;
            }
            Ok(Output::BatchGetResults(results))
        }
        Command::KvBatchDelete { keys, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                keys.len()
            ];
            for (index, key) in keys.into_iter().enumerate() {
                if let Err(error) = validate_key(&key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let full_key = Key::new_kv(namespace.clone(), &key);
                let existed = ctx.exists(&full_key).map_err(Error::from)?;
                if let Err(error) = ctx.delete(full_key) {
                    results[index].error = Some(error.to_string());
                } else if existed {
                    effects.record_kv(space, &key);
                }
            }
            Ok(Output::BatchResults(results))
        }
        Command::KvBatchExists { keys, .. } => {
            let mut values = Vec::with_capacity(keys.len());
            for key in &keys {
                convert_result(validate_key(key))?;
            }
            for key in keys {
                let full_key = Key::new_kv(namespace.clone(), &key);
                values.push(ctx.exists(&full_key).map_err(Error::from)?);
            }
            Ok(Output::BoolList(values))
        }

        Command::JsonGet { key, path, .. } => {
            convert_result(validate_key(&key))?;
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let path = convert_result(parse_path(&path))?;
            let result = txn.json_get_path(&key, &path).map_err(Error::from)?;
            Ok(Output::Maybe(match result {
                Some(value) => Some(convert_result(json_to_value(value))?),
                None => None,
            }))
        }
        Command::JsonSet {
            key, path, value, ..
        } => {
            convert_result(validate_key(&key))?;
            convert_result(validate_value(&value, &executor.primitives().limits))?;
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let path = convert_result(parse_path(&path))?;
            let value = convert_result(value_to_json(value))?;
            let version = txn.json_set(&key, &path, value).map_err(Error::from)?;
            effects.record_json(space, &key);
            Ok(Output::WriteResult {
                key,
                version: extract_version(&version),
            })
        }
        Command::JsonDelete { key, path, .. } => {
            convert_result(validate_key(&key))?;
            let path = convert_result(parse_path(&path))?;
            if path.is_root() {
                let mut txn = ScopedTransaction::new(ctx, namespace);
                let deleted = txn.json_delete(&key).map_err(Error::from)?;
                if deleted {
                    effects.record_json(space, &key);
                }
                Ok(Output::DeleteResult { key, deleted })
            } else {
                let mut txn = ScopedTransaction::new(ctx, namespace);
                let Some(mut doc) = txn
                    .json_get_path(&key, &strata_core::JsonPath::root())
                    .map_err(Error::from)?
                else {
                    return Ok(Output::DeleteResult {
                        key,
                        deleted: false,
                    });
                };

                let deleted = match strata_core::delete_at_path(&mut doc, &path) {
                    Ok(Some(_)) => true,
                    Ok(None) | Err(strata_core::JsonPathError::NotFound) => false,
                    Err(error) => {
                        return Err(Error::InvalidInput {
                            reason: error.to_string(),
                            hint: None,
                        });
                    }
                };

                if deleted {
                    txn.json_set(&key, &strata_core::JsonPath::root(), doc)
                        .map_err(Error::from)?;
                    effects.record_json(space, &key);
                }

                Ok(Output::DeleteResult { key, deleted })
            }
        }
        Command::JsonList {
            prefix,
            cursor,
            limit,
            ..
        } => {
            let prefix_key = match prefix {
                Some(ref prefix) => Key::new_json(namespace.clone(), prefix),
                None => Key::new(namespace.clone(), TypeTag::Json, vec![]),
            };
            let entries = ctx.scan_prefix(&prefix_key).map_err(Error::from)?;
            let mut keys: Vec<String> = entries
                .into_iter()
                .filter_map(|(key, _)| key.user_key_string())
                .collect();
            if let Some(ref cursor) = cursor {
                keys.retain(|key| key.as_str() > cursor.as_str());
            }
            let has_more = keys.len() > limit as usize;
            keys.truncate(limit as usize);
            let next_cursor = if has_more { keys.last().cloned() } else { None };
            Ok(Output::JsonListResult {
                keys,
                has_more,
                cursor: next_cursor,
            })
        }
        Command::JsonBatchSet { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            let mut txn = ScopedTransaction::new(ctx, namespace);
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = validate_value(&entry.value, &executor.primitives().limits) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let path = match parse_path(&entry.path) {
                    Ok(path) => path,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                let value = match value_to_json(entry.value) {
                    Ok(value) => value,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                match txn.json_set(&entry.key, &path, value) {
                    Ok(version) => {
                        results[index].version = Some(extract_version(&version));
                        effects.record_json(space, &entry.key);
                    }
                    Err(error) => results[index].error = Some(error.to_string()),
                }
            }
            Ok(Output::BatchResults(results))
        }
        Command::JsonBatchGet { entries, .. } => {
            let mut results = vec![
                BatchGetItemResult {
                    value: None,
                    version: None,
                    timestamp: None,
                    error: None,
                };
                entries.len()
            ];
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let output = match dispatch_in_txn(
                    executor,
                    ctx,
                    namespace.clone(),
                    branch_id,
                    space,
                    Command::JsonGet {
                        branch: None,
                        space: Some(space.to_string()),
                        key: entry.key,
                        path: entry.path,
                        as_of: None,
                    },
                    effects,
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                match output {
                    Output::Maybe(Some(value)) => results[index].value = Some(value),
                    Output::Maybe(None) => {}
                    Output::MaybeVersioned(Some(value)) => {
                        results[index].value = Some(value.value);
                        results[index].version = Some(value.version);
                        results[index].timestamp = Some(value.timestamp);
                    }
                    Output::MaybeVersioned(None) => {}
                    other => {
                        results[index].error = Some(format!(
                            "unexpected output for JsonBatchGet entry: {other:?}"
                        ));
                    }
                }
            }
            Ok(Output::BatchGetResults(results))
        }
        Command::JsonBatchDelete { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_key(&entry.key) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                let output = match dispatch_in_txn(
                    executor,
                    ctx,
                    namespace.clone(),
                    branch_id,
                    space,
                    Command::JsonDelete {
                        branch: None,
                        space: Some(space.to_string()),
                        key: entry.key,
                        path: entry.path,
                    },
                    effects,
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        results[index].error = Some(error.to_string());
                        continue;
                    }
                };
                match output {
                    Output::DeleteResult { deleted, .. } if deleted => {
                        results[index].version = Some(0)
                    }
                    Output::DeleteResult { .. } => {}
                    other => {
                        results[index].error = Some(format!(
                            "unexpected output for JsonBatchDelete entry: {other:?}"
                        ));
                    }
                }
            }
            Ok(Output::BatchResults(results))
        }

        Command::EventAppend {
            event_type,
            payload,
            ..
        } => {
            convert_result(validate_value(&payload, &executor.primitives().limits))?;
            validate_event_type_input(&event_type)?;
            validate_event_payload_input(&payload)?;
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let version = txn
                .event_append(&event_type, payload)
                .map_err(Error::from)?;
            let sequence = extract_version(&version);
            effects.record_event(space, sequence);
            Ok(Output::EventAppendResult {
                sequence,
                event_type,
            })
        }
        Command::EventGet { sequence, .. } => {
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let result = txn.event_get(sequence).map_err(Error::from)?;
            Ok(Output::MaybeVersioned(result.map(|value| {
                to_versioned_value(strata_core::Versioned::new(
                    value.value.payload.clone(),
                    value.version,
                ))
            })))
        }
        Command::EventGetByType {
            event_type,
            limit,
            after_sequence,
            ..
        } => {
            let prefix = Key::new_event_type_idx_prefix(namespace.clone(), &event_type);
            let index_entries = ctx.scan_prefix(&prefix).map_err(Error::from)?;
            let mut events = Vec::new();
            for (index_key, _) in index_entries {
                let user_key = &index_key.user_key;
                if user_key.len() < 8 {
                    continue;
                }
                let bytes: [u8; 8] = user_key[user_key.len() - 8..]
                    .try_into()
                    .expect("sequence suffix should be eight bytes");
                let sequence = u64::from_be_bytes(bytes);
                if after_sequence.is_some_and(|after| sequence <= after) {
                    continue;
                }
                let event_key = Key::new_event(namespace.clone(), sequence);
                if let Some(strata_core::Value::String(json)) =
                    ctx.get(&event_key).map_err(Error::from)?
                {
                    let event: strata_core::Event =
                        serde_json::from_str(&json).map_err(|error| Error::Serialization {
                            reason: format!("corrupt event at sequence {}: {}", sequence, error),
                        })?;
                    events.push(VersionedValue {
                        value: event.payload.clone(),
                        version: sequence,
                        timestamp: event.timestamp.into(),
                    });
                }
                if limit.is_some_and(|limit| events.len() >= limit as usize) {
                    break;
                }
            }
            Ok(Output::VersionedValues(events))
        }
        Command::EventLen { .. } => {
            let mut txn = ScopedTransaction::new(ctx, namespace);
            let len = txn.event_len().map_err(Error::from)?;
            Ok(Output::Uint(len))
        }
        Command::EventBatchAppend { entries, .. } => {
            let mut results = vec![
                BatchItemResult {
                    version: None,
                    error: None,
                };
                entries.len()
            ];
            let mut txn = ScopedTransaction::new(ctx, namespace);
            for (index, entry) in entries.into_iter().enumerate() {
                if let Err(error) = validate_value(&entry.payload, &executor.primitives().limits) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = validate_event_type_input(&entry.event_type) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = validate_event_payload_input(&entry.payload) {
                    results[index].error = Some(error.to_string());
                    continue;
                }
                match txn.event_append(&entry.event_type, entry.payload) {
                    Ok(version) => {
                        let sequence = extract_version(&version);
                        results[index].version = Some(sequence);
                        effects.record_event(space, sequence);
                    }
                    Err(error) => results[index].error = Some(error.to_string()),
                }
            }
            Ok(Output::BatchResults(results))
        }

        Command::GraphCreate {
            graph,
            cascade_policy,
            ..
        } => {
            let policy = parse_cascade_policy(cascade_policy.as_deref())?;
            let meta = GraphMeta {
                cascade_policy: policy,
                ..Default::default()
            };
            convert_result(ctx.graph_create(branch_id, space, &graph, meta))?;
            Ok(Output::Unit)
        }
        Command::GraphAddNode {
            graph,
            node_id,
            entity_ref,
            properties,
            object_type,
            ..
        } => {
            let properties = properties
                .map(value_to_serde_json_public)
                .transpose()
                .map_err(Error::from)?;
            let data = NodeData {
                entity_ref,
                properties,
                object_type,
            };
            let backend_state = convert_result(executor.primitives().graph.state())?;
            let created = convert_result(ctx.graph_add_node(
                branch_id,
                space,
                &graph,
                &node_id,
                &data,
                &backend_state,
            ))?;
            Ok(Output::GraphWriteResult { node_id, created })
        }
        Command::GraphGetNode { graph, node_id, .. } => {
            let node = convert_result(ctx.graph_get_node(branch_id, space, &graph, &node_id))?;
            match node {
                Some(data) => {
                    let json =
                        serde_json::to_value(&data).map_err(|error| Error::Serialization {
                            reason: error.to_string(),
                        })?;
                    Ok(Output::Maybe(Some(convert_result(
                        serde_json_to_value_public(json),
                    )?)))
                }
                None => Ok(Output::Maybe(None)),
            }
        }
        Command::GraphRemoveNode { graph, node_id, .. } => {
            let backend_state = convert_result(executor.primitives().graph.state())?;
            convert_result(ctx.graph_remove_node(
                branch_id,
                space,
                &graph,
                &node_id,
                &backend_state,
            ))?;
            Ok(Output::Unit)
        }
        Command::GraphListNodes { graph, .. } => {
            let nodes = convert_result(ctx.graph_list_nodes(branch_id, space, &graph))?;
            Ok(Output::Keys(nodes))
        }
        Command::GraphGetMeta { graph, .. } => {
            let meta = convert_result(ctx.graph_get_meta(branch_id, space, &graph))?;
            match meta {
                Some(meta) => {
                    let json =
                        serde_json::to_value(&meta).map_err(|error| Error::Serialization {
                            reason: error.to_string(),
                        })?;
                    Ok(Output::Maybe(Some(convert_result(
                        serde_json_to_value_public(json),
                    )?)))
                }
                None => Ok(Output::Maybe(None)),
            }
        }
        Command::GraphList { .. } => {
            let graphs = convert_result(ctx.graph_list(branch_id, space))?;
            Ok(Output::Keys(graphs))
        }
        Command::GraphAddEdge {
            graph,
            src,
            dst,
            edge_type,
            weight,
            properties,
            ..
        } => {
            let properties = properties
                .map(value_to_serde_json_public)
                .transpose()
                .map_err(Error::from)?;
            let data = EdgeData {
                weight: weight.unwrap_or(1.0),
                properties,
            };
            let created = convert_result(
                ctx.graph_add_edge(branch_id, space, &graph, &src, &dst, &edge_type, &data),
            )?;
            Ok(Output::GraphEdgeWriteResult {
                src,
                dst,
                edge_type,
                created,
            })
        }
        Command::GraphRemoveEdge {
            graph,
            src,
            dst,
            edge_type,
            ..
        } => {
            convert_result(
                ctx.graph_remove_edge(branch_id, space, &graph, &src, &dst, &edge_type),
            )?;
            Ok(Output::Unit)
        }
        Command::GraphNeighbors {
            graph,
            node_id,
            direction,
            edge_type,
            ..
        } => {
            let direction = parse_direction(direction.as_deref())?;
            let neighbors = match direction {
                Direction::Outgoing => convert_result(ctx.graph_outgoing_neighbors(
                    branch_id,
                    space,
                    &graph,
                    &node_id,
                    edge_type.as_deref(),
                ))?,
                Direction::Incoming => convert_result(ctx.graph_incoming_neighbors(
                    branch_id,
                    space,
                    &graph,
                    &node_id,
                    edge_type.as_deref(),
                ))?,
                Direction::Both => {
                    let mut outgoing = convert_result(ctx.graph_outgoing_neighbors(
                        branch_id,
                        space,
                        &graph,
                        &node_id,
                        edge_type.as_deref(),
                    ))?;
                    let incoming = convert_result(ctx.graph_incoming_neighbors(
                        branch_id,
                        space,
                        &graph,
                        &node_id,
                        edge_type.as_deref(),
                    ))?;
                    outgoing.extend(incoming);
                    outgoing
                }
            };
            Ok(Output::GraphNeighbors(
                neighbors
                    .into_iter()
                    .map(|neighbor| GraphNeighborHit {
                        node_id: neighbor.node_id,
                        edge_type: neighbor.edge_type,
                        weight: neighbor.edge_data.weight,
                    })
                    .collect(),
            ))
        }

        Command::VectorUpsert {
            collection,
            key,
            vector,
            metadata,
            ..
        } => {
            validate_not_internal_collection(&collection).map_err(Error::from)?;
            let primitives = executor.primitives();
            primitives
                .vector
                .ensure_collection_loaded(branch_id, space, &collection)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let metadata = metadata
                .map(value_to_serde_json_public)
                .transpose()
                .map_err(Error::from)?;
            let state = primitives
                .vector
                .state()
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let (version, _) = ctx
                .vector_upsert(
                    branch_id,
                    space,
                    &collection,
                    &key,
                    &vector,
                    metadata,
                    None,
                    &state,
                )
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            Ok(Output::VectorWriteResult {
                collection,
                key,
                version: extract_version(&version),
            })
        }
        Command::VectorDelete {
            collection, key, ..
        } => {
            validate_not_internal_collection(&collection).map_err(Error::from)?;
            let primitives = executor.primitives();
            primitives
                .vector
                .ensure_collection_loaded(branch_id, space, &collection)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let state = primitives
                .vector
                .state()
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            let (deleted, _) = ctx
                .vector_delete(branch_id, space, &collection, &key, &state)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            Ok(Output::VectorDeleteResult {
                collection,
                key,
                deleted,
            })
        }
        Command::VectorGet {
            collection, key, ..
        } => {
            let record = ctx
                .vector_get(branch_id, space, &collection, &key)
                .map_err(|error| Error::from(error.into_strata_error(branch_id)))?;
            match record {
                Some(record) => {
                    let version = extract_version(&strata_core::Version::counter(record.version));
                    let metadata = record
                        .metadata
                        .map(serde_json_to_value_public)
                        .transpose()
                        .map_err(Error::from)?;
                    Ok(Output::VectorData(Some(VersionedVectorData {
                        key,
                        data: VectorData {
                            embedding: record.embedding,
                            metadata,
                        },
                        version,
                        timestamp: record.updated_at,
                    })))
                }
                None => Ok(Output::VectorData(None)),
            }
        }

        other => executor.execute(other),
    }
}

#[derive(Default)]
struct TxnSideEffects {
    kv: BTreeSet<(String, String)>,
    json: BTreeSet<(String, String)>,
    events: BTreeSet<(String, u64)>,
}

impl TxnSideEffects {
    fn record_kv(&mut self, space: &str, key: &str) {
        self.kv.insert((space.to_string(), key.to_string()));
    }

    fn record_json(&mut self, space: &str, key: &str) {
        self.json.insert((space.to_string(), key.to_string()));
    }

    fn record_event(&mut self, space: &str, sequence: u64) {
        self.events.insert((space.to_string(), sequence));
    }
}

fn apply_txn_side_effects(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    effects: TxnSideEffects,
) -> Result<()> {
    let index = primitives
        .db
        .extension::<strata_engine::search::InvertedIndex>()
        .map_err(Error::from)?;

    for (space, key) in effects.kv {
        apply_kv_post_commit(primitives, &index, branch_id, &space, &key)?;
    }
    for (space, key) in effects.json {
        apply_json_post_commit(primitives, &index, branch_id, &space, &key)?;
    }
    for (space, sequence) in effects.events {
        apply_event_post_commit(primitives, &index, branch_id, &space, sequence)?;
    }

    Ok(())
}

fn apply_kv_post_commit(
    primitives: &Arc<Primitives>,
    index: &strata_engine::search::InvertedIndex,
    branch_id: strata_core::types::BranchId,
    space: &str,
    key: &str,
) -> Result<()> {
    let entity_ref = strata_engine::search::EntityRef::Kv {
        branch_id,
        space: space.to_string(),
        key: key.to_string(),
    };

    match convert_result(primitives.kv.get(&branch_id, space, key))? {
        Some(value) => {
            if index.is_enabled() {
                if let Some(text) = value.extractable_text() {
                    index.index_document(&entity_ref, &text, None);
                } else {
                    index.remove_document(&entity_ref);
                }
            }
            if let Some(text) = embed_runtime::extract_text(&value) {
                embed_runtime::maybe_embed_text(
                    primitives,
                    branch_id,
                    space,
                    embed_runtime::SHADOW_KV,
                    key,
                    &text,
                    strata_core::EntityRef::kv(branch_id, space, key),
                );
            } else {
                embed_runtime::maybe_remove_embedding(
                    primitives,
                    branch_id,
                    space,
                    embed_runtime::SHADOW_KV,
                    key,
                );
            }
        }
        None => {
            if index.is_enabled() {
                index.remove_document(&entity_ref);
            }
            embed_runtime::maybe_remove_embedding(
                primitives,
                branch_id,
                space,
                embed_runtime::SHADOW_KV,
                key,
            );
        }
    }

    Ok(())
}

fn apply_json_post_commit(
    primitives: &Arc<Primitives>,
    index: &strata_engine::search::InvertedIndex,
    branch_id: strata_core::types::BranchId,
    space: &str,
    key: &str,
) -> Result<()> {
    let entity_ref = strata_engine::search::EntityRef::Json {
        branch_id,
        space: space.to_string(),
        doc_id: key.to_string(),
    };

    match read_committed_json_root(primitives, branch_id, space, key)? {
        Some(json_value) => {
            if index.is_enabled() {
                let text = serde_json::to_string(json_value.as_inner()).unwrap_or_default();
                index.index_document(&entity_ref, &text, None);
            }
            if let Ok(value) = json_to_value(json_value) {
                if let Some(text) = embed_runtime::extract_text(&value) {
                    embed_runtime::maybe_embed_text(
                        primitives,
                        branch_id,
                        space,
                        embed_runtime::SHADOW_JSON,
                        key,
                        &text,
                        strata_core::EntityRef::json(branch_id, space, key),
                    );
                } else {
                    embed_runtime::maybe_remove_embedding(
                        primitives,
                        branch_id,
                        space,
                        embed_runtime::SHADOW_JSON,
                        key,
                    );
                }
            } else {
                embed_runtime::maybe_remove_embedding(
                    primitives,
                    branch_id,
                    space,
                    embed_runtime::SHADOW_JSON,
                    key,
                );
            }
        }
        None => {
            if index.is_enabled() {
                index.remove_document(&entity_ref);
            }
            embed_runtime::maybe_remove_embedding(
                primitives,
                branch_id,
                space,
                embed_runtime::SHADOW_JSON,
                key,
            );
        }
    }

    Ok(())
}

fn read_committed_json_root(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    key: &str,
) -> Result<Option<strata_core::JsonValue>> {
    primitives
        .json
        .get(&branch_id, space, key, &strata_core::JsonPath::root())
        .map_err(Error::from)
}

fn apply_event_post_commit(
    primitives: &Arc<Primitives>,
    index: &strata_engine::search::InvertedIndex,
    branch_id: strata_core::types::BranchId,
    space: &str,
    sequence: u64,
) -> Result<()> {
    let Some(event) = convert_result(primitives.event.get(&branch_id, space, sequence))? else {
        return Ok(());
    };

    if index.is_enabled() {
        let text = format!(
            "{} {}",
            event.value.event_type,
            serde_json::to_string(&event.value.payload).unwrap_or_default()
        );
        let entity_ref = strata_engine::search::EntityRef::Event {
            branch_id,
            space: space.to_string(),
            sequence,
        };
        index.index_document(&entity_ref, &text, None);
    }

    if let Some(text) = embed_runtime::extract_text(&event.value.payload) {
        let event_key = sequence.to_string();
        embed_runtime::maybe_embed_text(
            primitives,
            branch_id,
            space,
            embed_runtime::SHADOW_EVENT,
            &event_key,
            &text,
            strata_core::EntityRef::event(branch_id, space, sequence),
        );
    }

    Ok(())
}

fn validate_event_type_input(event_type: &str) -> Result<()> {
    if event_type.is_empty() {
        return Err(Error::InvalidInput {
            reason: "Event type must not be empty".to_string(),
            hint: None,
        });
    }
    if event_type.len() > 256 {
        return Err(Error::InvalidInput {
            reason: format!(
                "Event type is too long: {} bytes (max 256)",
                event_type.len()
            ),
            hint: None,
        });
    }
    Ok(())
}

fn validate_event_payload_input(payload: &strata_core::Value) -> Result<()> {
    if !matches!(payload, strata_core::Value::Object(_)) {
        return Err(Error::InvalidInput {
            reason: "Event payload must be an object".to_string(),
            hint: None,
        });
    }
    if contains_non_finite_float(payload) {
        return Err(Error::InvalidInput {
            reason: "Event payload must not contain non-finite floats".to_string(),
            hint: None,
        });
    }
    Ok(())
}

fn contains_non_finite_float(value: &strata_core::Value) -> bool {
    match value {
        strata_core::Value::Float(f) => !f.is_finite(),
        strata_core::Value::Object(map) => map.values().any(contains_non_finite_float),
        strata_core::Value::Array(values) => values.iter().any(contains_non_finite_float),
        _ => false,
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
