use std::collections::BTreeSet;
use std::sync::Arc;

use strata_engine::{Database, Transaction, TransactionContext};
use strata_security::AccessMode;
use strata_storage::Namespace;

use crate::bridge::{
    access_denied, bypasses_active_transaction, is_read_only, json_to_value,
    reject_reserved_branch, runtime_default_branch, to_core_branch_id, transaction_already_active,
    transaction_branch_mismatch, transaction_not_active, validate_branch_exists,
    validate_session_space, Primitives,
};
use crate::convert::convert_result;
use crate::handlers::{embed_runtime, event, graph, json, kv, vector};
use crate::ipc::IpcClient;
use crate::{BranchId, Command, Error, Executor, Output, Result, TransactionInfo, TxnStatus};

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
    // `LocalSession` carries an engine `Transaction` and effects ledger;
    // it dwarfs `IpcClient`, so box it to keep the enum compact.
    Local(Box<LocalSession>),
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
                strata_engine::StrataError::TransactionAborted { .. }
                | strata_engine::StrataError::Conflict { .. }
                | strata_engine::StrataError::VersionConflict { .. }
                | strata_engine::StrataError::WriteConflict { .. } => {
                    Err(Error::TransactionConflict {
                        reason: error.to_string(),
                        hint: Some(
                            "Another write modified this key. Retry your transaction."
                                .to_string(),
                        ),
                    })
                }
                strata_engine::StrataError::Storage { .. }
                | strata_engine::StrataError::Corruption { .. } => Err(Error::Io {
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
            backend: SessionBackend::Local(Box::new(LocalSession::new(db.clone()))),
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
            backend: SessionBackend::Local(Box::new(LocalSession::new(db.clone()))),
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
            reject_reserved_branch(branch)?;
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

        other @ Command::KvGet { .. }
        | other @ Command::KvList { .. }
        | other @ Command::KvScan { .. }
        | other @ Command::KvPut { .. }
        | other @ Command::KvDelete { .. }
        | other @ Command::KvBatchPut { .. }
        | other @ Command::KvBatchGet { .. }
        | other @ Command::KvBatchDelete { .. }
        | other @ Command::KvBatchExists { .. } => {
            kv::execute_in_txn(executor.primitives(), ctx, namespace, space, other, effects)
        }
        other @ Command::JsonGet { .. }
        | other @ Command::JsonSet { .. }
        | other @ Command::JsonDelete { .. }
        | other @ Command::JsonList { .. }
        | other @ Command::JsonBatchSet { .. }
        | other @ Command::JsonBatchGet { .. }
        | other @ Command::JsonBatchDelete { .. } => {
            json::execute_in_txn(executor.primitives(), ctx, namespace, space, other, effects)
        }
        other @ Command::EventAppend { .. }
        | other @ Command::EventGet { .. }
        | other @ Command::EventGetByType { .. }
        | other @ Command::EventLen { .. }
        | other @ Command::EventBatchAppend { .. } => {
            event::execute_in_txn(executor.primitives(), ctx, namespace, space, other, effects)
        }
        other @ Command::GraphCreate { .. }
        | other @ Command::GraphAddNode { .. }
        | other @ Command::GraphGetNode { .. }
        | other @ Command::GraphRemoveNode { .. }
        | other @ Command::GraphListNodes { .. }
        | other @ Command::GraphGetMeta { .. }
        | other @ Command::GraphList { .. }
        | other @ Command::GraphAddEdge { .. }
        | other @ Command::GraphRemoveEdge { .. }
        | other @ Command::GraphNeighbors { .. } => {
            graph::execute_in_txn(executor.primitives(), ctx, branch_id, space, other)
        }
        other @ Command::VectorUpsert { .. }
        | other @ Command::VectorDelete { .. }
        | other @ Command::VectorGet { .. } => {
            vector::execute_in_txn(executor.primitives(), ctx, branch_id, space, other)
        }

        other => executor.execute(other),
    }
}

#[derive(Default)]
pub(crate) struct TxnSideEffects {
    kv: BTreeSet<(String, String)>,
    json: BTreeSet<(String, String)>,
    events: BTreeSet<(String, u64)>,
}

impl TxnSideEffects {
    pub(crate) fn record_kv(&mut self, space: &str, key: &str) {
        self.kv.insert((space.to_string(), key.to_string()));
    }

    pub(crate) fn record_json(&mut self, space: &str, key: &str) {
        self.json.insert((space.to_string(), key.to_string()));
    }

    pub(crate) fn record_event(&mut self, space: &str, sequence: u64) {
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
                if let Some(text) = strata_engine::search::extract_search_text(&value) {
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
    use strata_storage::{Key, Namespace};

    use super::SessionBackend;
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

    #[test]
    fn commit_does_not_report_failure_after_durable_commit() {
        let db = test_db();
        let branch = crate::bridge::runtime_default_branch(&db);
        let branch_id =
            crate::bridge::to_core_branch_id(&branch).expect("default branch should parse");

        let corrupt_json_key = Key::new_json(
            Arc::new(Namespace::for_branch_space(branch_id, "default")),
            "corrupt-json",
        );
        db.transaction(branch_id, |txn| {
            txn.put(corrupt_json_key.clone(), Value::Bytes(vec![0xff, 0x00]))
        })
        .expect("corrupt fixture should be written directly");

        let mut session = Session::new(db.clone());
        session
            .execute(Command::TxnBegin {
                branch: Some("main".into()),
                options: None,
            })
            .expect("transaction should begin");
        session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "durable".into(),
                value: Value::String("committed".into()),
            })
            .expect("txn kv put should succeed");

        let SessionBackend::Local(local) = &mut session.backend else {
            panic!("test uses a local session backend");
        };
        local.effects.record_json("default", "corrupt-json");

        let result = session.execute(Command::TxnCommit);
        assert!(
            result.is_ok(),
            "post-commit side-effect replay should not flip a durable commit into an error: {result:?}"
        );

        let committed = Session::new(db)
            .execute(Command::KvGet {
                branch: Some("main".into()),
                space: Some("default".into()),
                key: "durable".into(),
                as_of: None,
            })
            .expect("committed value should remain readable");
        match committed {
            Output::Maybe(Some(Value::String(value))) => assert_eq!(value, "committed"),
            Output::MaybeVersioned(Some(versioned)) => {
                assert_eq!(versioned.value, Value::String("committed".into()))
            }
            other => panic!("unexpected committed read output: {other:?}"),
        }
    }
}
