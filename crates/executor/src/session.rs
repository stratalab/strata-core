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
//!
//! # Post-Commit Work
//!
//! Post-commit work (derived index updates) is handled by subsystem-owned
//! observers registered with the Database:
//!
//! - `VectorCommitObserver` (T2-E2): applies queued HNSW operations
//! - `GraphCommitObserver` (T2-E5): applies queued BM25 index operations
//!
//! During transactions, operations are queued in `VectorBackendState` and
//! `GraphBackendState` (stored as Database extensions). After commit, the
//! corresponding observers apply the queued operations. On abort or commit
//! failure, Session clears the queued operations without applying them.

use std::sync::Arc;

use strata_core::types::{Key, Namespace, TypeTag};
use strata_engine::database::search_only_cache_spec;
use strata_engine::{Database, Transaction, TransactionContext, TransactionOps};
use strata_graph::ext::GraphStoreExt;
use strata_graph::types::NodeData;
use strata_security::AccessMode;
use strata_vector::ext::VectorStoreExt;

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
        // Use an in-memory cache DB as a placeholder.
        let db = Database::open_runtime(search_only_cache_spec())
            .expect("failed to create in-memory placeholder DB (out of memory?)");
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

            // Vector collection DDL modifies in-memory backend state (DashMap)
            // and is not rollback-safe, so it's rejected inside transactions.
            // Vector upsert/delete now participate in OCC via VectorStoreExt.
            Command::VectorCreateCollection { .. } | Command::VectorDeleteCollection { .. }
                if self.txn_ctx.is_some() =>
            {
                Err(Error::InvalidInput {
                    reason:
                        "Collection create/delete operations are not supported inside a transaction"
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

            // Graph delete uses batched multi-transaction deletion and cannot
            // participate in a user transaction. Schema DDL (ontology) operations
            // are also non-transactional.
            Command::GraphDelete { .. }
            | Command::GraphDefineObjectType { .. }
            | Command::GraphDeleteObjectType { .. }
            | Command::GraphDefineLinkType { .. }
            | Command::GraphDeleteLinkType { .. }
            | Command::GraphFreezeOntology { .. }
            | Command::GraphBulkInsert { .. }
                if self.txn_ctx.is_some() =>
            {
                Err(Error::InvalidInput {
                    reason: "Graph delete, bulk insert, and ontology operations are not supported inside a transaction"
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
            // Vector: search/list always read committed HNSW (not txn-aware).
            // DDL delegates to executor. Upsert/Delete/Get route through txn
            // when active (handled in dispatch_in_txn).
            | Command::VectorQuery { .. }
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
            // Version history commands (KvGetv, JsonGetv, VectorGetv) require
            // storage-layer version chains which are not available through the
            // transaction context. These always read from the committed store,
            // even during an active transaction.
            | Command::KvGetv { .. }
            | Command::JsonGetv { .. }
            | Command::VectorGetv { .. } => self.executor.execute(cmd),

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
        let txn_id = ctx.txn_id;
        self.txn_branch_id = None;

        match self.db.commit_transaction(&mut ctx) {
            Ok(version) => {
                // T2-E5: Post-commit work is now handled by subsystem observers
                // (GraphCommitObserver, VectorCommitObserver) registered with the database.
                self.db.end_transaction(ctx);
                Ok(Output::TxnCommitted { version })
            }
            Err(e) => {
                // Return context to pool even on failure
                self.db.end_transaction(ctx);
                // Clear pending ops for uncommitted transactions (T2-E2, T2-E5: subsystem-owned cleanup).
                if let Ok(state) = self.executor.primitives().vector.state() {
                    state.clear_pending_ops(txn_id);
                }
                if let Ok(state) = self.executor.primitives().graph.state() {
                    state.clear_pending_ops(txn_id);
                }
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
        let txn_id = ctx.txn_id;
        self.txn_branch_id.take();

        // Clear pending ops for aborted transactions (T2-E2, T2-E5: subsystem-owned cleanup).
        // These ops were queued by VectorStoreExt/GraphStoreExt but will not be committed.
        if let Ok(state) = self.executor.primitives().vector.state() {
            state.clear_pending_ops(txn_id);
        }
        if let Ok(state) = self.executor.primitives().graph.state() {
            state.clear_pending_ops(txn_id);
        }

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
            | Command::KvScan { space, .. }
            | Command::EventAppend { space, .. }
            | Command::EventGet { space, .. }
            | Command::EventGetByType { space, .. }
            | Command::EventLen { space, .. }
            | Command::JsonSet { space, .. }
            | Command::JsonGet { space, .. }
            | Command::JsonGetv { space, .. }
            | Command::JsonDelete { space, .. }
            | Command::JsonList { space, .. }
            | Command::KvBatchPut { space, .. }
            | Command::KvBatchGet { space, .. }
            | Command::KvBatchDelete { space, .. }
            | Command::EventBatchAppend { space, .. }
            // Graph commands also carry an optional space.
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
            | Command::GraphSssp { space, .. } => {
                space.clone().unwrap_or_else(|| "default".to_string())
            }
            _ => "default".to_string(),
        };
        let ns = Arc::new(Namespace::for_branch_space(branch_id, &space));

        // Temporarily take the context to create a Transaction
        let mut ctx = self.txn_ctx.take().unwrap();
        let result = Self::dispatch_in_txn(&self.executor, &mut ctx, ns, branch_id, &space, cmd);
        self.txn_ctx = Some(ctx);

        result
    }

    fn dispatch_in_txn(
        executor: &Executor,
        ctx: &mut TransactionContext,
        ns: Arc<Namespace>,
        branch_id: strata_core::types::BranchId,
        space: &str,
        cmd: Command,
    ) -> Result<Output> {
        // Read commands use ctx.get() / ctx.scan_prefix() directly so they
        // fall through to the snapshot when the key isn't in the write-set.
        // Write commands create a Transaction which handles event sequencing
        // and other write-specific logic.
        match cmd {
            // === KV / JSON reads with as_of — bypass txn, use committed storage ===
            // Time-travel reads need the snapshot version chain, not the
            // transaction's write-set (which only tracks the txn's
            // start_version snapshot). Mirrors Event/Graph/Vector bypass
            // pattern.
            Command::KvGet { as_of: Some(_), .. }
            | Command::KvList { as_of: Some(_), .. }
            | Command::JsonGet { as_of: Some(_), .. }
            | Command::JsonList { as_of: Some(_), .. } => executor.execute(cmd),

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

            Command::KvScan { start, limit, .. } => {
                let prefix_key = Key::new(ns, TypeTag::KV, vec![]);
                let entries = ctx.scan_prefix(&prefix_key).map_err(Error::from)?;
                let iter = entries
                    .into_iter()
                    .filter_map(|(k, v)| k.user_key_string().map(|key| (key, v)));

                let iter: Box<dyn Iterator<Item = (String, strata_core::value::Value)>> =
                    if let Some(s) = start {
                        Box::new(iter.skip_while(move |(k, _)| k.as_str() < s.as_str()))
                    } else {
                        Box::new(iter)
                    };

                let pairs: Vec<(String, strata_core::value::Value)> = if let Some(lim) = limit {
                    iter.take(lim as usize).collect()
                } else {
                    iter.collect()
                };
                Ok(Output::KvScanResult(pairs))
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

            // === Event reads with as_of — bypass txn, use committed storage ===
            // Time-travel reads need the snapshot version chain, not the
            // transaction's write-set. Mirrors Graph/Vector bypass pattern.
            Command::EventGet { as_of: Some(_), .. }
            | Command::EventGetByType { as_of: Some(_), .. }
            | Command::EventLen { as_of: Some(_), .. } => executor.execute(cmd),

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

            // === Graph writes — via GraphStoreExt on TransactionContext ===
            Command::GraphCreate {
                graph,
                cascade_policy,
                ..
            } => {
                let policy =
                    crate::handlers::graph::parse_cascade_policy(cascade_policy.as_deref())?;
                let meta = strata_graph::types::GraphMeta {
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
                let props = match properties {
                    Some(v) => {
                        let json = crate::bridge::value_to_serde_json_public(v)?;
                        Some(json)
                    }
                    None => None,
                };
                let data = NodeData {
                    entity_ref,
                    properties: props,
                    object_type,
                };
                // Get backend state for subsystem-owned index maintenance.
                // T2-E5: graph_add_node queues ops that CommitObserver applies after commit.
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
            Command::GraphRemoveNode { graph, node_id, .. } => {
                // Get backend state for subsystem-owned index maintenance.
                // T2-E5: graph_remove_node queues ops that CommitObserver applies after commit.
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
            Command::GraphAddEdge {
                graph,
                src,
                dst,
                edge_type,
                weight,
                properties,
                ..
            } => {
                let props = match properties {
                    Some(v) => {
                        let json = crate::bridge::value_to_serde_json_public(v)?;
                        Some(json)
                    }
                    None => None,
                };
                let data = strata_graph::types::EdgeData {
                    weight: weight.unwrap_or(1.0),
                    properties: props,
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

            // === Graph reads with as_of — bypass txn, use committed storage ===
            Command::GraphGetNode { as_of: Some(_), .. }
            | Command::GraphListNodes { as_of: Some(_), .. }
            | Command::GraphNeighbors { as_of: Some(_), .. } => {
                // as_of (time-travel) reads need committed store, not txn snapshot
                executor.execute(cmd)
            }

            // === Graph reads — via GraphStoreExt for snapshot isolation ===
            Command::GraphGetNode { graph, node_id, .. } => {
                let node = convert_result(ctx.graph_get_node(branch_id, space, &graph, &node_id))?;
                match node {
                    Some(data) => {
                        let json =
                            serde_json::to_value(&data).map_err(|e| Error::Serialization {
                                reason: e.to_string(),
                            })?;
                        Ok(Output::Maybe(Some(
                            crate::handlers::graph::serde_json_to_value(json)?,
                        )))
                    }
                    None => Ok(Output::Maybe(None)),
                }
            }
            Command::GraphListNodes { graph, .. } => {
                let nodes = convert_result(ctx.graph_list_nodes(branch_id, space, &graph))?;
                Ok(Output::Keys(nodes))
            }
            Command::GraphGetMeta { graph, .. } => {
                let meta = convert_result(ctx.graph_get_meta(branch_id, space, &graph))?;
                match meta {
                    Some(m) => {
                        let json = serde_json::to_value(&m).map_err(|e| Error::Serialization {
                            reason: e.to_string(),
                        })?;
                        Ok(Output::Maybe(Some(
                            crate::handlers::graph::serde_json_to_value(json)?,
                        )))
                    }
                    None => Ok(Output::Maybe(None)),
                }
            }
            Command::GraphList { .. } => {
                let graphs = convert_result(ctx.graph_list(branch_id, space))?;
                Ok(Output::Keys(graphs))
            }
            Command::GraphNeighbors {
                graph,
                node_id,
                direction,
                edge_type,
                ..
            } => {
                let dir = crate::handlers::graph::parse_direction(direction.as_deref())?;
                let neighbors = match dir {
                    strata_graph::types::Direction::Outgoing => {
                        convert_result(ctx.graph_outgoing_neighbors(
                            branch_id,
                            space,
                            &graph,
                            &node_id,
                            edge_type.as_deref(),
                        ))?
                    }
                    strata_graph::types::Direction::Incoming => {
                        convert_result(ctx.graph_incoming_neighbors(
                            branch_id,
                            space,
                            &graph,
                            &node_id,
                            edge_type.as_deref(),
                        ))?
                    }
                    strata_graph::types::Direction::Both => {
                        let mut out = convert_result(ctx.graph_outgoing_neighbors(
                            branch_id,
                            space,
                            &graph,
                            &node_id,
                            edge_type.as_deref(),
                        ))?;
                        let inc = convert_result(ctx.graph_incoming_neighbors(
                            branch_id,
                            space,
                            &graph,
                            &node_id,
                            edge_type.as_deref(),
                        ))?;
                        out.extend(inc);
                        out
                    }
                };
                let hits: Vec<_> = neighbors
                    .into_iter()
                    .map(|n| crate::types::GraphNeighborHit {
                        node_id: n.node_id,
                        edge_type: n.edge_type,
                        weight: n.edge_data.weight,
                    })
                    .collect();
                Ok(Output::GraphNeighbors(hits))
            }

            // === Vector writes — via VectorStoreExt on TransactionContext ===
            Command::VectorUpsert {
                space,
                collection,
                key,
                vector,
                metadata,
                ..
            } => {
                let space = space.unwrap_or_else(|| "default".to_string());
                convert_result(crate::bridge::validate_not_internal_collection(&collection))?;
                let primitives = executor.primitives();
                primitives
                    .vector
                    .ensure_collection_loaded(branch_id, &space, &collection)
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                let json_metadata = metadata
                    .map(crate::bridge::value_to_serde_json_public)
                    .transpose()
                    .map_err(crate::Error::from)?;
                let state = primitives
                    .vector
                    .state()
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                // VectorStoreExt queues ops internally for VectorCommitObserver;
                // we ignore the returned staged_op (T2-E2: subsystem-owned).
                let (version, _staged_op) = ctx
                    .vector_upsert(
                        branch_id,
                        &space,
                        &collection,
                        &key,
                        &vector,
                        json_metadata,
                        None, // source_ref: only used by internal auto-embed
                        &state,
                    )
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                Ok(Output::VectorWriteResult {
                    collection,
                    key,
                    version: extract_version(&version),
                })
            }
            Command::VectorDelete {
                space,
                collection,
                key,
                ..
            } => {
                let space = space.unwrap_or_else(|| "default".to_string());
                convert_result(crate::bridge::validate_not_internal_collection(&collection))?;
                let primitives = executor.primitives();
                primitives
                    .vector
                    .ensure_collection_loaded(branch_id, &space, &collection)
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                let state = primitives
                    .vector
                    .state()
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                // VectorStoreExt queues ops internally for VectorCommitObserver;
                // we ignore the returned staged_op (T2-E2: subsystem-owned).
                let (existed, _staged_op) = ctx
                    .vector_delete(branch_id, &space, &collection, &key, &state)
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                Ok(Output::VectorDeleteResult {
                    collection,
                    key,
                    deleted: existed,
                })
            }

            // === Vector reads — via VectorStoreExt for read-your-writes ===
            Command::VectorGet { as_of: Some(_), .. } => {
                // as_of (time-travel) reads need committed store, not txn snapshot
                executor.execute(cmd)
            }
            Command::VectorGet {
                space,
                collection,
                key,
                ..
            } => {
                let space = space.unwrap_or_else(|| "default".to_string());
                let record = ctx
                    .vector_get(branch_id, &space, &collection, &key)
                    .map_err(|e| Error::from(e.into_strata_error(branch_id)))?;
                match record {
                    Some(rec) => {
                        let version = extract_version(&strata_core::Version::counter(rec.version));
                        let metadata = rec
                            .metadata
                            .map(crate::bridge::serde_json_to_value_public)
                            .transpose()
                            .map_err(crate::Error::from)?;
                        Ok(Output::VectorData(Some(
                            crate::types::VersionedVectorData {
                                key,
                                data: crate::types::VectorData {
                                    embedding: rec.embedding,
                                    metadata,
                                },
                                version,
                                timestamp: rec.updated_at,
                            },
                        )))
                    }
                    None => Ok(Output::VectorData(None)),
                }
            }

            // === JsonList — txn-aware via scan_prefix (write-set merge) ===
            Command::JsonList {
                prefix,
                cursor,
                limit,
                ..
            } => {
                let prefix_key = match prefix {
                    Some(ref p) => Key::new_json(ns.clone(), p),
                    None => Key::new(ns.clone(), TypeTag::Json, vec![]),
                };
                let entries = ctx.scan_prefix(&prefix_key).map_err(Error::from)?;
                let mut keys: Vec<String> = entries
                    .into_iter()
                    .filter_map(|(k, _)| k.user_key_string())
                    .collect();
                if let Some(ref cur) = cursor {
                    keys.retain(|k| k.as_str() > cur.as_str());
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

            // === EventGetByType — txn-aware via type index scan ===
            Command::EventGetByType {
                event_type,
                limit,
                after_sequence,
                ..
            } => {
                let type_prefix = Key::new_event_type_idx_prefix(ns.clone(), &event_type);
                let idx_entries = ctx.scan_prefix(&type_prefix).map_err(Error::from)?;

                let mut events = Vec::new();
                for (idx_key, _) in &idx_entries {
                    let user_key = &idx_key.user_key;
                    if user_key.len() >= 8 {
                        let seq_bytes: [u8; 8] = user_key[user_key.len() - 8..].try_into().unwrap();
                        let seq = u64::from_be_bytes(seq_bytes);

                        if after_sequence.is_some_and(|after| seq <= after) {
                            continue;
                        }

                        let event_key = Key::new_event(ns.clone(), seq);
                        if let Some(strata_core::Value::String(json)) =
                            ctx.get(&event_key).map_err(Error::from)?
                        {
                            let event: strata_core::Event =
                                serde_json::from_str(&json).map_err(|e| Error::Serialization {
                                    reason: format!("corrupt event at sequence {}: {}", seq, e),
                                })?;
                            events.push(crate::types::VersionedValue {
                                value: event.payload.clone(),
                                version: seq,
                                timestamp: event.timestamp.into(),
                            });
                        }

                        if limit.is_some_and(|l| events.len() >= l as usize) {
                            break;
                        }
                    }
                }
                Ok(Output::VersionedValues(events))
            }

            // === Batch KV operations — txn-aware ===
            // Route through `Transaction::kv_put` (not raw `ctx.put`)
            // so each entry inherits the space-registration contract
            // from the engine layer (Phase 3). Without the wrapper,
            // a batch put against a never-used non-default space
            // committed the data but left the space metadata key
            // unwritten.
            Command::KvBatchPut { entries, .. } => {
                let mut results = vec![
                    crate::types::BatchItemResult {
                        version: None,
                        error: None,
                    };
                    entries.len()
                ];
                let mut txn = Transaction::new(ctx, ns);
                for (i, entry) in entries.into_iter().enumerate() {
                    match txn.kv_put(&entry.key, entry.value) {
                        Ok(_) => {} // version assigned at commit time
                        Err(e) => results[i].error = Some(e.to_string()),
                    }
                }
                Ok(Output::BatchResults(results))
            }
            Command::KvBatchGet { keys, .. } => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let full_key = Key::new_kv(ns.clone(), &key);
                    let value = ctx.get(&full_key).map_err(Error::from)?;
                    results.push(crate::types::BatchGetItemResult {
                        value,
                        version: None,
                        timestamp: None,
                        error: None,
                    });
                }
                Ok(Output::BatchGetResults(results))
            }
            Command::KvBatchDelete { keys, .. } => {
                let mut results = vec![
                    crate::types::BatchItemResult {
                        version: None,
                        error: None,
                    };
                    keys.len()
                ];
                for (i, key) in keys.into_iter().enumerate() {
                    let full_key = Key::new_kv(ns.clone(), &key);
                    match ctx.delete(full_key) {
                        Ok(()) => {}
                        Err(e) => results[i].error = Some(e.to_string()),
                    }
                }
                Ok(Output::BatchResults(results))
            }

            // === EventBatchAppend — txn-aware via Transaction wrapper ===
            Command::EventBatchAppend { entries, .. } => {
                let mut results = vec![
                    crate::types::BatchItemResult {
                        version: None,
                        error: None,
                    };
                    entries.len()
                ];
                let mut txn = Transaction::new(ctx, ns);
                for (i, entry) in entries.into_iter().enumerate() {
                    match txn.event_append(&entry.event_type, entry.payload) {
                        Ok(version) => results[i].version = Some(extract_version(&version)),
                        Err(e) => results[i].error = Some(e.to_string()),
                    }
                }
                Ok(Output::BatchResults(results))
            }

            // Commands not directly mapped — delegate to executor.
            // This includes version history, graph analytics, search, etc.
            other => executor.execute(other),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Clean up any in-progress transaction on drop.
        // Clear pending ops and return context to pool.
        if let Some(ctx) = self.txn_ctx.take() {
            // Clear pending ops for the abandoned transaction (T2-E2, T2-E5).
            if let Ok(state) = self.executor.primitives().vector.state() {
                state.clear_pending_ops(ctx.txn_id);
            }
            if let Ok(state) = self.executor.primitives().graph.state() {
                state.clear_pending_ops(ctx.txn_id);
            }
            self.txn_branch_id = None;
            self.db.end_transaction(ctx);
        }
    }
}
