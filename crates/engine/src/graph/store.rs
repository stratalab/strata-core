//! Shared backend state for GraphStore.
//!
//! This module provides `GraphBackendState` which is stored as a Database
//! extension. This follows the same pattern as `VectorBackendState` in the
//! vector crate.
//!
//! ## Design
//!
//! Graph index operations (BM25 search indexing) cannot participate in OCC
//! because the inverted index is an in-memory structure that isn't rollback-safe.
//! Operations are queued during transactions and applied after successful commit
//! by `GraphCommitObserver`. Followers apply the same graph-specific search
//! maintenance through `GraphRefreshHook` during `Database::refresh()`.
//!
//! ## Thread Safety
//!
//! Uses `DashMap` for per-transaction pending ops, allowing concurrent
//! transactions to queue operations without blocking each other.

use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use strata_core::id::TxnId;
use strata_core::BranchId;
use strata_storage::{Key, TypeTag};

use crate::graph::types::NodeData;

/// Staged graph index operation to be applied after commit.
///
/// These operations update derived indices (BM25 search index) that cannot
/// participate in OCC. They're queued during transaction execution and
/// applied by `GraphCommitObserver` after the transaction commits.
#[derive(Debug, Clone)]
pub enum StagedGraphOp {
    /// Index a node's text into the BM25 inverted index.
    IndexNode {
        /// Branch where the node was added.
        branch_id: BranchId,
        /// Space containing the graph.
        space: String,
        /// Name of the graph.
        graph: String,
        /// ID of the node to index.
        node_id: String,
        /// Node data containing text to index.
        data: NodeData,
    },
    /// Remove a node from the BM25 inverted index.
    DeindexNode {
        /// Branch where the node was removed.
        branch_id: BranchId,
        /// Space containing the graph.
        space: String,
        /// Name of the graph.
        graph: String,
        /// ID of the node to deindex.
        node_id: String,
    },
}

/// Shared backend state for GraphStore.
///
/// This struct is stored in the Database via the extension mechanism,
/// ensuring all GraphStore instances for the same Database share the same
/// state. This follows the same pattern as `VectorBackendState`.
///
/// # Thread Safety
///
/// Uses `DashMap` for pending ops so concurrent transactions on different
/// branches don't block each other.
pub struct GraphBackendState {
    /// Pending index operations to be applied after transaction commit.
    ///
    /// Operations are keyed by transaction id so concurrent writers cannot
    /// apply or clear each other's deferred work. On commit, the
    /// `GraphCommitObserver` drains and applies only the committed
    /// transaction's ops. On abort or failed commit, engine-owned abort
    /// observers clear that transaction's queued ops without touching other
    /// in-flight writers.
    pending_ops: DashMap<TxnId, Vec<StagedGraphOp>>,

    /// Tracks whether runtime-only hooks have been registered for this
    /// database instance.
    pub(crate) runtime_wired: AtomicBool,
}

impl Default for GraphBackendState {
    fn default() -> Self {
        Self {
            pending_ops: DashMap::new(),
            runtime_wired: AtomicBool::new(false),
        }
    }
}

impl GraphBackendState {
    /// Queue a staged graph operation for post-commit application.
    ///
    /// Called during transaction execution when graph nodes are added or
    /// removed. The operation will be applied by `GraphCommitObserver`
    /// after the transaction commits.
    pub fn queue_pending_op(&self, txn_id: TxnId, op: StagedGraphOp) {
        self.pending_ops.entry(txn_id).or_default().push(op);
    }

    /// Apply and clear all pending operations for a transaction.
    ///
    /// Called by `GraphCommitObserver` after transaction commit.
    /// Returns the number of operations applied.
    pub fn apply_pending_ops(
        &self,
        txn_id: TxnId,
        graph_store: &crate::graph::GraphStore,
    ) -> usize {
        if let Some((_, ops)) = self.pending_ops.remove(&txn_id) {
            let count = ops.len();
            for op in ops {
                apply_staged_graph_op(graph_store, op);
            }
            count
        } else {
            0
        }
    }

    /// Clear pending operations for a transaction without applying them.
    ///
    /// Called by engine-owned abort observers on transaction abort or commit
    /// failure to clean up uncommitted ops.
    pub fn clear_pending_ops(&self, txn_id: TxnId) {
        self.pending_ops.remove(&txn_id);
    }
}

/// Apply a single staged graph operation.
fn apply_staged_graph_op(graph_store: &crate::graph::GraphStore, op: StagedGraphOp) {
    match op {
        StagedGraphOp::IndexNode {
            branch_id,
            space,
            graph,
            node_id,
            data,
        } => {
            graph_store.index_node_for_search(branch_id, &space, &graph, &node_id, &data);
        }
        StagedGraphOp::DeindexNode {
            branch_id,
            space,
            graph,
            node_id,
        } => {
            graph_store.deindex_node_for_search(branch_id, &space, &graph, &node_id);
        }
    }
}

// =============================================================================
// Commit Observers and Refresh Hooks
// =============================================================================

use crate::database::observers::{
    AbortInfo, AbortObserver, CommitInfo, CommitObserver, ObserverError,
};
use crate::{Database, RefreshHook, RefreshHookError};
use std::sync::Weak;

/// Ensure runtime hooks are wired for this database instance.
///
/// This is called from `GraphSubsystem::initialize()` to register commit/abort
/// observers and the follower refresh hook. Uses atomic flag to ensure
/// idempotent registration.
pub fn ensure_runtime_wiring(db: &Arc<Database>, state: &Arc<GraphBackendState>) {
    use std::sync::atomic::Ordering;

    if state.runtime_wired.swap(true, Ordering::AcqRel) {
        return;
    }

    let commit_observer = Arc::new(GraphCommitObserver {
        db: Arc::downgrade(db),
    });
    db.commit_observers().register(commit_observer);

    let abort_observer = Arc::new(GraphAbortObserver {
        db: Arc::downgrade(db),
    });
    db.abort_observers().register(abort_observer);

    let refresh_hook = Arc::new(GraphRefreshHook {
        db: Arc::downgrade(db),
    });
    if let Ok(hooks) = db.extension::<crate::RefreshHooks>() {
        hooks.register(refresh_hook);
    }
}

/// Observer that applies pending graph index operations after commit.
///
/// Graph write methods queue `StagedGraphOp`s in `GraphBackendState` during
/// transactions. This observer applies them after successful commit, updating
/// the BM25 inverted index.
///
/// This moves graph index maintenance ownership from executor-local deferred
/// work to subsystem-owned observers (GraphSubsystem), fulfilling the T2-E5
/// requirement.
struct GraphCommitObserver {
    db: Weak<Database>,
}

impl CommitObserver for GraphCommitObserver {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn on_commit(&self, info: &CommitInfo) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(()); // Database dropped
        };

        // Get graph backend state and apply pending ops for this commit only.
        if let Ok(state) = db.extension::<GraphBackendState>() {
            let graph_store = crate::graph::GraphStore::new(db.clone());
            let applied = state.apply_pending_ops(info.txn_id, &graph_store);
            if applied > 0 {
                tracing::debug!(
                    target: "strata::graph",
                    txn_id = info.txn_id.0,
                    branch_id = ?info.branch_id,
                    commit_version = info.commit_version.0,
                    ops_applied = applied,
                    "Applied pending graph index operations"
                );
            }
        }

        Ok(())
    }
}

struct GraphAbortObserver {
    db: Weak<Database>,
}

impl AbortObserver for GraphAbortObserver {
    fn name(&self) -> &'static str {
        "graph-abort"
    }

    fn on_abort(&self, info: &AbortInfo) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };

        if let Ok(state) = db.extension::<GraphBackendState>() {
            state.clear_pending_ops(info.txn_id);
        }

        Ok(())
    }
}

fn parse_graph_node_key(key: &Key) -> Result<Option<(String, String)>, String> {
    if key.type_tag != TypeTag::Graph {
        return Ok(None);
    }

    let user_key = key
        .user_key_string()
        .ok_or_else(|| "graph key is not valid UTF-8".to_string())?;
    let parts: Vec<&str> = user_key.splitn(3, '/').collect();
    if parts.len() != 3 || parts[1] != "n" {
        return Ok(None);
    }

    Ok(Some((parts[0].to_string(), parts[2].to_string())))
}

fn decode_graph_node_data(
    value: &strata_core::Value,
) -> Result<crate::graph::types::NodeData, String> {
    use strata_core::Value;

    let Value::String(json) = value else {
        return Err("graph node payload stored as non-string value".to_string());
    };

    serde_json::from_str(json).map_err(|e| format!("failed to decode graph node payload: {e}"))
}

/// Refresh hook that updates graph search index during follower replay.
///
/// Graph node indexing semantics live in `GraphStore::index_node_for_search`.
/// Followers must use the same path instead of the generic search hook so
/// replay stays consistent with primary commit-time indexing.
struct GraphRefreshHook {
    db: Weak<Database>,
}

enum GraphRefreshOp {
    Index {
        branch_id: BranchId,
        space: String,
        graph: String,
        node_id: String,
        data: crate::graph::types::NodeData,
    },
    Remove {
        branch_id: BranchId,
        space: String,
        graph: String,
        node_id: String,
    },
}

struct PendingGraphRefresh {
    graph_store: crate::graph::GraphStore,
    ops: Vec<GraphRefreshOp>,
}

impl crate::PreparedRefresh for PendingGraphRefresh {
    fn publish(self: Box<Self>) {
        for op in self.ops {
            match op {
                GraphRefreshOp::Index {
                    branch_id,
                    space,
                    graph,
                    node_id,
                    data,
                } => {
                    self.graph_store
                        .index_node_for_search(branch_id, &space, &graph, &node_id, &data);
                }
                GraphRefreshOp::Remove {
                    branch_id,
                    space,
                    graph,
                    node_id,
                } => {
                    self.graph_store
                        .deindex_node_for_search(branch_id, &space, &graph, &node_id);
                }
            }
        }
    }
}

impl RefreshHook for GraphRefreshHook {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn pre_delete_read(&self, _db: &Database, deletes: &[Key]) -> Vec<(Key, Vec<u8>)> {
        deletes
            .iter()
            .filter(|key| key.type_tag == TypeTag::Graph)
            .cloned()
            .map(|key| (key, Vec::new()))
            .collect()
    }

    fn apply_refresh(
        &self,
        puts: &[(Key, strata_core::Value)],
        pre_read_deletes: &[(Key, Vec<u8>)],
    ) -> Result<Box<dyn crate::PreparedRefresh>, RefreshHookError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(Box::new(crate::NoopPreparedRefresh));
        };
        let graph_store = crate::graph::GraphStore::new(db);
        let mut ops = Vec::with_capacity(puts.len() + pre_read_deletes.len());

        for (key, value) in puts {
            let Some((graph, node_id)) =
                parse_graph_node_key(key).map_err(|e| RefreshHookError::new("graph", e))?
            else {
                continue;
            };

            let data =
                decode_graph_node_data(value).map_err(|e| RefreshHookError::new("graph", e))?;
            ops.push(GraphRefreshOp::Index {
                branch_id: key.namespace.branch_id,
                space: key.namespace.space.to_string(),
                graph,
                node_id,
                data,
            });
        }

        for (key, _) in pre_read_deletes {
            let Some((graph, node_id)) =
                parse_graph_node_key(key).map_err(|e| RefreshHookError::new("graph", e))?
            else {
                continue;
            };

            ops.push(GraphRefreshOp::Remove {
                branch_id: key.namespace.branch_id,
                space: key.namespace.space.to_string(),
                graph,
                node_id,
            });
        }

        Ok(Box::new(PendingGraphRefresh { graph_store, ops }))
    }

    fn freeze_to_disk(&self, _db: &Database) -> crate::StrataResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::OpenSpec;
    use crate::graph::ext::GraphStoreExt;
    use crate::graph::types::NodeData;
    use crate::search::EntityRef;
    use crate::search::Searchable;
    use crate::RefreshOutcome;
    use crate::{
        Database, NoopPreparedRefresh, PreparedRefresh, RefreshHook, RefreshHookError,
        RefreshHooks, SearchRequest, SearchSubsystem, Subsystem,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use strata_core::BranchId;

    #[derive(Clone)]
    struct FailOnceRefreshSubsystem {
        fail_once: Arc<AtomicBool>,
    }

    impl FailOnceRefreshSubsystem {
        fn new(fail_once: Arc<AtomicBool>) -> Self {
            Self { fail_once }
        }
    }

    struct FailOnceRefreshHook {
        fail_once: Arc<AtomicBool>,
    }

    impl RefreshHook for FailOnceRefreshHook {
        fn name(&self) -> &'static str {
            "graph-test-fail-once"
        }

        fn pre_delete_read(&self, _db: &Database, _deletes: &[Key]) -> Vec<(Key, Vec<u8>)> {
            Vec::new()
        }

        fn apply_refresh(
            &self,
            puts: &[(Key, strata_core::Value)],
            _pre_read_deletes: &[(Key, Vec<u8>)],
        ) -> Result<Box<dyn PreparedRefresh>, RefreshHookError> {
            if !puts.is_empty() && self.fail_once.swap(false, Ordering::SeqCst) {
                return Err(RefreshHookError::new(
                    "graph-test-fail-once",
                    "injected refresh hook failure",
                ));
            }
            Ok(Box::new(NoopPreparedRefresh))
        }

        fn freeze_to_disk(&self, _db: &Database) -> crate::StrataResult<()> {
            Ok(())
        }
    }

    impl Subsystem for FailOnceRefreshSubsystem {
        fn name(&self) -> &'static str {
            "graph-test-fail-once"
        }

        fn recover(&self, _db: &Arc<Database>) -> crate::StrataResult<()> {
            Ok(())
        }

        fn initialize(&self, db: &Arc<Database>) -> crate::StrataResult<()> {
            let hooks = db.extension::<RefreshHooks>()?;
            hooks.register(Arc::new(FailOnceRefreshHook {
                fail_once: self.fail_once.clone(),
            }));
            Ok(())
        }
    }

    fn unique_test_dir(prefix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_manual_abort_clears_pending_graph_ops() {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        let graph_store = crate::graph::GraphStore::new(db.clone());
        let branch_id = BranchId::new();

        graph_store
            .create_graph(branch_id, "default", "g", None)
            .unwrap();

        let state = graph_store.state().unwrap();
        let mut txn = db.begin_transaction(branch_id).unwrap();
        let txn_id = txn.txn_id;

        txn.graph_add_node(
            branch_id,
            "default",
            "g",
            "n_abort",
            &crate::graph::types::NodeData::default(),
            &state,
        )
        .unwrap();

        assert!(
            state.pending_ops.contains_key(&txn_id),
            "graph add node should stage index work until commit"
        );

        txn.abort();

        assert!(
            !state.pending_ops.contains_key(&txn_id),
            "abort should clear staged graph work via engine observers"
        );
    }

    #[test]
    fn graph_refresh_indexes_follower_nodes_by_graph_semantics() {
        let path = unique_test_dir("engine-graph-follower-refresh");

        {
            let primary = Database::open_runtime(
                OpenSpec::primary(&path)
                    .with_subsystem(SearchSubsystem)
                    .with_subsystem(crate::graph::GraphSubsystem),
            )
            .unwrap();
            let follower = Database::open_runtime(
                OpenSpec::follower(&path)
                    .with_subsystem(SearchSubsystem)
                    .with_subsystem(crate::graph::GraphSubsystem),
            )
            .unwrap();

            let branch_id = BranchId::new();
            let space = "tenant_a";
            let graph = "papers";
            let node_id = "quasar-node";

            let primary_graph = crate::graph::GraphStore::new(primary.clone());
            let follower_graph = crate::graph::GraphStore::new(follower.clone());

            primary_graph
                .create_graph(branch_id, space, graph, None)
                .unwrap();
            primary_graph
                .add_node(
                    branch_id,
                    space,
                    graph,
                    node_id,
                    NodeData {
                        entity_ref: None,
                        object_type: Some("paper".to_string()),
                        properties: Some(serde_json::json!({
                            "title": "baseline indexing"
                        })),
                    },
                )
                .unwrap();
            primary.flush().unwrap();

            let req = SearchRequest::new(branch_id, "quasar").with_space(space);
            assert!(
                follower_graph.search(&req).unwrap().hits.is_empty(),
                "follower search should remain stale before refresh"
            );

            let create_outcome = follower.refresh();
            assert!(
                matches!(
                    create_outcome,
                    RefreshOutcome::CaughtUp { applied, .. } if applied >= 1
                ),
                "expected follower refresh to apply graph records, got {create_outcome:?}"
            );

            let after_create = follower_graph.search(&req).unwrap();
            assert_eq!(after_create.hits.len(), 1);
            assert!(matches!(
                &after_create.hits[0].doc_ref,
                EntityRef::Graph { key, .. } if key == "papers/n/quasar-node"
            ));

            primary_graph
                .remove_node(branch_id, space, graph, node_id)
                .unwrap();
            primary.flush().unwrap();

            let delete_outcome = follower.refresh();
            assert!(
                matches!(
                    delete_outcome,
                    RefreshOutcome::CaughtUp { applied, .. } if applied >= 1
                ),
                "expected follower refresh to deindex deleted graph node, got {delete_outcome:?}"
            );
            assert!(
                follower_graph.search(&req).unwrap().hits.is_empty(),
                "deleted graph node should be removed from follower search index"
            );
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn graph_refresh_does_not_leak_before_visibility_advance() {
        let path = unique_test_dir("engine-graph-follower-blocked");
        let branch_id = BranchId::new();
        let space = "tenant_a";
        let fail_once = Arc::new(AtomicBool::new(false));

        let primary = Database::open_runtime(
            OpenSpec::primary(&path)
                .with_subsystem(SearchSubsystem)
                .with_subsystem(crate::graph::GraphSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        let follower = Database::open_runtime(
            OpenSpec::follower(&path)
                .with_subsystem(SearchSubsystem)
                .with_subsystem(crate::graph::GraphSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();

        let primary_graph = crate::graph::GraphStore::new(primary.clone());
        let follower_graph = crate::graph::GraphStore::new(follower.clone());
        primary_graph
            .create_graph(branch_id, space, "papers", None)
            .unwrap();
        primary.flush().unwrap();

        fail_once.store(true, Ordering::SeqCst);
        primary_graph
            .add_node(
                branch_id,
                space,
                "papers",
                "quasar-node",
                NodeData {
                    entity_ref: None,
                    object_type: Some("paper".to_string()),
                    properties: Some(serde_json::json!({
                        "title": "blocked graph refresh"
                    })),
                },
            )
            .unwrap();
        primary.flush().unwrap();

        let outcome = follower.refresh();
        assert!(
            matches!(outcome, RefreshOutcome::Stuck { .. }),
            "graph refresh should block after staging graph index work"
        );

        let req = SearchRequest::new(branch_id, "quasar").with_space(space);
        assert!(
            follower_graph.search(&req).unwrap().hits.is_empty(),
            "graph search must not expose blocked staged index updates"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn graph_restart_keeps_blocked_search_state_clamped() {
        let path = unique_test_dir("engine-graph-follower-restart-blocked");
        let branch_id = BranchId::new();
        let space = "tenant_a";
        let fail_once = Arc::new(AtomicBool::new(false));

        let primary = Database::open_runtime(
            OpenSpec::primary(&path)
                .with_subsystem(SearchSubsystem)
                .with_subsystem(crate::graph::GraphSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        let follower = Database::open_runtime(
            OpenSpec::follower(&path)
                .with_subsystem(SearchSubsystem)
                .with_subsystem(crate::graph::GraphSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();

        let primary_graph = crate::graph::GraphStore::new(primary.clone());
        let follower_graph = crate::graph::GraphStore::new(follower.clone());
        primary_graph
            .create_graph(branch_id, space, "papers", None)
            .unwrap();
        primary.flush().unwrap();
        let _ = follower.refresh();

        fail_once.store(true, Ordering::SeqCst);
        primary_graph
            .add_node(
                branch_id,
                space,
                "papers",
                "restart-quasar",
                NodeData {
                    entity_ref: None,
                    object_type: Some("paper".to_string()),
                    properties: Some(serde_json::json!({
                        "title": "restart quasar"
                    })),
                },
            )
            .unwrap();
        primary.flush().unwrap();

        assert!(
            matches!(follower.refresh(), RefreshOutcome::Stuck { .. }),
            "graph refresh should block before publishing the node"
        );

        let req = SearchRequest::new(branch_id, "quasar").with_space(space);
        assert!(
            follower_graph.search(&req).unwrap().hits.is_empty(),
            "blocked graph node must remain invisible before restart"
        );

        drop(follower);
        primary.shutdown().unwrap();
        drop(primary);

        let reopened = Database::open_runtime(
            OpenSpec::follower(&path)
                .with_subsystem(SearchSubsystem)
                .with_subsystem(crate::graph::GraphSubsystem)
                .with_subsystem(FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(
                    false,
                )))),
        )
        .unwrap();
        let reopened_graph = crate::graph::GraphStore::new(reopened);
        assert!(
            reopened_graph.search(&req).unwrap().hits.is_empty(),
            "follower reopen must not load blocked graph docs from primary caches"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn graph_search_recovery_rebuilds_node_id_text_on_slow_path() {
        let path = unique_test_dir("engine-graph-recovery-slow");
        let branch_id = BranchId::new();
        let space = "tenant_a";

        {
            let db = Database::open_runtime(
                OpenSpec::primary(&path)
                    .with_subsystem(crate::graph::GraphSubsystem)
                    .with_subsystem(SearchSubsystem),
            )
            .unwrap();
            let graph = crate::graph::GraphStore::new(db.clone());

            graph
                .create_graph(branch_id, space, "papers", None)
                .unwrap();
            graph
                .add_node(
                    branch_id,
                    space,
                    "papers",
                    "quasar-node",
                    NodeData {
                        entity_ref: None,
                        object_type: Some("paper".to_string()),
                        properties: Some(serde_json::json!({
                            "title": "baseline indexing"
                        })),
                    },
                )
                .unwrap();
            db.flush().unwrap();
        }

        let _ = std::fs::remove_dir_all(path.join("search"));

        {
            let db = Database::open_runtime(
                OpenSpec::primary(&path)
                    .with_subsystem(crate::graph::GraphSubsystem)
                    .with_subsystem(SearchSubsystem),
            )
            .unwrap();
            let graph = crate::graph::GraphStore::new(db);
            let req = SearchRequest::new(branch_id, "quasar").with_space(space);
            let response = graph.search(&req).unwrap();
            assert_eq!(response.hits.len(), 1);
            assert!(matches!(
                &response.hits[0].doc_ref,
                EntityRef::Graph { key, .. } if key == "papers/n/quasar-node"
            ));
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn graph_search_recovery_fast_path_prunes_stale_graph_docs() {
        let path = unique_test_dir("engine-graph-recovery-fast");
        let branch_id = BranchId::new();
        let space = "tenant_a";

        {
            let db = Database::open_runtime(
                OpenSpec::primary(&path)
                    .with_subsystem(crate::graph::GraphSubsystem)
                    .with_subsystem(SearchSubsystem),
            )
            .unwrap();
            let graph = crate::graph::GraphStore::new(db.clone());
            graph
                .create_graph(branch_id, space, "papers", None)
                .unwrap();
            graph
                .add_node(
                    branch_id,
                    space,
                    "papers",
                    "real-node",
                    NodeData {
                        entity_ref: None,
                        object_type: Some("paper".to_string()),
                        properties: Some(serde_json::json!({
                            "title": "baseline indexing"
                        })),
                    },
                )
                .unwrap();

            let index = db.extension::<crate::InvertedIndex>().unwrap();
            index.index_document(
                &EntityRef::Graph {
                    branch_id,
                    space: space.to_string(),
                    key: "papers/n/ghost-node".to_string(),
                },
                "ghostterm",
                None,
            );
            db.flush().unwrap();

            let stale_req = SearchRequest::new(branch_id, "ghostterm").with_space(space);
            assert_eq!(graph.search(&stale_req).unwrap().hits.len(), 1);
        }

        {
            let db = Database::open_runtime(
                OpenSpec::primary(&path)
                    .with_subsystem(crate::graph::GraphSubsystem)
                    .with_subsystem(SearchSubsystem),
            )
            .unwrap();
            let graph = crate::graph::GraphStore::new(db);

            let stale_req = SearchRequest::new(branch_id, "ghostterm").with_space(space);
            assert!(
                graph.search(&stale_req).unwrap().hits.is_empty(),
                "fast-path recovery should remove graph docs that are not present in storage"
            );

            let real_req = SearchRequest::new(branch_id, "real").with_space(space);
            let real_response = graph.search(&real_req).unwrap();
            assert_eq!(real_response.hits.len(), 1);
            assert!(matches!(
                &real_response.hits[0].doc_ref,
                EntityRef::Graph { key, .. } if key == "papers/n/real-node"
            ));
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn graph_search_recovery_fast_path_is_noop_when_storage_is_unchanged() {
        let path = unique_test_dir("engine-graph-recovery-fast-noop");
        let branch_id = BranchId::new();
        let space = "tenant_a";
        let manifest_path = path.join("search").join("search.manifest");

        {
            let db = Database::open_runtime(
                OpenSpec::primary(&path)
                    .with_subsystem(crate::graph::GraphSubsystem)
                    .with_subsystem(SearchSubsystem),
            )
            .unwrap();
            let graph = crate::graph::GraphStore::new(db.clone());
            graph
                .create_graph(branch_id, space, "papers", None)
                .unwrap();
            graph
                .add_node(
                    branch_id,
                    space,
                    "papers",
                    "quasar-node",
                    NodeData {
                        entity_ref: None,
                        object_type: Some("paper".to_string()),
                        properties: Some(serde_json::json!({
                            "title": "baseline indexing"
                        })),
                    },
                )
                .unwrap();
            db.flush().unwrap();
        }

        let modified_before = std::fs::metadata(&manifest_path)
            .unwrap()
            .modified()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let reopened = Database::open_runtime(
            OpenSpec::primary(&path)
                .with_subsystem(crate::graph::GraphSubsystem)
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        let graph = crate::graph::GraphStore::new(reopened);
        let modified_after = std::fs::metadata(&manifest_path)
            .unwrap()
            .modified()
            .unwrap();

        let req = SearchRequest::new(branch_id, "quasar").with_space(space);
        assert_eq!(graph.search(&req).unwrap().hits.len(), 1);
        assert_eq!(
            modified_after, modified_before,
            "fast-path recovery should not rewrite the manifest when graph storage is unchanged"
        );

        let _ = std::fs::remove_dir_all(path);
    }
}
