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
//! by `GraphCommitObserver`.
//!
//! ## Thread Safety
//!
//! Uses `DashMap` for per-transaction pending ops, allowing concurrent
//! transactions to queue operations without blocking each other.

use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use strata_core::id::TxnId;
use strata_core::types::BranchId;

use crate::types::NodeData;

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
    /// transaction's ops. On abort or failed commit, the Session clears
    /// that transaction's queued ops without touching other in-flight writers.
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
    pub fn apply_pending_ops(&self, txn_id: TxnId, graph_store: &crate::GraphStore) -> usize {
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
    /// Called by Session on transaction abort or commit failure to clean up
    /// uncommitted ops.
    pub fn clear_pending_ops(&self, txn_id: TxnId) {
        self.pending_ops.remove(&txn_id);
    }
}

/// Apply a single staged graph operation.
fn apply_staged_graph_op(graph_store: &crate::GraphStore, op: StagedGraphOp) {
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
// Commit and Replay Observers
// =============================================================================

use std::sync::Weak;
use strata_engine::database::observers::{CommitInfo, CommitObserver, ObserverError};
use strata_engine::Database;

/// Ensure runtime hooks are wired for this database instance.
///
/// This is called lazily from `GraphStore::state()`. It registers the
/// commit and replay observers if they haven't been registered yet.
pub(crate) fn ensure_runtime_wiring(db: &Arc<Database>, state: &Arc<GraphBackendState>) {
    use std::sync::atomic::Ordering;

    if state.runtime_wired.swap(true, Ordering::AcqRel) {
        return;
    }

    let commit_observer = Arc::new(GraphCommitObserver {
        db: Arc::downgrade(db),
    });
    db.commit_observers().register(commit_observer);

    let replay_observer = Arc::new(GraphReplayObserver {
        db: Arc::downgrade(db),
    });
    db.replay_observers().register(replay_observer);
}

/// Observer that applies pending graph index operations after commit.
///
/// Graph write methods queue `StagedGraphOp`s in `GraphBackendState` during
/// transactions. This observer applies them after successful commit, updating
/// the BM25 inverted index.
///
/// This moves graph index maintenance ownership from executor (Session's
/// `PostCommitOp::GraphIndexNode/GraphDeindexNode`) to subsystem
/// (GraphSubsystem), fulfilling the T2-E5 requirement.
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
            let graph_store = crate::GraphStore::new(db.clone());
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

/// Observer that updates graph search index during follower replay.
///
/// When a follower replays WAL records, it needs to update the BM25 index
/// for any graph nodes that were added or removed.
struct GraphReplayObserver {
    db: Weak<Database>,
}

use strata_engine::database::observers::{ReplayInfo, ReplayObserver};

impl ReplayObserver for GraphReplayObserver {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn on_replay(&self, info: &ReplayInfo) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };

        // Apply graph index updates for replayed graph node changes.
        apply_replayed_graph_changes(&db, &info.puts, &info.deleted_values);

        Ok(())
    }
}

/// Apply graph index updates from replayed WAL records.
///
/// Scans the puts and deletes for graph node keys and updates the BM25 index.
/// Graph nodes are identified by their key format: `{graph}/n/{node_id}`.
/// Nodes can be in ANY space (user-defined or system `_graph_`).
fn apply_replayed_graph_changes(
    db: &Arc<Database>,
    puts: &[(strata_core::types::Key, strata_core::value::Value)],
    deleted_values: &[(strata_core::types::Key, strata_core::value::Value)],
) {
    use strata_core::types::TypeTag;
    use strata_core::value::Value;

    let graph_store = crate::GraphStore::new(db.clone());

    // Index new/updated graph nodes
    for (key, value) in puts {
        // Graph nodes are stored with TypeTag::KV with key format: "{graph}/n/{node_id}"
        if key.type_tag != TypeTag::KV {
            continue;
        }

        let user_key = match key.user_key_string() {
            Some(s) => s,
            None => continue,
        };

        // Parse key format: "{graph}/n/{node_id}"
        // Only process keys that match the graph node pattern.
        let parts: Vec<&str> = user_key.splitn(3, '/').collect();
        if parts.len() != 3 || parts[1] != "n" {
            continue;
        }
        let graph = parts[0];
        let node_id = parts[2];

        // Deserialize node data
        let bytes = match value {
            Value::Bytes(b) => b,
            _ => continue,
        };
        let data: crate::types::NodeData = match serde_json::from_slice(bytes) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let branch_id = key.namespace.branch_id;
        let space = key.namespace.space.as_str();
        graph_store.index_node_for_search(branch_id, space, graph, node_id, &data);
    }

    // Deindex deleted graph nodes
    for (key, _value) in deleted_values {
        if key.type_tag != TypeTag::KV {
            continue;
        }

        let user_key = match key.user_key_string() {
            Some(s) => s,
            None => continue,
        };

        // Parse key format: "{graph}/n/{node_id}"
        let parts: Vec<&str> = user_key.splitn(3, '/').collect();
        if parts.len() != 3 || parts[1] != "n" {
            continue;
        }
        let graph = parts[0];
        let node_id = parts[2];

        let branch_id = key.namespace.branch_id;
        let space = key.namespace.space.as_str();
        graph_store.deindex_node_for_search(branch_id, space, graph, node_id);
    }
}
