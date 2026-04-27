//! Shared helpers for branching lifecycle coverage.
//!
//! Synthesize lifecycle states no production path produces (most notably
//! `Archived`), capture branch-op observer output, and snapshot the
//! lineage surface so tests can assert "no side effects on reject".
//!
//! These helpers stay next to the tests because the canonical
//! `BranchControlStore` is `pub(crate)`. The engine exposes a small
//! feature-gated `test-support` surface for lifecycle synthesis and
//! lineage probes; production builds do not ship those accessors.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use strata_core::BranchId;
use strata_engine::database::{BranchOpEvent, BranchOpObserver, ObserverError};
use strata_engine::primitives::branch::resolve_branch_name;
use strata_engine::{BranchLifecycleStatus, BranchRef, Database};
use strata_graph::branch_dag::dag_branch_node_id_for_ref;
use strata_graph::keys::{validate_node_id, GRAPH_SPACE};
use strata_graph::types::NodeData;
use strata_graph::{GraphStore, BRANCH_DAG_GRAPH, SYSTEM_BRANCH};

// =============================================================================
// Lifecycle synthesis
// =============================================================================

/// Flip the live control record of `name` to `Archived`.
///
/// Panics if the branch has no live record. Callers must create the
/// branch via `db.branches().create(name)` (or fork it) first.
pub(crate) fn archive_branch_for_test(db: &Arc<Database>, name: &str) -> BranchRef {
    db.branches()
        .set_lifecycle_for_test(name, BranchLifecycleStatus::Archived)
        .unwrap_or_else(|e| panic!("archive_branch_for_test({name}): {e:?}"))
}

/// Flip the live control record of `name` to `status`.
///
/// Wraps `BranchService::set_lifecycle_for_test` so tests can synthesize
/// any lifecycle state (`Active`, `Archived`, `Deleted`) without going
/// through the production delete path (which also purges legacy
/// metadata). Returns the affected `BranchRef`.
pub(crate) fn set_lifecycle_for_test(
    db: &Arc<Database>,
    name: &str,
    status: BranchLifecycleStatus,
) -> BranchRef {
    db.branches()
        .set_lifecycle_for_test(name, status)
        .unwrap_or_else(|e| panic!("set_lifecycle_for_test({name}, {status:?}): {e:?}"))
}

// =============================================================================
// Observer capture
// =============================================================================

/// Capture every `BranchOpEvent` the engine fires. Register with
/// `db.branch_op_observers().register(capturing.clone())` before the
/// workload under test.
pub(crate) struct CapturingBranchObserver {
    events: Mutex<Vec<BranchOpEvent>>,
}

impl CapturingBranchObserver {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }

    /// Current number of captured events. Cheap — snapshot only.
    pub(crate) fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Clone of every captured event so far.
    pub(crate) fn snapshot(&self) -> Vec<BranchOpEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl BranchOpObserver for CapturingBranchObserver {
    fn name(&self) -> &'static str {
        "branching_lifecycle::CapturingBranchObserver"
    }

    fn on_branch_op(&self, event: &BranchOpEvent) -> Result<(), ObserverError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

// =============================================================================
// Side-effect checkpoint
// =============================================================================

/// Snapshot of the observable reject-path side-effect surface:
/// observer events, lineage edges for the tracked lifecycle instance,
/// the tracked branch-node payloads in `_branch_dag`, and total
/// `_branch_dag` node/edge counts.
///
/// Taken before a gate-refused mutation and re-compared after — any drift
/// means the reject path leaked a side effect.
#[derive(Debug, Clone)]
pub(crate) struct BranchSideEffectCheckpoint {
    pub(crate) target_name: String,
    pub(crate) target_ref: Option<BranchRef>,
    pub(crate) observer_count: usize,
    pub(crate) lineage_edge_count: usize,
    pub(crate) canonical_branch_node: Option<NodeData>,
    pub(crate) legacy_branch_node: Option<NodeData>,
    pub(crate) dag_node_count: usize,
    pub(crate) dag_edge_count: usize,
}

impl BranchSideEffectCheckpoint {
    pub(crate) fn capture(
        db: &Arc<Database>,
        observer: &CapturingBranchObserver,
        target_name: &str,
        target_ref: Option<BranchRef>,
    ) -> Self {
        let lineage_edge_count = target_ref
            .map(|branch_ref| {
                db.branches()
                    .lineage_edge_count_for_branch_ref_for_test(branch_ref)
                    .unwrap_or_else(|e| {
                        panic!(
                            "lineage_edge_count_for_branch_ref_for_test({branch_ref:?}) failed while checkpointing {target_name}: {e:?}"
                        )
                    })
            })
            .unwrap_or(0);
        let graph_store = GraphStore::new(db.clone());
        let system_branch = BranchId::from_user_name(SYSTEM_BRANCH);
        let canonical_branch_node = target_ref
            .and_then(|branch_ref| {
                graph_store
                    .get_node(
                        system_branch,
                        GRAPH_SPACE,
                        BRANCH_DAG_GRAPH,
                        &dag_branch_node_id_for_ref(branch_ref),
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "get_node(canonical branch DAG node for {branch_ref:?}) failed while checkpointing {target_name}: {e:?}"
                        )
                    })
            });
        let legacy_branch_node_id = if validate_node_id(target_name).is_ok() {
            target_name.to_string()
        } else {
            format!("_branch_{}", resolve_branch_name(target_name))
        };
        let legacy_branch_node = graph_store
            .get_node(
                system_branch,
                GRAPH_SPACE,
                BRANCH_DAG_GRAPH,
                &legacy_branch_node_id,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "get_node(legacy branch DAG node for {target_name}) failed while checkpointing: {e:?}"
                )
            });
        let dag_stats = graph_store
            .snapshot_stats(
                system_branch,
                GRAPH_SPACE,
                BRANCH_DAG_GRAPH,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "snapshot_stats(_system_/_branch_dag) failed while checkpointing {target_name}: {e:?}"
                )
            });
        Self {
            target_name: target_name.to_string(),
            target_ref,
            observer_count: observer.count(),
            lineage_edge_count,
            canonical_branch_node,
            legacy_branch_node,
            dag_node_count: dag_stats.node_count,
            dag_edge_count: dag_stats.edge_count,
        }
    }
}

/// Assert `(observer events, lineage edges, tracked DAG branch nodes,
/// DAG stats)` did not change since `before`. Panics with a diagnostic
/// when the reject path leaked a side effect.
pub(crate) fn assert_no_side_effects_since(
    db: &Arc<Database>,
    observer: &CapturingBranchObserver,
    before: &BranchSideEffectCheckpoint,
    scenario: &str,
) {
    let after =
        BranchSideEffectCheckpoint::capture(db, observer, &before.target_name, before.target_ref);
    assert_eq!(
        after.observer_count, before.observer_count,
        "[{scenario}] BranchOpObserver fired on a lifecycle-refused mutation; expected no side effect (target={})",
        before.target_name
    );
    assert_eq!(
        after.lineage_edge_count, before.lineage_edge_count,
        "[{scenario}] Lineage edge appended on a lifecycle-refused mutation; expected no side effect (target={})",
        before.target_name
    );
    assert_eq!(
        after.canonical_branch_node, before.canonical_branch_node,
        "[{scenario}] Canonical _branch_dag branch node mutated on a lifecycle-refused mutation; expected no DAG side effect (target={})",
        before.target_name
    );
    assert_eq!(
        after.legacy_branch_node, before.legacy_branch_node,
        "[{scenario}] Legacy/name-keyed _branch_dag branch node mutated on a lifecycle-refused mutation; expected no DAG side effect (target={})",
        before.target_name
    );
    assert_eq!(
        after.dag_node_count, before.dag_node_count,
        "[{scenario}] _branch_dag node count changed on a lifecycle-refused mutation; expected no DAG event (target={})",
        before.target_name
    );
    assert_eq!(
        after.dag_edge_count, before.dag_edge_count,
        "[{scenario}] _branch_dag edge count changed on a lifecycle-refused mutation; expected no DAG event (target={})",
        before.target_name
    );
}
