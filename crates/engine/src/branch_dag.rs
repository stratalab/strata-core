//! Branch DAG infrastructure for the `_system_` branch.
//!
//! Reserves the `_system_` namespace, auto-creates the system branch and
//! `_branch_dag` graph on database init, and provides helpers for seeding
//! default branch nodes.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::database::Database;
use crate::graph::types::NodeData;
use crate::graph::GraphStore;
use crate::primitives::branch::{resolve_branch_name, BranchIndex};

/// Reserved branch name for system-internal data.
pub const SYSTEM_BRANCH: &str = "_system_";

/// Graph name for the branch DAG on the `_system_` branch.
pub const BRANCH_DAG_GRAPH: &str = "_branch_dag";

/// Status of a branch in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DagBranchStatus {
    /// Branch is active and writable.
    Active,
    /// Branch is archived (read-only).
    Archived,
}

/// Returns `true` if the given name starts with the reserved `_system` prefix.
pub fn is_system_branch(name: &str) -> bool {
    name.starts_with("_system")
}

/// Ensure the `_system_` branch exists in BranchIndex.
fn ensure_system_branch(db: &Arc<Database>) -> Result<(), String> {
    let branch_index = BranchIndex::new(db.clone());
    match branch_index.exists(SYSTEM_BRANCH) {
        Ok(true) => Ok(()),
        Ok(false) => branch_index
            .create_branch(SYSTEM_BRANCH)
            .map(|_| ())
            .map_err(|e| format!("failed to create _system_ branch: {e}")),
        Err(e) => Err(format!("failed to check _system_ branch: {e}")),
    }
}

/// Ensure the `_branch_dag` graph exists on the `_system_` branch.
fn ensure_branch_dag(db: &Arc<Database>) -> Result<(), String> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    match graph_store.get_graph_meta(branch_id, BRANCH_DAG_GRAPH) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => graph_store
            .create_graph(branch_id, BRANCH_DAG_GRAPH, None)
            .map_err(|e| format!("failed to create _branch_dag graph: {e}")),
        Err(e) => Err(format!("failed to check _branch_dag graph: {e}")),
    }
}

/// Seed a "default" branch node in the DAG if not already present.
fn seed_default_branch(db: &Arc<Database>) -> Result<(), String> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    match graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, "default") {
        Ok(Some(_)) => Ok(()),
        Ok(None) => {
            let props = serde_json::json!({
                "status": "active",
                "created_by": "system",
            });
            let node = NodeData {
                entity_ref: None,
                properties: Some(props),
                object_type: Some("branch".to_string()),
            };
            graph_store
                .add_node(branch_id, BRANCH_DAG_GRAPH, "default", node)
                .map(|_| ())
                .map_err(|e| format!("failed to seed default branch node: {e}"))
        }
        Err(e) => Err(format!("failed to check default branch node: {e}")),
    }
}

/// Initialise system branch infrastructure on database open.
///
/// Creates the `_system_` branch, the `_branch_dag` graph, and seeds
/// the "default" branch node. All steps are best-effort — failures are
/// logged but do not prevent the database from opening.
pub fn init_system_branch(db: &Arc<Database>) {
    if let Err(e) = ensure_system_branch(db) {
        warn!("system branch init: {e}");
        return;
    }
    if let Err(e) = ensure_branch_dag(db) {
        warn!("system branch init: {e}");
        return;
    }
    if let Err(e) = seed_default_branch(db) {
        warn!("system branch init: {e}");
    }
}

/// Populate the branch status cache from the DAG (read-only, for followers).
///
/// Followers cannot write, so this only reads existing DAG state into the
/// in-memory cache. If the `_system_` branch or `_branch_dag` graph
/// doesn't exist yet, this is a no-op.
pub fn load_status_cache_readonly(db: &Arc<Database>) {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // If the graph doesn't exist yet, nothing to load.
    match graph_store.get_graph_meta(branch_id, BRANCH_DAG_GRAPH) {
        Ok(Some(_)) => {}
        _ => return,
    }

    // Best-effort: populate the cache from existing nodes.
    if let Ok(cache) = db.extension::<crate::branch_status_cache::BranchStatusCache>() {
        // Read the "default" node to seed cache
        if let Ok(Some(node)) = graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, "default") {
            let status = status_from_node_props(&node);
            cache.set("default".to_string(), status);
        }
    }
}

/// Extract `DagBranchStatus` from node properties.
fn status_from_node_props(node: &NodeData) -> DagBranchStatus {
    if let Some(props) = &node.properties {
        if let Some(s) = props.get("status").and_then(|v| v.as_str()) {
            if s == "archived" {
                return DagBranchStatus::Archived;
            }
        }
    }
    DagBranchStatus::Active
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Database::cache() already calls init_system_branch() internally,
    // so these tests verify state after automatic init (and idempotency of
    // additional explicit calls).

    #[test]
    fn system_branch_auto_created() {
        // Database::cache() calls init_system_branch automatically
        let db = Database::cache().unwrap();
        let branch_index = BranchIndex::new(db.clone());
        assert!(branch_index.exists(SYSTEM_BRANCH).unwrap());
    }

    #[test]
    fn branch_dag_graph_exists() {
        let db = Database::cache().unwrap();
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let meta = graph_store
            .get_graph_meta(branch_id, BRANCH_DAG_GRAPH)
            .unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn default_branch_seeded() {
        let db = Database::cache().unwrap();
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, "default")
            .unwrap();
        assert!(node.is_some());
        let node = node.unwrap();
        assert_eq!(node.object_type.as_deref(), Some("branch"));
        // Verify properties are populated
        let props = node.properties.as_ref().unwrap();
        assert_eq!(props.get("status").and_then(|v| v.as_str()), Some("active"));
    }

    #[test]
    fn init_is_idempotent() {
        let db = Database::cache().unwrap();
        // cache() already called init once; call again explicitly
        init_system_branch(&db);
        let branch_index = BranchIndex::new(db.clone());
        assert!(branch_index.exists(SYSTEM_BRANCH).unwrap());
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, "default")
            .unwrap();
        assert!(node.is_some());
    }

    #[test]
    fn status_from_node_props_extracts_correctly() {
        // Active (explicit)
        let node = NodeData {
            properties: Some(serde_json::json!({"status": "active"})),
            ..Default::default()
        };
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Active);

        // Archived
        let node = NodeData {
            properties: Some(serde_json::json!({"status": "archived"})),
            ..Default::default()
        };
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Archived);

        // Missing status field → defaults to Active
        let node = NodeData {
            properties: Some(serde_json::json!({"other": "field"})),
            ..Default::default()
        };
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Active);

        // No properties at all → defaults to Active
        let node = NodeData::default();
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Active);
    }

    #[test]
    fn is_system_branch_detection() {
        assert!(is_system_branch("_system_"));
        assert!(is_system_branch("_system_foo"));
        assert!(!is_system_branch("default"));
        assert!(!is_system_branch("my_branch"));
    }
}
