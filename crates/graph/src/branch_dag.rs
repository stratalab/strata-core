//! Branch DAG infrastructure for the `_system_` branch.
//!
//! Reserves the `_system_` namespace, auto-creates the system branch and
//! `_branch_dag` graph on database init, seeds the "default" branch node,
//! and records branch lifecycle events (creation, forks, merges, deletion)
//! as nodes and edges in the DAG.
//!
//! Write helpers (`dag_add_branch`, `dag_record_fork`, `dag_record_merge`,
//! `dag_set_status`, `dag_mark_deleted`, `dag_set_message`) are called
//! best-effort from executor handlers — failures are logged, never propagated.
//!
//! Read helpers (`dag_get_status`, `dag_get_branch_info`, `find_children`)
//! assemble branch lineage from the graph.

pub use strata_core::branch_dag::*;

use std::sync::Arc;

use strata_core::contract::Timestamp;
use strata_core::{StrataError, StrataResult};
use tracing::warn;

use crate::types::{Direction, EdgeData, NodeData};
use crate::GraphStore;
use strata_engine::primitives::branch::{resolve_branch_name, BranchIndex};
use strata_engine::Database;

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
/// the "default" branch node. All steps are best-effort -- failures are
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
        if let Ok(nodes) = graph_store.list_nodes(branch_id, BRANCH_DAG_GRAPH) {
            for node_id in nodes {
                if let Ok(Some(node)) = graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, &node_id)
                {
                    if node.object_type.as_deref() == Some("branch") {
                        let status = status_from_node_props(&node);
                        cache.set(node_id, status);
                    }
                }
            }
        }
    }
}

// =========================================================================
// DAG Write Helpers
// =========================================================================

/// Add a branch node to the DAG.
pub fn dag_add_branch(
    db: &Arc<Database>,
    name: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "status": "active",
        "created_at": now,
        "updated_at": now,
    });
    if let Some(msg) = message {
        props["message"] = serde_json::json!(msg);
    }
    if let Some(cr) = creator {
        props["creator"] = serde_json::json!(cr);
    }

    let node = NodeData {
        entity_ref: None,
        properties: Some(props),
        object_type: Some("branch".to_string()),
    };
    graph_store.add_node(branch_id, BRANCH_DAG_GRAPH, name, node)?;
    Ok(())
}

/// Ensure a branch node exists in the DAG (idempotent).
fn ensure_branch_node_exists(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    if graph_store
        .get_node(branch_id, BRANCH_DAG_GRAPH, name)?
        .is_some()
    {
        return Ok(());
    }
    dag_add_branch(db, name, None, None)
}

/// Record a fork event in the DAG.
pub fn dag_record_fork(
    db: &Arc<Database>,
    parent: &str,
    child: &str,
    fork_version: Option<u64>,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    ensure_branch_node_exists(db, parent)?;
    ensure_branch_node_exists(db, child)?;

    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let event_id = DagEventId::new_fork();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
    });
    if let Some(fv) = fork_version {
        props["fork_version"] = serde_json::json!(fv);
    }
    if let Some(msg) = message {
        props["message"] = serde_json::json!(msg);
    }
    if let Some(cr) = creator {
        props["creator"] = serde_json::json!(cr);
    }

    let node = NodeData {
        entity_ref: None,
        properties: Some(props),
        object_type: Some("fork".to_string()),
    };
    graph_store.add_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str(), node)?;

    // parent --[parent]--> fork_event
    graph_store.add_edge(
        branch_id,
        BRANCH_DAG_GRAPH,
        parent,
        event_id.as_str(),
        "parent",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;
    // fork_event --[child]--> child
    graph_store.add_edge(
        branch_id,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        child,
        "child",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;

    Ok(event_id)
}

/// Record a merge event in the DAG.
pub fn dag_record_merge(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    merge_info: &strata_engine::branch_ops::MergeInfo,
    strategy: Option<&str>,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    ensure_branch_node_exists(db, source)?;
    ensure_branch_node_exists(db, target)?;

    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let event_id = DagEventId::new_merge();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
        "keys_applied": merge_info.keys_applied,
        "spaces_merged": merge_info.spaces_merged,
        "conflicts": merge_info.conflicts.len() as u64,
    });
    if let Some(mv) = merge_info.merge_version {
        props["merge_version"] = serde_json::json!(mv);
    }
    if let Some(s) = strategy {
        props["strategy"] = serde_json::json!(s);
    }
    if let Some(msg) = message {
        props["message"] = serde_json::json!(msg);
    }
    if let Some(cr) = creator {
        props["creator"] = serde_json::json!(cr);
    }

    let node = NodeData {
        entity_ref: None,
        properties: Some(props),
        object_type: Some("merge".to_string()),
    };
    graph_store.add_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str(), node)?;

    // source --[source]--> merge_event
    graph_store.add_edge(
        branch_id,
        BRANCH_DAG_GRAPH,
        source,
        event_id.as_str(),
        "source",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;
    // merge_event --[target]--> target
    graph_store.add_edge(
        branch_id,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        target,
        "target",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;

    Ok(event_id)
}

/// Set the status of a branch in the DAG.
pub fn dag_set_status(db: &Arc<Database>, name: &str, status: DagBranchStatus) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let node = graph_store
        .get_node(branch_id, BRANCH_DAG_GRAPH, name)?
        .ok_or_else(|| StrataError::internal(format!("branch node not found in DAG: {name}")))?;

    let mut props = node.properties.unwrap_or_else(|| serde_json::json!({}));
    props["status"] = serde_json::json!(status.as_str());
    props["updated_at"] = serde_json::json!(Timestamp::now().as_micros());

    let updated = NodeData {
        entity_ref: node.entity_ref,
        properties: Some(props),
        object_type: node.object_type,
    };
    graph_store.add_node(branch_id, BRANCH_DAG_GRAPH, name, updated)?;
    Ok(())
}

/// Mark a branch as deleted in the DAG. No-op if node doesn't exist.
pub fn dag_mark_deleted(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let node = match graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        Some(n) => n,
        None => return Ok(()),
    };

    let mut props = node.properties.unwrap_or_else(|| serde_json::json!({}));
    let now = Timestamp::now().as_micros();
    props["status"] = serde_json::json!("deleted");
    props["updated_at"] = serde_json::json!(now);
    props["deleted_at"] = serde_json::json!(now);

    let updated = NodeData {
        entity_ref: node.entity_ref,
        properties: Some(props),
        object_type: node.object_type,
    };
    graph_store.add_node(branch_id, BRANCH_DAG_GRAPH, name, updated)?;
    Ok(())
}

/// Set the message on a branch node in the DAG.
pub fn dag_set_message(db: &Arc<Database>, name: &str, message: &str) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let node = graph_store
        .get_node(branch_id, BRANCH_DAG_GRAPH, name)?
        .ok_or_else(|| StrataError::internal(format!("branch node not found in DAG: {name}")))?;

    let mut props = node.properties.unwrap_or_else(|| serde_json::json!({}));
    props["message"] = serde_json::json!(message);
    props["updated_at"] = serde_json::json!(Timestamp::now().as_micros());

    let updated = NodeData {
        entity_ref: node.entity_ref,
        properties: Some(props),
        object_type: node.object_type,
    };
    graph_store.add_node(branch_id, BRANCH_DAG_GRAPH, name, updated)?;
    Ok(())
}

// =========================================================================
// DAG Read Helpers
// =========================================================================

/// Get the status of a branch from the DAG.
pub fn dag_get_status(db: &Arc<Database>, name: &str) -> StrataResult<DagBranchStatus> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    match graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        Some(node) => Ok(status_from_node_props(&node)),
        None => Ok(DagBranchStatus::Active),
    }
}

/// Get full branch info from the DAG.
pub fn dag_get_branch_info(db: &Arc<Database>, name: &str) -> StrataResult<Option<DagBranchInfo>> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    let node = match graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        Some(n) => n,
        None => return Ok(None),
    };

    let props = node.properties.as_ref();
    let status = status_from_node_props(&node);
    let created_at = props
        .and_then(|p| p.get("created_at"))
        .and_then(|v| v.as_u64());
    let updated_at = props
        .and_then(|p| p.get("updated_at"))
        .and_then(|v| v.as_u64());
    let message = props
        .and_then(|p| p.get("message"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let creator = props
        .and_then(|p| p.get("creator"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let forked_from = find_fork_origin(db, name)?;
    let merges = find_merge_history(db, name)?;
    let children = find_children(db, name)?;

    Ok(Some(DagBranchInfo {
        name: name.to_string(),
        status,
        created_at,
        updated_at,
        message,
        creator,
        forked_from,
        merges,
        children,
    }))
}

/// Find the fork origin of a branch (if it was forked).
fn find_fork_origin(db: &Arc<Database>, name: &str) -> StrataResult<Option<ForkRecord>> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Find incoming "child" edges to this branch -> fork event nodes
    let incoming = graph_store.neighbors(
        branch_id,
        BRANCH_DAG_GRAPH,
        name,
        Direction::Incoming,
        Some("child"),
    )?;

    for neighbor in incoming {
        // Check if the neighbor is a fork event node
        if let Some(event_node) =
            graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id)?
        {
            if event_node.object_type.as_deref() != Some("fork") {
                continue;
            }
            // Find the parent branch via incoming "parent" edges to the fork event
            let parents = graph_store.neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                &neighbor.node_id,
                Direction::Incoming,
                Some("parent"),
            )?;
            if let Some(parent) = parents.first() {
                let props = event_node.properties.as_ref();
                return Ok(Some(ForkRecord {
                    event_id: DagEventId::from_string(neighbor.node_id.clone()),
                    parent: parent.node_id.clone(),
                    child: name.to_string(),
                    timestamp: props
                        .and_then(|p| p.get("timestamp"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    fork_version: props
                        .and_then(|p| p.get("fork_version"))
                        .and_then(|v| v.as_u64()),
                    message: props
                        .and_then(|p| p.get("message"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    creator: props
                        .and_then(|p| p.get("creator"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                }));
            }
        }
    }
    Ok(None)
}

/// Find merge history for a branch (merges where this branch was the source).
fn find_merge_history(db: &Arc<Database>, name: &str) -> StrataResult<Vec<MergeRecord>> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Find outgoing "source" edges from this branch -> merge event nodes
    let outgoing = graph_store.neighbors(
        branch_id,
        BRANCH_DAG_GRAPH,
        name,
        Direction::Outgoing,
        Some("source"),
    )?;

    let mut records = Vec::new();
    for neighbor in outgoing {
        if let Some(event_node) =
            graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id)?
        {
            if event_node.object_type.as_deref() != Some("merge") {
                continue;
            }
            // Find the target branch via outgoing "target" edges
            let targets = graph_store.neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                &neighbor.node_id,
                Direction::Outgoing,
                Some("target"),
            )?;
            let target_name = targets
                .first()
                .map(|t| t.node_id.clone())
                .unwrap_or_default();
            let props = event_node.properties.as_ref();
            records.push(MergeRecord {
                event_id: DagEventId::from_string(neighbor.node_id.clone()),
                source: name.to_string(),
                target: target_name,
                timestamp: props
                    .and_then(|p| p.get("timestamp"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                merge_version: props
                    .and_then(|p| p.get("merge_version"))
                    .and_then(|v| v.as_u64()),
                strategy: props
                    .and_then(|p| p.get("strategy"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                keys_applied: props
                    .and_then(|p| p.get("keys_applied"))
                    .and_then(|v| v.as_u64()),
                spaces_merged: props
                    .and_then(|p| p.get("spaces_merged"))
                    .and_then(|v| v.as_u64()),
                conflicts: props
                    .and_then(|p| p.get("conflicts"))
                    .and_then(|v| v.as_u64()),
                message: props
                    .and_then(|p| p.get("message"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                creator: props
                    .and_then(|p| p.get("creator"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            });
        }
    }
    // Sort by timestamp
    records.sort_by_key(|r| r.timestamp);
    Ok(records)
}

/// Find children of a branch (branches forked from this branch).
pub fn find_children(db: &Arc<Database>, name: &str) -> StrataResult<Vec<String>> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Find outgoing "parent" edges from this branch -> fork event nodes
    let outgoing = graph_store.neighbors(
        branch_id,
        BRANCH_DAG_GRAPH,
        name,
        Direction::Outgoing,
        Some("parent"),
    )?;

    let mut children = Vec::new();
    for neighbor in outgoing {
        // Verify it's a fork event
        if let Some(event_node) =
            graph_store.get_node(branch_id, BRANCH_DAG_GRAPH, &neighbor.node_id)?
        {
            if event_node.object_type.as_deref() != Some("fork") {
                continue;
            }
            // Follow outgoing "child" edges from the fork event
            let child_neighbors = graph_store.neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                &neighbor.node_id,
                Direction::Outgoing,
                Some("child"),
            )?;
            for child in child_neighbors {
                children.push(child.node_id);
            }
        }
    }
    Ok(children)
}

/// Find the most recent `merge_version` between two branches (in either direction).
///
/// Checks merges where `source` merged into `target` and also where `target`
/// merged into `source`, returning the highest `merge_version` found.
pub fn find_last_merge_version(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> StrataResult<Option<u64>> {
    // Merges where source was the source branch
    let forward = find_merge_history(db, source)?;
    // Merges where target was the source branch (reverse direction)
    let reverse = find_merge_history(db, target)?;

    let best = forward
        .iter()
        .filter(|r| r.target == target)
        .chain(reverse.iter().filter(|r| r.target == source))
        .filter_map(|r| r.merge_version)
        .max();

    Ok(best)
}

/// Find the fork version between two branches.
///
/// Checks whether `source` was forked from `target` or `target` was forked
/// from `source`. Returns `(child_branch_name, fork_version)` if found.
pub fn find_fork_version(
    db: &Arc<Database>,
    source: &str,
    target: &str,
) -> StrataResult<Option<(String, u64)>> {
    // Check if source was forked from target
    if let Some(fork) = find_fork_origin(db, source)? {
        if fork.parent == target {
            if let Some(fv) = fork.fork_version {
                return Ok(Some((source.to_string(), fv)));
            }
        }
    }
    // Check if target was forked from source
    if let Some(fork) = find_fork_origin(db, target)? {
        if fork.parent == source {
            if let Some(fv) = fork.fork_version {
                return Ok(Some((target.to_string(), fv)));
            }
        }
    }
    Ok(None)
}

/// Extract `DagBranchStatus` from node properties.
fn status_from_node_props(node: &NodeData) -> DagBranchStatus {
    if let Some(props) = &node.properties {
        if let Some(s) = props.get("status").and_then(|v| v.as_str()) {
            if let Some(status) = DagBranchStatus::parse_str(s) {
                return status;
            }
        }
    }
    DagBranchStatus::Active
}

/// Subsystem implementation for graph initialization and branch DAG management.
///
/// Handles:
/// - Creating the `_system_` branch on database open
/// - Creating the `_branch_dag` graph for tracking branch lifecycle
/// - Populating the branch status cache (for followers)
pub struct GraphSubsystem;

impl strata_engine::Subsystem for GraphSubsystem {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn recover(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
    ) -> strata_core::StrataResult<()> {
        init_system_branch(db);
        load_status_cache_readonly(db);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Arc<Database> {
        let db = Database::cache().unwrap();
        init_system_branch(&db);
        db
    }

    #[test]
    fn system_branch_auto_created() {
        let db = setup();
        let branch_index = BranchIndex::new(db.clone());
        assert!(branch_index.exists(SYSTEM_BRANCH).unwrap());
    }

    #[test]
    fn branch_dag_graph_exists() {
        let db = setup();
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let meta = graph_store
            .get_graph_meta(branch_id, BRANCH_DAG_GRAPH)
            .unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn default_branch_seeded() {
        let db = setup();
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
        let db = setup();
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

        // Missing status field -> defaults to Active
        let node = NodeData {
            properties: Some(serde_json::json!({"other": "field"})),
            ..Default::default()
        };
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Active);

        // No properties at all -> defaults to Active
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

    #[test]
    fn dag_branch_status_expanded() {
        // from_str / as_str round-trip for all 4 variants
        for (s, expected) in [
            ("active", DagBranchStatus::Active),
            ("archived", DagBranchStatus::Archived),
            ("merged", DagBranchStatus::Merged),
            ("deleted", DagBranchStatus::Deleted),
        ] {
            assert_eq!(DagBranchStatus::parse_str(s), Some(expected));
            assert_eq!(expected.as_str(), s);
        }
        // is_writable
        assert!(DagBranchStatus::Active.is_writable());
        assert!(!DagBranchStatus::Archived.is_writable());
        assert!(!DagBranchStatus::Merged.is_writable());
        assert!(!DagBranchStatus::Deleted.is_writable());
        // Unknown string
        assert_eq!(DagBranchStatus::parse_str("unknown"), None);
        // Display
        assert_eq!(format!("{}", DagBranchStatus::Active), "active");
    }

    #[test]
    fn status_from_node_props_merged_deleted() {
        let node = NodeData {
            properties: Some(serde_json::json!({"status": "merged"})),
            ..Default::default()
        };
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Merged);

        let node = NodeData {
            properties: Some(serde_json::json!({"status": "deleted"})),
            ..Default::default()
        };
        assert_eq!(status_from_node_props(&node), DagBranchStatus::Deleted);
    }

    #[test]
    fn dag_add_branch_creates_node() {
        let db = setup();
        dag_add_branch(&db, "test-add", Some("test message"), Some("tester")).unwrap();

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, "test-add")
            .unwrap()
            .unwrap();
        assert_eq!(node.object_type.as_deref(), Some("branch"));
        let props = node.properties.as_ref().unwrap();
        assert_eq!(props.get("status").and_then(|v| v.as_str()), Some("active"));
        assert_eq!(
            props.get("message").and_then(|v| v.as_str()),
            Some("test message")
        );
        assert_eq!(
            props.get("creator").and_then(|v| v.as_str()),
            Some("tester")
        );
        assert!(props.get("created_at").and_then(|v| v.as_u64()).is_some());
    }

    #[test]
    fn dag_record_fork_creates_event_and_edges() {
        let db = setup();
        dag_add_branch(&db, "parent-branch", None, None).unwrap();
        dag_add_branch(&db, "child-branch", None, None).unwrap();

        let event_id = dag_record_fork(
            &db,
            "parent-branch",
            "child-branch",
            Some(42),
            Some("fork msg"),
            None,
        )
        .unwrap();
        assert!(event_id.is_fork());
        assert!(!event_id.is_merge());

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);

        // Verify fork event node exists with correct properties
        let event_node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(event_node.object_type.as_deref(), Some("fork"));
        let event_props = event_node.properties.as_ref().unwrap();
        assert_eq!(
            event_props.get("message").and_then(|v| v.as_str()),
            Some("fork msg")
        );
        assert!(event_props
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .is_some());
        assert_eq!(
            event_props.get("fork_version").and_then(|v| v.as_u64()),
            Some(42),
            "fork_version should be persisted in DAG node properties"
        );

        // Verify edges: parent --[parent]--> fork_event
        let parent_neighbors = graph_store
            .neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                "parent-branch",
                Direction::Outgoing,
                Some("parent"),
            )
            .unwrap();
        assert!(parent_neighbors
            .iter()
            .any(|n| n.node_id == event_id.as_str()));

        // Verify edges: fork_event --[child]--> child
        let child_neighbors = graph_store
            .neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                event_id.as_str(),
                Direction::Outgoing,
                Some("child"),
            )
            .unwrap();
        assert!(child_neighbors.iter().any(|n| n.node_id == "child-branch"));
    }

    #[test]
    fn dag_record_merge_creates_event_and_edges() {
        let db = setup();
        dag_add_branch(&db, "merge-src", None, None).unwrap();
        dag_add_branch(&db, "merge-tgt", None, None).unwrap();

        let merge_info = strata_engine::branch_ops::MergeInfo {
            source: "merge-src".to_string(),
            target: "merge-tgt".to_string(),
            keys_applied: 10,
            conflicts: vec![],
            spaces_merged: 2,
            keys_deleted: 0,
            merge_version: None,
        };
        let event_id = dag_record_merge(
            &db,
            "merge-src",
            "merge-tgt",
            &merge_info,
            Some("lww"),
            None,
            None,
        )
        .unwrap();
        assert!(event_id.is_merge());

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);

        // Verify merge event node
        let event_node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str())
            .unwrap()
            .unwrap();
        assert_eq!(event_node.object_type.as_deref(), Some("merge"));
        let props = event_node.properties.as_ref().unwrap();
        assert_eq!(props.get("keys_applied").and_then(|v| v.as_u64()), Some(10));
        assert_eq!(props.get("spaces_merged").and_then(|v| v.as_u64()), Some(2));

        // Verify edge: source --[source]--> merge_event
        let src_neighbors = graph_store
            .neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                "merge-src",
                Direction::Outgoing,
                Some("source"),
            )
            .unwrap();
        assert!(src_neighbors.iter().any(|n| n.node_id == event_id.as_str()));

        // Verify edge: merge_event --[target]--> target
        let tgt_neighbors = graph_store
            .neighbors(
                branch_id,
                BRANCH_DAG_GRAPH,
                event_id.as_str(),
                Direction::Outgoing,
                Some("target"),
            )
            .unwrap();
        assert!(tgt_neighbors.iter().any(|n| n.node_id == "merge-tgt"));
    }

    #[test]
    fn dag_set_status_updates_node() {
        let db = setup();
        dag_add_branch(&db, "status-test", None, None).unwrap();
        assert_eq!(
            dag_get_status(&db, "status-test").unwrap(),
            DagBranchStatus::Active
        );

        dag_set_status(&db, "status-test", DagBranchStatus::Archived).unwrap();
        assert_eq!(
            dag_get_status(&db, "status-test").unwrap(),
            DagBranchStatus::Archived
        );
    }

    #[test]
    fn dag_mark_deleted_sets_status() {
        let db = setup();
        dag_add_branch(&db, "del-test", None, None).unwrap();

        dag_mark_deleted(&db, "del-test").unwrap();
        assert_eq!(
            dag_get_status(&db, "del-test").unwrap(),
            DagBranchStatus::Deleted
        );

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, "del-test")
            .unwrap()
            .unwrap();
        assert!(node
            .properties
            .as_ref()
            .unwrap()
            .get("deleted_at")
            .is_some());
    }

    #[test]
    fn dag_mark_deleted_noop_if_missing() {
        let db = setup();
        // Should not error for non-existent branch
        dag_mark_deleted(&db, "nonexistent-branch-xyz").unwrap();
    }

    #[test]
    fn dag_get_branch_info_assembles_full_record() {
        let db = setup();
        dag_add_branch(&db, "info-parent", Some("parent branch"), Some("admin")).unwrap();
        dag_add_branch(&db, "info-child", None, None).unwrap();
        dag_record_fork(
            &db,
            "info-parent",
            "info-child",
            Some(100),
            Some("forked!"),
            None,
        )
        .unwrap();

        let merge_info = strata_engine::branch_ops::MergeInfo {
            source: "info-child".to_string(),
            target: "info-parent".to_string(),
            keys_applied: 5,
            conflicts: vec![],
            spaces_merged: 1,
            keys_deleted: 0,
            merge_version: None,
        };
        dag_record_merge(
            &db,
            "info-child",
            "info-parent",
            &merge_info,
            Some("lww"),
            None,
            None,
        )
        .unwrap();

        // Check parent info
        let parent_info = dag_get_branch_info(&db, "info-parent").unwrap().unwrap();
        assert_eq!(parent_info.name, "info-parent");
        assert_eq!(parent_info.status, DagBranchStatus::Active);
        assert_eq!(parent_info.message.as_deref(), Some("parent branch"));
        assert_eq!(parent_info.creator.as_deref(), Some("admin"));
        assert!(!parent_info.children.is_empty());
        assert!(parent_info.children.contains(&"info-child".to_string()));

        // Check child info
        let child_info = dag_get_branch_info(&db, "info-child").unwrap().unwrap();
        assert!(child_info.forked_from.is_some());
        let fork = child_info.forked_from.unwrap();
        assert_eq!(fork.parent, "info-parent");
        assert_eq!(fork.child, "info-child");
        assert_eq!(fork.fork_version, Some(100));
        assert!(!child_info.merges.is_empty());
        assert_eq!(child_info.merges[0].target, "info-parent");
    }

    #[test]
    fn find_children_returns_forked_branches() {
        let db = setup();
        dag_add_branch(&db, "fc-parent", None, None).unwrap();
        dag_add_branch(&db, "fc-child1", None, None).unwrap();
        dag_add_branch(&db, "fc-child2", None, None).unwrap();
        dag_record_fork(&db, "fc-parent", "fc-child1", None, None, None).unwrap();
        dag_record_fork(&db, "fc-parent", "fc-child2", None, None, None).unwrap();

        let children = find_children(&db, "fc-parent").unwrap();
        assert!(children.contains(&"fc-child1".to_string()));
        assert!(children.contains(&"fc-child2".to_string()));
    }

    #[test]
    fn ensure_branch_node_exists_is_idempotent() {
        let db = setup();
        ensure_branch_node_exists(&db, "idempotent-test").unwrap();
        ensure_branch_node_exists(&db, "idempotent-test").unwrap();

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, "idempotent-test")
            .unwrap();
        assert!(node.is_some());
    }

    #[test]
    fn dag_get_branch_info_returns_none_for_missing() {
        let db = setup();
        let info = dag_get_branch_info(&db, "no-such-branch-xyz").unwrap();
        assert!(info.is_none());
    }

    #[test]
    fn dag_set_status_errors_for_missing_branch() {
        let db = setup();
        let result = dag_set_status(&db, "no-such-branch-xyz", DagBranchStatus::Archived);
        assert!(result.is_err());
    }

    #[test]
    fn dag_set_message_errors_for_missing_branch() {
        let db = setup();
        let result = dag_set_message(&db, "no-such-branch-xyz", "hello");
        assert!(result.is_err());
    }

    #[test]
    fn dag_set_message_updates_node() {
        let db = setup();
        dag_add_branch(&db, "msg-test", None, None).unwrap();

        dag_set_message(&db, "msg-test", "hello world").unwrap();

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, "msg-test")
            .unwrap()
            .unwrap();
        let props = node.properties.as_ref().unwrap();
        assert_eq!(
            props.get("message").and_then(|v| v.as_str()),
            Some("hello world")
        );
    }

    #[test]
    fn test_merge_version_round_trip() {
        let db = setup();
        dag_add_branch(&db, "mv-src", None, None).unwrap();
        dag_add_branch(&db, "mv-tgt", None, None).unwrap();

        let merge_info = strata_engine::branch_ops::MergeInfo {
            source: "mv-src".to_string(),
            target: "mv-tgt".to_string(),
            keys_applied: 3,
            conflicts: vec![],
            spaces_merged: 1,
            keys_deleted: 0,
            merge_version: Some(42),
        };
        let event_id = dag_record_merge(
            &db,
            "mv-src",
            "mv-tgt",
            &merge_info,
            Some("lww"),
            None,
            None,
        )
        .unwrap();

        // Verify the merge_version is stored in the DAG node
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let event_node = graph_store
            .get_node(branch_id, BRANCH_DAG_GRAPH, event_id.as_str())
            .unwrap()
            .unwrap();
        let props = event_node.properties.as_ref().unwrap();
        assert_eq!(
            props.get("merge_version").and_then(|v| v.as_u64()),
            Some(42),
            "merge_version should be persisted in DAG node properties"
        );

        // Verify it comes back via dag_get_branch_info -> merges
        let info = dag_get_branch_info(&db, "mv-src").unwrap().unwrap();
        assert_eq!(info.merges.len(), 1);
        assert_eq!(info.merges[0].merge_version, Some(42));
    }

    #[test]
    fn test_find_last_merge_version() {
        let db = setup();
        dag_add_branch(&db, "flmv-parent", None, None).unwrap();
        dag_add_branch(&db, "flmv-child", None, None).unwrap();
        dag_record_fork(&db, "flmv-parent", "flmv-child", Some(10), None, None).unwrap();

        let merge_info = strata_engine::branch_ops::MergeInfo {
            source: "flmv-child".to_string(),
            target: "flmv-parent".to_string(),
            keys_applied: 5,
            conflicts: vec![],
            spaces_merged: 1,
            keys_deleted: 0,
            merge_version: Some(20),
        };
        dag_record_merge(
            &db,
            "flmv-child",
            "flmv-parent",
            &merge_info,
            Some("lww"),
            None,
            None,
        )
        .unwrap();

        let result = find_last_merge_version(&db, "flmv-child", "flmv-parent").unwrap();
        assert_eq!(result, Some(20));
    }

    #[test]
    fn test_find_last_merge_version_both_directions() {
        let db = setup();
        dag_add_branch(&db, "bidir-a", None, None).unwrap();
        dag_add_branch(&db, "bidir-b", None, None).unwrap();

        // Merge A -> B with merge_version 10
        let merge_info_ab = strata_engine::branch_ops::MergeInfo {
            source: "bidir-a".to_string(),
            target: "bidir-b".to_string(),
            keys_applied: 2,
            conflicts: vec![],
            spaces_merged: 1,
            keys_deleted: 0,
            merge_version: Some(10),
        };
        dag_record_merge(&db, "bidir-a", "bidir-b", &merge_info_ab, None, None, None).unwrap();

        // Merge B -> A with merge_version 25
        let merge_info_ba = strata_engine::branch_ops::MergeInfo {
            source: "bidir-b".to_string(),
            target: "bidir-a".to_string(),
            keys_applied: 3,
            conflicts: vec![],
            spaces_merged: 1,
            keys_deleted: 0,
            merge_version: Some(25),
        };
        dag_record_merge(&db, "bidir-b", "bidir-a", &merge_info_ba, None, None, None).unwrap();

        // Both directions should find the max (25)
        let fwd = find_last_merge_version(&db, "bidir-a", "bidir-b").unwrap();
        assert_eq!(fwd, Some(25));
        let rev = find_last_merge_version(&db, "bidir-b", "bidir-a").unwrap();
        assert_eq!(rev, Some(25));
    }

    #[test]
    fn test_find_fork_version() {
        let db = setup();
        dag_add_branch(&db, "fv-parent", None, None).unwrap();
        dag_add_branch(&db, "fv-child", None, None).unwrap();
        dag_record_fork(&db, "fv-parent", "fv-child", Some(55), None, None).unwrap();

        // source=child, target=parent => child was forked from parent
        let result = find_fork_version(&db, "fv-child", "fv-parent").unwrap();
        assert_eq!(result, Some(("fv-child".to_string(), 55)));
    }

    #[test]
    fn test_find_fork_version_reverse() {
        let db = setup();
        dag_add_branch(&db, "fvr-parent", None, None).unwrap();
        dag_add_branch(&db, "fvr-child", None, None).unwrap();
        dag_record_fork(&db, "fvr-parent", "fvr-child", Some(77), None, None).unwrap();

        // source=parent, target=child => should still find that child was forked from parent
        let result = find_fork_version(&db, "fvr-parent", "fvr-child").unwrap();
        assert_eq!(result, Some(("fvr-child".to_string(), 77)));

        // Unrelated branches should return None
        dag_add_branch(&db, "fvr-unrelated", None, None).unwrap();
        let result = find_fork_version(&db, "fvr-parent", "fvr-unrelated").unwrap();
        assert_eq!(result, None);
    }
}
