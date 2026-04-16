//! Branch DAG infrastructure for the `_system_` branch.
//!
//! Reserves the `_system_` namespace, auto-creates the system branch and
//! `_branch_dag` graph on database init, and records branch lifecycle
//! events (creation, forks, merges, deletion) as nodes and edges in the DAG.
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
use strata_core::types::BranchId;
use strata_core::{StrataError, StrataResult};
use tracing::warn;

use crate::keys::{validate_node_id, GRAPH_SPACE};
use crate::types::{Direction, EdgeData, NodeData};
use crate::GraphStore;
use strata_engine::primitives::branch::resolve_branch_name;
use strata_engine::{CherryPickInfo, Database, MergeInfo, MergeStrategy, RevertInfo};

const BRANCH_NODE_ID_PREFIX: &str = "_branch_";

fn dag_branch_node_id(name: &str) -> String {
    if validate_node_id(name).is_ok() {
        name.to_string()
    } else {
        format!("{BRANCH_NODE_ID_PREFIX}{}", resolve_branch_name(name))
    }
}

fn branch_name_from_node(node_id: &str, node: &NodeData) -> String {
    node.properties
        .as_ref()
        .and_then(|value| value.as_object())
        .and_then(|props| props.get("branch_name"))
        .and_then(|value| value.as_str())
        .unwrap_or(node_id)
        .to_string()
}

fn branch_name_prop(
    props: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    props
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

/// Ensure the reserved `_system_` branch exists before DAG bootstrap.
fn ensure_system_branch(db: &Arc<Database>) -> Result<(), String> {
    db.ensure_system_branch_exists()
        .map_err(|e| format!("failed to create _system_ branch: {e}"))
}

/// Ensure the `_branch_dag` graph exists on the `_system_` branch.
fn ensure_branch_dag(db: &Arc<Database>) -> Result<(), String> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    match graph_store.get_graph_meta(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => graph_store
            .create_graph(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, None)
            .map_err(|e| format!("failed to create _branch_dag graph: {e}")),
        Err(e) => Err(format!("failed to check _branch_dag graph: {e}")),
    }
}

/// Initialise system branch infrastructure on database open.
///
/// Creates the `_system_` branch and the `_branch_dag` graph. All steps are
/// best-effort -- failures are logged but do not prevent the database from
/// opening.
pub fn init_system_branch(db: &Arc<Database>) {
    if let Err(e) = bootstrap_system_branch(db) {
        warn!("system branch init: {e}");
    }
}

fn bootstrap_system_branch(db: &Arc<Database>) -> StrataResult<()> {
    ensure_system_branch(db).map_err(StrataError::internal)?;
    ensure_branch_dag(db).map_err(StrataError::internal)?;
    Ok(())
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
    match graph_store.get_graph_meta(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH) {
        Ok(Some(_)) => {}
        _ => return,
    }

    // Best-effort: populate the cache from existing nodes.
    if let Ok(cache) = db.extension::<crate::branch_status_cache::BranchStatusCache>() {
        if let Ok(nodes) = graph_store.list_nodes(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH) {
            for node_id in nodes {
                if let Ok(Some(node)) =
                    graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)
                {
                    if node.object_type.as_deref() == Some("branch") {
                        let status = status_from_node_props(&node);
                        cache.set(branch_name_from_node(&node_id, &node), status);
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
    let node_id = dag_branch_node_id(name);
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "branch_name": name,
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
    graph_store.add_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id, node)?;
    Ok(())
}

/// Ensure a branch node exists in the DAG (idempotent).
fn ensure_branch_node_exists(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let node_id = dag_branch_node_id(name);
    if graph_store
        .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)?
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
    let parent_node_id = dag_branch_node_id(parent);
    let child_node_id = dag_branch_node_id(child);
    let event_id = DagEventId::new_fork();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
        "parent_branch": parent,
        "child_branch": child,
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
    graph_store.add_node(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        node,
    )?;

    // parent --[parent]--> fork_event
    graph_store.add_edge(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &parent_node_id,
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
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        &child_node_id,
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
    merge_info: &MergeInfo,
    strategy: Option<&str>,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    ensure_branch_node_exists(db, source)?;
    ensure_branch_node_exists(db, target)?;

    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let source_node_id = dag_branch_node_id(source);
    let target_node_id = dag_branch_node_id(target);
    let event_id = DagEventId::new_merge();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
        "source_branch": source,
        "target_branch": target,
        "keys_applied": merge_info.keys_applied,
        "keys_deleted": merge_info.keys_deleted,
        "spaces_merged": merge_info.spaces_merged,
        "conflicts": merge_info.conflicts.len() as u64,
        "merge_info": serde_json::to_value(merge_info)
            .map_err(|e| StrataError::serialization(e.to_string()))?,
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
    graph_store.add_node(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        node,
    )?;

    // source --[source]--> merge_event
    graph_store.add_edge(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &source_node_id,
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
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        &target_node_id,
        "target",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;

    Ok(event_id)
}

/// Record a revert event in the DAG.
///
/// Creates a `revert` event node and a single `branch → revert_event` edge
/// of type `reverted`. Revert is a self-event (one branch, no source/target
/// pair) so it has only one edge, unlike fork and merge.
pub fn dag_record_revert(
    db: &Arc<Database>,
    revert_info: &RevertInfo,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId> {
    ensure_branch_node_exists(db, &revert_info.branch)?;

    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let branch_node_id = dag_branch_node_id(&revert_info.branch);
    let event_id = DagEventId::new_revert();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
        "branch": &revert_info.branch,
        "from_version": revert_info.from_version,
        "to_version": revert_info.to_version,
        "keys_reverted": revert_info.keys_reverted,
        "revert_info": serde_json::to_value(revert_info)
            .map_err(|e| StrataError::serialization(e.to_string()))?,
    });
    if let Some(rv) = revert_info.revert_version {
        props["revert_version"] = serde_json::json!(rv);
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
        object_type: Some("revert".to_string()),
    };
    graph_store.add_node(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        node,
    )?;

    // branch --[reverted]--> revert_event
    graph_store.add_edge(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &branch_node_id,
        event_id.as_str(),
        "reverted",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;

    Ok(event_id)
}

/// Record a cherry-pick event in the DAG.
///
/// Creates a `cherry_pick` event node and `source → event → target` edges
/// of types `cherry_pick_source` and `cherry_pick_target`. The shape mirrors
/// merge but uses distinct edge types so callers can distinguish full
/// merges from partial cherry-picks when walking the DAG.
pub fn dag_record_cherry_pick(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    info: &CherryPickInfo,
) -> StrataResult<DagEventId> {
    ensure_branch_node_exists(db, source)?;
    ensure_branch_node_exists(db, target)?;

    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let source_node_id = dag_branch_node_id(source);
    let target_node_id = dag_branch_node_id(target);
    let event_id = DagEventId::new_cherry_pick();
    let now = Timestamp::now().as_micros();

    let mut props = serde_json::json!({
        "timestamp": now,
        "source_branch": source,
        "target_branch": target,
        "keys_applied": info.keys_applied,
        "keys_deleted": info.keys_deleted,
        "cherry_pick_info": serde_json::to_value(info)
            .map_err(|e| StrataError::serialization(e.to_string()))?,
    });
    if let Some(cv) = info.cherry_pick_version {
        props["cherry_pick_version"] = serde_json::json!(cv);
    }

    let node = NodeData {
        entity_ref: None,
        properties: Some(props),
        object_type: Some("cherry_pick".to_string()),
    };
    graph_store.add_node(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        node,
    )?;

    // source --[cherry_pick_source]--> event
    graph_store.add_edge(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &source_node_id,
        event_id.as_str(),
        "cherry_pick_source",
        EdgeData {
            weight: 1.0,
            properties: None,
        },
    )?;
    // event --[cherry_pick_target]--> target
    graph_store.add_edge(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        event_id.as_str(),
        &target_node_id,
        "cherry_pick_target",
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
    let node_id = dag_branch_node_id(name);

    let node = graph_store
        .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)?
        .ok_or_else(|| StrataError::internal(format!("branch node not found in DAG: {name}")))?;

    let mut props = node.properties.unwrap_or_else(|| serde_json::json!({}));
    props["status"] = serde_json::json!(status.as_str());
    props["updated_at"] = serde_json::json!(Timestamp::now().as_micros());

    let updated = NodeData {
        entity_ref: node.entity_ref,
        properties: Some(props),
        object_type: node.object_type,
    };
    graph_store.add_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id, updated)?;
    Ok(())
}

/// Mark a branch as deleted in the DAG. No-op if node doesn't exist.
pub fn dag_mark_deleted(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let node_id = dag_branch_node_id(name);

    let node = match graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)? {
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
    graph_store.add_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id, updated)?;
    Ok(())
}

/// Set the message on a branch node in the DAG.
pub fn dag_set_message(db: &Arc<Database>, name: &str, message: &str) -> StrataResult<()> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let node_id = dag_branch_node_id(name);

    let node = graph_store
        .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)?
        .ok_or_else(|| StrataError::internal(format!("branch node not found in DAG: {name}")))?;

    let mut props = node.properties.unwrap_or_else(|| serde_json::json!({}));
    props["message"] = serde_json::json!(message);
    props["updated_at"] = serde_json::json!(Timestamp::now().as_micros());

    let updated = NodeData {
        entity_ref: node.entity_ref,
        properties: Some(props),
        object_type: node.object_type,
    };
    graph_store.add_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id, updated)?;
    Ok(())
}

// =========================================================================
// DAG Read Helpers
// =========================================================================

/// Get the status of a branch from the DAG.
pub fn dag_get_status(db: &Arc<Database>, name: &str) -> StrataResult<DagBranchStatus> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let node_id = dag_branch_node_id(name);

    match graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)? {
        Some(node) => Ok(status_from_node_props(&node)),
        None => Ok(DagBranchStatus::Active),
    }
}

/// Get full branch info from the DAG.
pub fn dag_get_branch_info(db: &Arc<Database>, name: &str) -> StrataResult<Option<DagBranchInfo>> {
    let graph_store = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);
    let node_id = dag_branch_node_id(name);

    let node = match graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &node_id)? {
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
    let branch_node_id = dag_branch_node_id(name);

    // Find incoming "child" edges to this branch -> fork event nodes
    let incoming = graph_store.neighbors(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &branch_node_id,
        Direction::Incoming,
        Some("child"),
    )?;

    for neighbor in incoming {
        // Check if the neighbor is a fork event node
        if let Some(event_node) =
            graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &neighbor.node_id)?
        {
            if event_node.object_type.as_deref() != Some("fork") {
                continue;
            }
            let props = event_node.properties.as_ref();
            let parent = props
                .and_then(|p| p.get("parent_branch"))
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .unwrap_or_default();
            if !parent.is_empty() {
                return Ok(Some(ForkRecord {
                    event_id: DagEventId::from_string(neighbor.node_id.clone()),
                    parent,
                    child: props
                        .and_then(|p| p.get("child_branch"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(name)
                        .to_string(),
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
    let branch_node_id = dag_branch_node_id(name);

    // Find outgoing "source" edges from this branch -> merge event nodes
    let outgoing = graph_store.neighbors(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &branch_node_id,
        Direction::Outgoing,
        Some("source"),
    )?;

    let mut records = Vec::new();
    for neighbor in outgoing {
        if let Some(event_node) =
            graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &neighbor.node_id)?
        {
            if event_node.object_type.as_deref() != Some("merge") {
                continue;
            }
            let props = event_node.properties.as_ref();
            records.push(MergeRecord {
                event_id: DagEventId::from_string(neighbor.node_id.clone()),
                source: props
                    .and_then(|p| p.get("source_branch"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(name)
                    .to_string(),
                target: props
                    .and_then(|p| p.get("target_branch"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
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
    let branch_node_id = dag_branch_node_id(name);

    // Find outgoing "parent" edges from this branch -> fork event nodes
    let outgoing = graph_store.neighbors(
        branch_id,
        GRAPH_SPACE,
        BRANCH_DAG_GRAPH,
        &branch_node_id,
        Direction::Outgoing,
        Some("parent"),
    )?;

    let mut children = Vec::new();
    for neighbor in outgoing {
        // Verify it's a fork event
        if let Some(event_node) =
            graph_store.get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &neighbor.node_id)?
        {
            if event_node.object_type.as_deref() != Some("fork") {
                continue;
            }
            if let Some(child) = event_node
                .properties
                .as_ref()
                .and_then(|p| p.get("child_branch"))
                .and_then(|v| v.as_str())
            {
                children.push(child.to_string());
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
/// - Registering graph semantic merge handler with the engine
/// - Installing the per-database BranchDagHook
pub struct GraphSubsystem;

impl strata_engine::Subsystem for GraphSubsystem {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn recover(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
    ) -> strata_core::StrataResult<()> {
        // Read-only phase: hydrate branch status cache from existing DAG state.
        load_status_cache_readonly(db);
        Ok(())
    }

    fn initialize(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
    ) -> strata_core::StrataResult<()> {
        // Register graph semantic merge handler with the per-database registry.
        // This replaces the old global `register_graph_merge_plan()` pattern.
        db.merge_registry()
            .register_graph(crate::merge_handler::graph_plan_fn);

        // Install the per-database BranchDagHook. This replaces the old global
        // `register_branch_dag_hooks()` pattern. The hook is fail-fast: errors
        // propagate and abort the calling branch operation.
        let hook = Arc::new(GraphBranchDagHook::new(db.clone()));
        db.install_dag_hook(hook)
            .map_err(|e| StrataError::internal(format!("failed to install graph DAG hook: {e}")))?;

        // Register audit observer for branch operations.
        // This moves audit emission from executor handlers into the engine layer,
        // ensuring all branch operations (even direct API calls) get audited.
        let audit_observer = Arc::new(AuditBranchOpObserver {
            db: Arc::downgrade(db),
        });
        db.branch_op_observers().register(audit_observer);

        // Register commit/abort observers and the follower refresh hook for
        // graph index maintenance. T2-E5/E3 move graph index maintenance into
        // subsystem-owned lifecycle wiring.
        let state = db
            .extension::<crate::GraphBackendState>()
            .map_err(|e| StrataError::internal(format!("failed to get GraphBackendState: {e}")))?;
        crate::store::ensure_runtime_wiring(db, &state);

        Ok(())
    }

    fn bootstrap(
        &self,
        db: &std::sync::Arc<strata_engine::Database>,
    ) -> strata_core::StrataResult<()> {
        bootstrap_system_branch(db)
    }
}

// =============================================================================
// Audit Branch Operation Observer
// =============================================================================

use std::sync::Weak;
use strata_engine::database::observers::{
    BranchOpEvent, BranchOpKind, BranchOpObserver, ObserverError,
};
use strata_engine::primitives::event::EventLog;

/// Observer that emits audit events to the `_system_` branch event log.
///
/// This replaces the `emit_audit_event` calls scattered throughout executor
/// branch handlers. By registering as a `BranchOpObserver`, all branch
/// operations (including direct API calls) are audited consistently.
///
/// Best-effort: failures are logged but never propagate to the caller.
struct AuditBranchOpObserver {
    db: Weak<strata_engine::Database>,
}

impl BranchOpObserver for AuditBranchOpObserver {
    fn name(&self) -> &'static str {
        "audit"
    }

    fn on_branch_op(&self, event: &BranchOpEvent) -> Result<(), ObserverError> {
        let Some(db) = self.db.upgrade() else {
            return Ok(()); // Database dropped, nothing to audit
        };

        let event_log = EventLog::new(db);
        let system_branch_id = resolve_branch_name(SYSTEM_BRANCH);

        // Build audit payload matching the format from executor handlers
        let event_type = match event.kind {
            BranchOpKind::Create => "branch.create",
            BranchOpKind::Delete => "branch.delete",
            BranchOpKind::Fork => "branch.fork",
            BranchOpKind::Merge => "branch.merge",
            BranchOpKind::Revert => "branch.revert",
            BranchOpKind::CherryPick => "branch.cherry_pick",
            BranchOpKind::Tag => "branch.tag",
            BranchOpKind::Untag => "branch.untag",
            // BranchOpKind is #[non_exhaustive] - handle future variants
            _ => "branch.unknown",
        };

        let mut payload = serde_json::Map::new();

        // Core fields present in most events
        if let Some(ref name) = event.branch_name {
            payload.insert(
                "branch".to_string(),
                serde_json::Value::String(name.clone()),
            );
        }
        if let Some(ref source_name) = event.source_branch_name {
            // Use field names matching original executor payloads
            match event.kind {
                BranchOpKind::Fork => {
                    // Fork uses parent/child naming
                    payload.insert(
                        "parent".to_string(),
                        serde_json::Value::String(source_name.clone()),
                    );
                    if let Some(ref child) = event.branch_name {
                        payload.insert(
                            "child".to_string(),
                            serde_json::Value::String(child.clone()),
                        );
                    }
                }
                _ => {
                    // Merge/cherry-pick use source/target naming
                    payload.insert(
                        "source".to_string(),
                        serde_json::Value::String(source_name.clone()),
                    );
                    if let Some(ref target) = event.branch_name {
                        payload.insert(
                            "target".to_string(),
                            serde_json::Value::String(target.clone()),
                        );
                    }
                }
            }
        }
        if let Some(version) = event.commit_version {
            // Use operation-specific version field names
            let version_key = match event.kind {
                BranchOpKind::Fork => "fork_version",
                BranchOpKind::Merge => "merge_version",
                _ => "version",
            };
            payload.insert(
                version_key.to_string(),
                serde_json::Value::Number(version.0.into()),
            );
        }
        if let Some(ref tag_name) = event.tag_name {
            payload.insert(
                "tag".to_string(),
                serde_json::Value::String(tag_name.clone()),
            );
        }
        if let Some(ref message) = event.message {
            payload.insert(
                "message".to_string(),
                serde_json::Value::String(message.clone()),
            );
        }
        if let Some(ref creator) = event.creator {
            payload.insert(
                "creator".to_string(),
                serde_json::Value::String(creator.clone()),
            );
        }

        // Merge-specific fields
        if let Some(ref strategy) = event.merge_strategy {
            payload.insert(
                "strategy".to_string(),
                serde_json::Value::String(strategy.clone()),
            );
        }

        // Keys applied/deleted (merge, cherry-pick)
        if let Some(keys_applied) = event.keys_applied {
            payload.insert(
                "keys_applied".to_string(),
                serde_json::Value::Number(keys_applied.into()),
            );
        }
        if let Some(keys_deleted) = event.keys_deleted {
            payload.insert(
                "keys_deleted".to_string(),
                serde_json::Value::Number(keys_deleted.into()),
            );
        }

        // Revert-specific fields
        if let Some(keys_reverted) = event.keys_reverted {
            payload.insert(
                "keys_reverted".to_string(),
                serde_json::Value::Number(keys_reverted.into()),
            );
        }
        if let Some(from_version) = event.from_version {
            payload.insert(
                "from_version".to_string(),
                serde_json::Value::Number(from_version.0.into()),
            );
        }
        if let Some(to_version) = event.to_version {
            payload.insert(
                "to_version".to_string(),
                serde_json::Value::Number(to_version.0.into()),
            );
        }

        // Convert to core Value
        let core_payload: strata_core::value::Value = serde_json::Value::Object(payload).into();

        // Append to audit log (best-effort)
        if let Err(e) = event_log.append(&system_branch_id, "default", event_type, core_payload) {
            tracing::warn!(
                target: "strata::audit",
                event_type,
                error = %e,
                "Failed to emit audit event via observer"
            );
        }

        Ok(())
    }
}

// =============================================================================
// Per-Database BranchDagHook Implementation
// =============================================================================

use strata_core::id::CommitVersion;
use strata_engine::database::dag_hook::{
    AncestryEntry, BranchDagError, BranchDagErrorKind, BranchDagHook, DagEvent, DagEventKind,
    MergeBaseResult,
};

fn node_props<'a>(
    node: &'a NodeData,
    node_id: &str,
) -> Result<&'a serde_json::Map<String, serde_json::Value>, BranchDagError> {
    node.properties
        .as_ref()
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("DAG node '{}' is missing object properties", node_id),
            )
        })
}

fn node_timestamp(node: &NodeData) -> u64 {
    node.properties
        .as_ref()
        .and_then(|value| value.as_object())
        .and_then(|props| {
            props
                .get("timestamp")
                .or_else(|| props.get("deleted_at"))
                .or_else(|| props.get("created_at"))
                .or_else(|| props.get("updated_at"))
                .and_then(|value| value.as_u64())
        })
        .unwrap_or(0)
}

fn branch_lifecycle_events(branch_name: &str, node: &NodeData) -> Vec<(u64, DagEvent)> {
    let props = match node.properties.as_ref().and_then(|value| value.as_object()) {
        Some(props) => props,
        None => return Vec::new(),
    };

    let branch_id = resolve_branch_name(branch_name);
    let mut events = Vec::new();

    let mut create = DagEvent::create(branch_id, branch_name);
    create.message = props
        .get("message")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    create.creator = props
        .get("creator")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    events.push((
        props
            .get("created_at")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        create,
    ));

    if props.get("status").and_then(|value| value.as_str()) == Some("deleted") {
        let delete = DagEvent::delete(branch_id, branch_name);
        let timestamp = props
            .get("deleted_at")
            .or_else(|| props.get("updated_at"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        events.push((timestamp, delete));
    }

    events
}

fn deserialize_prop<T: serde::de::DeserializeOwned>(
    props: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<T>, BranchDagError> {
    let Some(value) = props.get(key) else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|e| {
            BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("failed to decode '{}' from DAG node: {}", key, e),
            )
        })
}

fn require_version(
    props: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    node_id: &str,
) -> Result<CommitVersion, BranchDagError> {
    props
        .get(key)
        .and_then(|value| value.as_u64())
        .map(CommitVersion)
        .ok_or_else(|| {
            BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("DAG node '{}' is missing '{}'", node_id, key),
            )
        })
}

fn single_neighbor(
    graph_store: &GraphStore,
    system_id: BranchId,
    node_id: &str,
    direction: crate::types::Direction,
    edge_type: &str,
) -> Result<String, BranchDagError> {
    let neighbors = graph_store
        .neighbors(
            system_id,
            GRAPH_SPACE,
            BRANCH_DAG_GRAPH,
            node_id,
            direction,
            Some(edge_type),
        )
        .map_err(|e| BranchDagError::new(BranchDagErrorKind::ReadFailed, e.to_string()))?;
    neighbors
        .first()
        .map(|neighbor| neighbor.node_id.clone())
        .ok_or_else(|| {
            BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("DAG node '{}' is missing '{}' neighbor", node_id, edge_type),
            )
        })
}

fn branch_name_from_neighbor(
    graph_store: &GraphStore,
    system_id: BranchId,
    node_id: &str,
    direction: crate::types::Direction,
    edge_type: &str,
) -> Result<String, BranchDagError> {
    let neighbor_id = single_neighbor(graph_store, system_id, node_id, direction, edge_type)?;
    let node = graph_store
        .get_node(system_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &neighbor_id)
        .map_err(|e| BranchDagError::new(BranchDagErrorKind::ReadFailed, e.to_string()))?
        .ok_or_else(|| {
            BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("DAG branch node '{}' disappeared during read", neighbor_id),
            )
        })?;
    Ok(branch_name_from_node(&neighbor_id, &node))
}

fn event_node_to_dag_event(
    graph_store: &GraphStore,
    system_id: BranchId,
    node_id: &str,
    node: &NodeData,
) -> Result<DagEvent, BranchDagError> {
    let props = node_props(node, node_id)?;
    let message = props
        .get("message")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let creator = props
        .get("creator")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);

    let mut event = match node.object_type.as_deref() {
        Some("fork") => {
            let parent =
                branch_name_prop(props, "parent_branch").unwrap_or(branch_name_from_neighbor(
                    graph_store,
                    system_id,
                    node_id,
                    crate::types::Direction::Incoming,
                    "parent",
                )?);
            let child =
                branch_name_prop(props, "child_branch").unwrap_or(branch_name_from_neighbor(
                    graph_store,
                    system_id,
                    node_id,
                    crate::types::Direction::Outgoing,
                    "child",
                )?);
            DagEvent::fork(
                resolve_branch_name(&child),
                child,
                resolve_branch_name(&parent),
                parent,
                require_version(props, "fork_version", node_id)?,
            )
        }
        Some("merge") => {
            let source =
                branch_name_prop(props, "source_branch").unwrap_or(branch_name_from_neighbor(
                    graph_store,
                    system_id,
                    node_id,
                    crate::types::Direction::Incoming,
                    "source",
                )?);
            let target =
                branch_name_prop(props, "target_branch").unwrap_or(branch_name_from_neighbor(
                    graph_store,
                    system_id,
                    node_id,
                    crate::types::Direction::Outgoing,
                    "target",
                )?);
            let info = deserialize_prop::<MergeInfo>(props, "merge_info")?.unwrap_or(MergeInfo {
                source: source.clone(),
                target: target.clone(),
                keys_applied: props
                    .get("keys_applied")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0),
                keys_deleted: props
                    .get("keys_deleted")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0),
                conflicts: Vec::new(),
                spaces_merged: props
                    .get("spaces_merged")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0),
                merge_version: Some(require_version(props, "merge_version", node_id)?.0),
            });
            let strategy = match props
                .get("strategy")
                .and_then(|value| value.as_str())
                .unwrap_or("last_writer_wins")
            {
                "strict" => MergeStrategy::Strict,
                _ => MergeStrategy::LastWriterWins,
            };
            DagEvent::merge(
                resolve_branch_name(&target),
                target,
                resolve_branch_name(&source),
                source,
                require_version(props, "merge_version", node_id)?,
                info,
                strategy,
            )
        }
        Some("revert") => {
            let info =
                deserialize_prop::<RevertInfo>(props, "revert_info")?.unwrap_or(RevertInfo {
                    branch: props
                        .get("branch")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            BranchDagError::new(
                                BranchDagErrorKind::Corrupted,
                                format!("DAG node '{}' is missing branch name", node_id),
                            )
                        })?
                        .to_string(),
                    from_version: deserialize_prop::<CommitVersion>(props, "from_version")?
                        .ok_or_else(|| {
                            BranchDagError::new(
                                BranchDagErrorKind::Corrupted,
                                format!("DAG node '{}' is missing from_version", node_id),
                            )
                        })?,
                    to_version: deserialize_prop::<CommitVersion>(props, "to_version")?
                        .ok_or_else(|| {
                            BranchDagError::new(
                                BranchDagErrorKind::Corrupted,
                                format!("DAG node '{}' is missing to_version", node_id),
                            )
                        })?,
                    keys_reverted: props
                        .get("keys_reverted")
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0),
                    revert_version: Some(require_version(props, "revert_version", node_id)?),
                });
            DagEvent::revert(
                resolve_branch_name(&info.branch),
                info.branch.clone(),
                require_version(props, "revert_version", node_id)?,
                info,
            )
        }
        Some("cherry_pick") => {
            let source =
                branch_name_prop(props, "source_branch").unwrap_or(branch_name_from_neighbor(
                    graph_store,
                    system_id,
                    node_id,
                    crate::types::Direction::Incoming,
                    "cherry_pick_source",
                )?);
            let target =
                branch_name_prop(props, "target_branch").unwrap_or(branch_name_from_neighbor(
                    graph_store,
                    system_id,
                    node_id,
                    crate::types::Direction::Outgoing,
                    "cherry_pick_target",
                )?);
            let info = deserialize_prop::<CherryPickInfo>(props, "cherry_pick_info")?.unwrap_or(
                CherryPickInfo {
                    source: source.clone(),
                    target: target.clone(),
                    keys_applied: props
                        .get("keys_applied")
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0),
                    keys_deleted: props
                        .get("keys_deleted")
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0),
                    cherry_pick_version: Some(
                        require_version(props, "cherry_pick_version", node_id)?.0,
                    ),
                },
            );
            DagEvent::cherry_pick(
                resolve_branch_name(&target),
                target,
                resolve_branch_name(&source),
                source,
                require_version(props, "cherry_pick_version", node_id)?,
                info,
            )
        }
        Some(other) => {
            return Err(BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("unknown DAG event node type '{}'", other),
            ));
        }
        None => {
            return Err(BranchDagError::new(
                BranchDagErrorKind::Corrupted,
                format!("DAG node '{}' is missing object_type", node_id),
            ));
        }
    };

    event.message = message;
    event.creator = creator;
    Ok(event)
}

/// Per-database implementation of `BranchDagHook`.
///
/// Records branch lifecycle events to the `_branch_dag` graph and provides
/// queries for merge-base, log, and ancestry. This hook is fail-fast: errors
/// propagate and abort the calling branch operation.
struct GraphBranchDagHook {
    db: Weak<Database>,
}

impl GraphBranchDagHook {
    fn new(db: Arc<Database>) -> Self {
        Self {
            db: Arc::downgrade(&db),
        }
    }

    /// Map a StrataError to BranchDagError.
    fn map_write_error(e: StrataError) -> BranchDagError {
        BranchDagError::new(BranchDagErrorKind::WriteFailed, e.to_string())
    }

    fn upgrade_db(&self) -> Result<Arc<Database>, BranchDagError> {
        self.db.upgrade().ok_or_else(|| {
            BranchDagError::new(
                BranchDagErrorKind::Other,
                "database dropped while branch DAG hook was still referenced",
            )
        })
    }
}

impl BranchDagHook for GraphBranchDagHook {
    fn name(&self) -> &'static str {
        "graph"
    }

    fn record_event(&self, event: &DagEvent) -> Result<(), BranchDagError> {
        use strata_core::branch_dag::is_system_branch;

        let db = self.upgrade_db()?;

        // Skip system branch events (same guard as the old global hooks).
        if is_system_branch(&event.branch_name) {
            return Ok(());
        }
        if let Some(ref source) = event.source_branch_name {
            if is_system_branch(source) {
                return Ok(());
            }
        }

        match event.kind {
            DagEventKind::BranchCreate => {
                dag_add_branch(
                    &db,
                    &event.branch_name,
                    event.message.as_deref(),
                    event.creator.as_deref(),
                )
                .map_err(Self::map_write_error)?;
            }
            DagEventKind::BranchDelete => {
                dag_mark_deleted(&db, &event.branch_name).map_err(Self::map_write_error)?;
            }
            DagEventKind::Fork => {
                let source = event.source_branch_name.as_deref().ok_or_else(|| {
                    BranchDagError::new(
                        BranchDagErrorKind::Other,
                        "fork event missing source branch name",
                    )
                })?;
                dag_record_fork(
                    &db,
                    source,
                    &event.branch_name,
                    Some(event.commit_version.0),
                    event.message.as_deref(),
                    event.creator.as_deref(),
                )
                .map_err(Self::map_write_error)?;
            }
            DagEventKind::Merge => {
                let merge_info = event.merge_info.as_ref().ok_or_else(|| {
                    BranchDagError::new(BranchDagErrorKind::Other, "merge event missing MergeInfo")
                })?;
                let source = event.source_branch_name.as_deref().ok_or_else(|| {
                    BranchDagError::new(
                        BranchDagErrorKind::Other,
                        "merge event missing source branch name",
                    )
                })?;
                dag_record_merge(
                    &db,
                    source,
                    &event.branch_name,
                    merge_info,
                    event.strategy.as_deref(),
                    event.message.as_deref(),
                    event.creator.as_deref(),
                )
                .map_err(Self::map_write_error)?;
            }
            DagEventKind::Revert => {
                let revert_info = event.revert_info.as_ref().ok_or_else(|| {
                    BranchDagError::new(
                        BranchDagErrorKind::Other,
                        "revert event missing RevertInfo",
                    )
                })?;
                dag_record_revert(
                    &db,
                    revert_info,
                    event.message.as_deref(),
                    event.creator.as_deref(),
                )
                .map_err(Self::map_write_error)?;
            }
            DagEventKind::CherryPick => {
                let cherry_pick_info = event.cherry_pick_info.as_ref().ok_or_else(|| {
                    BranchDagError::new(
                        BranchDagErrorKind::Other,
                        "cherry-pick event missing CherryPickInfo",
                    )
                })?;
                let source = event.source_branch_name.as_deref().ok_or_else(|| {
                    BranchDagError::new(
                        BranchDagErrorKind::Other,
                        "cherry-pick event missing source branch name",
                    )
                })?;
                dag_record_cherry_pick(&db, source, &event.branch_name, cherry_pick_info)
                    .map_err(Self::map_write_error)?;
            }
            // DagEventKind is non-exhaustive; handle future variants gracefully.
            _ => {
                tracing::debug!(
                    target: "strata::branch_dag",
                    kind = ?event.kind,
                    "unknown DAG event kind, skipping"
                );
            }
        }

        Ok(())
    }

    fn find_merge_base(
        &self,
        branch_a: &str,
        branch_b: &str,
    ) -> Result<Option<MergeBaseResult>, BranchDagError> {
        let db = self.upgrade_db()?;

        // Check for previous merge first (takes priority over fork)
        if let Ok(Some(merge_version)) = find_last_merge_version(&db, branch_a, branch_b) {
            // The merge was into the target branch, so that's the merge base
            return Ok(Some(MergeBaseResult {
                branch_id: resolve_branch_name(branch_b),
                branch_name: branch_b.to_string(),
                commit_version: CommitVersion(merge_version),
            }));
        }

        // Check for fork relationship
        if let Ok(Some((child_name, fork_version))) = find_fork_version(&db, branch_a, branch_b) {
            // Return the forked-from branch as the merge base
            let (base_branch_name, base_name) = if child_name == branch_a {
                (branch_b, branch_b.to_string()) // a was forked from b
            } else {
                (branch_a, branch_a.to_string()) // b was forked from a
            };
            return Ok(Some(MergeBaseResult {
                branch_id: resolve_branch_name(base_branch_name),
                branch_name: base_name,
                commit_version: CommitVersion(fork_version),
            }));
        }

        // No merge base found
        Ok(None)
    }

    fn log(&self, branch: &str, limit: usize) -> Result<Vec<DagEvent>, BranchDagError> {
        let db = self.upgrade_db()?;
        let graph_store = GraphStore::new(db);
        let system_id = resolve_branch_name(SYSTEM_BRANCH);
        let branch_node_id = dag_branch_node_id(branch);
        let mut timeline: Vec<(u64, DagEvent)> = Vec::new();

        let branch_node = graph_store
            .get_node(system_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &branch_node_id)
            .map_err(|e| BranchDagError::new(BranchDagErrorKind::ReadFailed, e.to_string()))?
            .ok_or_else(|| BranchDagError::branch_not_found(branch))?;
        timeline.extend(branch_lifecycle_events(branch, &branch_node));

        let mut event_ids = std::collections::BTreeSet::new();
        for direction in [
            crate::types::Direction::Incoming,
            crate::types::Direction::Outgoing,
        ] {
            let neighbors = graph_store
                .neighbors(
                    system_id,
                    GRAPH_SPACE,
                    BRANCH_DAG_GRAPH,
                    &branch_node_id,
                    direction,
                    None,
                )
                .map_err(|e| BranchDagError::new(BranchDagErrorKind::ReadFailed, e.to_string()))?;
            for neighbor in neighbors {
                event_ids.insert(neighbor.node_id);
            }
        }

        for event_id in event_ids {
            let node = graph_store
                .get_node(system_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, &event_id)
                .map_err(|e| BranchDagError::new(BranchDagErrorKind::ReadFailed, e.to_string()))?
                .ok_or_else(|| {
                    BranchDagError::new(
                        BranchDagErrorKind::Corrupted,
                        format!("DAG event node '{}' disappeared during log read", event_id),
                    )
                })?;
            if node.object_type.as_deref() == Some("branch") {
                continue;
            }
            let event = event_node_to_dag_event(&graph_store, system_id, &event_id, &node)?;
            timeline.push((node_timestamp(&node), event));
        }

        timeline.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(timeline
            .into_iter()
            .take(limit)
            .map(|(_, event)| event)
            .collect())
    }

    fn ancestors(&self, branch: &str) -> Result<Vec<AncestryEntry>, BranchDagError> {
        let db = self.upgrade_db()?;
        let mut ancestors = Vec::new();
        let mut current = branch.to_string();
        let mut visited = std::collections::HashSet::new();

        // Walk up the parent chain via fork relationships
        while let Ok(Some(fork)) = find_fork_origin(&db, &current) {
            if visited.contains(&fork.parent) {
                break; // Avoid infinite loop
            }
            visited.insert(fork.parent.clone());

            let fork_version = fork.fork_version.ok_or_else(|| {
                BranchDagError::new(
                    BranchDagErrorKind::Corrupted,
                    format!(
                        "fork origin for branch '{}' is missing fork_version",
                        current
                    ),
                )
            })?;
            let parent_id = resolve_branch_name(&fork.parent);
            ancestors.push(AncestryEntry {
                branch_id: parent_id,
                branch_name: fork.parent.clone(),
                relation: DagEventKind::Fork,
                commit_version: CommitVersion(fork_version),
            });
            current = fork.parent;
        }

        Ok(ancestors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_engine::database::{BranchDagHook, DagEventKind, OpenSpec};
    use strata_engine::SearchSubsystem;

    fn setup() -> Arc<Database> {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
        init_system_branch(&db);
        db
    }

    #[test]
    fn system_branch_auto_created() {
        let db = setup();
        assert!(db.branches().exists(SYSTEM_BRANCH).unwrap());
    }

    #[test]
    fn branch_dag_graph_exists() {
        let db = setup();
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let meta = graph_store
            .get_graph_meta(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH)
            .unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn init_does_not_seed_default_branch() {
        let db = setup();
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "default")
            .unwrap();
        assert!(node.is_none());
    }

    #[test]
    fn init_is_idempotent() {
        let db = setup();
        // cache() already called init once; call again explicitly
        init_system_branch(&db);
        assert!(db.branches().exists(SYSTEM_BRANCH).unwrap());
        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "default")
            .unwrap();
        assert!(node.is_none());
    }

    #[test]
    fn open_runtime_bootstraps_configured_default_branch_through_dag() {
        let db = Database::open_runtime(
            OpenSpec::cache()
                .with_subsystem(GraphSubsystem)
                .with_default_branch("main"),
        )
        .unwrap();

        assert!(db.branches().exists("main").unwrap());

        let graph_store = GraphStore::new(db.clone());
        let system_id = resolve_branch_name(SYSTEM_BRANCH);
        assert!(
            graph_store
                .get_node(system_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "main")
                .unwrap()
                .is_some(),
            "configured default branch must have a DAG node"
        );
        assert!(
            graph_store
                .get_node(system_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "default")
                .unwrap()
                .is_none(),
            "graph bootstrap must not inject a stray literal default node"
        );

        let log = db.branches().log("main", 10).unwrap();
        assert!(
            log.iter()
                .any(|event| event.kind == DagEventKind::BranchCreate),
            "configured default branch must be created through BranchService so the DAG log works"
        );
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
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "test-add")
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
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, event_id.as_str())
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
                GRAPH_SPACE,
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
                GRAPH_SPACE,
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

        let merge_info = MergeInfo {
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
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, event_id.as_str())
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
                GRAPH_SPACE,
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
                GRAPH_SPACE,
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
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "del-test")
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

        let merge_info = MergeInfo {
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
    fn slash_branch_names_round_trip_through_dag() {
        let db = setup();
        let parent = "feature/main";
        let child = "feature/new-model";

        dag_add_branch(&db, parent, Some("parent"), Some("alice")).unwrap();
        dag_add_branch(&db, child, None, None).unwrap();
        dag_record_fork(
            &db,
            parent,
            child,
            Some(12),
            Some("fork slash"),
            Some("alice"),
        )
        .unwrap();

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        assert!(graph_store
            .get_node(
                branch_id,
                GRAPH_SPACE,
                BRANCH_DAG_GRAPH,
                &dag_branch_node_id(parent),
            )
            .unwrap()
            .is_some());
        assert!(graph_store
            .get_node(
                branch_id,
                GRAPH_SPACE,
                BRANCH_DAG_GRAPH,
                &dag_branch_node_id(child),
            )
            .unwrap()
            .is_some());

        let info = dag_get_branch_info(&db, child).unwrap().unwrap();
        let fork = info.forked_from.unwrap();
        assert_eq!(fork.parent, parent);
        assert_eq!(fork.child, child);

        let children = find_children(&db, parent).unwrap();
        assert_eq!(children, vec![child.to_string()]);

        let hook = GraphBranchDagHook::new(db);
        let log = hook.log(child, usize::MAX).unwrap();
        assert!(log.iter().any(|event| event.kind == DagEventKind::Fork));
    }

    #[test]
    fn ensure_branch_node_exists_is_idempotent() {
        let db = setup();
        ensure_branch_node_exists(&db, "idempotent-test").unwrap();
        ensure_branch_node_exists(&db, "idempotent-test").unwrap();

        let graph_store = GraphStore::new(db.clone());
        let branch_id = resolve_branch_name(SYSTEM_BRANCH);
        let node = graph_store
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "idempotent-test")
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
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, "msg-test")
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

        let merge_info = MergeInfo {
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
            .get_node(branch_id, GRAPH_SPACE, BRANCH_DAG_GRAPH, event_id.as_str())
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

        let merge_info = MergeInfo {
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
        let merge_info_ab = MergeInfo {
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
        let merge_info_ba = MergeInfo {
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

    #[test]
    fn test_hook_log_reconstructs_branch_history() {
        let db = setup();
        dag_add_branch(&db, "log-target", Some("create target"), Some("alice")).unwrap();
        dag_add_branch(&db, "log-source", Some("create source"), Some("alice")).unwrap();
        dag_record_fork(
            &db,
            "log-target",
            "log-source",
            Some(7),
            Some("fork"),
            Some("alice"),
        )
        .unwrap();
        dag_record_merge(
            &db,
            "log-source",
            "log-target",
            &MergeInfo {
                source: "log-source".to_string(),
                target: "log-target".to_string(),
                keys_applied: 3,
                conflicts: vec![],
                spaces_merged: 1,
                keys_deleted: 1,
                merge_version: Some(11),
            },
            Some("last_writer_wins"),
            Some("merge"),
            Some("bob"),
        )
        .unwrap();

        let hook = GraphBranchDagHook::new(db.clone());
        let log = hook.log("log-target", usize::MAX).unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].kind, DagEventKind::Merge);
        assert!(log
            .iter()
            .any(|event| event.kind == DagEventKind::BranchCreate));
        assert_eq!(hook.log("log-target", 1).unwrap().len(), 1);
    }

    #[test]
    fn test_hook_ancestors_follow_fork_chain() {
        let db = setup();
        dag_add_branch(&db, "root", None, None).unwrap();
        dag_add_branch(&db, "mid", None, None).unwrap();
        dag_add_branch(&db, "leaf", None, None).unwrap();
        dag_record_fork(&db, "root", "mid", Some(5), None, None).unwrap();
        dag_record_fork(&db, "mid", "leaf", Some(9), None, None).unwrap();

        let hook = GraphBranchDagHook::new(db.clone());
        let ancestors = hook.ancestors("leaf").unwrap();
        assert_eq!(ancestors.len(), 2);
        assert_eq!(ancestors[0].branch_name, "mid");
        assert_eq!(ancestors[0].relation, DagEventKind::Fork);
        assert_eq!(ancestors[0].commit_version, CommitVersion(9));
        assert_eq!(ancestors[1].branch_name, "root");
        assert_eq!(ancestors[1].relation, DagEventKind::Fork);
        assert_eq!(ancestors[1].commit_version, CommitVersion(5));
    }
}
