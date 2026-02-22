//! Referential integrity hooks for graph-entity bindings.
//!
//! When an entity (KV key, JSON doc) is deleted, this module handles cleanup
//! of any graph nodes bound to it via entity_ref.

use strata_core::types::BranchId;
use strata_core::StrataResult;

use super::types::CascadePolicy;
use super::GraphStore;

impl GraphStore {
    /// Handle deletion of an entity that may be bound to graph nodes.
    ///
    /// Looks up all (graph, node_id) pairs bound to the given entity_ref URI,
    /// then applies each graph's cascade policy:
    /// - Cascade: remove the node and all its edges
    /// - Detach: keep the node but clear its entity_ref
    /// - Ignore: do nothing
    ///
    /// Errors during individual graph operations are logged but don't propagate,
    /// so the caller's delete always succeeds.
    pub fn on_entity_deleted(&self, branch_id: BranchId, entity_ref_uri: &str) -> StrataResult<()> {
        let bindings = self.nodes_for_entity(branch_id, entity_ref_uri)?;

        for (graph, node_id) in bindings {
            let policy = self
                .get_graph_meta(branch_id, &graph)?
                .map(|m| m.cascade_policy)
                .unwrap_or(CascadePolicy::Ignore);

            let result = match policy {
                CascadePolicy::Cascade => self.remove_node(branch_id, &graph, &node_id),
                CascadePolicy::Detach => {
                    // Read current node, clear entity_ref, re-write
                    if let Some(mut data) = self.get_node(branch_id, &graph, &node_id)? {
                        data.entity_ref = None;
                        self.add_node(branch_id, &graph, &node_id, data)
                    } else {
                        Ok(())
                    }
                }
                CascadePolicy::Ignore => Ok(()),
            };

            // Best-effort: log but don't propagate errors
            if let Err(e) = result {
                tracing::warn!(
                    graph = %graph,
                    node_id = %node_id,
                    entity_ref = %entity_ref_uri,
                    error = %e,
                    "Graph integrity hook failed"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::graph::types::*;
    use std::sync::Arc;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::cache().unwrap();
        let gs = GraphStore::new(db.clone());
        (db, gs)
    }

    fn branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    #[test]
    fn cascade_removes_node_and_edges() {
        let (_db, gs) = setup();
        let b = branch();

        gs.create_graph(
            b,
            "g",
            Some(GraphMeta {
                cascade_policy: CascadePolicy::Cascade,
            }),
        )
        .unwrap();
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: None,
            },
        )
        .unwrap();
        gs.add_node(b, "g", "n2", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "n1", "n2", "E", EdgeData::default())
            .unwrap();

        gs.on_entity_deleted(b, "kv://main/key1").unwrap();

        assert!(gs.get_node(b, "g", "n1").unwrap().is_none());
        assert!(gs.get_edge(b, "g", "n1", "n2", "E").unwrap().is_none());
        // n2 survives
        assert!(gs.get_node(b, "g", "n2").unwrap().is_some());
    }

    #[test]
    fn detach_keeps_node_clears_ref() {
        let (_db, gs) = setup();
        let b = branch();

        gs.create_graph(
            b,
            "g",
            Some(GraphMeta {
                cascade_policy: CascadePolicy::Detach,
            }),
        )
        .unwrap();
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: Some(serde_json::json!({"important": true})),
            },
        )
        .unwrap();

        gs.on_entity_deleted(b, "kv://main/key1").unwrap();

        let node = gs.get_node(b, "g", "n1").unwrap().unwrap();
        assert!(node.entity_ref.is_none());
        assert_eq!(
            node.properties,
            Some(serde_json::json!({"important": true}))
        );
    }

    #[test]
    fn ignore_does_nothing() {
        let (_db, gs) = setup();
        let b = branch();

        gs.create_graph(
            b,
            "g",
            Some(GraphMeta {
                cascade_policy: CascadePolicy::Ignore,
            }),
        )
        .unwrap();
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                entity_ref: Some("kv://main/key1".to_string()),
                properties: None,
            },
        )
        .unwrap();

        gs.on_entity_deleted(b, "kv://main/key1").unwrap();

        let node = gs.get_node(b, "g", "n1").unwrap().unwrap();
        assert_eq!(node.entity_ref, Some("kv://main/key1".to_string()));
    }

    #[test]
    fn multiple_graphs_different_policies() {
        let (_db, gs) = setup();
        let b = branch();

        gs.create_graph(
            b,
            "cascade_g",
            Some(GraphMeta {
                cascade_policy: CascadePolicy::Cascade,
            }),
        )
        .unwrap();
        gs.create_graph(
            b,
            "detach_g",
            Some(GraphMeta {
                cascade_policy: CascadePolicy::Detach,
            }),
        )
        .unwrap();

        let uri = "kv://main/shared";
        gs.add_node(
            b,
            "cascade_g",
            "n1",
            NodeData {
                entity_ref: Some(uri.to_string()),
                properties: None,
            },
        )
        .unwrap();
        gs.add_node(
            b,
            "detach_g",
            "n1",
            NodeData {
                entity_ref: Some(uri.to_string()),
                properties: None,
            },
        )
        .unwrap();

        gs.on_entity_deleted(b, uri).unwrap();

        // cascade_g: node removed
        assert!(gs.get_node(b, "cascade_g", "n1").unwrap().is_none());
        // detach_g: node kept, ref cleared
        let node = gs.get_node(b, "detach_g", "n1").unwrap().unwrap();
        assert!(node.entity_ref.is_none());
    }

    #[test]
    fn unbound_entity_is_noop() {
        let (_db, gs) = setup();
        let b = branch();

        gs.create_graph(b, "g", None).unwrap();
        // No nodes bound to this URI
        gs.on_entity_deleted(b, "kv://main/nothing").unwrap();
    }
}
