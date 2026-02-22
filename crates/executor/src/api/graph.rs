//! Graph operations on the Strata API surface.

use super::Strata;
use crate::types::{GraphBfsResult, GraphNeighborHit};
use crate::{Command, Error, Output, Result, Value};

impl Strata {
    // =========================================================================
    // Graph Lifecycle
    // =========================================================================

    /// Create a new graph on the current branch.
    pub fn graph_create(&self, graph: &str) -> Result<()> {
        self.graph_create_with_policy(graph, None)
    }

    /// Create a new graph with an explicit cascade policy.
    ///
    /// `cascade_policy` can be `"cascade"`, `"detach"`, or `"ignore"` (default).
    pub fn graph_create_with_policy(
        &self,
        graph: &str,
        cascade_policy: Option<&str>,
    ) -> Result<()> {
        match self.executor.execute(Command::GraphCreate {
            branch: self.branch_id(),
            graph: graph.to_string(),
            cascade_policy: cascade_policy.map(|s| s.to_string()),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphCreate".into(),
            }),
        }
    }

    /// Delete a graph and all its nodes/edges.
    pub fn graph_delete(&self, graph: &str) -> Result<()> {
        match self.executor.execute(Command::GraphDelete {
            branch: self.branch_id(),
            graph: graph.to_string(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphDelete".into(),
            }),
        }
    }

    /// List all graph names on the current branch.
    pub fn graph_list(&self) -> Result<Vec<String>> {
        match self.executor.execute(Command::GraphList {
            branch: self.branch_id(),
        })? {
            Output::Keys(names) => Ok(names),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphList".into(),
            }),
        }
    }

    /// Get graph metadata as a Value, or None if graph doesn't exist.
    pub fn graph_get_meta(&self, graph: &str) -> Result<Option<Value>> {
        match self.executor.execute(Command::GraphGetMeta {
            branch: self.branch_id(),
            graph: graph.to_string(),
        })? {
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphGetMeta".into(),
            }),
        }
    }

    // =========================================================================
    // Node CRUD
    // =========================================================================

    /// Add or update a node in a graph.
    pub fn graph_add_node(
        &self,
        graph: &str,
        node_id: &str,
        entity_ref: Option<&str>,
        properties: Option<Value>,
    ) -> Result<()> {
        match self.executor.execute(Command::GraphAddNode {
            branch: self.branch_id(),
            graph: graph.to_string(),
            node_id: node_id.to_string(),
            entity_ref: entity_ref.map(|s| s.to_string()),
            properties,
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphAddNode".into(),
            }),
        }
    }

    /// Get a node's data as a Value, or None if it doesn't exist.
    pub fn graph_get_node(&self, graph: &str, node_id: &str) -> Result<Option<Value>> {
        match self.executor.execute(Command::GraphGetNode {
            branch: self.branch_id(),
            graph: graph.to_string(),
            node_id: node_id.to_string(),
        })? {
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphGetNode".into(),
            }),
        }
    }

    /// Remove a node and all its incident edges.
    pub fn graph_remove_node(&self, graph: &str, node_id: &str) -> Result<()> {
        match self.executor.execute(Command::GraphRemoveNode {
            branch: self.branch_id(),
            graph: graph.to_string(),
            node_id: node_id.to_string(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphRemoveNode".into(),
            }),
        }
    }

    /// List all node IDs in a graph.
    pub fn graph_list_nodes(&self, graph: &str) -> Result<Vec<String>> {
        match self.executor.execute(Command::GraphListNodes {
            branch: self.branch_id(),
            graph: graph.to_string(),
        })? {
            Output::Keys(ids) => Ok(ids),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphListNodes".into(),
            }),
        }
    }

    // =========================================================================
    // Edge CRUD
    // =========================================================================

    /// Add or update an edge in a graph.
    pub fn graph_add_edge(
        &self,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        weight: Option<f64>,
        properties: Option<Value>,
    ) -> Result<()> {
        match self.executor.execute(Command::GraphAddEdge {
            branch: self.branch_id(),
            graph: graph.to_string(),
            src: src.to_string(),
            dst: dst.to_string(),
            edge_type: edge_type.to_string(),
            weight,
            properties,
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphAddEdge".into(),
            }),
        }
    }

    /// Remove an edge.
    pub fn graph_remove_edge(
        &self,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> Result<()> {
        match self.executor.execute(Command::GraphRemoveEdge {
            branch: self.branch_id(),
            graph: graph.to_string(),
            src: src.to_string(),
            dst: dst.to_string(),
            edge_type: edge_type.to_string(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphRemoveEdge".into(),
            }),
        }
    }

    // =========================================================================
    // Bulk Insert
    // =========================================================================

    /// Bulk insert nodes and edges into a graph using chunked transactions.
    ///
    /// Much faster than individual `graph_add_node`/`graph_add_edge` calls for
    /// loading large datasets.
    ///
    /// Returns `(nodes_inserted, edges_inserted)`.
    #[allow(clippy::type_complexity)]
    pub fn graph_bulk_insert(
        &self,
        graph: &str,
        nodes: &[(&str, Option<&str>, Option<Value>)],
        edges: &[(&str, &str, &str, Option<f64>, Option<Value>)],
    ) -> Result<(u64, u64)> {
        let bulk_nodes: Vec<crate::types::BulkGraphNode> = nodes
            .iter()
            .map(
                |(node_id, entity_ref, properties)| crate::types::BulkGraphNode {
                    node_id: node_id.to_string(),
                    entity_ref: entity_ref.map(|s| s.to_string()),
                    properties: properties.clone(),
                },
            )
            .collect();
        let bulk_edges: Vec<crate::types::BulkGraphEdge> = edges
            .iter()
            .map(
                |(src, dst, edge_type, weight, properties)| crate::types::BulkGraphEdge {
                    src: src.to_string(),
                    dst: dst.to_string(),
                    edge_type: edge_type.to_string(),
                    weight: *weight,
                    properties: properties.clone(),
                },
            )
            .collect();

        match self.executor.execute(Command::GraphBulkInsert {
            branch: self.branch_id(),
            graph: graph.to_string(),
            nodes: bulk_nodes,
            edges: bulk_edges,
            chunk_size: None,
        })? {
            Output::GraphBulkInsertResult {
                nodes_inserted,
                edges_inserted,
            } => Ok((nodes_inserted, edges_inserted)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphBulkInsert".into(),
            }),
        }
    }

    // =========================================================================
    // Traversal
    // =========================================================================

    /// Get neighbors of a node.
    ///
    /// `direction` can be `"outgoing"` (default), `"incoming"`, or `"both"`.
    pub fn graph_neighbors(
        &self,
        graph: &str,
        node_id: &str,
        direction: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<GraphNeighborHit>> {
        match self.executor.execute(Command::GraphNeighbors {
            branch: self.branch_id(),
            graph: graph.to_string(),
            node_id: node_id.to_string(),
            direction: Some(direction.to_string()),
            edge_type: edge_type.map(|s| s.to_string()),
        })? {
            Output::GraphNeighbors(hits) => Ok(hits),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphNeighbors".into(),
            }),
        }
    }

    /// BFS traversal from a start node.
    pub fn graph_bfs(
        &self,
        graph: &str,
        start: &str,
        max_depth: usize,
        max_nodes: Option<usize>,
        edge_types: Option<Vec<String>>,
        direction: Option<&str>,
    ) -> Result<GraphBfsResult> {
        match self.executor.execute(Command::GraphBfs {
            branch: self.branch_id(),
            graph: graph.to_string(),
            start: start.to_string(),
            max_depth,
            max_nodes,
            edge_types,
            direction: direction.map(|s| s.to_string()),
        })? {
            Output::GraphBfs(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GraphBfs".into(),
            }),
        }
    }
}
