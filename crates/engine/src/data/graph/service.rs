//! Graph core service.

use std::collections::BTreeMap;

use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::BranchName;
use crate::commit::CommitOutcome;
use crate::control::ControlPlane;
use crate::data::event::EventSequence;
use crate::data::json::JsonDocumentId;
use crate::data::kv::ProductSpace;
use crate::diagnostics::{EngineError, EngineResult};
use crate::persistence::{
    decode_graph_binding_key, decode_graph_edge_key, decode_graph_metadata_key,
    decode_graph_node_key, decode_graph_reverse_edge_key, encode_event_key,
    encode_graph_binding_key, encode_graph_binding_space_prefix,
    encode_graph_binding_target_prefix, encode_graph_edge_key, encode_graph_edge_prefix,
    encode_graph_incoming_edge_prefix, encode_graph_metadata_key, encode_graph_metadata_prefix,
    encode_graph_node_key, encode_graph_node_prefix, encode_graph_ontology_key,
    encode_graph_outgoing_edge_prefix, encode_graph_reverse_edge_key,
    encode_graph_reverse_edge_prefix, encode_graph_type_index_graph_prefix,
    encode_graph_type_index_key, encode_graph_type_index_type_prefix, encode_json_key,
    encode_kv_key_bytes, CommitPlan, PersistenceReadRow, ReadSelector, RowAddress, RowClass,
    RowMutation, StoragePersistence,
};

use super::{
    decode_graph_binding_record, decode_graph_edge_record, decode_graph_metadata_record,
    decode_graph_node_record, decode_graph_ontology_record, decode_graph_type_index_record,
    encode_graph_binding_record, encode_graph_edge_record, encode_graph_metadata_record,
    encode_graph_node_record, encode_graph_ontology_record, encode_graph_type_index_record,
    GraphAdjacencyIndex, GraphAdjacencyIndexBuilder, GraphAnalyticsBudget, GraphBatchOpOutcome,
    GraphBatchOperation, GraphBatchWrite, GraphBatchWriteOutcome, GraphBinding, GraphBindingPage,
    GraphBindingPrimitive, GraphBindingRecord, GraphBindingTarget, GraphBulkInsertOutcome,
    GraphDeleteOutcome, GraphDeletePolicy, GraphDeletePolicyOutcome, GraphDirection, GraphEdge,
    GraphEdgeRecord, GraphEdgeType, GraphEdgeWriteOutcome, GraphInfo, GraphLinkTypeDef,
    GraphLinkTypeSummary, GraphName, GraphNamePage, GraphNeighbor, GraphNeighborPage, GraphNode,
    GraphNodeId, GraphNodePage, GraphNodeRecord, GraphObjectTypeDef, GraphObjectTypeSummary,
    GraphOntology, GraphOntologyFreezeOutcome, GraphOntologyRecord, GraphOntologySummary,
    GraphOntologyWriteOutcome, GraphTargetStatus, GraphTypeIndexRecord, GraphTypeName,
    GraphWriteOutcome,
};

type EdgeIdentity = (GraphNodeId, GraphEdgeType, GraphNodeId);
type MutationKey = (RowClass, Vec<u8>);

/// Service for graph core operations.
pub struct GraphService<'a> {
    persistence: &'a StoragePersistence,
    control: &'a ControlPlane,
    branch: BranchName,
    space: ProductSpace,
}

impl<'a> GraphService<'a> {
    pub(crate) const fn new(
        persistence: &'a StoragePersistence,
        control: &'a ControlPlane,
        branch: BranchName,
        space: ProductSpace,
    ) -> Self {
        Self {
            persistence,
            control,
            branch,
            space,
        }
    }

    /// Creates a graph, returning the new metadata and the create commit.
    pub fn create_graph(
        &mut self,
        name: GraphName,
    ) -> Result<(GraphInfo, CommitOutcome), EngineError> {
        let record = self.branch_record()?;
        let address = self.metadata_address(&record, &name);
        if self
            .persistence
            .read_row(address.clone(), ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone())
        {
            return Err(EngineError::conflict(
                "already_exists.engine.graph",
                "graph already exists",
            ));
        }
        let metadata = super::GraphMetadataRecord::new(name.clone());
        let commit = self.commit_batch(
            &record,
            vec![RowMutation::put(
                address,
                encode_graph_metadata_record(&metadata)?,
            )],
        )?;
        let info = GraphInfo::new(
            name,
            0,
            0,
            commit.version(),
            commit.timestamp(),
            commit.version(),
            commit.timestamp(),
        );
        Ok((info, commit))
    }

    /// Deletes a graph and all visible graph data rows.
    pub fn delete_graph(&mut self, name: &GraphName) -> Result<GraphDeleteOutcome, EngineError> {
        let record = self.branch_record()?;
        if self
            .graph_metadata_row(&record, name, ReadSelector::Latest)?
            .is_none()
        {
            return Ok(GraphDeleteOutcome::new(name.clone(), false, None));
        }

        let mut mutations = Vec::new();
        mutations.push(RowMutation::delete(self.metadata_address(&record, name)));
        if self
            .ontology_row(&record, name, ReadSelector::Latest)?
            .is_some()
        {
            mutations.push(RowMutation::delete(self.ontology_address(&record, name)));
        }
        for row in self.node_rows(&record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::GraphNode,
                    row.key().to_vec(),
                )));
            }
        }
        for row in self.edge_rows(&record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::GraphEdge,
                    row.key().to_vec(),
                )));
            }
        }
        for row in self.reverse_edge_rows(&record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::GraphReverseEdge,
                    row.key().to_vec(),
                )));
            }
        }
        for row in self.binding_rows_for_space(&record, ReadSelector::Latest)? {
            if row.is_tombstone() {
                continue;
            }
            let (_, graph, _) = decode_graph_binding_key(&self.space, row.key())?;
            if &graph == name {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::GraphBindingIndex,
                    row.key().to_vec(),
                )));
            }
        }
        for row in self.type_index_rows(&record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::GraphTypeIndex,
                    row.key().to_vec(),
                )));
            }
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(GraphDeleteOutcome::new(name.clone(), true, Some(commit)))
    }

    /// Lists visible graphs.
    pub fn list_graphs(
        &mut self,
        cursor: Option<&GraphName>,
        limit: usize,
    ) -> Result<GraphNamePage, EngineError> {
        self.list_graphs_with_selector(cursor, limit, ReadSelector::Latest)
    }

    /// Lists graphs visible at a commit version.
    pub fn list_graphs_at_version(
        &mut self,
        cursor: Option<&GraphName>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphNamePage, EngineError> {
        self.list_graphs_with_selector(cursor, limit, ReadSelector::AtVersion(version))
    }

    /// Lists graphs visible at a timestamp.
    pub fn list_graphs_at(
        &mut self,
        cursor: Option<&GraphName>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphNamePage, EngineError> {
        self.list_graphs_with_selector(cursor, limit, ReadSelector::AtTimestamp(timestamp))
    }

    fn list_graphs_with_selector(
        &mut self,
        cursor: Option<&GraphName>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphNamePage, EngineError> {
        let record = self.branch_record()?;
        if limit == 0 {
            return Ok(GraphNamePage::new(Vec::new(), false, None));
        }
        let mut graphs = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::GraphMetadata,
                encode_graph_metadata_prefix(&self.space),
                selector,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| decode_graph_metadata_key(&self.space, row.key()))
            .collect::<EngineResult<Vec<_>>>()?;
        graphs.sort();
        if let Some(cursor) = cursor {
            graphs.retain(|graph| graph > cursor);
        }
        let has_more = graphs.len() > limit;
        if has_more {
            graphs.truncate(limit);
        }
        let cursor = has_more.then(|| graphs.last().expect("non-empty page").clone());
        Ok(GraphNamePage::new(graphs, has_more, cursor))
    }

    /// Returns graph metadata when the graph exists.
    pub fn graph_info(&mut self, name: &GraphName) -> Result<Option<GraphInfo>, EngineError> {
        self.graph_info_with_selector(name, ReadSelector::Latest)
    }

    /// Returns graph metadata visible at a commit version.
    pub fn graph_info_at_version(
        &mut self,
        name: &GraphName,
        version: CommitVersion,
    ) -> Result<Option<GraphInfo>, EngineError> {
        self.graph_info_with_selector(name, ReadSelector::AtVersion(version))
    }

    /// Returns graph metadata visible at a timestamp.
    pub fn graph_info_at(
        &mut self,
        name: &GraphName,
        timestamp: Timestamp,
    ) -> Result<Option<GraphInfo>, EngineError> {
        self.graph_info_with_selector(name, ReadSelector::AtTimestamp(timestamp))
    }

    fn graph_info_with_selector(
        &mut self,
        name: &GraphName,
        selector: ReadSelector,
    ) -> Result<Option<GraphInfo>, EngineError> {
        let record = self.branch_record()?;
        self.graph_metadata_row(&record, name, selector)?
            .map(|row| self.graph_info_from_row(&record, &row, selector))
            .transpose()
    }

    /// Rejects a relationship binding whose target names a different branch.
    ///
    /// Cross-branch references are forbidden (CLAUDE.md Hard Rule 18;
    /// entity-ref-and-relationship-layer-contract Branch Scope rule 4 / Binding
    /// Decision 6 / conformance test 9). A `None` target branch means "the
    /// node's own branch" and is accepted; an explicit target branch is accepted
    /// only when it equals the node's branch.
    /// Applies an explicit delete policy to every graph fact bound to
    /// `target`, across all graphs in this space. The typical caller
    /// just deleted (or is about to delete) the bound entity.
    ///
    /// Candidates come from the binding reverse index and are verified
    /// against the authoritative node row's binding before any row is
    /// mutated. Cascade deletes the bound nodes and their incident
    /// edges; detach preserves the nodes and removes their bindings;
    /// keep-dangling mutates nothing — traversal reports the target's
    /// status instead.
    pub fn apply_binding_delete_policy(
        &mut self,
        target: &GraphBindingTarget,
        policy: GraphDeletePolicy,
    ) -> Result<GraphDeletePolicyOutcome, EngineError> {
        let record = self.branch_record()?;
        self.validate_binding_target(target)?;

        // Reverse-index candidates, verified against the authoritative
        // node binding (reverse maps are candidate indexes, not truth).
        let mut verified: Vec<(GraphName, GraphNodeId, GraphNodeRecord)> = Vec::new();
        for row in self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphBindingIndex,
            encode_graph_binding_target_prefix(&self.space, target),
            ReadSelector::Latest,
            None,
        )? {
            if row.is_tombstone() {
                continue;
            }
            let (_, graph, node_id) = decode_graph_binding_key(&self.space, row.key())?;
            let Some(node) = self.node_record(&record, &graph, &node_id)? else {
                continue;
            };
            if node.data().binding().map(super::GraphEntityBinding::target) == Some(target) {
                verified.push((graph, node_id, node));
            }
        }
        let nodes_affected = verified.len() as u64;

        let mut mutations = MutationMap::default();
        match policy {
            GraphDeletePolicy::KeepDangling => {}
            GraphDeletePolicy::Detach => {
                for (graph, node_id, node) in &verified {
                    let mut data =
                        super::GraphNodeData::new(node.data().properties().cloned(), None);
                    if let Some(object_type) = node.data().object_type() {
                        data = data.with_object_type(object_type.clone());
                    }
                    let detached = GraphNodeRecord::new(graph.clone(), node_id.clone(), data);
                    mutations.put(
                        self.node_address(&record, graph, node_id),
                        encode_graph_node_record(&detached)?,
                    );
                    mutations.delete(self.binding_address(&record, target, graph, node_id));
                }
            }
            GraphDeletePolicy::Cascade => {
                let mut by_graph: BTreeMap<GraphName, Vec<GraphNodeId>> = BTreeMap::new();
                for (graph, node_id, node) in &verified {
                    mutations.delete(self.node_address(&record, graph, node_id));
                    mutations.delete(self.binding_address(&record, target, graph, node_id));
                    if let Some(object_type) = node.data().object_type() {
                        mutations.delete(self.type_index_address(
                            &record,
                            graph,
                            object_type,
                            node_id,
                        ));
                    }
                    by_graph
                        .entry(graph.clone())
                        .or_default()
                        .push(node_id.clone());
                }
                for (graph, node_ids) in &by_graph {
                    for edge in self
                        .edge_record_map(&record, graph, ReadSelector::Latest)?
                        .into_values()
                    {
                        if node_ids.contains(edge.src()) || node_ids.contains(edge.dst()) {
                            self.delete_edge_mutations(&record, &mut mutations, &edge);
                        }
                    }
                }
            }
        }

        let mutations = mutations.into_mutations();
        let commit = if mutations.is_empty() {
            None
        } else {
            Some(self.commit_batch(&record, mutations)?)
        };
        Ok(GraphDeletePolicyOutcome::new(
            policy,
            nodes_affected,
            commit,
        ))
    }

    /// Resolves the current status of one binding target: whether the
    /// bound entity's row is visible, tombstoned, or absent. Vector and
    /// graph targets use composite addresses and report
    /// [`GraphTargetStatus::Unsupported`].
    pub fn resolve_binding_target(
        &mut self,
        target: &GraphBindingTarget,
    ) -> Result<GraphTargetStatus, EngineError> {
        let record = self.branch_record()?;
        self.binding_target_status(&record, target, ReadSelector::Latest)
    }

    /// Resolves a binding target's status at a commit version.
    pub fn resolve_binding_target_at_version(
        &mut self,
        target: &GraphBindingTarget,
        version: CommitVersion,
    ) -> Result<GraphTargetStatus, EngineError> {
        let record = self.branch_record()?;
        self.binding_target_status(&record, target, ReadSelector::AtVersion(version))
    }

    /// Resolves a binding target's status at a timestamp.
    pub fn resolve_binding_target_at(
        &mut self,
        target: &GraphBindingTarget,
        timestamp: Timestamp,
    ) -> Result<GraphTargetStatus, EngineError> {
        let record = self.branch_record()?;
        self.binding_target_status(&record, target, ReadSelector::AtTimestamp(timestamp))
    }

    /// Point-reads the target's row in its owning capability's row class.
    /// Row existence only: value decoding and interpretation stay with
    /// the owning capability.
    fn binding_target_status(
        &mut self,
        record: &BranchCatalogRecord,
        target: &GraphBindingTarget,
        selector: ReadSelector,
    ) -> Result<GraphTargetStatus, EngineError> {
        let (class, key) = match target.primitive() {
            GraphBindingPrimitive::Kv => (
                RowClass::Kv,
                encode_kv_key_bytes(target.space(), target.key().as_bytes()),
            ),
            GraphBindingPrimitive::Json => {
                let Ok(id) = JsonDocumentId::new(target.key()) else {
                    return Ok(GraphTargetStatus::MalformedTarget);
                };
                (RowClass::Json, encode_json_key(target.space(), &id))
            }
            GraphBindingPrimitive::Event => {
                let Ok(sequence) = target.key().parse::<u64>() else {
                    return Ok(GraphTargetStatus::MalformedTarget);
                };
                (
                    RowClass::Event,
                    encode_event_key(target.space(), EventSequence::new(sequence)),
                )
            }
            GraphBindingPrimitive::Vector | GraphBindingPrimitive::Graph => {
                return Ok(GraphTargetStatus::Unsupported);
            }
        };
        let row = self.persistence.read_row(
            RowAddress::new(record.storage_branch_id(), class, key),
            selector,
        )?;
        Ok(match row {
            None => GraphTargetStatus::Missing,
            Some(row) if row.is_tombstone() => GraphTargetStatus::Deleted,
            Some(_) => GraphTargetStatus::Present,
        })
    }

    fn neighbor_target_status(
        &mut self,
        record: &BranchCatalogRecord,
        node: &GraphNode,
        selector: ReadSelector,
    ) -> Result<Option<GraphTargetStatus>, EngineError> {
        match node.data().binding() {
            Some(binding) => Ok(Some(self.binding_target_status(
                record,
                binding.target(),
                selector,
            )?)),
            None => Ok(None),
        }
    }

    fn validate_binding_target(&self, target: &GraphBindingTarget) -> Result<(), EngineError> {
        if let Some(target_branch) = target.branch() {
            if target_branch != &self.branch {
                return Err(EngineError::unsupported(
                    "unsupported.engine.graph_binding_cross_branch",
                    format!(
                        "graph relationship binding targets branch `{}` but the node lives on branch `{}`; cross-branch bindings are not supported",
                        target_branch.as_str(),
                        self.branch.as_str(),
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Upserts one graph node.
    pub fn upsert_node(
        &mut self,
        graph: &GraphName,
        node_id: GraphNodeId,
        data: super::GraphNodeData,
    ) -> Result<GraphWriteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        if let Some(binding) = data.binding() {
            self.validate_binding_target(binding.target())?;
        }
        if let Some(ontology) = self.frozen_ontology(&record, graph)? {
            ontology.validate_node(&data)?;
        }
        let current = self.node_record(&record, graph, &node_id)?;
        let created = current.is_none();
        let new_record = GraphNodeRecord::new(graph.clone(), node_id.clone(), data);
        let mut mutations = Vec::new();
        if let Some(old) = current.as_ref().and_then(|record| record.data().binding()) {
            if Some(old) != new_record.data().binding() {
                mutations.push(RowMutation::delete(self.binding_address(
                    &record,
                    old.target(),
                    graph,
                    &node_id,
                )));
            }
        }
        // Derived type-index maintenance: drop the old row on retype or
        // untype, (re)write the row while the node declares a type.
        let old_type = current
            .as_ref()
            .and_then(|record| record.data().object_type());
        let new_type = new_record.data().object_type();
        if let Some(old_type) = old_type {
            if Some(old_type) != new_type {
                mutations.push(RowMutation::delete(
                    self.type_index_address(&record, graph, old_type, &node_id),
                ));
            }
        }
        if let Some(new_type) = new_type {
            mutations.push(RowMutation::put(
                self.type_index_address(&record, graph, new_type, &node_id),
                encode_graph_type_index_record(&GraphTypeIndexRecord::new(
                    graph.clone(),
                    new_type.clone(),
                    node_id.clone(),
                ))?,
            ));
        }
        mutations.push(RowMutation::put(
            self.node_address(&record, graph, &node_id),
            encode_graph_node_record(&new_record)?,
        ));
        if let Some(binding) = new_record.data().binding() {
            let binding_record =
                GraphBindingRecord::new(graph.clone(), node_id.clone(), binding.clone());
            mutations.push(RowMutation::put(
                self.binding_address(&record, binding.target(), graph, &node_id),
                encode_graph_binding_record(&binding_record)?,
            ));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(GraphWriteOutcome::new(
            graph.clone(),
            node_id,
            created,
            commit,
        ))
    }

    /// Reads one visible graph node.
    pub fn get_node(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.get_node_with_selector(graph, node_id, ReadSelector::Latest)
    }

    /// Reads one graph node visible at a commit version.
    pub fn get_node_at_version(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        version: CommitVersion,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.get_node_with_selector(graph, node_id, ReadSelector::AtVersion(version))
    }

    /// Reads one graph node visible at a timestamp.
    pub fn get_node_at(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        timestamp: Timestamp,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.get_node_with_selector(graph, node_id, ReadSelector::AtTimestamp(timestamp))
    }

    fn get_node_with_selector(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<Option<GraphNode>, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        self.node_row_with_selector(&record, graph, node_id, selector)?
            .map(|row| self.node_from_row(&row))
            .transpose()
    }

    /// Deletes one graph node and its incident edges.
    pub fn delete_node(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> Result<GraphDeleteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let Some(current) = self.node_record(&record, graph, node_id)? else {
            return Ok(GraphDeleteOutcome::new(graph.clone(), false, None));
        };
        let mut mutations = MutationMap::default();
        mutations.delete(self.node_address(&record, graph, node_id));
        if let Some(binding) = current.data().binding() {
            mutations.delete(self.binding_address(&record, binding.target(), graph, node_id));
        }
        if let Some(object_type) = current.data().object_type() {
            mutations.delete(self.type_index_address(&record, graph, object_type, node_id));
        }
        for edge in self
            .edge_record_map(&record, graph, ReadSelector::Latest)?
            .into_values()
        {
            if edge.src() == node_id || edge.dst() == node_id {
                self.delete_edge_mutations(&record, &mut mutations, &edge);
            }
        }
        let commit = self.commit_batch(&record, mutations.into_mutations())?;
        Ok(GraphDeleteOutcome::new(graph.clone(), true, Some(commit)))
    }

    /// Samples up to `count` nodes from a graph using a deterministic stride
    /// over the ordered live nodes. Returns the total live node count and the
    /// sample.
    pub fn sample_nodes(
        &mut self,
        graph: &GraphName,
        count: usize,
    ) -> Result<(u64, Vec<GraphNode>), EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, ReadSelector::Latest)?;
        let mut nodes = self
            .node_rows(&record, graph, ReadSelector::Latest)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| self.node_from_row(&row))
            .collect::<EngineResult<Vec<_>>>()?;
        nodes.sort_by(|left, right| left.node_id().cmp(right.node_id()));
        let total_count = u64::try_from(nodes.len()).unwrap_or(u64::MAX);
        if count == 0 || nodes.is_empty() {
            return Ok((total_count, Vec::new()));
        }
        if count >= nodes.len() {
            return Ok((total_count, nodes));
        }
        let row_count = nodes.len();
        let sampled = (0..count)
            .map(|index| nodes[(index * row_count) / count].clone())
            .collect();
        Ok((total_count, sampled))
    }

    /// Lists visible graph nodes.
    pub fn list_nodes(
        &mut self,
        graph: &GraphName,
        prefix: Option<&GraphNodeId>,
        cursor: Option<&GraphNodeId>,
        limit: usize,
    ) -> Result<GraphNodePage, EngineError> {
        self.list_nodes_with_selector(graph, prefix, cursor, limit, ReadSelector::Latest)
    }

    /// Lists graph nodes visible at a commit version.
    pub fn list_nodes_at_version(
        &mut self,
        graph: &GraphName,
        prefix: Option<&GraphNodeId>,
        cursor: Option<&GraphNodeId>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphNodePage, EngineError> {
        self.list_nodes_with_selector(
            graph,
            prefix,
            cursor,
            limit,
            ReadSelector::AtVersion(version),
        )
    }

    /// Lists graph nodes visible at a timestamp.
    pub fn list_nodes_at(
        &mut self,
        graph: &GraphName,
        prefix: Option<&GraphNodeId>,
        cursor: Option<&GraphNodeId>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphNodePage, EngineError> {
        self.list_nodes_with_selector(
            graph,
            prefix,
            cursor,
            limit,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    fn list_nodes_with_selector(
        &mut self,
        graph: &GraphName,
        prefix: Option<&GraphNodeId>,
        cursor: Option<&GraphNodeId>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphNodePage, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        if limit == 0 {
            return Ok(GraphNodePage::new(Vec::new(), false, None));
        }
        let mut nodes = self
            .node_rows(&record, graph, selector)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| self.node_from_row(&row))
            .collect::<EngineResult<Vec<_>>>()?;
        nodes.sort_by(|left, right| left.node_id().cmp(right.node_id()));
        if let Some(prefix) = prefix {
            nodes.retain(|node| node.node_id().as_str().starts_with(prefix.as_str()));
        }
        if let Some(cursor) = cursor {
            nodes.retain(|node| node.node_id() > cursor);
        }
        let has_more = nodes.len() > limit;
        if has_more {
            nodes.truncate(limit);
        }
        let cursor = has_more.then(|| nodes.last().expect("non-empty page").node_id().clone());
        Ok(GraphNodePage::new(nodes, has_more, cursor))
    }

    /// Upserts one graph edge.
    pub fn upsert_edge(
        &mut self,
        graph: &GraphName,
        src: GraphNodeId,
        edge_type: GraphEdgeType,
        dst: GraphNodeId,
        data: super::GraphEdgeData,
    ) -> Result<GraphEdgeWriteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let src_record = self
            .node_record(&record, graph, &src)?
            .ok_or_else(missing_edge_endpoint)?;
        let dst_record = self
            .node_record(&record, graph, &dst)?
            .ok_or_else(missing_edge_endpoint)?;
        if let Some(ontology) = self.frozen_ontology(&record, graph)? {
            ontology.validate_edge(&edge_type, src_record.data(), dst_record.data())?;
        }
        let created = self
            .edge_record(&record, graph, &src, &edge_type, &dst)?
            .is_none();
        let edge = GraphEdgeRecord::new(
            graph.clone(),
            src.clone(),
            edge_type.clone(),
            dst.clone(),
            data,
        );
        let commit = self.commit_batch(
            &record,
            vec![
                RowMutation::put(
                    self.edge_address(&record, graph, &src, &edge_type, &dst),
                    encode_graph_edge_record(&edge)?,
                ),
                RowMutation::put(
                    self.reverse_edge_address(&record, graph, &dst, &edge_type, &src),
                    encode_graph_edge_record(&edge)?,
                ),
            ],
        )?;
        Ok(GraphEdgeWriteOutcome::new(
            graph.clone(),
            src,
            edge_type,
            dst,
            created,
            commit,
        ))
    }

    /// Default number of input items per bulk-ingest chunk commit.
    ///
    /// Sized against the storage layer's per-commit mutation budget
    /// (4096 by default): a node item can produce up to five row
    /// mutations (stale binding and type-index deletes, node row, new
    /// binding and type-index rows) and an edge item two, so chunks are
    /// capped at [`Self::MAX_BULK_CHUNK_SIZE`] items to keep every
    /// chunk inside one storage commit.
    pub const DEFAULT_BULK_CHUNK_SIZE: usize = 512;

    /// Largest admitted items-per-chunk value; larger requests clamp
    /// here so a chunk commit cannot exceed the storage mutation budget
    /// (800 items x 5 mutations <= 4096).
    pub const MAX_BULK_CHUNK_SIZE: usize = 800;

    /// Ingests nodes and edges in chunked commits — the ingest-scale
    /// companion to the transactional `batch_write`. Nodes commit before
    /// edges, so edges may reference nodes from the same call; every
    /// endpoint must exist among committed rows or this call's nodes.
    /// Upsert semantics match `upsert_node` / `upsert_edge`, including
    /// derived-index maintenance and frozen-ontology enforcement.
    ///
    /// Returns per-kind counts and the number of chunk commits. An empty
    /// input commits nothing.
    pub fn bulk_insert(
        &mut self,
        graph: &GraphName,
        nodes: &[(GraphNodeId, super::GraphNodeData)],
        edges: &[(
            GraphNodeId,
            GraphEdgeType,
            GraphNodeId,
            super::GraphEdgeData,
        )],
        chunk_size: Option<usize>,
    ) -> Result<GraphBulkInsertOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        if nodes.is_empty() && edges.is_empty() {
            return Ok(GraphBulkInsertOutcome::new(graph.clone(), 0, 0, 0, None));
        }
        let chunk_size = chunk_size
            .unwrap_or(Self::DEFAULT_BULK_CHUNK_SIZE)
            .clamp(1, Self::MAX_BULK_CHUNK_SIZE);
        self.validate_bulk_input(&record, graph, nodes, edges)?;

        let mut commits = 0u64;
        let mut last_commit = None;
        for chunk in nodes.chunks(chunk_size) {
            let mut mutations = MutationMap::default();
            for (node_id, data) in chunk {
                // Upsert discipline: drop stale derived rows before the
                // new node row lands, exactly like `upsert_node`.
                let current = self.node_record(&record, graph, node_id)?;
                let new_record = GraphNodeRecord::new(graph.clone(), node_id.clone(), data.clone());
                if let Some(old) = current.as_ref().and_then(|record| record.data().binding()) {
                    if Some(old) != new_record.data().binding() {
                        mutations.delete(self.binding_address(
                            &record,
                            old.target(),
                            graph,
                            node_id,
                        ));
                    }
                }
                let old_type = current
                    .as_ref()
                    .and_then(|record| record.data().object_type());
                let new_type = new_record.data().object_type();
                if let Some(old_type) = old_type {
                    if Some(old_type) != new_type {
                        mutations
                            .delete(self.type_index_address(&record, graph, old_type, node_id));
                    }
                }
                if let Some(new_type) = new_type {
                    mutations.put(
                        self.type_index_address(&record, graph, new_type, node_id),
                        encode_graph_type_index_record(&GraphTypeIndexRecord::new(
                            graph.clone(),
                            new_type.clone(),
                            node_id.clone(),
                        ))?,
                    );
                }
                mutations.put(
                    self.node_address(&record, graph, node_id),
                    encode_graph_node_record(&new_record)?,
                );
                if let Some(binding) = new_record.data().binding() {
                    mutations.put(
                        self.binding_address(&record, binding.target(), graph, node_id),
                        encode_graph_binding_record(&GraphBindingRecord::new(
                            graph.clone(),
                            node_id.clone(),
                            binding.clone(),
                        ))?,
                    );
                }
            }
            last_commit = Some(self.commit_batch(&record, mutations.into_mutations())?);
            commits += 1;
        }
        for chunk in edges.chunks(chunk_size) {
            let mut mutations = MutationMap::default();
            for (src, edge_type, dst, data) in chunk {
                let edge = GraphEdgeRecord::new(
                    graph.clone(),
                    src.clone(),
                    edge_type.clone(),
                    dst.clone(),
                    data.clone(),
                );
                mutations.put(
                    self.edge_address(&record, graph, src, edge_type, dst),
                    encode_graph_edge_record(&edge)?,
                );
                mutations.put(
                    self.reverse_edge_address(&record, graph, dst, edge_type, src),
                    encode_graph_edge_record(&edge)?,
                );
            }
            last_commit = Some(self.commit_batch(&record, mutations.into_mutations())?);
            commits += 1;
        }

        Ok(GraphBulkInsertOutcome::new(
            graph.clone(),
            nodes.len() as u64,
            edges.len() as u64,
            commits,
            last_commit,
        ))
    }

    /// Validates every bulk input before the first commit: a mid-stream
    /// refusal must not leave earlier chunks half-applied.
    fn validate_bulk_input(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        nodes: &[(GraphNodeId, super::GraphNodeData)],
        edges: &[(
            GraphNodeId,
            GraphEdgeType,
            GraphNodeId,
            super::GraphEdgeData,
        )],
    ) -> Result<(), EngineError> {
        let ontology = self.frozen_ontology(record, graph)?;
        let mut call_nodes: BTreeMap<&GraphNodeId, &super::GraphNodeData> = BTreeMap::new();
        for (node_id, data) in nodes {
            if let Some(binding) = data.binding() {
                self.validate_binding_target(binding.target())?;
            }
            if let Some(ontology) = ontology.as_ref() {
                ontology.validate_node(data)?;
            }
            call_nodes.insert(node_id, data);
        }
        let mut endpoint_cache: BTreeMap<GraphNodeId, Option<super::GraphNodeData>> =
            BTreeMap::new();
        for (src, edge_type, dst, _) in edges {
            for endpoint in [src, dst] {
                if call_nodes.contains_key(endpoint) || endpoint_cache.contains_key(endpoint) {
                    continue;
                }
                let data = self
                    .node_record(record, graph, endpoint)?
                    .map(|existing| existing.data().clone());
                endpoint_cache.insert(endpoint.clone(), data);
            }
            let resolve = |endpoint: &GraphNodeId| {
                call_nodes
                    .get(endpoint)
                    .copied()
                    .or_else(|| endpoint_cache.get(endpoint).and_then(Option::as_ref))
            };
            let (Some(src_data), Some(dst_data)) = (resolve(src), resolve(dst)) else {
                return Err(missing_edge_endpoint());
            };
            if let Some(ontology) = ontology.as_ref() {
                ontology.validate_edge(edge_type, src_data, dst_data)?;
            }
        }
        Ok(())
    }

    /// Reads one graph edge.
    pub fn get_edge(
        &mut self,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
    ) -> Result<Option<GraphEdge>, EngineError> {
        self.get_edge_with_selector(graph, src, edge_type, dst, ReadSelector::Latest)
    }

    /// Reads one graph edge visible at a commit version.
    pub fn get_edge_at_version(
        &mut self,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
        version: CommitVersion,
    ) -> Result<Option<GraphEdge>, EngineError> {
        self.get_edge_with_selector(graph, src, edge_type, dst, ReadSelector::AtVersion(version))
    }

    /// Reads one graph edge visible at a timestamp.
    pub fn get_edge_at(
        &mut self,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
        timestamp: Timestamp,
    ) -> Result<Option<GraphEdge>, EngineError> {
        self.get_edge_with_selector(
            graph,
            src,
            edge_type,
            dst,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    fn get_edge_with_selector(
        &mut self,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<Option<GraphEdge>, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        self.edge_row_with_selector(&record, graph, src, edge_type, dst, selector)?
            .map(|row| self.edge_from_forward_row(&row))
            .transpose()
    }

    /// Deletes one graph edge.
    pub fn delete_edge(
        &mut self,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
    ) -> Result<GraphDeleteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let Some(edge) = self.edge_record(&record, graph, src, edge_type, dst)? else {
            return Ok(GraphDeleteOutcome::new(graph.clone(), false, None));
        };
        let mut mutations = MutationMap::default();
        self.delete_edge_mutations(&record, &mut mutations, &edge);
        let commit = self.commit_batch(&record, mutations.into_mutations())?;
        Ok(GraphDeleteOutcome::new(graph.clone(), true, Some(commit)))
    }

    /// Looks up neighboring nodes.
    pub fn neighbors(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        direction: GraphDirection,
        edge_type: Option<&GraphEdgeType>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<GraphNeighborPage, EngineError> {
        self.neighbors_with_selector(
            graph,
            node_id,
            direction,
            edge_type,
            cursor,
            limit,
            ReadSelector::Latest,
        )
    }

    /// Looks up neighboring nodes visible at a commit version.
    pub fn neighbors_at_version(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        direction: GraphDirection,
        edge_type: Option<&GraphEdgeType>,
        cursor: Option<&str>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphNeighborPage, EngineError> {
        self.neighbors_with_selector(
            graph,
            node_id,
            direction,
            edge_type,
            cursor,
            limit,
            ReadSelector::AtVersion(version),
        )
    }

    /// Looks up neighboring nodes visible at a timestamp.
    pub fn neighbors_at(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        direction: GraphDirection,
        edge_type: Option<&GraphEdgeType>,
        cursor: Option<&str>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphNeighborPage, EngineError> {
        self.neighbors_with_selector(
            graph,
            node_id,
            direction,
            edge_type,
            cursor,
            limit,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    fn neighbors_with_selector(
        &mut self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        direction: GraphDirection,
        edge_type: Option<&GraphEdgeType>,
        cursor: Option<&str>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphNeighborPage, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        if limit == 0
            || self
                .node_record_with_selector(&record, graph, node_id, selector)?
                .is_none()
        {
            return Ok(GraphNeighborPage::new(Vec::new(), false, None));
        }
        let mut hits = Vec::new();
        if matches!(direction, GraphDirection::Outgoing | GraphDirection::Both) {
            hits.extend(self.outgoing_neighbors(&record, graph, node_id, edge_type, selector)?);
        }
        if matches!(direction, GraphDirection::Incoming | GraphDirection::Both) {
            hits.extend(self.incoming_neighbors(&record, graph, node_id, edge_type, selector)?);
        }
        hits.sort_by_key(neighbor_cursor);
        if let Some(cursor) = cursor {
            hits.retain(|hit| neighbor_cursor(hit).as_str() > cursor);
        }
        let has_more = hits.len() > limit;
        if has_more {
            hits.truncate(limit);
        }
        let cursor = has_more.then(|| neighbor_cursor(hits.last().expect("non-empty page")));
        Ok(GraphNeighborPage::new(hits, has_more, cursor))
    }

    /// Looks up graph nodes bound to an entity target.
    pub fn bindings_for_entity(
        &mut self,
        target: &GraphBindingTarget,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<GraphBindingPage, EngineError> {
        self.bindings_for_entity_with_selector(target, cursor, limit, ReadSelector::Latest)
    }

    /// Looks up graph nodes bound to an entity target at a commit version.
    pub fn bindings_for_entity_at_version(
        &mut self,
        target: &GraphBindingTarget,
        cursor: Option<&str>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphBindingPage, EngineError> {
        self.bindings_for_entity_with_selector(
            target,
            cursor,
            limit,
            ReadSelector::AtVersion(version),
        )
    }

    /// Looks up graph nodes bound to an entity target at a timestamp.
    pub fn bindings_for_entity_at(
        &mut self,
        target: &GraphBindingTarget,
        cursor: Option<&str>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphBindingPage, EngineError> {
        self.bindings_for_entity_with_selector(
            target,
            cursor,
            limit,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    fn bindings_for_entity_with_selector(
        &mut self,
        target: &GraphBindingTarget,
        cursor: Option<&str>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphBindingPage, EngineError> {
        let record = self.branch_record()?;
        if limit == 0 {
            return Ok(GraphBindingPage::new(Vec::new(), false, None));
        }
        let mut bindings = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::GraphBindingIndex,
                encode_graph_binding_target_prefix(&self.space, target),
                selector,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| self.binding_from_row(&row))
            .collect::<EngineResult<Vec<_>>>()?;
        bindings.sort_by_key(binding_cursor);
        if let Some(cursor) = cursor {
            bindings.retain(|binding| binding_cursor(binding).as_str() > cursor);
        }
        let has_more = bindings.len() > limit;
        if has_more {
            bindings.truncate(limit);
        }
        let cursor = has_more.then(|| binding_cursor(bindings.last().expect("non-empty page")));
        Ok(GraphBindingPage::new(bindings, has_more, cursor))
    }

    /// Applies an all-or-nothing graph batch.
    #[allow(clippy::too_many_lines)]
    pub fn batch_write(
        &mut self,
        graph: &GraphName,
        batch: &GraphBatchWrite,
    ) -> Result<GraphBatchWriteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        if batch.is_empty() {
            return Ok(GraphBatchWriteOutcome::new(graph.clone(), Vec::new(), None));
        }

        let frozen = self.frozen_ontology(&record, graph)?;
        let mut nodes = self.node_record_map(&record, graph, ReadSelector::Latest)?;
        let mut edges = self.edge_record_map(&record, graph, ReadSelector::Latest)?;
        let mut mutations = MutationMap::default();
        let mut outcomes = Vec::with_capacity(batch.operations().len());

        for (index, operation) in batch.operations().iter().enumerate() {
            match operation {
                GraphBatchOperation::UpsertNode { node_id, data } => {
                    if let Some(binding) = data.binding() {
                        self.validate_binding_target(binding.target())?;
                    }
                    if let Some(ontology) = frozen.as_ref() {
                        ontology.validate_node(data)?;
                    }
                    let created = !nodes.contains_key(node_id);
                    if let Some(old) = nodes
                        .get(node_id)
                        .and_then(|record| record.data().binding())
                    {
                        if data.binding() != Some(old) {
                            mutations.delete(self.binding_address(
                                &record,
                                old.target(),
                                graph,
                                node_id,
                            ));
                        }
                    }
                    let old_type = nodes
                        .get(node_id)
                        .and_then(|record| record.data().object_type());
                    if let Some(old_type) = old_type {
                        if Some(old_type) != data.object_type() {
                            mutations
                                .delete(self.type_index_address(&record, graph, old_type, node_id));
                        }
                    }
                    if let Some(new_type) = data.object_type() {
                        mutations.put(
                            self.type_index_address(&record, graph, new_type, node_id),
                            encode_graph_type_index_record(&GraphTypeIndexRecord::new(
                                graph.clone(),
                                new_type.clone(),
                                node_id.clone(),
                            ))?,
                        );
                    }
                    let node = GraphNodeRecord::new(graph.clone(), node_id.clone(), data.clone());
                    mutations.put(
                        self.node_address(&record, graph, node_id),
                        encode_graph_node_record(&node)?,
                    );
                    if let Some(binding) = node.data().binding() {
                        let binding_record = GraphBindingRecord::new(
                            graph.clone(),
                            node_id.clone(),
                            binding.clone(),
                        );
                        mutations.put(
                            self.binding_address(&record, binding.target(), graph, node_id),
                            encode_graph_binding_record(&binding_record)?,
                        );
                    }
                    nodes.insert(node_id.clone(), node);
                    outcomes.push(GraphBatchOpOutcome::created(index, created));
                }
                GraphBatchOperation::DeleteNode { node_id } => {
                    let removed = nodes.remove(node_id);
                    let deleted = removed.is_some();
                    if let Some(removed) = removed {
                        mutations.delete(self.node_address(&record, graph, node_id));
                        if let Some(binding) = removed.data().binding() {
                            mutations.delete(self.binding_address(
                                &record,
                                binding.target(),
                                graph,
                                node_id,
                            ));
                        }
                        if let Some(object_type) = removed.data().object_type() {
                            mutations.delete(self.type_index_address(
                                &record,
                                graph,
                                object_type,
                                node_id,
                            ));
                        }
                        let incident = edges
                            .values()
                            .filter(|edge| edge.src() == node_id || edge.dst() == node_id)
                            .cloned()
                            .collect::<Vec<_>>();
                        for edge in incident {
                            edges.remove(&edge_identity(&edge));
                            self.delete_edge_mutations(&record, &mut mutations, &edge);
                        }
                    }
                    outcomes.push(GraphBatchOpOutcome::deleted(index, deleted));
                }
                GraphBatchOperation::UpsertEdge {
                    src,
                    edge_type,
                    dst,
                    data,
                } => {
                    let (Some(src_record), Some(dst_record)) = (nodes.get(src), nodes.get(dst))
                    else {
                        return Err(missing_edge_endpoint());
                    };
                    if let Some(ontology) = frozen.as_ref() {
                        // Validated against the batch-local state: a node
                        // typed earlier in this batch governs its edges.
                        ontology.validate_edge(edge_type, src_record.data(), dst_record.data())?;
                    }
                    let identity = (src.clone(), edge_type.clone(), dst.clone());
                    let created = !edges.contains_key(&identity);
                    let edge = GraphEdgeRecord::new(
                        graph.clone(),
                        src.clone(),
                        edge_type.clone(),
                        dst.clone(),
                        data.clone(),
                    );
                    self.put_edge_mutations(&record, &mut mutations, &edge)?;
                    edges.insert(identity, edge);
                    outcomes.push(GraphBatchOpOutcome::created(index, created));
                }
                GraphBatchOperation::DeleteEdge {
                    src,
                    edge_type,
                    dst,
                } => {
                    let identity = (src.clone(), edge_type.clone(), dst.clone());
                    let deleted = edges.remove(&identity).is_some();
                    if deleted {
                        let edge = GraphEdgeRecord::new(
                            graph.clone(),
                            src.clone(),
                            edge_type.clone(),
                            dst.clone(),
                            super::GraphEdgeData::default(),
                        );
                        self.delete_edge_mutations(&record, &mut mutations, &edge);
                    }
                    outcomes.push(GraphBatchOpOutcome::deleted(index, deleted));
                }
            }
        }

        let mutations = mutations.into_mutations();
        if mutations.is_empty() {
            return Ok(GraphBatchWriteOutcome::new(graph.clone(), outcomes, None));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(GraphBatchWriteOutcome::new(
            graph.clone(),
            outcomes,
            Some(commit),
        ))
    }

    /// Returns the graph's ontology, or `None` before any type was defined.
    pub fn ontology(&mut self, graph: &GraphName) -> Result<Option<GraphOntology>, EngineError> {
        self.ontology_with_selector(graph, ReadSelector::Latest)
    }

    /// Returns the ontology visible at a commit version.
    pub fn ontology_at_version(
        &mut self,
        graph: &GraphName,
        version: CommitVersion,
    ) -> Result<Option<GraphOntology>, EngineError> {
        self.ontology_with_selector(graph, ReadSelector::AtVersion(version))
    }

    /// Returns the ontology visible at a timestamp.
    pub fn ontology_at(
        &mut self,
        graph: &GraphName,
        timestamp: Timestamp,
    ) -> Result<Option<GraphOntology>, EngineError> {
        self.ontology_with_selector(graph, ReadSelector::AtTimestamp(timestamp))
    }

    fn ontology_with_selector(
        &mut self,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Option<GraphOntology>, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        let Some(row) = self.ontology_row(&record, graph, selector)? else {
            return Ok(None);
        };
        let ontology = Self::ontology_record_from_row(graph, &row)?;
        Ok(Some(GraphOntology::new(
            graph.clone(),
            ontology.status(),
            ontology.object_types().values().cloned().collect(),
            ontology.link_types().values().cloned().collect(),
            row.commit_version(),
            row.commit_timestamp(),
        )))
    }

    /// Defines (or, while the ontology is Draft, redefines) an object type.
    /// The first definition puts the graph's ontology in Draft; a Frozen
    /// ontology refuses with `failed_precondition.engine.graph_ontology_frozen`.
    pub fn define_object_type(
        &mut self,
        graph: &GraphName,
        def: GraphObjectTypeDef,
    ) -> Result<GraphOntologyWriteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let mut ontology = self.mutable_ontology(&record, graph)?;
        let type_name = def.name().clone();
        let created = ontology.put_object_type(def);
        let commit = self.write_ontology(&record, graph, &ontology)?;
        Ok(GraphOntologyWriteOutcome::new(
            graph.clone(),
            type_name,
            created,
            commit,
        ))
    }

    /// Defines (or, while Draft, redefines) a link type. Endpoint object
    /// types need not exist yet — freeze validates them.
    pub fn define_link_type(
        &mut self,
        graph: &GraphName,
        def: GraphLinkTypeDef,
    ) -> Result<GraphOntologyWriteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let mut ontology = self.mutable_ontology(&record, graph)?;
        let type_name = def.name().clone();
        let created = ontology.put_link_type(def);
        let commit = self.write_ontology(&record, graph, &ontology)?;
        Ok(GraphOntologyWriteOutcome::new(
            graph.clone(),
            type_name,
            created,
            commit,
        ))
    }

    /// Deletes an object type (Draft only). `deleted` is false when the
    /// type was never defined. A link type may still reference the deleted
    /// name in Draft — freeze validation catches the dangling endpoint.
    pub fn delete_object_type(
        &mut self,
        graph: &GraphName,
        name: &GraphTypeName,
    ) -> Result<GraphDeleteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let mut ontology = self.mutable_ontology(&record, graph)?;
        if !ontology.remove_object_type(name) {
            return Ok(GraphDeleteOutcome::new(graph.clone(), false, None));
        }
        let commit = self.write_ontology(&record, graph, &ontology)?;
        Ok(GraphDeleteOutcome::new(graph.clone(), true, Some(commit)))
    }

    /// Deletes a link type (Draft only). `deleted` is false when absent.
    pub fn delete_link_type(
        &mut self,
        graph: &GraphName,
        name: &GraphTypeName,
    ) -> Result<GraphDeleteOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let mut ontology = self.mutable_ontology(&record, graph)?;
        if !ontology.remove_link_type(name) {
            return Ok(GraphDeleteOutcome::new(graph.clone(), false, None));
        }
        let commit = self.write_ontology(&record, graph, &ontology)?;
        Ok(GraphDeleteOutcome::new(graph.clone(), true, Some(commit)))
    }

    /// Freezes the ontology: validates that at least one type is declared
    /// and every link endpoint references a declared object type
    /// (`failed_precondition.engine.graph_ontology_freeze` otherwise), then
    /// flips the status to Frozen in one atomic row update. A Frozen
    /// ontology refuses to freeze again.
    pub fn freeze_ontology(
        &mut self,
        graph: &GraphName,
    ) -> Result<GraphOntologyFreezeOutcome, EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, graph)?;
        let mut ontology = self.mutable_ontology(&record, graph)?;
        ontology.validate_for_freeze().map_err(|detail| {
            EngineError::conflict(
                "failed_precondition.engine.graph_ontology_freeze",
                format!("ontology cannot freeze: {detail}"),
            )
        })?;
        ontology.freeze();
        let object_types = ontology.object_types().len();
        let link_types = ontology.link_types().len();
        let commit = self.write_ontology(&record, graph, &ontology)?;
        Ok(GraphOntologyFreezeOutcome::new(
            graph.clone(),
            object_types,
            link_types,
            commit,
        ))
    }

    /// Lists visible nodes declaring `object_type`, node-id ordered, via
    /// the derived type index. The index tracks whatever type nodes carry
    /// regardless of ontology status, so this works for draft-era and
    /// undeclared types too.
    pub fn nodes_by_type(
        &mut self,
        graph: &GraphName,
        object_type: &GraphTypeName,
        cursor: Option<&GraphNodeId>,
        limit: usize,
    ) -> Result<GraphNodePage, EngineError> {
        self.nodes_by_type_with_selector(graph, object_type, cursor, limit, ReadSelector::Latest)
    }

    /// Lists nodes declaring `object_type` visible at a commit version.
    pub fn nodes_by_type_at_version(
        &mut self,
        graph: &GraphName,
        object_type: &GraphTypeName,
        cursor: Option<&GraphNodeId>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphNodePage, EngineError> {
        self.nodes_by_type_with_selector(
            graph,
            object_type,
            cursor,
            limit,
            ReadSelector::AtVersion(version),
        )
    }

    /// Lists nodes declaring `object_type` visible at a timestamp.
    pub fn nodes_by_type_at(
        &mut self,
        graph: &GraphName,
        object_type: &GraphTypeName,
        cursor: Option<&GraphNodeId>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphNodePage, EngineError> {
        self.nodes_by_type_with_selector(
            graph,
            object_type,
            cursor,
            limit,
            ReadSelector::AtTimestamp(timestamp),
        )
    }

    fn nodes_by_type_with_selector(
        &mut self,
        graph: &GraphName,
        object_type: &GraphTypeName,
        cursor: Option<&GraphNodeId>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphNodePage, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        if limit == 0 {
            return Ok(GraphNodePage::new(Vec::new(), false, None));
        }
        let mut node_ids = Vec::new();
        for row in self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphTypeIndex,
            encode_graph_type_index_type_prefix(&self.space, graph, object_type),
            selector,
            None,
        )? {
            if row.is_tombstone() {
                continue;
            }
            let (row_graph, row_type, node_id) =
                crate::persistence::decode_graph_type_index_key(&self.space, row.key())?;
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.graph_type_index_record",
                    "stored graph type index row is missing a value",
                )
            })?;
            decode_graph_type_index_record(&row_graph, &row_type, &node_id, value)?;
            node_ids.push(node_id);
        }
        node_ids.sort();
        if let Some(cursor) = cursor {
            node_ids.retain(|node_id| node_id > cursor);
        }
        let has_more = node_ids.len() > limit;
        if has_more {
            node_ids.truncate(limit);
        }
        let mut nodes = Vec::with_capacity(node_ids.len());
        for node_id in &node_ids {
            let row = self
                .node_row_with_selector(&record, graph, node_id, selector)?
                .ok_or_else(|| {
                    EngineError::corruption(
                        "data_loss.engine.graph_index",
                        "graph type index names a node with no visible row",
                    )
                })?;
            nodes.push(self.node_from_row(&row)?);
        }
        let cursor = has_more.then(|| node_ids.last().expect("non-empty page").clone());
        Ok(GraphNodePage::new(nodes, has_more, cursor))
    }

    /// Returns the ontology with per-type usage counts, or `None` before
    /// any type was defined. Counts are exact at read time: node counts
    /// from the type index, edge counts from one pass over the graph's
    /// visible edges (no counter rows).
    pub fn ontology_summary(
        &mut self,
        graph: &GraphName,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        self.ontology_summary_with_selector(graph, ReadSelector::Latest)
    }

    /// Returns the ontology summary visible at a commit version.
    pub fn ontology_summary_at_version(
        &mut self,
        graph: &GraphName,
        version: CommitVersion,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        self.ontology_summary_with_selector(graph, ReadSelector::AtVersion(version))
    }

    /// Returns the ontology summary visible at a timestamp.
    pub fn ontology_summary_at(
        &mut self,
        graph: &GraphName,
        timestamp: Timestamp,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        self.ontology_summary_with_selector(graph, ReadSelector::AtTimestamp(timestamp))
    }

    fn ontology_summary_with_selector(
        &mut self,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        let Some(row) = self.ontology_row(&record, graph, selector)? else {
            return Ok(None);
        };
        let ontology = Self::ontology_record_from_row(graph, &row)?;

        let mut object_types = Vec::with_capacity(ontology.object_types().len());
        for (name, def) in ontology.object_types() {
            let count = self
                .persistence
                .scan_prefix(
                    record.storage_branch_id(),
                    RowClass::GraphTypeIndex,
                    encode_graph_type_index_type_prefix(&self.space, graph, name),
                    selector,
                    None,
                )?
                .iter()
                .filter(|row| !row.is_tombstone())
                .count();
            object_types.push(GraphObjectTypeSummary::new(
                def.clone(),
                u64::try_from(count).unwrap_or(u64::MAX),
            ));
        }

        let mut edge_counts: BTreeMap<&GraphTypeName, u64> =
            ontology.link_types().keys().map(|name| (name, 0)).collect();
        for edge_row in self.edge_rows(&record, graph, selector)? {
            if edge_row.is_tombstone() {
                continue;
            }
            let (_, _, edge_type, _) = decode_graph_edge_key(&self.space, edge_row.key())?;
            if let Some(count) = edge_counts
                .iter_mut()
                .find_map(|(name, count)| (name.as_str() == edge_type.as_str()).then_some(count))
            {
                *count += 1;
            }
        }
        let link_types = ontology
            .link_types()
            .iter()
            .map(|(name, def)| {
                GraphLinkTypeSummary::new(def.clone(), edge_counts.get(name).copied().unwrap_or(0))
            })
            .collect();

        Ok(Some(GraphOntologySummary::new(
            graph.clone(),
            ontology.status(),
            object_types,
            link_types,
            row.commit_version(),
            row.commit_timestamp(),
        )))
    }

    fn type_index_address(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        object_type: &GraphTypeName,
        node_id: &GraphNodeId,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphTypeIndex,
            encode_graph_type_index_key(&self.space, graph, object_type, node_id),
        )
    }

    fn type_index_rows(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphTypeIndex,
            encode_graph_type_index_graph_prefix(&self.space, graph),
            selector,
            None,
        )
    }

    /// Builds an in-memory adjacency snapshot of the graph's visible
    /// nodes and edges at one consistent read — the substrate for the
    /// traversal and analytics stages. Refuses graphs beyond `budget`
    /// with `resource_exhausted.engine.graph_analytics_budget` instead
    /// of exhausting memory.
    pub fn adjacency_index(
        &mut self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        self.adjacency_index_with_selector(graph, budget, ReadSelector::Latest)
    }

    /// Builds the adjacency snapshot visible at a commit version.
    pub fn adjacency_index_at_version(
        &mut self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
        version: CommitVersion,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        self.adjacency_index_with_selector(graph, budget, ReadSelector::AtVersion(version))
    }

    /// Builds the adjacency snapshot visible at a timestamp.
    pub fn adjacency_index_at(
        &mut self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
        timestamp: Timestamp,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        self.adjacency_index_with_selector(graph, budget, ReadSelector::AtTimestamp(timestamp))
    }

    fn adjacency_index_with_selector(
        &mut self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
        selector: ReadSelector,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        let mut builder = GraphAdjacencyIndexBuilder::new(graph.clone(), *budget);
        for row in self.node_rows(&record, graph, selector)? {
            if row.is_tombstone() {
                continue;
            }
            let (_, node_id) = decode_graph_node_key(&self.space, row.key())?;
            builder.add_node(node_id)?;
        }
        builder.finish_nodes();
        for row in self.edge_rows(&record, graph, selector)? {
            if row.is_tombstone() {
                continue;
            }
            let edge = self.edge_record_from_forward_row(&row)?;
            builder.add_edge(
                edge.src(),
                edge.edge_type(),
                edge.dst(),
                edge.data().weight(),
            )?;
        }
        Ok(builder.finish())
    }

    fn ontology_address(&self, record: &BranchCatalogRecord, graph: &GraphName) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphOntology,
            encode_graph_ontology_key(&self.space, graph),
        )
    }

    fn ontology_row(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Option<PersistenceReadRow>, EngineError> {
        let address = self.ontology_address(record, graph);
        Ok(self
            .persistence
            .read_row(address, selector)?
            .filter(|row| !row.is_tombstone()))
    }

    fn ontology_record_from_row(
        graph: &GraphName,
        row: &PersistenceReadRow,
    ) -> Result<GraphOntologyRecord, EngineError> {
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_ontology_record",
                "stored graph ontology row is missing a value",
            )
        })?;
        decode_graph_ontology_record(graph, value)
    }

    /// The frozen ontology governing writes, if any: `None` while the
    /// ontology is absent or still Draft (no write validation in either
    /// case — GO2 enforcement is freeze-gated).
    fn frozen_ontology(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
    ) -> Result<Option<GraphOntologyRecord>, EngineError> {
        match self.ontology_row(record, graph, ReadSelector::Latest)? {
            Some(row) => {
                let ontology = Self::ontology_record_from_row(graph, &row)?;
                Ok(ontology.is_frozen().then_some(ontology))
            }
            None => Ok(None),
        }
    }

    /// Loads the ontology for mutation: the stored record, or an empty
    /// Draft when none exists yet. Frozen ontologies are immutable —
    /// every mutation (freeze included) refuses.
    fn mutable_ontology(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
    ) -> Result<GraphOntologyRecord, EngineError> {
        let ontology = match self.ontology_row(record, graph, ReadSelector::Latest)? {
            Some(row) => Self::ontology_record_from_row(graph, &row)?,
            None => GraphOntologyRecord::empty_draft(graph.clone()),
        };
        if ontology.is_frozen() {
            return Err(EngineError::conflict(
                "failed_precondition.engine.graph_ontology_frozen",
                "graph ontology is frozen and cannot change",
            ));
        }
        Ok(ontology)
    }

    fn write_ontology(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        ontology: &GraphOntologyRecord,
    ) -> Result<CommitOutcome, EngineError> {
        debug_assert_eq!(ontology.graph(), graph);
        self.commit_batch(
            record,
            vec![RowMutation::put(
                self.ontology_address(record, graph),
                encode_graph_ontology_record(ontology)?,
            )],
        )
    }

    fn branch_record(&self) -> Result<BranchCatalogRecord, EngineError> {
        self.control.require_healthy()?;
        self.control
            .lookup_branch(&self.branch)
            .cloned()
            .ok_or_else(|| {
                EngineError::not_found(
                    "not_found.engine.branch",
                    format!("branch `{}` does not exist", self.branch),
                )
            })
    }

    fn metadata_address(&self, record: &BranchCatalogRecord, graph: &GraphName) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphMetadata,
            encode_graph_metadata_key(&self.space, graph),
        )
    }

    fn node_address(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphNode,
            encode_graph_node_key(&self.space, graph, node_id),
        )
    }

    fn edge_address(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphEdge,
            encode_graph_edge_key(&self.space, graph, src, edge_type, dst),
        )
    }

    fn reverse_edge_address(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        dst: &GraphNodeId,
        edge_type: &GraphEdgeType,
        src: &GraphNodeId,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphReverseEdge,
            encode_graph_reverse_edge_key(&self.space, graph, dst, edge_type, src),
        )
    }

    fn binding_address(
        &self,
        record: &BranchCatalogRecord,
        target: &GraphBindingTarget,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::GraphBindingIndex,
            encode_graph_binding_key(&self.space, target, graph, node_id),
        )
    }

    fn graph_metadata_row(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Option<PersistenceReadRow>, EngineError> {
        let address = self.metadata_address(record, graph);
        Ok(self
            .persistence
            .read_row(address, selector)?
            .filter(|row| !row.is_tombstone()))
    }

    fn require_graph(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
    ) -> Result<(), EngineError> {
        self.require_graph_with_selector(record, graph, ReadSelector::Latest)
    }

    fn require_graph_with_selector(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<(), EngineError> {
        let Some(row) = self.graph_metadata_row(record, graph, selector)? else {
            return Err(EngineError::not_found(
                "not_found.engine.graph",
                "graph does not exist",
            ));
        };
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_metadata",
                "stored graph metadata row is missing a value",
            )
        })?;
        let _ = decode_graph_metadata_record(graph, value)?;
        Ok(())
    }

    fn graph_info_from_row(
        &mut self,
        record: &BranchCatalogRecord,
        row: &PersistenceReadRow,
        selector: ReadSelector,
    ) -> Result<GraphInfo, EngineError> {
        let graph = decode_graph_metadata_key(&self.space, row.key())?;
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_metadata",
                "stored graph metadata row is missing a value",
            )
        })?;
        let _ = decode_graph_metadata_record(&graph, value)?;
        let node_rows = self.node_rows(record, &graph, selector)?;
        let edge_rows = self.edge_rows(record, &graph, selector)?;
        let mut node_count = 0_u64;
        for row in node_rows.iter().filter(|row| !row.is_tombstone()) {
            let _ = self.node_record_from_row(row)?;
            node_count = node_count.saturating_add(1);
        }
        let mut edge_count = 0_u64;
        for row in edge_rows.iter().filter(|row| !row.is_tombstone()) {
            let _ = self.edge_record_from_forward_row(row)?;
            edge_count = edge_count.saturating_add(1);
        }
        let mut updated_version = row.commit_version();
        let mut updated_timestamp = row.commit_timestamp();
        for candidate in node_rows.iter().chain(edge_rows.iter()) {
            if candidate.commit_version() > updated_version {
                updated_version = candidate.commit_version();
                updated_timestamp = candidate.commit_timestamp();
            }
        }
        Ok(GraphInfo::new(
            graph,
            node_count,
            edge_count,
            row.commit_version(),
            row.commit_timestamp(),
            updated_version,
            updated_timestamp,
        ))
    }

    fn node_row_with_selector(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<Option<PersistenceReadRow>, EngineError> {
        let address = self.node_address(record, graph, node_id);
        Ok(self
            .persistence
            .read_row(address, selector)?
            .filter(|row| !row.is_tombstone()))
    }

    fn node_record(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> Result<Option<GraphNodeRecord>, EngineError> {
        self.node_record_with_selector(record, graph, node_id, ReadSelector::Latest)
    }

    fn node_record_with_selector(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<Option<GraphNodeRecord>, EngineError> {
        self.node_row_with_selector(record, graph, node_id, selector)?
            .map(|row| self.node_record_from_row(&row))
            .transpose()
    }

    fn edge_row_with_selector(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<Option<PersistenceReadRow>, EngineError> {
        let address = self.edge_address(record, graph, src, edge_type, dst);
        Ok(self
            .persistence
            .read_row(address, selector)?
            .filter(|row| !row.is_tombstone()))
    }

    fn edge_record(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
    ) -> Result<Option<GraphEdgeRecord>, EngineError> {
        self.edge_row_with_selector(record, graph, src, edge_type, dst, ReadSelector::Latest)?
            .map(|row| self.edge_record_from_forward_row(&row))
            .transpose()
    }

    fn node_rows(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphNode,
            encode_graph_node_prefix(&self.space, graph),
            selector,
            None,
        )
    }

    fn edge_rows(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphEdge,
            encode_graph_edge_prefix(&self.space, graph),
            selector,
            None,
        )
    }

    fn reverse_edge_rows(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphReverseEdge,
            encode_graph_reverse_edge_prefix(&self.space, graph),
            selector,
            None,
        )
    }

    fn binding_rows_for_space(
        &mut self,
        record: &BranchCatalogRecord,
        selector: ReadSelector,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::GraphBindingIndex,
            encode_graph_binding_space_prefix(&self.space),
            selector,
            None,
        )
    }

    fn node_record_map(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<BTreeMap<GraphNodeId, GraphNodeRecord>, EngineError> {
        self.node_rows(record, graph, selector)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| {
                let record = self.node_record_from_row(&row)?;
                Ok((record.node_id().clone(), record))
            })
            .collect()
    }

    fn edge_record_map(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<BTreeMap<EdgeIdentity, GraphEdgeRecord>, EngineError> {
        self.edge_rows(record, graph, selector)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| {
                let record = self.edge_record_from_forward_row(&row)?;
                Ok((edge_identity(&record), record))
            })
            .collect()
    }

    fn outgoing_neighbors(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        edge_type: Option<&GraphEdgeType>,
        selector: ReadSelector,
    ) -> Result<Vec<GraphNeighbor>, EngineError> {
        self.persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::GraphEdge,
                encode_graph_outgoing_edge_prefix(&self.space, graph, node_id),
                selector,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| {
                let edge = self.edge_from_forward_row(&row)?;
                if edge_type.is_some_and(|expected| edge.edge_type() != expected) {
                    return Ok(None);
                }
                let node = self.visible_node_or_corruption(record, graph, edge.dst(), selector)?;
                let target_status = self.neighbor_target_status(record, &node, selector)?;
                Ok(Some(GraphNeighbor::new(
                    node,
                    edge,
                    GraphDirection::Outgoing,
                    target_status,
                )))
            })
            .filter_map(Result::transpose)
            .collect()
    }

    fn incoming_neighbors(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        edge_type: Option<&GraphEdgeType>,
        selector: ReadSelector,
    ) -> Result<Vec<GraphNeighbor>, EngineError> {
        self.persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::GraphReverseEdge,
                encode_graph_incoming_edge_prefix(&self.space, graph, node_id),
                selector,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| {
                let edge = self.edge_from_reverse_row(&row)?;
                if edge_type.is_some_and(|expected| edge.edge_type() != expected) {
                    return Ok(None);
                }
                let node = self.visible_node_or_corruption(record, graph, edge.src(), selector)?;
                let target_status = self.neighbor_target_status(record, &node, selector)?;
                Ok(Some(GraphNeighbor::new(
                    node,
                    edge,
                    GraphDirection::Incoming,
                    target_status,
                )))
            })
            .filter_map(Result::transpose)
            .collect()
    }

    fn visible_node_or_corruption(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<GraphNode, EngineError> {
        self.get_node_with_record(record, graph, node_id, selector)?
            .ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.graph_index",
                    "stored graph edge index points at a missing node",
                )
            })
    }

    fn get_node_with_record(
        &mut self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        selector: ReadSelector,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.node_row_with_selector(record, graph, node_id, selector)?
            .map(|row| self.node_from_row(&row))
            .transpose()
    }

    fn node_from_row(&self, row: &PersistenceReadRow) -> Result<GraphNode, EngineError> {
        let record = self.node_record_from_row(row)?;
        Ok(GraphNode::new(
            record.graph().clone(),
            record.node_id().clone(),
            record.data().clone(),
            row.commit_version(),
            row.commit_timestamp(),
        ))
    }

    fn node_record_from_row(
        &self,
        row: &PersistenceReadRow,
    ) -> Result<GraphNodeRecord, EngineError> {
        let (graph, node_id) = decode_graph_node_key(&self.space, row.key())?;
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_node_record",
                "stored graph node row is missing a value",
            )
        })?;
        decode_graph_node_record(&graph, &node_id, value)
    }

    fn edge_from_forward_row(&self, row: &PersistenceReadRow) -> Result<GraphEdge, EngineError> {
        let record = self.edge_record_from_forward_row(row)?;
        Ok(Self::edge_from_record(&record, row))
    }

    fn edge_from_reverse_row(&self, row: &PersistenceReadRow) -> Result<GraphEdge, EngineError> {
        let record = self.edge_record_from_reverse_row(row)?;
        Ok(Self::edge_from_record(&record, row))
    }

    fn edge_from_record(record: &GraphEdgeRecord, row: &PersistenceReadRow) -> GraphEdge {
        GraphEdge::new(
            record.graph().clone(),
            record.src().clone(),
            record.edge_type().clone(),
            record.dst().clone(),
            record.data().clone(),
            row.commit_version(),
            row.commit_timestamp(),
        )
    }

    fn edge_record_from_forward_row(
        &self,
        row: &PersistenceReadRow,
    ) -> Result<GraphEdgeRecord, EngineError> {
        let (graph, src, edge_type, dst) = decode_graph_edge_key(&self.space, row.key())?;
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_edge_record",
                "stored graph edge row is missing a value",
            )
        })?;
        decode_graph_edge_record(&graph, &src, &edge_type, &dst, value)
    }

    fn edge_record_from_reverse_row(
        &self,
        row: &PersistenceReadRow,
    ) -> Result<GraphEdgeRecord, EngineError> {
        let (graph, dst, edge_type, src) = decode_graph_reverse_edge_key(&self.space, row.key())?;
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_edge_record",
                "stored graph reverse edge row is missing a value",
            )
        })?;
        decode_graph_edge_record(&graph, &src, &edge_type, &dst, value)
    }

    fn binding_from_row(&self, row: &PersistenceReadRow) -> Result<GraphBinding, EngineError> {
        binding_from_index_row(&self.space, row)
    }

    fn put_edge_mutations(
        &self,
        record: &BranchCatalogRecord,
        mutations: &mut MutationMap,
        edge: &GraphEdgeRecord,
    ) -> Result<(), EngineError> {
        let encoded = encode_graph_edge_record(edge)?;
        mutations.put(
            self.edge_address(
                record,
                edge.graph(),
                edge.src(),
                edge.edge_type(),
                edge.dst(),
            ),
            encoded.clone(),
        );
        mutations.put(
            self.reverse_edge_address(
                record,
                edge.graph(),
                edge.dst(),
                edge.edge_type(),
                edge.src(),
            ),
            encoded,
        );
        Ok(())
    }

    fn delete_edge_mutations(
        &self,
        record: &BranchCatalogRecord,
        mutations: &mut MutationMap,
        edge: &GraphEdgeRecord,
    ) {
        mutations.delete(self.edge_address(
            record,
            edge.graph(),
            edge.src(),
            edge.edge_type(),
            edge.dst(),
        ));
        mutations.delete(self.reverse_edge_address(
            record,
            edge.graph(),
            edge.dst(),
            edge.edge_type(),
            edge.src(),
        ));
    }

    fn commit_batch(
        &mut self,
        record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
    ) -> Result<CommitOutcome, EngineError> {
        let mut mutations = mutations;
        if mutations.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.graph_batch",
                "graph batch must contain at least one mutation",
            ));
        }
        // Count only authored rows (graph metadata, nodes, forward edges) for
        // the user-facing commit counts. Derived reverse-edge and binding-index
        // rows are engine-maintained and must not inflate the caller's view of
        // rows written/deleted (one edge upsert would otherwise report 2).
        let user_put_count = mutations
            .iter()
            .filter(|mutation| {
                mutation.is_put() && is_authored_graph_row(mutation.address().row_class())
            })
            .count();
        let user_delete_count = mutations
            .iter()
            .filter(|mutation| {
                mutation.is_delete() && is_authored_graph_row(mutation.address().row_class())
            })
            .count();
        let mut space_mutations =
            ControlPlane::space_registration_mutations(self.persistence, record, &self.space)?;
        if !space_mutations.is_empty() {
            space_mutations.extend(mutations);
            mutations = space_mutations;
        }
        let plan = CommitPlan::new(
            record.storage_branch_id(),
            mutations,
            Some(record.generation()),
        );
        Ok(self
            .persistence
            .commit(&plan)?
            .with_counts(user_put_count, user_delete_count))
    }
}

/// Returns true for graph row classes that represent authored data (metadata,
/// ontology, nodes, forward edges) as opposed to engine-derived rows (reverse
/// edges, binding index) that must not be counted in user-facing commit
/// outcomes.
const fn is_authored_graph_row(row_class: RowClass) -> bool {
    matches!(
        row_class,
        RowClass::GraphMetadata
            | RowClass::GraphOntology
            | RowClass::GraphNode
            | RowClass::GraphEdge
    )
}

#[derive(Default)]
struct MutationMap {
    mutations: BTreeMap<MutationKey, RowMutation>,
}

impl MutationMap {
    fn put(&mut self, address: RowAddress, value: Vec<u8>) {
        self.mutations
            .insert(mutation_key(&address), RowMutation::put(address, value));
    }

    fn delete(&mut self, address: RowAddress) {
        self.mutations
            .insert(mutation_key(&address), RowMutation::delete(address));
    }

    fn into_mutations(self) -> Vec<RowMutation> {
        self.mutations.into_values().collect()
    }
}

fn mutation_key(address: &RowAddress) -> MutationKey {
    (address.row_class(), address.key().to_vec())
}

fn missing_edge_endpoint() -> EngineError {
    EngineError::invalid_input(
        "invalid_argument.engine.graph_edge_endpoint",
        "graph edge endpoints must exist before an edge can be written",
    )
}

fn edge_identity(edge: &GraphEdgeRecord) -> EdgeIdentity {
    (
        edge.src().clone(),
        edge.edge_type().clone(),
        edge.dst().clone(),
    )
}

fn neighbor_cursor(hit: &GraphNeighbor) -> String {
    let direction = match hit.direction() {
        GraphDirection::Outgoing => "o",
        GraphDirection::Incoming => "i",
        GraphDirection::Both => "b",
    };
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        direction,
        hit.edge().edge_type().as_str(),
        hit.node().node_id().as_str(),
        hit.edge().dst().as_str()
    )
}

fn binding_cursor(binding: &GraphBinding) -> String {
    format!(
        "{}\u{1f}{}",
        binding.graph().as_str(),
        binding.node_id().as_str()
    )
}

fn binding_from_index_row(
    space: &ProductSpace,
    row: &PersistenceReadRow,
) -> Result<GraphBinding, EngineError> {
    let (target, graph, node_id) = decode_graph_binding_key(space, row.key())?;
    let value = row.value().ok_or_else(|| {
        EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding row is missing a value",
        )
    })?;
    let record = decode_graph_binding_record(&graph, &node_id, value)?;
    if &target != record.binding().target() {
        return Err(EngineError::corruption(
            "data_loss.engine.graph_binding_record",
            "stored graph binding target does not match its row key",
        ));
    }
    Ok(GraphBinding::new(
        graph,
        node_id,
        record.binding().clone(),
        row.commit_version(),
        row.commit_timestamp(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{binding_from_index_row, neighbor_cursor};
    use crate::data::graph::{
        encode_graph_binding_record, GraphBindingPrimitive, GraphBindingRecord, GraphBindingTarget,
        GraphDirection, GraphEdge, GraphEdgeData, GraphEdgeType, GraphEntityBinding, GraphName,
        GraphNeighbor, GraphNode, GraphNodeData, GraphNodeId,
    };
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{encode_graph_binding_key, PersistenceReadRow};
    use strata_core::{CommitVersion, Timestamp};

    #[test]
    fn neighbor_cursor_orders_direction_and_identity() {
        let graph = GraphName::new("deps").expect("graph");
        let edge_type = GraphEdgeType::new("links").expect("edge type");
        let node_a = GraphNodeId::new("a").expect("node");
        let node_b = GraphNodeId::new("b").expect("node");
        let node = GraphNode::new(
            graph.clone(),
            node_b.clone(),
            GraphNodeData::default(),
            CommitVersion::new(1),
            Timestamp::from_micros(1),
        );
        let edge = GraphEdge::new(
            graph,
            node_a,
            edge_type,
            node_b,
            GraphEdgeData::default(),
            CommitVersion::new(1),
            Timestamp::from_micros(1),
        );
        let hit = GraphNeighbor::new(node, edge, GraphDirection::Outgoing, None);
        assert!(neighbor_cursor(&hit).starts_with("o\u{1f}links"));
    }

    #[test]
    fn binding_index_row_rejects_target_mismatch() {
        let space = ProductSpace::new("default").expect("space");
        let graph = GraphName::new("deps").expect("graph");
        let node_id = GraphNodeId::new("doc").expect("node");
        let key_target = GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            None,
            ProductSpace::new("docs").expect("space"),
            "doc-a",
        )
        .expect("key target");
        let stored_target = GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            None,
            ProductSpace::new("docs").expect("space"),
            "doc-b",
        )
        .expect("stored target");
        let binding = GraphEntityBinding::new(stored_target);
        let record = GraphBindingRecord::new(graph.clone(), node_id.clone(), binding);
        let row = PersistenceReadRow::for_test(
            encode_graph_binding_key(&space, &key_target, &graph, &node_id),
            Some(encode_graph_binding_record(&record).expect("record encodes")),
            false,
        );

        let error = binding_from_index_row(&space, &row).expect_err("target mismatch rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_binding_record");
    }
}
