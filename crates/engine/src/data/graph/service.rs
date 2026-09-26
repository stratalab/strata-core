//! Graph core service.

use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap, HashSet};

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
    encode_graph_incoming_edge_prefix, encode_graph_incoming_edge_type_prefix,
    encode_graph_metadata_key, encode_graph_metadata_prefix, encode_graph_node_key,
    encode_graph_node_prefix, encode_graph_ontology_key, encode_graph_outgoing_edge_prefix,
    encode_graph_outgoing_edge_type_prefix, encode_graph_reverse_edge_key,
    encode_graph_reverse_edge_prefix, encode_graph_type_index_graph_prefix,
    encode_graph_type_index_key, encode_graph_type_index_type_prefix, encode_json_key,
    encode_kv_key_bytes, CommitPlan, OrderedTextScan, PersistenceReadRow, ReadSelector, RowAddress,
    RowClass, RowMutation, StoragePersistence,
};

use super::{
    decode_graph_binding_record, decode_graph_edge_record, decode_graph_metadata_record,
    decode_graph_node_record, decode_graph_ontology_record, decode_graph_type_index_record,
    encode_graph_binding_record, encode_graph_edge_record, encode_graph_metadata_record,
    encode_graph_node_record, encode_graph_ontology_record, encode_graph_type_index_record,
    GraphAdjacencyIndex, GraphAdjacencyIndexBuilder, GraphAnalyticsBudget, GraphBatchOpOutcome,
    GraphBatchOperation, GraphBatchWrite, GraphBatchWriteOutcome, GraphBinding, GraphBindingPage,
    GraphBindingPrimitive, GraphBindingRecord, GraphBindingTarget, GraphBulkInsertOutcome,
    GraphCommitPoint, GraphCounts, GraphDeleteOutcome, GraphDeletePolicy, GraphDeletePolicyOutcome,
    GraphDirection, GraphEdge, GraphEdgePage, GraphEdgeRecord, GraphEdgeType,
    GraphEdgeWriteOutcome, GraphInfo, GraphLinkTypeDef, GraphLinkTypeSummary, GraphMetadataRecord,
    GraphName, GraphNamePage, GraphNeighbor, GraphNeighborPage, GraphNode, GraphNodeId,
    GraphNodePage, GraphNodeRecord, GraphObjectTypeDef, GraphObjectTypeSummary, GraphOntology,
    GraphOntologyFreezeOutcome, GraphOntologyRecord, GraphOntologySummary,
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
        if let Some(row) = self.stored_graph_metadata_row(&record, &name, ReadSelector::Latest)? {
            if Self::graph_metadata_from_row(&name, &row)?.deleting() {
                // #3477: a deletion interrupted before its sweep finished.
                // Finish it first, so no row of the old graph survives under
                // the new one's name.
                self.sweep_deleted_graph(&record, &name)?;
            } else {
                return Err(EngineError::conflict(
                    "already_exists.engine.graph",
                    "graph already exists",
                ));
            }
        }
        let metadata = super::GraphMetadataRecord::new(name.clone());
        let commit = self.commit_batch(
            &record,
            vec![RowMutation::put(
                address,
                encode_graph_metadata_record(&metadata),
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
            false,
        );
        Ok((info, commit))
    }

    /// Deletes a graph and all visible graph data rows.
    pub fn delete_graph(
        &mut self,
        name: &GraphName,
        force: bool,
    ) -> Result<GraphDeleteOutcome, EngineError> {
        let record = self.branch_record()?;
        let Some(row) = self.stored_graph_metadata_row(&record, name, ReadSelector::Latest)? else {
            return Ok(GraphDeleteOutcome::new(name.clone(), false, None));
        };
        let metadata = Self::graph_metadata_from_row(name, &row)?;
        if metadata.deleting() {
            // #3477: a deletion interrupted before its sweep finished. The
            // graph is already gone to every observer; finish the sweep.
            let last = self.sweep_deleted_graph(&record, name)?;
            return Ok(GraphDeleteOutcome::new(name.clone(), true, Some(last)));
        }

        // #3122: a populated graph refuses deletion without force, matching
        // `space delete` — deleting a graph destroys every node and edge in it,
        // so a mistyped name must not silently wipe data the caller did not
        // name. Nodes are the content (edges require both endpoints).
        if !force
            && self
                .node_rows(&record, name, ReadSelector::Latest)?
                .iter()
                .any(|row| !row.is_tombstone())
        {
            return Err(EngineError::conflict(
                "failed_precondition.engine.graph_not_empty",
                format!(
                    "graph `{}` contains visible data; retry with force=true to delete it",
                    name.as_str()
                ),
            ));
        }

        let rows = self.graph_row_tombstones(&record, name)?;
        if rows.len() < Self::DELETE_CHUNK_ROWS {
            // Small enough for one commit: the graph and its rows go together.
            let mut mutations = Vec::with_capacity(rows.len() + 1);
            mutations.push(RowMutation::delete(self.metadata_address(&record, name)));
            mutations.extend(rows);
            let commit = self.commit_batch(&record, mutations)?;
            return Ok(GraphDeleteOutcome::new(name.clone(), true, Some(commit)));
        }
        // #3477: too many rows for the storage commit budget. Mark first —
        // from this commit the graph is absent to every reader and writer —
        // then sweep the rows in commits the budget admits, and tombstone the
        // marked row last. An interruption leaves a marked row, which the
        // next `delete_graph` or `create_graph` of this name finishes.
        self.commit_batch(
            &record,
            vec![RowMutation::put(
                self.metadata_address(&record, name),
                encode_graph_metadata_record(&metadata.marked_deleting()),
            )],
        )?;
        let last = self.sweep_deleted_graph_rows(&record, name, rows)?;
        Ok(GraphDeleteOutcome::new(name.clone(), true, Some(last)))
    }

    /// Rows tombstoned per sweep commit of a chunked deletion (#3477): the
    /// chunk shared with space deletion (#3574), half the storage layer's
    /// default per-commit mutation budget (4096), so a chunk fits beside
    /// whatever else a commit carries. A graph with fewer rows than this
    /// deletes in one commit, as it always did.
    pub(crate) const DELETE_CHUNK_ROWS: usize = crate::control::space::DELETE_CHUNK_ROWS;

    /// Every row a graph owns beyond its metadata row, as tombstones, in the
    /// order a sweep removes them: the index rows another graph's readers
    /// could surface first (bindings, node types), then edges both ways,
    /// then nodes, then the ontology. Rows already tombstoned are not seen,
    /// so the list is exactly what remains — which is what makes a sweep
    /// resumable.
    fn graph_row_tombstones(
        &self,
        record: &BranchCatalogRecord,
        name: &GraphName,
    ) -> Result<Vec<RowMutation>, EngineError> {
        let mut mutations = Vec::new();
        let tombstone = |class: RowClass, row: &PersistenceReadRow| {
            RowMutation::delete(RowAddress::new(
                record.storage_branch_id(),
                class,
                row.key().to_vec(),
            ))
        };
        for row in self.binding_rows_for_space(record, ReadSelector::Latest)? {
            if row.is_tombstone() {
                continue;
            }
            let (_, graph, _) = decode_graph_binding_key(&self.space, row.key())?;
            if &graph == name {
                mutations.push(tombstone(RowClass::GraphBindingIndex, &row));
            }
        }
        for row in self.type_index_rows(record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(tombstone(RowClass::GraphTypeIndex, &row));
            }
        }
        for row in self.reverse_edge_rows(record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(tombstone(RowClass::GraphReverseEdge, &row));
            }
        }
        for row in self.edge_rows(record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(tombstone(RowClass::GraphEdge, &row));
            }
        }
        for row in self.node_rows(record, name, ReadSelector::Latest)? {
            if !row.is_tombstone() {
                mutations.push(tombstone(RowClass::GraphNode, &row));
            }
        }
        if self
            .ontology_row(record, name, ReadSelector::Latest)?
            .is_some()
        {
            mutations.push(RowMutation::delete(self.ontology_address(record, name)));
        }
        Ok(mutations)
    }

    /// Finishes the deletion of a graph whose metadata row carries the mark:
    /// whatever rows remain, then the row. Returns the final commit.
    fn sweep_deleted_graph(
        &self,
        record: &BranchCatalogRecord,
        name: &GraphName,
    ) -> Result<CommitOutcome, EngineError> {
        let rows = self.graph_row_tombstones(record, name)?;
        self.sweep_deleted_graph_rows(record, name, rows)
    }

    /// Tombstones `rows` in commits of at most [`Self::DELETE_CHUNK_ROWS`],
    /// then tombstones the marked metadata row in a commit of its own, so
    /// the row outlives every row it describes and a crash at any point
    /// leaves a resumable deletion, never an orphaned one. The returned
    /// outcome is the final commit carrying the row counts of the whole
    /// sweep, so the acknowledgement reads like a single-commit deletion's.
    fn sweep_deleted_graph_rows(
        &self,
        record: &BranchCatalogRecord,
        name: &GraphName,
        mut rows: Vec<RowMutation>,
    ) -> Result<CommitOutcome, EngineError> {
        let mut deleted = 0_usize;
        while !rows.is_empty() {
            let rest = rows.split_off(rows.len().min(Self::DELETE_CHUNK_ROWS));
            deleted = deleted.saturating_add(self.commit_batch(record, rows)?.delete_count());
            rows = rest;
        }
        let last = self.commit_batch(
            record,
            vec![RowMutation::delete(self.metadata_address(record, name))],
        )?;
        let deleted = deleted.saturating_add(last.delete_count());
        Ok(last.with_counts(0, deleted))
    }

    /// Performs only the first commit of a chunked deletion — the mark — and
    /// stops, leaving the graph exactly as a crash between the mark and the
    /// sweep would: absent to observers, its rows still stored, its deletion
    /// waiting for the next `delete_graph` or `create_graph` of its name.
    #[cfg(any(test, feature = "testkit"))]
    pub fn begin_graph_delete_for_test(
        &mut self,
        name: &GraphName,
    ) -> Result<CommitOutcome, EngineError> {
        let record = self.branch_record()?;
        let row = self
            .graph_metadata_row(&record, name, ReadSelector::Latest)?
            .ok_or_else(|| {
                EngineError::not_found("not_found.engine.graph", "graph does not exist")
            })?;
        let metadata = Self::graph_metadata_from_row(name, &row)?;
        self.commit_batch(
            &record,
            vec![RowMutation::put(
                self.metadata_address(&record, name),
                encode_graph_metadata_record(&metadata.marked_deleting()),
            )],
        )
    }

    /// Lists visible graphs.
    pub fn list_graphs(
        &self,
        cursor: Option<&GraphName>,
        limit: usize,
    ) -> Result<GraphNamePage, EngineError> {
        self.list_graphs_with_selector(cursor, limit, ReadSelector::Latest)
    }

    /// Lists graphs visible at a commit version.
    pub fn list_graphs_at_version(
        &self,
        cursor: Option<&GraphName>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphNamePage, EngineError> {
        self.list_graphs_with_selector(cursor, limit, ReadSelector::AtVersion(version))
    }

    /// Lists graphs visible at a timestamp.
    pub fn list_graphs_at(
        &self,
        cursor: Option<&GraphName>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphNamePage, EngineError> {
        self.list_graphs_with_selector(cursor, limit, ReadSelector::AtTimestamp(timestamp))
    }

    fn list_graphs_with_selector(
        &self,
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
            .map(|row| {
                let graph = decode_graph_metadata_key(&self.space, row.key())?;
                // #3477: a graph mid-deletion is absent, here as everywhere.
                let listed = !Self::graph_metadata_from_row(&graph, &row)?.deleting();
                Ok(listed.then_some(graph))
            })
            .filter_map(Result::transpose)
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
    pub fn graph_info(&self, name: &GraphName) -> Result<Option<GraphInfo>, EngineError> {
        self.graph_info_with_selector(name, ReadSelector::Latest)
    }

    /// Returns graph metadata visible at a commit version.
    pub fn graph_info_at_version(
        &self,
        name: &GraphName,
        version: CommitVersion,
    ) -> Result<Option<GraphInfo>, EngineError> {
        self.graph_info_with_selector(name, ReadSelector::AtVersion(version))
    }

    /// Returns graph metadata visible at a timestamp.
    pub fn graph_info_at(
        &self,
        name: &GraphName,
        timestamp: Timestamp,
    ) -> Result<Option<GraphInfo>, EngineError> {
        self.graph_info_with_selector(name, ReadSelector::AtTimestamp(timestamp))
    }

    /// Writes the graph the way a release before #3474 left it: the metadata
    /// row in its old form (the graph name alone — no counts, no create
    /// point), then one plain node row per id, each in its own later commit
    /// with no metadata rewrite. A test can then hold the scan fallback —
    /// counts by scan, `updated` from rows newer than the metadata row — and
    /// the backfill the graph's next write performs, to the maintained row.
    /// Returns the metadata commit and the last node commit, when any.
    #[cfg(any(test, feature = "testkit"))]
    pub fn write_legacy_graph_rows_for_test(
        &mut self,
        name: &GraphName,
        node_ids: &[GraphNodeId],
    ) -> Result<(CommitOutcome, Option<CommitOutcome>), EngineError> {
        let record = self.branch_record()?;
        self.require_graph(&record, name)?;
        let metadata = self.commit_batch(
            &record,
            vec![RowMutation::put(
                self.metadata_address(&record, name),
                super::record::encode_legacy_graph_metadata_record_for_test(name),
            )],
        )?;
        let mut last_node = None;
        for node_id in node_ids {
            let node = GraphNodeRecord::new(
                name.clone(),
                node_id.clone(),
                super::GraphNodeData::default(),
            );
            last_node = Some(self.commit_batch(
                &record,
                vec![RowMutation::put(
                    self.node_address(&record, name, node_id),
                    encode_graph_node_record(&node),
                )],
            )?);
        }
        Ok((metadata, last_node))
    }

    fn graph_info_with_selector(
        &self,
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
    /// The nodes bound to `target` across the space's graphs: reverse-index
    /// candidates verified against the authoritative node binding (reverse
    /// maps are candidate indexes, not truth). #3477: a graph mid-deletion
    /// contributes none — its rows are the sweep's, not a policy's.
    fn verified_binding_candidates(
        &self,
        record: &BranchCatalogRecord,
        target: &GraphBindingTarget,
    ) -> Result<Vec<(GraphName, GraphNodeId, GraphNodeRecord)>, EngineError> {
        let mut verified = Vec::new();
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
            if self
                .graph_metadata_row(record, &graph, ReadSelector::Latest)?
                .is_none()
            {
                continue;
            }
            let Some(node) = self.node_record(record, &graph, &node_id)? else {
                continue;
            };
            if node.data().binding().map(super::GraphEntityBinding::target) == Some(target) {
                verified.push((graph, node_id, node));
            }
        }
        Ok(verified)
    }

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

        let verified = self.verified_binding_candidates(&record, target)?;
        let nodes_affected = verified.len() as u64;

        let mut mutations = MutationMap::default();
        // #3474: every graph whose node or edge rows this commit touches gets
        // its metadata row rewritten with the change to its live counts.
        let mut touched: BTreeMap<GraphName, (i64, i64)> = BTreeMap::new();
        match policy {
            GraphDeletePolicy::KeepDangling => {}
            GraphDeletePolicy::Detach => {
                for (graph, node_id, node) in &verified {
                    touched.entry(graph.clone()).or_insert((0, 0));
                    let mut data =
                        super::GraphNodeData::new(node.data().properties().cloned(), None);
                    if let Some(object_type) = node.data().object_type() {
                        data = data.with_object_type(object_type.clone());
                    }
                    let detached = GraphNodeRecord::new(graph.clone(), node_id.clone(), data);
                    mutations.put(
                        self.node_address(&record, graph, node_id),
                        encode_graph_node_record(&detached),
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
                    // #3472: each node's own adjacency, merged by identity —
                    // an edge between two cascaded nodes is found from both
                    // ends and removed once.
                    let mut incident: BTreeMap<EdgeIdentity, GraphEdgeRecord> = BTreeMap::new();
                    for node_id in node_ids {
                        incident.extend(self.stored_incident_edges(&record, graph, node_id)?);
                    }
                    for edge in incident.values() {
                        self.delete_edge_mutations(&record, &mut mutations, edge);
                    }
                    touched.insert(
                        graph.clone(),
                        (
                            -i64::try_from(node_ids.len()).unwrap_or(i64::MAX),
                            -i64::try_from(incident.len()).unwrap_or(i64::MAX),
                        ),
                    );
                }
            }
        }
        for (graph, (node_delta, edge_delta)) in &touched {
            let (metadata_address, metadata_value) =
                self.metadata_mutation(&record, graph, *node_delta, *edge_delta)?;
            mutations.put(metadata_address, metadata_value);
        }

        let mutations = mutations.into_mutations();
        let commit = if mutations.is_empty() {
            None
        } else {
            Some(self.commit_batch_maintaining(&record, mutations, touched.len())?)
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
        &self,
        target: &GraphBindingTarget,
    ) -> Result<GraphTargetStatus, EngineError> {
        let record = self.branch_record()?;
        self.binding_target_status(&record, target, ReadSelector::Latest)
    }

    /// Resolves a binding target's status at a commit version.
    pub fn resolve_binding_target_at_version(
        &self,
        target: &GraphBindingTarget,
        version: CommitVersion,
    ) -> Result<GraphTargetStatus, EngineError> {
        let record = self.branch_record()?;
        self.binding_target_status(&record, target, ReadSelector::AtVersion(version))
    }

    /// Resolves a binding target's status at a timestamp.
    pub fn resolve_binding_target_at(
        &self,
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
        &self,
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
        &self,
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
                )),
            ));
        }
        mutations.push(RowMutation::put(
            self.node_address(&record, graph, &node_id),
            encode_graph_node_record(&new_record),
        ));
        if let Some(binding) = new_record.data().binding() {
            let binding_record =
                GraphBindingRecord::new(graph.clone(), node_id.clone(), binding.clone());
            mutations.push(RowMutation::put(
                self.binding_address(&record, binding.target(), graph, &node_id),
                encode_graph_binding_record(&binding_record),
            ));
        }
        let (metadata_address, metadata_value) =
            self.metadata_mutation(&record, graph, i64::from(created), 0)?;
        mutations.push(RowMutation::put(metadata_address, metadata_value));
        let commit = self.commit_batch_maintaining(&record, mutations, 1)?;
        Ok(GraphWriteOutcome::new(
            graph.clone(),
            node_id,
            created,
            commit,
        ))
    }

    /// Reads one visible graph node.
    pub fn get_node(
        &self,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.get_node_with_selector(graph, node_id, ReadSelector::Latest)
    }

    /// Reads one graph node visible at a commit version.
    pub fn get_node_at_version(
        &self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        version: CommitVersion,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.get_node_with_selector(graph, node_id, ReadSelector::AtVersion(version))
    }

    /// Reads one graph node visible at a timestamp.
    pub fn get_node_at(
        &self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        timestamp: Timestamp,
    ) -> Result<Option<GraphNode>, EngineError> {
        self.get_node_with_selector(graph, node_id, ReadSelector::AtTimestamp(timestamp))
    }

    fn get_node_with_selector(
        &self,
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
    ///
    /// The incident-edge cascade is unconditional; there is no refuse-if-wired
    /// mode in V1 (#3194). To refuse when a node still has edges, `neighbors()`
    /// first and decide before calling this.
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
        let incident = self.stored_incident_edges(&record, graph, node_id)?;
        for edge in incident.values() {
            self.delete_edge_mutations(&record, &mut mutations, edge);
        }
        let removed_edges = i64::try_from(incident.len()).unwrap_or(i64::MAX);
        let (metadata_address, metadata_value) =
            self.metadata_mutation(&record, graph, -1, -removed_edges)?;
        mutations.put(metadata_address, metadata_value);
        let commit = self.commit_batch_maintaining(&record, mutations.into_mutations(), 1)?;
        Ok(GraphDeleteOutcome::new(graph.clone(), true, Some(commit)))
    }

    /// Samples up to `count` nodes from a graph using a deterministic stride
    /// over the ordered live nodes. Returns the total live node count and the
    /// sample.
    pub fn sample_nodes(
        &self,
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
        &self,
        graph: &GraphName,
        prefix: Option<&GraphNodeId>,
        cursor: Option<&GraphNodeId>,
        limit: usize,
    ) -> Result<GraphNodePage, EngineError> {
        self.list_nodes_with_selector(graph, prefix, cursor, limit, ReadSelector::Latest)
    }

    /// Lists graph nodes visible at a commit version.
    pub fn list_nodes_at_version(
        &self,
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
        &self,
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
        &self,
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
        // Seek from the cursor rather than slice a whole-graph scan: the page
        // reads its rows plus one lookahead, in node-id order, and decodes
        // only those (#3458).
        let mut nodes = OrderedTextScan::new(
            self.persistence,
            record.storage_branch_id(),
            RowClass::GraphNode,
            encode_graph_node_prefix(&self.space, graph),
            selector,
            "data_loss.engine.graph_node_key",
        )
        .rows_after(
            cursor.map(|id| id.as_str().as_bytes()),
            prefix.map(|id| id.as_str().as_bytes()),
            limit + 1,
        )?
        .iter()
        .map(|row| self.node_from_row(row))
        .collect::<EngineResult<Vec<_>>>()?;
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
        let (metadata_address, metadata_value) =
            self.metadata_mutation(&record, graph, 0, i64::from(created))?;
        let commit = self.commit_batch_maintaining(
            &record,
            vec![
                RowMutation::put(
                    self.edge_address(&record, graph, &src, &edge_type, &dst),
                    encode_graph_edge_record(&edge),
                ),
                RowMutation::put(
                    self.reverse_edge_address(&record, graph, &dst, &edge_type, &src),
                    encode_graph_edge_record(&edge),
                ),
                RowMutation::put(metadata_address, metadata_value),
            ],
            1,
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
    ///
    /// Each chunk is a commit, so an interruption keeps the chunks that
    /// landed (#3464). An import spanning more than one commit sets the
    /// graph's import watermark with its first commit and clears it with
    /// its last — [`GraphInfo::import_pending`] reports it in between and
    /// after a cut-short import. Every row is an upsert, so re-running the
    /// same payload completes an interrupted import and clears the
    /// watermark; the graph stays readable and writable meanwhile.
    ///
    /// The watermark tracks "a multi-commit import is in progress", not "this
    /// payload is complete": whichever `bulk_insert`'s last chunk lands next
    /// clears it, `chunk_size` notwithstanding, while ordinary writes leave it
    /// untouched. So to *finish* an interrupted import re-run its own payload —
    /// a smaller or different import clears the watermark once its own last (or
    /// only) chunk lands without having filled in the rows the interrupted
    /// import still owes.
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
        self.bulk_insert_limited(graph, nodes, edges, chunk_size, None)
    }

    /// [`Self::bulk_insert`] that stops after `max_commits` chunk commits,
    /// leaving the graph exactly as a crash there would — the chunks that
    /// landed, the import watermark set — for tests of the resume contract.
    #[cfg(any(test, feature = "testkit"))]
    pub fn bulk_insert_interrupted_for_test(
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
        max_commits: u64,
    ) -> Result<GraphBulkInsertOutcome, EngineError> {
        self.bulk_insert_limited(graph, nodes, edges, chunk_size, Some(max_commits))
    }

    // Two chunk loops that mirror `upsert_node` and `upsert_edge` row for
    // row, plus the per-chunk count accounting (#3474) and the import
    // watermark (#3464); splitting them would hide the one-commit-per-chunk
    // shape the outcome reports.
    #[allow(clippy::too_many_lines)]
    fn bulk_insert_limited(
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
        max_commits: Option<u64>,
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
        // #3464: the watermark is set by every chunk but the last, so a
        // one-commit import never carries it and an interrupted one does.
        let total_chunks = nodes.chunks(chunk_size).len() + edges.chunks(chunk_size).len();
        let mut chunk_index = 0_usize;
        'chunks: {
            for chunk in nodes.chunks(chunk_size) {
                // The interruption seam stops *before* the (`commits`+1)-th
                // chunk, so `max_commits == 0` commits nothing — the state a
                // crash before the first chunk landed leaves.
                if max_commits.is_some_and(|max| commits >= max) {
                    break 'chunks;
                }
                let mut mutations = MutationMap::default();
                // #3474: the chunk's commit carries the graph's counts, so a node
                // is new when it is neither stored nor earlier in this chunk.
                let mut new_nodes = 0_i64;
                let mut seen = HashSet::new();
                for (node_id, data) in chunk {
                    // Upsert discipline: drop stale derived rows before the
                    // new node row lands, exactly like `upsert_node`.
                    let current = self.node_record(&record, graph, node_id)?;
                    if current.is_none() && seen.insert(node_id) {
                        new_nodes += 1;
                    }
                    let new_record =
                        GraphNodeRecord::new(graph.clone(), node_id.clone(), data.clone());
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
                            )),
                        );
                    }
                    mutations.put(
                        self.node_address(&record, graph, node_id),
                        encode_graph_node_record(&new_record),
                    );
                    if let Some(binding) = new_record.data().binding() {
                        mutations.put(
                            self.binding_address(&record, binding.target(), graph, node_id),
                            encode_graph_binding_record(&GraphBindingRecord::new(
                                graph.clone(),
                                node_id.clone(),
                                binding.clone(),
                            )),
                        );
                    }
                }
                let (metadata_address, metadata_value) = self.metadata_mutation_marking(
                    &record,
                    graph,
                    new_nodes,
                    0,
                    Some(chunk_index + 1 < total_chunks),
                )?;
                mutations.put(metadata_address, metadata_value);
                last_commit =
                    Some(self.commit_batch_maintaining(&record, mutations.into_mutations(), 1)?);
                commits += 1;
                chunk_index += 1;
            }
            for chunk in edges.chunks(chunk_size) {
                if max_commits.is_some_and(|max| commits >= max) {
                    break 'chunks;
                }
                let mut mutations = MutationMap::default();
                let mut new_edges = 0_i64;
                let mut seen = HashSet::new();
                for (src, edge_type, dst, data) in chunk {
                    if self
                        .edge_record(&record, graph, src, edge_type, dst)?
                        .is_none()
                        && seen.insert((src, edge_type, dst))
                    {
                        new_edges += 1;
                    }
                    let edge = GraphEdgeRecord::new(
                        graph.clone(),
                        src.clone(),
                        edge_type.clone(),
                        dst.clone(),
                        data.clone(),
                    );
                    mutations.put(
                        self.edge_address(&record, graph, src, edge_type, dst),
                        encode_graph_edge_record(&edge),
                    );
                    mutations.put(
                        self.reverse_edge_address(&record, graph, dst, edge_type, src),
                        encode_graph_edge_record(&edge),
                    );
                }
                let (metadata_address, metadata_value) = self.metadata_mutation_marking(
                    &record,
                    graph,
                    0,
                    new_edges,
                    Some(chunk_index + 1 < total_chunks),
                )?;
                mutations.put(metadata_address, metadata_value);
                last_commit =
                    Some(self.commit_batch_maintaining(&record, mutations.into_mutations(), 1)?);
                commits += 1;
                chunk_index += 1;
            }
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
        &self,
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
        &self,
        graph: &GraphName,
        src: &GraphNodeId,
        edge_type: &GraphEdgeType,
        dst: &GraphNodeId,
    ) -> Result<Option<GraphEdge>, EngineError> {
        self.get_edge_with_selector(graph, src, edge_type, dst, ReadSelector::Latest)
    }

    /// Reads one graph edge visible at a commit version.
    pub fn get_edge_at_version(
        &self,
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
        &self,
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
        &self,
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
        let (metadata_address, metadata_value) = self.metadata_mutation(&record, graph, 0, -1)?;
        mutations.put(metadata_address, metadata_value);
        let commit = self.commit_batch_maintaining(&record, mutations.into_mutations(), 1)?;
        Ok(GraphDeleteOutcome::new(graph.clone(), true, Some(commit)))
    }

    /// Looks up neighboring nodes.
    pub fn neighbors(
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        let position = cursor.map(parse_neighbor_cursor).transpose()?;
        if limit == 0
            || self
                .node_record_with_selector(&record, graph, node_id, selector)?
                .is_none()
        {
            return Ok(GraphNeighborPage::new(Vec::new(), false, None));
        }
        // Incoming hits order before outgoing ones (a cursor's leading `i`
        // sorts before `o`), so `Both` drains the reverse space and then the
        // forward space — a concatenation, not a merge. Each leg seeks from
        // the cursor when the cursor names it, starts fresh when the cursor
        // precedes it, and is skipped when the cursor is already past it.
        // Only the page's hits are hydrated (#3489).
        let target = limit + 1;
        let mut hits = Vec::with_capacity(target);
        for leg in [GraphDirection::Incoming, GraphDirection::Outgoing] {
            if hits.len() >= target || !direction_includes(direction, leg) {
                continue;
            }
            let leg_position = match position.as_ref() {
                Some(position) => match leg_rank(leg).cmp(&leg_rank(position.direction)) {
                    // The cursor names this leg: seek from it.
                    Ordering::Equal => Some(position),
                    // The cursor is already past this leg.
                    Ordering::Less => continue,
                    // The cursor precedes this leg: start it fresh.
                    Ordering::Greater => None,
                },
                None => None,
            };
            self.neighbor_leg(
                &record,
                graph,
                node_id,
                leg,
                edge_type,
                leg_position,
                target,
                selector,
                &mut hits,
            )?;
        }
        let has_more = hits.len() > limit;
        if has_more {
            hits.truncate(limit);
        }
        let cursor = has_more.then(|| neighbor_cursor(hits.last().expect("non-empty page")));
        Ok(GraphNeighborPage::new(hits, has_more, cursor))
    }

    /// Appends one direction's hits to `hits` until it holds `target`, in
    /// `(edge type, neighbor)` order, seeking from `position` when the cursor
    /// named this leg.
    #[allow(clippy::too_many_arguments)]
    fn neighbor_leg(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
        leg: GraphDirection,
        edge_type: Option<&GraphEdgeType>,
        position: Option<&NeighborPosition>,
        target: usize,
        selector: ReadSelector,
        hits: &mut Vec<GraphNeighbor>,
    ) -> Result<(), EngineError> {
        let (row_class, key_code) = match leg {
            GraphDirection::Incoming => (
                RowClass::GraphReverseEdge,
                "data_loss.engine.graph_reverse_edge_key",
            ),
            GraphDirection::Outgoing => (RowClass::GraphEdge, "data_loss.engine.graph_edge_key"),
            GraphDirection::Both => unreachable!("a leg is a single direction"),
        };
        if let Some(filter) = edge_type {
            // One type. A cursor past it means this leg has nothing left; a
            // cursor within it positions the neighbors; any other starts fresh.
            if position.is_some_and(|position| position.edge_type > *filter) {
                return Ok(());
            }
            let after = position
                .filter(|position| position.edge_type == *filter)
                .map(|position| position.neighbor.as_str().as_bytes());
            let prefix = self.edge_type_prefix(graph, node_id, leg, filter);
            return self.neighbor_rows_into(
                record, graph, leg, row_class, key_code, prefix, after, target, selector, hits,
            );
        }
        // Every type, in string order, from the cursor's own type inclusive —
        // it may still hold neighbors past the cursor. Each distinct type is
        // one seek, however many edges it has.
        let type_scan = OrderedTextScan::new(
            self.persistence,
            record.storage_branch_id(),
            row_class,
            self.edge_prefix(graph, node_id, leg),
            selector,
            key_code,
        );
        let mut from = position.map(|position| position.edge_type.as_str().as_bytes().to_vec());
        let mut exclusive = false;
        loop {
            let want = target.saturating_sub(hits.len());
            if want == 0 {
                break;
            }
            let types = type_scan.distinct_values(from.as_deref(), exclusive, want)?;
            let fetched = types.len();
            for (raw, discovered_from) in types {
                // The row a type was discovered from is a row this read
                // touched: decode it so corruption in it surfaces here, as it
                // would from any read that returned it.
                match leg {
                    GraphDirection::Incoming => self.edge_from_reverse_row(&discovered_from)?,
                    _ => self.edge_from_forward_row(&discovered_from)?,
                };
                let edge_type = edge_type_from_key(&raw, key_code)?;
                let after = position
                    .filter(|position| position.edge_type == edge_type)
                    .map(|position| position.neighbor.as_str().as_bytes());
                let prefix = self.edge_type_prefix(graph, node_id, leg, &edge_type);
                self.neighbor_rows_into(
                    record, graph, leg, row_class, key_code, prefix, after, target, selector, hits,
                )?;
                from = Some(raw);
                exclusive = true;
                if hits.len() >= target {
                    break;
                }
            }
            if fetched < want {
                break;
            }
        }
        Ok(())
    }

    /// Reads one type's neighbors after `after` until `hits` holds `target`,
    /// hydrating each hit's node as it goes — so only the page is hydrated.
    #[allow(clippy::too_many_arguments)]
    fn neighbor_rows_into(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        leg: GraphDirection,
        row_class: RowClass,
        key_code: &'static str,
        prefix: Vec<u8>,
        after: Option<&[u8]>,
        target: usize,
        selector: ReadSelector,
        hits: &mut Vec<GraphNeighbor>,
    ) -> Result<(), EngineError> {
        let rows = OrderedTextScan::new(
            self.persistence,
            record.storage_branch_id(),
            row_class,
            prefix,
            selector,
            key_code,
        )
        .rows_after(after, None, target.saturating_sub(hits.len()))?;
        for row in &rows {
            let edge = match leg {
                GraphDirection::Incoming => self.edge_from_reverse_row(row)?,
                _ => self.edge_from_forward_row(row)?,
            };
            let neighbor = match leg {
                GraphDirection::Incoming => edge.src(),
                _ => edge.dst(),
            };
            let node = self.visible_node_or_corruption(record, graph, neighbor, selector)?;
            let target_status = self.neighbor_target_status(record, &node, selector)?;
            hits.push(GraphNeighbor::new(node, edge, leg, target_status));
        }
        Ok(())
    }

    /// The adjacency prefix of one node in one direction.
    fn edge_prefix(
        &self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        leg: GraphDirection,
    ) -> Vec<u8> {
        match leg {
            GraphDirection::Incoming => {
                encode_graph_incoming_edge_prefix(&self.space, graph, node_id)
            }
            _ => encode_graph_outgoing_edge_prefix(&self.space, graph, node_id),
        }
    }

    /// The adjacency prefix of one node, one direction and one edge type.
    fn edge_type_prefix(
        &self,
        graph: &GraphName,
        node_id: &GraphNodeId,
        leg: GraphDirection,
        edge_type: &GraphEdgeType,
    ) -> Vec<u8> {
        match leg {
            GraphDirection::Incoming => {
                encode_graph_incoming_edge_type_prefix(&self.space, graph, node_id, edge_type)
            }
            _ => encode_graph_outgoing_edge_type_prefix(&self.space, graph, node_id, edge_type),
        }
    }

    /// Lists a graph's edges with their full data, page by page.
    ///
    /// #3457: a process that imported edges carrying properties (street names,
    /// integer lengths) can page them all back with `GraphEdgeData` attached,
    /// instead of round-tripping `neighbors` per node or re-joining labels from
    /// an external fixture. Edges order by `(src, type, dst)`; `cursor` resumes
    /// after the last edge of the previous page.
    pub fn list_edges(
        &self,
        graph: &GraphName,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<GraphEdgePage, EngineError> {
        self.list_edges_with_selector(graph, cursor, limit, ReadSelector::Latest)
    }

    /// Lists a graph's edges visible at a commit version.
    pub fn list_edges_at_version(
        &self,
        graph: &GraphName,
        cursor: Option<&str>,
        limit: usize,
        version: CommitVersion,
    ) -> Result<GraphEdgePage, EngineError> {
        self.list_edges_with_selector(graph, cursor, limit, ReadSelector::AtVersion(version))
    }

    /// Lists a graph's edges visible at a timestamp.
    pub fn list_edges_at(
        &self,
        graph: &GraphName,
        cursor: Option<&str>,
        limit: usize,
        timestamp: Timestamp,
    ) -> Result<GraphEdgePage, EngineError> {
        self.list_edges_with_selector(graph, cursor, limit, ReadSelector::AtTimestamp(timestamp))
    }

    fn list_edges_with_selector(
        &self,
        graph: &GraphName,
        cursor: Option<&str>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphEdgePage, EngineError> {
        let record = self.branch_record()?;
        self.require_graph_with_selector(&record, graph, selector)?;
        if limit == 0 {
            return Ok(GraphEdgePage::new(Vec::new(), false, None));
        }
        let mut edges = self
            .edge_rows(&record, graph, selector)?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| self.edge_from_forward_row(&row))
            .collect::<EngineResult<Vec<_>>>()?;
        edges.sort_by_key(edge_cursor);
        if let Some(cursor) = cursor {
            edges.retain(|edge| edge_cursor(edge).as_str() > cursor);
        }
        let has_more = edges.len() > limit;
        if has_more {
            edges.truncate(limit);
        }
        let cursor = has_more.then(|| edge_cursor(edges.last().expect("non-empty page")));
        Ok(GraphEdgePage::new(edges, has_more, cursor))
    }

    /// Looks up graph nodes bound to an entity target.
    pub fn bindings_for_entity(
        &self,
        target: &GraphBindingTarget,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<GraphBindingPage, EngineError> {
        self.bindings_for_entity_with_selector(target, cursor, limit, ReadSelector::Latest)
    }

    /// Looks up graph nodes bound to an entity target at a commit version.
    pub fn bindings_for_entity_at_version(
        &self,
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
        &self,
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
        &self,
        target: &GraphBindingTarget,
        cursor: Option<&str>,
        limit: usize,
        selector: ReadSelector,
    ) -> Result<GraphBindingPage, EngineError> {
        let record = self.branch_record()?;
        if limit == 0 {
            return Ok(GraphBindingPage::new(Vec::new(), false, None));
        }
        let bindings = self
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
        // #3477: a binding-index row of a graph mid-deletion is a row the
        // sweep has not reached yet, not a binding a reader may see.
        let mut present: BTreeMap<GraphName, bool> = BTreeMap::new();
        let mut kept = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let graph = binding.graph().clone();
            let visible = if let Some(visible) = present.get(&graph) {
                *visible
            } else {
                let visible = self
                    .graph_metadata_row(&record, &graph, selector)?
                    .is_some();
                present.insert(graph, visible);
                visible
            };
            if visible {
                kept.push(binding);
            }
        }
        let mut bindings = kept;
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
    ///
    /// Operations apply in order against the batch-local state: each reads
    /// the nodes and edges it needs from storage the first time and sees
    /// the batch's own earlier operations after that, so a batch costs what
    /// it touches — point reads per node and edge, a deleted node's degree
    /// in rows — and never a copy of the graph (#3472).
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
        let mut overlay = BatchOverlay::default();
        let mut mutations = MutationMap::default();
        let mut outcomes = Vec::with_capacity(batch.operations().len());
        // #3474: what the batch adds or removes among live nodes and edges,
        // for the graph's metadata row.
        let mut node_delta = 0_i64;
        let mut edge_delta = 0_i64;

        for (index, operation) in batch.operations().iter().enumerate() {
            match operation {
                GraphBatchOperation::UpsertNode { node_id, data } => {
                    if let Some(binding) = data.binding() {
                        self.validate_binding_target(binding.target())?;
                    }
                    if let Some(ontology) = frozen.as_ref() {
                        ontology.validate_node(data)?;
                    }
                    let current = self.overlay_node(&record, graph, &mut overlay, node_id)?;
                    let created = current.is_none();
                    if let Some(old) = current.and_then(|record| record.data().binding()) {
                        if data.binding() != Some(old) {
                            mutations.delete(self.binding_address(
                                &record,
                                old.target(),
                                graph,
                                node_id,
                            ));
                        }
                    }
                    let old_type = current.and_then(|record| record.data().object_type());
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
                            )),
                        );
                    }
                    let node = GraphNodeRecord::new(graph.clone(), node_id.clone(), data.clone());
                    mutations.put(
                        self.node_address(&record, graph, node_id),
                        encode_graph_node_record(&node),
                    );
                    if let Some(binding) = node.data().binding() {
                        let binding_record = GraphBindingRecord::new(
                            graph.clone(),
                            node_id.clone(),
                            binding.clone(),
                        );
                        mutations.put(
                            self.binding_address(&record, binding.target(), graph, node_id),
                            encode_graph_binding_record(&binding_record),
                        );
                    }
                    overlay.nodes.insert(node_id.clone(), Some(node));
                    node_delta += i64::from(created);
                    outcomes.push(GraphBatchOpOutcome::created(index, created));
                }
                GraphBatchOperation::DeleteNode { node_id } => {
                    self.overlay_node(&record, graph, &mut overlay, node_id)?;
                    // Loaded just above, so the entry is there to take.
                    let removed = overlay.nodes.insert(node_id.clone(), None).flatten();
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
                        let incident =
                            self.overlay_incident_edges(&record, graph, &overlay, node_id)?;
                        node_delta -= 1;
                        edge_delta -= i64::try_from(incident.len()).unwrap_or(i64::MAX);
                        for edge in incident {
                            overlay.edges.insert(edge_identity(&edge), None);
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
                    let (src_state, dst_state) =
                        self.overlay_endpoints(&record, graph, &mut overlay, src, dst)?;
                    let (Some(src_record), Some(dst_record)) = (src_state, dst_state) else {
                        // #3192: when a missing endpoint is upserted LATER in this
                        // batch, name the ordering rule — operations apply in
                        // order, so nodes must precede their edges — rather than
                        // report a bare missing endpoint.
                        let upserted_later = |id: &GraphNodeId| {
                            // Operations strictly after this edge: `.skip(index)`
                            // drops the ops already applied to `nodes`, and the
                            // extra `.skip(1)` drops this edge itself.
                            batch.operations().iter().skip(index).skip(1).any(|later| {
                                matches!(
                                    later,
                                    GraphBatchOperation::UpsertNode { node_id, .. }
                                        if node_id == id
                                )
                            })
                        };
                        if (src_state.is_none() && upserted_later(src))
                            || (dst_state.is_none() && upserted_later(dst))
                        {
                            return Err(EngineError::invalid_input(
                                "invalid_argument.engine.graph_edge_endpoint",
                                "graph edge endpoints must be upserted before the edge within \
                                 the same batch; a node upserted later in the batch is not yet \
                                 visible — sort nodes before edges",
                            ));
                        }
                        return Err(missing_edge_endpoint());
                    };
                    if let Some(ontology) = frozen.as_ref() {
                        // Validated against the batch-local state: a node
                        // typed earlier in this batch governs its edges.
                        ontology.validate_edge(edge_type, src_record.data(), dst_record.data())?;
                    }
                    let identity = (src.clone(), edge_type.clone(), dst.clone());
                    let created = self
                        .overlay_edge(&record, graph, &mut overlay, &identity)?
                        .is_none();
                    let edge = GraphEdgeRecord::new(
                        graph.clone(),
                        src.clone(),
                        edge_type.clone(),
                        dst.clone(),
                        data.clone(),
                    );
                    self.put_edge_mutations(&record, &mut mutations, &edge)?;
                    overlay.edges.insert(identity, Some(edge));
                    edge_delta += i64::from(created);
                    outcomes.push(GraphBatchOpOutcome::created(index, created));
                }
                GraphBatchOperation::DeleteEdge {
                    src,
                    edge_type,
                    dst,
                } => {
                    let identity = (src.clone(), edge_type.clone(), dst.clone());
                    let deleted = self
                        .overlay_edge(&record, graph, &mut overlay, &identity)?
                        .is_some();
                    if deleted {
                        overlay.edges.insert(identity, None);
                        edge_delta -= 1;
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

        if mutations.is_empty() {
            return Ok(GraphBatchWriteOutcome::new(graph.clone(), outcomes, None));
        }
        let (metadata_address, metadata_value) =
            self.metadata_mutation(&record, graph, node_delta, edge_delta)?;
        mutations.put(metadata_address, metadata_value);
        let commit = self.commit_batch_maintaining(&record, mutations.into_mutations(), 1)?;
        Ok(GraphBatchWriteOutcome::new(
            graph.clone(),
            outcomes,
            Some(commit),
        ))
    }

    /// Returns the graph's ontology, or `None` before any type was defined.
    pub fn ontology(&self, graph: &GraphName) -> Result<Option<GraphOntology>, EngineError> {
        self.ontology_with_selector(graph, ReadSelector::Latest)
    }

    /// Returns the ontology visible at a commit version.
    pub fn ontology_at_version(
        &self,
        graph: &GraphName,
        version: CommitVersion,
    ) -> Result<Option<GraphOntology>, EngineError> {
        self.ontology_with_selector(graph, ReadSelector::AtVersion(version))
    }

    /// Returns the ontology visible at a timestamp.
    pub fn ontology_at(
        &self,
        graph: &GraphName,
        timestamp: Timestamp,
    ) -> Result<Option<GraphOntology>, EngineError> {
        self.ontology_with_selector(graph, ReadSelector::AtTimestamp(timestamp))
    }

    fn ontology_with_selector(
        &self,
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
        &self,
        graph: &GraphName,
        object_type: &GraphTypeName,
        cursor: Option<&GraphNodeId>,
        limit: usize,
    ) -> Result<GraphNodePage, EngineError> {
        self.nodes_by_type_with_selector(graph, object_type, cursor, limit, ReadSelector::Latest)
    }

    /// Lists nodes declaring `object_type` visible at a commit version.
    pub fn nodes_by_type_at_version(
        &self,
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
        &self,
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
        &self,
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
        // Seek the type index from the cursor: the page reads its index rows
        // plus one lookahead, validates only those, and hydrates only the
        // page (#3473).
        let mut node_ids = Vec::with_capacity(limit + 1);
        for row in OrderedTextScan::new(
            self.persistence,
            record.storage_branch_id(),
            RowClass::GraphTypeIndex,
            encode_graph_type_index_type_prefix(&self.space, graph, object_type),
            selector,
            "data_loss.engine.graph_type_index_key",
        )
        .rows_after(cursor.map(|id| id.as_str().as_bytes()), None, limit + 1)?
        {
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
        &self,
        graph: &GraphName,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        self.ontology_summary_with_selector(graph, ReadSelector::Latest)
    }

    /// Returns the ontology summary visible at a commit version.
    pub fn ontology_summary_at_version(
        &self,
        graph: &GraphName,
        version: CommitVersion,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        self.ontology_summary_with_selector(graph, ReadSelector::AtVersion(version))
    }

    /// Returns the ontology summary visible at a timestamp.
    pub fn ontology_summary_at(
        &self,
        graph: &GraphName,
        timestamp: Timestamp,
    ) -> Result<Option<GraphOntologySummary>, EngineError> {
        self.ontology_summary_with_selector(graph, ReadSelector::AtTimestamp(timestamp))
    }

    fn ontology_summary_with_selector(
        &self,
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
        &self,
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
        &self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        self.adjacency_index_with_selector(graph, budget, ReadSelector::Latest)
    }

    /// Builds the adjacency snapshot visible at a commit version.
    pub fn adjacency_index_at_version(
        &self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
        version: CommitVersion,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        self.adjacency_index_with_selector(graph, budget, ReadSelector::AtVersion(version))
    }

    /// Builds the adjacency snapshot visible at a timestamp.
    pub fn adjacency_index_at(
        &self,
        graph: &GraphName,
        budget: &GraphAnalyticsBudget,
        timestamp: Timestamp,
    ) -> Result<GraphAdjacencyIndex, EngineError> {
        self.adjacency_index_with_selector(graph, budget, ReadSelector::AtTimestamp(timestamp))
    }

    fn adjacency_index_with_selector(
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        ontology: &GraphOntologyRecord,
    ) -> Result<CommitOutcome, EngineError> {
        debug_assert_eq!(ontology.graph(), graph);
        self.commit_batch(
            record,
            vec![RowMutation::put(
                self.ontology_address(record, graph),
                encode_graph_ontology_record(ontology),
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

    /// The graph's metadata row as every reader and writer sees it: absent
    /// when tombstoned, and absent while a deletion is sweeping the graph's
    /// rows (#3477) — the mark makes a chunked deletion atomic to observers.
    fn graph_metadata_row(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<Option<PersistenceReadRow>, EngineError> {
        let Some(row) = self.stored_graph_metadata_row(record, graph, selector)? else {
            return Ok(None);
        };
        let metadata = Self::graph_metadata_from_row(graph, &row)?;
        Ok((!metadata.deleting()).then_some(row))
    }

    /// The graph's metadata row whenever it is not tombstoned — a deletion
    /// in progress included, which only the deletion paths need to see.
    fn stored_graph_metadata_row(
        &self,
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

    fn graph_metadata_from_row(
        graph: &GraphName,
        row: &PersistenceReadRow,
    ) -> Result<GraphMetadataRecord, EngineError> {
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_metadata",
                "stored graph metadata row is missing a value",
            )
        })?;
        decode_graph_metadata_record(graph, value)
    }

    fn require_graph(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
    ) -> Result<(), EngineError> {
        self.require_graph_with_selector(record, graph, ReadSelector::Latest)
    }

    fn require_graph_with_selector(
        &self,
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
        &self,
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
        let metadata = decode_graph_metadata_record(&graph, value)?;
        // #3474: a maintained row answers from itself — every node/edge commit
        // rewrote it with the counts, so its own commit is the graph's last
        // change. A row written before counts were kept falls back to the
        // scan, until the graph's next write backfills it.
        let (node_count, edge_count, updated_version, updated_timestamp) =
            if let Some(counts) = metadata.counts() {
                (
                    counts.nodes(),
                    counts.edges(),
                    row.commit_version(),
                    row.commit_timestamp(),
                )
            } else {
                let scanned = self.scan_graph_state(record, &graph, selector)?;
                // The newer of the row's own commit and the newest node/edge
                // row; on a tie they are one commit, so either pair serves.
                let (updated_version, updated_timestamp) = [
                    (row.commit_version(), row.commit_timestamp()),
                    (scanned.updated_version, scanned.updated_timestamp),
                ]
                .into_iter()
                .max_by_key(|(version, _)| *version)
                .expect("two candidates");
                (
                    scanned.node_count,
                    scanned.edge_count,
                    updated_version,
                    updated_timestamp,
                )
            };
        let (created_version, created_timestamp) = metadata
            .created()
            .map_or((row.commit_version(), row.commit_timestamp()), |point| {
                (point.version(), point.timestamp())
            });
        Ok(GraphInfo::new(
            graph,
            node_count,
            edge_count,
            created_version,
            created_timestamp,
            updated_version,
            updated_timestamp,
            metadata.importing(),
        ))
    }

    /// Counts the live nodes and forward edges of a graph by decoding every
    /// visible row, and finds the latest commit among them (tombstones
    /// included — a delete changes the graph). This is the O(N + E) reading
    /// `graph_info` used before #3474; it remains the fallback for a metadata
    /// row written before counts were kept, the backfill source on that
    /// graph's next write, and the oracle the maintained counts are tested
    /// against.
    fn scan_graph_state(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        selector: ReadSelector,
    ) -> Result<ScannedGraphState, EngineError> {
        let node_rows = self.node_rows(record, graph, selector)?;
        let edge_rows = self.edge_rows(record, graph, selector)?;
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
        // The newest row of either kind, tombstones included; a graph with
        // no rows reports the zero commit and lets the caller's own row win.
        let (updated_version, updated_timestamp) = node_rows
            .iter()
            .chain(edge_rows.iter())
            .max_by_key(|candidate| candidate.commit_version())
            .map_or(
                (CommitVersion::new(0), Timestamp::from_micros(0)),
                |newest| (newest.commit_version(), newest.commit_timestamp()),
            );
        Ok(ScannedGraphState {
            node_count,
            edge_count,
            updated_version,
            updated_timestamp,
        })
    }

    /// #3474: the metadata row every node/edge-changing commit carries, as
    /// `(address, value)` for the caller's batch. Reads the graph's current
    /// row, applies this commit's change to the live counts — backfilling
    /// them by scan for a row written before counts were kept — and keeps
    /// the create commit, so the rewritten row's own commit becomes the
    /// graph's last change. Deltas are what the commit adds (positive) or
    /// removes (negative) among live nodes and forward edges; a replace is 0.
    ///
    /// This is a read-modify-write on a row shared by every writer of the
    /// graph, and its safety is the branch's single-writer discipline (one
    /// `&mut` service per handle, one writer per branch across processes),
    /// the same discipline every graph write already leans on for its
    /// `created` detection and derived-row cleanup. The commit fence is the
    /// branch generation, which does not move per data commit, so it would
    /// not catch a stale read; if parallel writers on one branch ever land,
    /// this row must join the commit's conflict set.
    fn metadata_mutation(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_delta: i64,
        edge_delta: i64,
    ) -> Result<(RowAddress, Vec<u8>), EngineError> {
        self.metadata_mutation_marking(record, graph, node_delta, edge_delta, None)
    }

    /// [`Self::metadata_mutation`] that also sets the import watermark
    /// (#3464): `Some(pending)` writes it, `None` keeps whatever the row
    /// carries — so an ordinary write during a pending import leaves the
    /// watermark set until the import's last chunk clears it.
    fn metadata_mutation_marking(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_delta: i64,
        edge_delta: i64,
        importing: Option<bool>,
    ) -> Result<(RowAddress, Vec<u8>), EngineError> {
        let row = self
            .graph_metadata_row(record, graph, ReadSelector::Latest)?
            .ok_or_else(|| {
                EngineError::not_found("not_found.engine.graph", "graph does not exist")
            })?;
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.graph_metadata",
                "stored graph metadata row is missing a value",
            )
        })?;
        let metadata = decode_graph_metadata_record(graph, value)?;
        let created = metadata
            .created()
            .unwrap_or_else(|| GraphCommitPoint::new(row.commit_version(), row.commit_timestamp()));
        let counts = if let Some(counts) = metadata.counts() {
            counts
        } else {
            let scanned = self.scan_graph_state(record, graph, ReadSelector::Latest)?;
            GraphCounts::new(scanned.node_count, scanned.edge_count)
        };
        let counts = counts.adjusted(node_delta, edge_delta)?;
        let importing = importing.unwrap_or_else(|| metadata.importing());
        Ok((
            self.metadata_address(record, graph),
            encode_graph_metadata_record(&GraphMetadataRecord::with_state(
                graph.clone(),
                created,
                counts,
                importing,
            )),
        ))
    }

    fn node_row_with_selector(
        &self,
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
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> Result<Option<GraphNodeRecord>, EngineError> {
        self.node_record_with_selector(record, graph, node_id, ReadSelector::Latest)
    }

    fn node_record_with_selector(
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
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

    /// The live edges stored on `node_id`'s two adjacency prefixes — its
    /// outgoing edges from the forward rows and its incoming edges from the
    /// reverse rows — keyed by identity, so a self-loop, which sits on both,
    /// is one edge. Costs the node's degree in rows, never the graph's
    /// (#3472).
    fn stored_incident_edges(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        node_id: &GraphNodeId,
    ) -> Result<BTreeMap<EdgeIdentity, GraphEdgeRecord>, EngineError> {
        let branch_id = record.storage_branch_id();
        let mut incident = BTreeMap::new();
        for row in self.persistence.scan_prefix(
            branch_id,
            RowClass::GraphEdge,
            encode_graph_outgoing_edge_prefix(&self.space, graph, node_id),
            ReadSelector::Latest,
            None,
        )? {
            if !row.is_tombstone() {
                let edge = self.edge_record_from_forward_row(&row)?;
                incident.insert(edge_identity(&edge), edge);
            }
        }
        for row in self.persistence.scan_prefix(
            branch_id,
            RowClass::GraphReverseEdge,
            encode_graph_incoming_edge_prefix(&self.space, graph, node_id),
            ReadSelector::Latest,
            None,
        )? {
            if !row.is_tombstone() {
                let edge = self.edge_record_from_reverse_row(&row)?;
                incident.insert(edge_identity(&edge), edge);
            }
        }
        Ok(incident)
    }

    /// The edges incident to `node_id` as a batch sees them: the stored
    /// adjacency, with the batch's own answer replacing every edge it has
    /// touched — one it deleted is gone, one it wrote is present, whether or
    /// not storage has it.
    fn overlay_incident_edges(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        overlay: &BatchOverlay,
        node_id: &GraphNodeId,
    ) -> Result<Vec<GraphEdgeRecord>, EngineError> {
        let mut incident = self.stored_incident_edges(record, graph, node_id)?;
        for (identity, state) in &overlay.edges {
            let (src, _, dst) = identity;
            if src != node_id && dst != node_id {
                continue;
            }
            match state {
                Some(edge) => {
                    incident.insert(identity.clone(), edge.clone());
                }
                None => {
                    incident.remove(identity);
                }
            }
        }
        Ok(incident.into_values().collect())
    }

    /// The batch-local state of a node: read from storage the first time an
    /// operation asks, then whatever the batch last made it.
    fn overlay_node<'o>(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        overlay: &'o mut BatchOverlay,
        node_id: &GraphNodeId,
    ) -> Result<Option<&'o GraphNodeRecord>, EngineError> {
        let state = match overlay.nodes.entry(node_id.clone()) {
            btree_map::Entry::Occupied(entry) => entry.into_mut(),
            btree_map::Entry::Vacant(entry) => {
                entry.insert(self.node_record(record, graph, node_id)?)
            }
        };
        Ok(state.as_ref())
    }

    /// Both endpoints of an edge in their batch-local state, loaded one
    /// after the other and then read side by side.
    fn overlay_endpoints<'o>(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        overlay: &'o mut BatchOverlay,
        src: &GraphNodeId,
        dst: &GraphNodeId,
    ) -> Result<(Option<&'o GraphNodeRecord>, Option<&'o GraphNodeRecord>), EngineError> {
        self.overlay_node(record, graph, overlay, src)?;
        self.overlay_node(record, graph, overlay, dst)?;
        let overlay: &'o BatchOverlay = overlay;
        Ok((overlay.node(src), overlay.node(dst)))
    }

    /// The batch-local state of an edge: read from storage the first time an
    /// operation asks, then whatever the batch last made it.
    fn overlay_edge<'o>(
        &self,
        record: &BranchCatalogRecord,
        graph: &GraphName,
        overlay: &'o mut BatchOverlay,
        identity: &EdgeIdentity,
    ) -> Result<Option<&'o GraphEdgeRecord>, EngineError> {
        let state = match overlay.edges.entry(identity.clone()) {
            btree_map::Entry::Occupied(entry) => entry.into_mut(),
            btree_map::Entry::Vacant(entry) => {
                let (src, edge_type, dst) = identity;
                entry.insert(self.edge_record(record, graph, src, edge_type, dst)?)
            }
        };
        Ok(state.as_ref())
    }

    fn visible_node_or_corruption(
        &self,
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
        &self,
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

    // #2651: infallible now that edge encoding is, but kept `Result` for
    // symmetry with the sibling mutation-builders (`put_node_mutations`,
    // ontology writes) that stay fallible, so callers treat the family
    // uniformly.
    #[allow(clippy::unnecessary_wraps)]
    fn put_edge_mutations(
        &self,
        record: &BranchCatalogRecord,
        mutations: &mut MutationMap,
        edge: &GraphEdgeRecord,
    ) -> Result<(), EngineError> {
        let encoded = encode_graph_edge_record(edge);
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
        &self,
        record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
    ) -> Result<CommitOutcome, EngineError> {
        self.commit_batch_maintaining(record, mutations, 0)
    }

    /// Commits a batch that carries `maintained_puts` engine-maintained
    /// metadata rewrites (#3474) beside the caller's rows. Those rewrites are
    /// bookkeeping the same way derived reverse-edge rows are, so they are
    /// left out of the user-facing put count: a node upsert still reports one
    /// row written. `create_graph`'s metadata row is the user's own and goes
    /// through [`Self::commit_batch`] unsubtracted.
    fn commit_batch_maintaining(
        &self,
        record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
        maintained_puts: usize,
    ) -> Result<CommitOutcome, EngineError> {
        let mut mutations = mutations;
        // #2651: every caller builds at least one mutation before reaching here
        // (an empty public batch returns success earlier), so the old
        // `invalid_argument.engine.graph_batch` refusal was unreachable. Keep
        // the invariant as a debug assertion rather than an unreachable code.
        debug_assert!(
            !mutations.is_empty(),
            "commit_batch requires at least one mutation"
        );
        // Count only authored rows (graph metadata, nodes, forward edges) for
        // the user-facing commit counts. Derived reverse-edge and binding-index
        // rows are engine-maintained and must not inflate the caller's view of
        // rows written/deleted (one edge upsert would otherwise report 2).
        let user_put_count = mutations
            .iter()
            .filter(|mutation| {
                mutation.is_put() && is_authored_graph_row(mutation.address().row_class())
            })
            .count()
            .saturating_sub(maintained_puts);
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

/// What a full scan of a graph's node and edge rows says: the live counts
/// and the latest commit among every row, tombstones included.
struct ScannedGraphState {
    node_count: u64,
    edge_count: u64,
    updated_version: CommitVersion,
    updated_timestamp: Timestamp,
}

/// What a batch has learned or decided about the nodes and edges it has
/// touched (#3472). `batch_write` reads a node or edge from storage the first
/// time an operation needs it and keeps every later answer here, so
/// operations see the batch's own earlier writes without a copy of the
/// graph. An entry is the current batch-local state: `Some` is live —
/// stored and unchanged, or written by the batch — and `None` is absent,
/// whether never stored or deleted earlier in the batch. A node or edge
/// without an entry has not been touched and is whatever storage says.
#[derive(Default)]
struct BatchOverlay {
    nodes: BTreeMap<GraphNodeId, Option<GraphNodeRecord>>,
    edges: BTreeMap<EdgeIdentity, Option<GraphEdgeRecord>>,
}

impl BatchOverlay {
    /// The state of a node already loaded through
    /// [`GraphService::overlay_node`].
    fn node(&self, node_id: &GraphNodeId) -> Option<&GraphNodeRecord> {
        self.nodes.get(node_id).and_then(Option::as_ref)
    }
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

    fn is_empty(&self) -> bool {
        self.mutations.is_empty()
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

/// The position a neighbor cursor names: the direction, edge type and
/// neighbor of the last hit on the previous page.
#[derive(Debug)]
struct NeighborPosition {
    direction: GraphDirection,
    edge_type: GraphEdgeType,
    neighbor: GraphNodeId,
}

/// Parses a cursor exactly as [`neighbor_cursor`] formats one —
/// `direction␟edge_type␟neighbor␟dst`. Anything else was never produced by
/// this listing and is refused rather than seeked from (#3489); a component
/// that no longer validates is refused with that component's own code.
fn parse_neighbor_cursor(cursor: &str) -> Result<NeighborPosition, EngineError> {
    let refuse = |why: &str| {
        EngineError::invalid_input(
            "invalid_argument.engine.graph_cursor",
            format!("graph neighbor cursor {why}"),
        )
    };
    let parts: Vec<&str> = cursor.split('\u{1f}').collect();
    let [direction, edge_type, neighbor, dst] = parts.as_slice() else {
        return Err(refuse(
            "does not have the four components this listing produces",
        ));
    };
    let direction = match *direction {
        "i" => GraphDirection::Incoming,
        "o" => GraphDirection::Outgoing,
        _ => return Err(refuse("names a direction this listing never produces")),
    };
    GraphNodeId::new(*dst)?;
    Ok(NeighborPosition {
        direction,
        edge_type: GraphEdgeType::new(*edge_type)?,
        neighbor: GraphNodeId::new(*neighbor)?,
    })
}

/// An edge type read back from an adjacency key component; one that no
/// longer validates is a corrupt key.
fn edge_type_from_key(raw: &[u8], key_code: &'static str) -> Result<GraphEdgeType, EngineError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| GraphEdgeType::new(text).ok())
        .ok_or_else(|| {
            EngineError::corruption(
                key_code,
                "stored adjacency row key names an edge type that does not validate",
            )
        })
}

/// Where a direction sorts among neighbor hits: incoming before outgoing,
/// matching the `i` / `o` a cursor leads with.
const fn leg_rank(direction: GraphDirection) -> u8 {
    match direction {
        GraphDirection::Incoming => 0,
        GraphDirection::Outgoing => 1,
        GraphDirection::Both => 2,
    }
}

/// Whether a requested direction walks the given single-direction leg.
const fn direction_includes(direction: GraphDirection, leg: GraphDirection) -> bool {
    matches!(
        (direction, leg),
        (GraphDirection::Both, _)
            | (GraphDirection::Incoming, GraphDirection::Incoming)
            | (GraphDirection::Outgoing, GraphDirection::Outgoing)
    )
}

/// #3457: orders a graph's edges by `(src, type, dst)` for `list_edges`
/// pagination, with a unit separator so component boundaries can't collide.
fn edge_cursor(edge: &GraphEdge) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        edge.src().as_str(),
        edge.edge_type().as_str(),
        edge.dst().as_str()
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
mod cursor_tests {
    use super::*;

    const US: char = '\u{1f}';

    /// A cursor this listing produced names its direction, type and neighbor.
    #[test]
    fn a_produced_cursor_parses_to_its_position() {
        let outgoing = parse_neighbor_cursor(&format!("o{US}knows{US}bob{US}bob")).expect("parses");
        assert_eq!(outgoing.direction, GraphDirection::Outgoing);
        assert_eq!(outgoing.edge_type.as_str(), "knows");
        assert_eq!(outgoing.neighbor.as_str(), "bob");

        let incoming =
            parse_neighbor_cursor(&format!("i{US}knows{US}alice{US}bob")).expect("parses");
        assert_eq!(incoming.direction, GraphDirection::Incoming);
        assert_eq!(incoming.neighbor.as_str(), "alice");
    }

    /// The wrong number of components, or a direction the listing never
    /// emits, is refused as a cursor.
    #[test]
    fn a_cursor_of_another_shape_is_refused() {
        for cursor in [
            String::from("not-a-cursor"),
            format!("o{US}knows{US}bob"),
            format!("o{US}knows{US}bob{US}bob{US}extra"),
            format!("b{US}knows{US}bob{US}bob"),
            format!("x{US}knows{US}bob{US}bob"),
        ] {
            let error = parse_neighbor_cursor(&cursor).expect_err("refused");
            assert_eq!(
                error.code(),
                "invalid_argument.engine.graph_cursor",
                "{cursor:?}"
            );
        }
    }

    /// A component that no longer validates carries its own code, so a caller
    /// learns which part is wrong.
    #[test]
    fn a_cursor_with_an_invalid_component_is_refused_by_that_component() {
        let error = parse_neighbor_cursor(&format!("o{US}{US}bob{US}bob")).expect_err("empty type");
        assert_eq!(error.code(), "invalid_argument.engine.graph_edge_type");
        let error =
            parse_neighbor_cursor(&format!("o{US}knows{US}{US}bob")).expect_err("empty neighbor");
        assert_eq!(error.code(), "invalid_argument.engine.graph_node_id");
        let error =
            parse_neighbor_cursor(&format!("o{US}knows{US}bob{US}")).expect_err("empty dst");
        assert_eq!(error.code(), "invalid_argument.engine.graph_node_id");
    }

    #[test]
    fn legs_rank_incoming_before_outgoing_and_directions_include_their_legs() {
        assert!(leg_rank(GraphDirection::Incoming) < leg_rank(GraphDirection::Outgoing));
        for (direction, incoming, outgoing) in [
            (GraphDirection::Incoming, true, false),
            (GraphDirection::Outgoing, false, true),
            (GraphDirection::Both, true, true),
        ] {
            assert_eq!(
                direction_includes(direction, GraphDirection::Incoming),
                incoming
            );
            assert_eq!(
                direction_includes(direction, GraphDirection::Outgoing),
                outgoing
            );
        }
    }
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
            Some(encode_graph_binding_record(&record)),
            false,
        );

        let error = binding_from_index_row(&space, &row).expect_err("target mismatch rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_binding_record");
    }

    /// #3474: a graph left by a release before counts were kept — an old
    /// metadata row with node rows committed after it — is answered by scan
    /// until its next write backfills the row, in both modes. In-crate so the
    /// default-feature mutation lane judges the fallback and backfill paths.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn legacy_graph_rows_fall_back_to_the_scan_and_backfill_on_the_next_write() {
        use strata_core::CommitVersion;

        use crate::branch::BranchName;
        use crate::data::graph::{GraphEdgeData, GraphInfo, GraphNodeData, GraphNodeId};
        use crate::{CacheOpenOptions, Database, DurableLocalOpenOptions};

        fn node(id: &str) -> GraphNodeId {
            GraphNodeId::new(id).expect("node id")
        }

        /// The row against the listing, with the commits it must report.
        fn info_matches_listing(
            graph: &super::GraphService<'_>,
            name: &GraphName,
            created: CommitVersion,
            updated: CommitVersion,
        ) -> GraphInfo {
            let info = graph.graph_info(name).expect("reads").expect("exists");
            let nodes = graph.list_nodes(name, None, None, 100).expect("nodes");
            let edges = graph.list_edges(name, None, 100).expect("edges");
            assert_eq!(
                info.node_count(),
                u64::try_from(nodes.nodes().len()).expect("fits")
            );
            assert_eq!(
                info.edge_count(),
                u64::try_from(edges.edges().len()).expect("fits")
            );
            assert_eq!(info.created_version(), created);
            assert_eq!(info.updated_version(), updated);
            info
        }

        fn exercise(database: &Database) {
            let mut graph = database
                .graph(
                    BranchName::new("default").expect("branch"),
                    ProductSpace::new("default").expect("space"),
                )
                .expect("service");
            let name = GraphName::new("roads").expect("graph");
            let road = GraphEdgeType::new("road").expect("type");
            graph.create_graph(name.clone()).expect("created");
            for id in ["a", "b"] {
                graph
                    .upsert_node(&name, node(id), GraphNodeData::default())
                    .expect("node");
            }
            graph
                .upsert_edge(&name, node("a"), road, node("b"), GraphEdgeData::default())
                .expect("edge");

            // The old shape: a bare metadata row, then nodes c and d landing
            // after it with nothing maintained.
            let (legacy, last_node) = graph
                .write_legacy_graph_rows_for_test(&name, &[node("c"), node("d")])
                .expect("legacy rows");
            let last_node = last_node.expect("two nodes were written");
            assert!(last_node.version() > legacy.version());
            let fallback =
                info_matches_listing(&graph, &name, legacy.version(), last_node.version());
            assert_eq!((fallback.node_count(), fallback.edge_count()), (4, 1));
            assert_eq!(
                fallback.updated_timestamp(),
                last_node.timestamp(),
                "the newest row's commit, not the metadata row's"
            );
            // At the metadata row's own version the later nodes do not exist
            // yet, and nothing is newer than the row.
            let at_legacy = graph
                .graph_info_at_version(&name, legacy.version())
                .expect("historical")
                .expect("existed");
            assert_eq!((at_legacy.node_count(), at_legacy.edge_count()), (2, 1));
            assert_eq!(at_legacy.updated_version(), legacy.version());
            assert_eq!(at_legacy.updated_timestamp(), legacy.timestamp());

            // The next write backfills: counted by scan once, then maintained,
            // and the create point is the legacy row's commit from here on.
            let write = graph
                .upsert_node(&name, node("e"), GraphNodeData::default())
                .expect("node");
            let backfilled =
                info_matches_listing(&graph, &name, legacy.version(), write.commit().version());
            assert_eq!((backfilled.node_count(), backfilled.edge_count()), (5, 1));
            let removed = graph.delete_node(&name, &node("a")).expect("delete");
            let maintained = info_matches_listing(
                &graph,
                &name,
                legacy.version(),
                removed.commit().expect("commits").version(),
            );
            assert_eq!(
                (maintained.node_count(), maintained.edge_count()),
                (4, 0),
                "b, c, d, e remain; a took its edge with it"
            );

            // History before the backfill still answers by scan.
            assert_eq!(
                graph
                    .graph_info_at_version(&name, last_node.version())
                    .expect("historical")
                    .expect("existed"),
                fallback
            );
        }

        exercise(
            &Database::open_cache(CacheOpenOptions::new())
                .expect("cache")
                .into_database(),
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise(
            &Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable")
                .into_database(),
        );
    }

    /// #3477: between the mark and the sweep — where a crash would leave a
    /// large deletion — the graph is absent to every reader and writer, its
    /// earlier versions still answer, other graphs' bindings do not surface
    /// it, and both `delete_graph` and `create_graph` finish the sweep so no
    /// row of it survives. In-crate so the default-feature mutation lane
    /// judges the resume paths.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn a_marked_graph_is_absent_and_its_deletion_resumes() {
        use crate::branch::BranchName;
        use crate::data::graph::{
            GraphBindingPrimitive, GraphBindingTarget, GraphDeletePolicy, GraphDirection,
            GraphEdgeData, GraphEntityBinding, GraphNodeData, GraphNodeId,
        };
        use crate::{CacheOpenOptions, Database, DurableLocalOpenOptions};

        fn node(id: &str) -> GraphNodeId {
            GraphNodeId::new(id).expect("node id")
        }

        fn bound(key: &str) -> GraphNodeData {
            GraphNodeData::new(
                None,
                Some(GraphEntityBinding::new(
                    GraphBindingTarget::new(
                        GraphBindingPrimitive::Kv,
                        None,
                        ProductSpace::new("docs").expect("space"),
                        key,
                    )
                    .expect("target"),
                )),
            )
        }

        fn exercise(database: &Database) {
            let mut graph = database
                .graph(
                    BranchName::new("default").expect("branch"),
                    ProductSpace::new("default").expect("space"),
                )
                .expect("service");
            let doomed = GraphName::new("doomed").expect("graph");
            let other = GraphName::new("other").expect("graph");
            let road = GraphEdgeType::new("road").expect("type");
            let target = GraphBindingTarget::new(
                GraphBindingPrimitive::Kv,
                None,
                ProductSpace::new("docs").expect("space"),
                "doc-1",
            )
            .expect("target");
            for g in [&doomed, &other] {
                graph.create_graph(g.clone()).expect("created");
                graph
                    .upsert_node(g, node("a"), bound("doc-1"))
                    .expect("node");
                graph
                    .upsert_node(g, node("b"), GraphNodeData::default())
                    .expect("node");
                graph
                    .upsert_edge(
                        g,
                        node("a"),
                        road.clone(),
                        node("b"),
                        GraphEdgeData::default(),
                    )
                    .expect("edge");
            }
            let before = graph.graph_info(&doomed).expect("reads").expect("exists");

            let mark = graph
                .begin_graph_delete_for_test(&doomed)
                .expect("mark commits");

            // Absent to readers and writers; earlier versions still answer.
            assert!(graph.graph_info(&doomed).expect("reads").is_none());
            assert_eq!(
                graph
                    .get_node(&doomed, &node("a"))
                    .expect_err("gone")
                    .code(),
                "not_found.engine.graph"
            );
            assert_eq!(
                graph
                    .neighbors(&doomed, &node("a"), GraphDirection::Both, None, None, 10)
                    .expect_err("gone")
                    .code(),
                "not_found.engine.graph"
            );
            assert_eq!(
                graph
                    .upsert_node(&doomed, node("c"), GraphNodeData::default())
                    .expect_err("no writes into a deleting graph")
                    .code(),
                "not_found.engine.graph"
            );
            let listed: Vec<String> = graph
                .list_graphs(None, 10)
                .expect("lists")
                .graphs()
                .iter()
                .map(|g| g.as_str().to_owned())
                .collect();
            assert_eq!(listed, ["other"]);
            assert_eq!(
                graph
                    .graph_info_at_version(&doomed, before.updated_version())
                    .expect("historical")
                    .expect("existed before the mark"),
                before
            );
            assert!(graph
                .graph_info_at_version(&doomed, mark.version())
                .expect("historical")
                .is_none());

            // Cross-graph reads see only the other graph's binding, and a
            // policy over the shared target touches only the other graph.
            let bindings = graph
                .bindings_for_entity(&target, None, 10)
                .expect("bindings");
            let bound_graphs: Vec<&str> = bindings
                .bindings()
                .iter()
                .map(|binding| binding.graph().as_str())
                .collect();
            assert_eq!(bound_graphs, ["other"]);
            let policy = graph
                .apply_binding_delete_policy(&target, GraphDeletePolicy::Detach)
                .expect("policy skips the deleting graph");
            assert!(policy.commit().is_some(), "the other graph's node detached");
            assert!(graph.graph_info(&other).expect("reads").is_some());

            // `create_graph` finishes the sweep, then creates an empty graph.
            let (fresh, create) = graph.create_graph(doomed.clone()).expect("recreated");
            assert_eq!((fresh.node_count(), fresh.edge_count()), (0, 0));
            assert!(create.version() > mark.version());
            assert!(graph
                .list_nodes(&doomed, None, None, 10)
                .expect("lists")
                .nodes()
                .is_empty());
            assert!(graph
                .get_node(&doomed, &node("a"))
                .expect("reads")
                .is_none());
            assert!(graph
                .list_edges(&doomed, None, 10)
                .expect("lists")
                .edges()
                .is_empty());

            // Mark again; this time `delete_graph` finishes it.
            graph
                .upsert_node(&doomed, node("z"), GraphNodeData::default())
                .expect("node");
            graph
                .begin_graph_delete_for_test(&doomed)
                .expect("mark commits");
            let finished = graph.delete_graph(&doomed, false).expect("resumes");
            assert!(finished.deleted(), "a marked graph is reported deleted");
            assert!(finished.commit().is_some());
            assert!(graph.graph_info(&doomed).expect("reads").is_none());
            let (again, _) = graph.create_graph(doomed.clone()).expect("recreated");
            assert_eq!(again.node_count(), 0, "z did not survive");
            // And deleting the finished graph again is the ordinary no-op.
            graph.delete_graph(&doomed, false).expect("empty deletes");
            assert!(!graph
                .delete_graph(&doomed, false)
                .expect("missing is fine")
                .deleted());
        }

        exercise(
            &Database::open_cache(CacheOpenOptions::new())
                .expect("cache")
                .into_database(),
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise(
            &Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable")
                .into_database(),
        );
    }

    /// #3477: resuming a marked deletion sweeps whatever the mark left and
    /// tombstones the metadata row last — a graph one row past the chunk
    /// (the resume spans several commits) and a graph with no rows to sweep
    /// (the resume is the metadata tombstone alone, so the sweep's empty-row
    /// loop is skipped yet its final commit still runs). Both resume paths —
    /// `delete_graph` and `create_graph` — are exercised on a graph larger
    /// than one chunk. In-crate so the default-feature mutation lane judges
    /// the resume's chunk arithmetic and the empty-sweep tombstone.
    #[test]
    fn a_marked_deletion_resumes_across_chunks_and_when_no_rows_remain() {
        use crate::branch::BranchName;
        use crate::data::graph::{GraphNodeData, GraphNodeId};
        use crate::{CacheOpenOptions, Database, DurableLocalOpenOptions};

        fn node(index: usize) -> GraphNodeId {
            GraphNodeId::new(format!("n:{index}")).expect("node id")
        }

        fn typeless_nodes(count: usize) -> Vec<(GraphNodeId, GraphNodeData)> {
            (0..count)
                .map(|index| (node(index), GraphNodeData::default()))
                .collect()
        }

        fn exercise(database: &Database, count: usize, expected_row_commits: u64) {
            let mut graph = database
                .graph(
                    BranchName::new("default").expect("branch"),
                    ProductSpace::new("default").expect("space"),
                )
                .expect("service");

            // An empty graph, marked, resumes in the single commit that
            // tombstones its metadata row: the sweep skips its empty row loop
            // yet still commits the tombstone.
            let empty = GraphName::new("empty").expect("graph");
            graph.create_graph(empty.clone()).expect("created");
            let mark = graph
                .begin_graph_delete_for_test(&empty)
                .expect("mark commits");
            let resumed = graph.delete_graph(&empty, false).expect("resumes");
            assert!(resumed.deleted(), "a marked graph is reported deleted");
            let last = resumed.commit().expect("resume commits");
            assert_eq!(
                last.version().as_u64() - mark.version().as_u64(),
                1,
                "an empty marked graph resumes in one commit: the metadata tombstone"
            );
            assert!(graph.graph_info(&empty).expect("reads").is_none());

            // A graph one row past the chunk, marked, resumes in exactly one
            // commit per chunk plus the metadata tombstone — proof the resume
            // sweeps everything and tombstones the row last.
            let big = GraphName::new("big").expect("graph");
            graph.create_graph(big.clone()).expect("created");
            graph
                .bulk_insert(&big, &typeless_nodes(count), &[], Some(512))
                .expect("nodes imported");
            let mark = graph
                .begin_graph_delete_for_test(&big)
                .expect("mark commits");
            let resumed = graph.delete_graph(&big, false).expect("resumes");
            assert!(resumed.deleted());
            let last = resumed.commit().expect("resume commits");
            assert_eq!(
                last.version().as_u64() - mark.version().as_u64(),
                expected_row_commits + 1,
                "a {count}-row marked graph resumes in {} commits",
                expected_row_commits + 1
            );
            assert!(graph.graph_info(&big).expect("reads").is_none());

            // `create_graph` resumes a large marked graph before creating an
            // empty one — no row of the old graph surfaces under the name.
            let reused = GraphName::new("reused").expect("graph");
            graph.create_graph(reused.clone()).expect("created");
            graph
                .bulk_insert(&reused, &typeless_nodes(count), &[], Some(512))
                .expect("nodes imported");
            graph
                .begin_graph_delete_for_test(&reused)
                .expect("mark commits");
            let (fresh, _) = graph.create_graph(reused.clone()).expect("recreated");
            assert_eq!((fresh.node_count(), fresh.edge_count()), (0, 0));
            assert!(graph
                .list_nodes(&reused, None, None, 10)
                .expect("lists")
                .nodes()
                .is_empty());
        }

        // One row past the chunk is the smallest graph whose sweep needs
        // more than one row commit; a typeless node is exactly one row, so
        // the row count equals the node count.
        let count = super::GraphService::DELETE_CHUNK_ROWS + 1;
        let expected_row_commits = count.div_ceil(super::GraphService::DELETE_CHUNK_ROWS) as u64;
        assert!(
            expected_row_commits >= 2,
            "the large case must span more than one row commit"
        );

        exercise(
            &Database::open_cache(CacheOpenOptions::new())
                .expect("cache")
                .into_database(),
            count,
            expected_row_commits,
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise(
            &Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable")
                .into_database(),
            count,
            expected_row_commits,
        );
    }
}

/// #3477: the exact commit boundary of a deletion, pinned where the chunk
/// size is visible — a graph whose rows plus its own row fit one chunk
/// deletes in the single commit it always did; one more row and it becomes
/// mark, one sweep chunk, and the row.
#[cfg(test)]
mod delete_boundary_tests {
    use crate::branch::BranchName;
    use crate::data::graph::{GraphName, GraphNodeData, GraphNodeId};
    use crate::data::kv::ProductSpace;
    use crate::{CacheOpenOptions, Database, DurableLocalOpenOptions};

    use super::GraphService;

    fn exercise(database: &Database) {
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        for (label, node_count, commits) in [
            ("at-the-chunk", GraphService::DELETE_CHUNK_ROWS - 1, 1),
            ("one-over", GraphService::DELETE_CHUNK_ROWS, 3),
        ] {
            let name = GraphName::new(label).expect("graph");
            graph.create_graph(name.clone()).expect("graph created");
            let nodes: Vec<_> = (0..node_count)
                .map(|index| {
                    (
                        GraphNodeId::new(format!("n:{index}")).expect("node id"),
                        GraphNodeData::default(),
                    )
                })
                .collect();
            graph
                .bulk_insert(&name, &nodes, &[], None)
                .expect("nodes imported");
            let before = graph
                .graph_info(&name)
                .expect("info reads")
                .expect("exists")
                .updated_version();
            let outcome = graph.delete_graph(&name, true).expect("deleted");
            let last = outcome.commit().expect("deletion commits");
            assert_eq!(
                last.version().as_u64() - before.as_u64(),
                commits,
                "{label}: {node_count} nodes take {commits} commit(s)"
            );
            assert_eq!(
                last.delete_count(),
                node_count + 1,
                "{label}: the receipt counts every node and the graph's row"
            );
            assert!(graph.graph_info(&name).expect("reads").is_none(), "{label}");
        }
    }

    #[test]
    fn the_single_commit_boundary_is_exact_in_cache_and_durable_modes() {
        exercise(
            &Database::open_cache(CacheOpenOptions::new())
                .expect("cache")
                .into_database(),
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise(
            &Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable")
                .into_database(),
        );
    }
}

/// #3464: the state a crash leaves between the first and last commits of a
/// bulk import — the chunks that landed, the watermark set — and how it is
/// left: by re-running the same payload. In-crate so the default-feature
/// mutation lane judges the watermark's set and clear.
#[cfg(test)]
mod bulk_resume_tests {
    use crate::branch::BranchName;
    use crate::data::graph::{GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId};
    use crate::data::kv::ProductSpace;
    use crate::{CacheOpenOptions, Database, DurableLocalOpenOptions};

    fn node(index: usize) -> GraphNodeId {
        GraphNodeId::new(format!("n:{index}")).expect("node id")
    }

    fn exercise(database: &Database) {
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let street = GraphEdgeType::new("street").expect("type");
        let nodes: Vec<_> = (0..20)
            .map(|index| (node(index), GraphNodeData::default()))
            .collect();
        let edges: Vec<_> = (0..20)
            .map(|index| {
                (
                    node(index),
                    street.clone(),
                    node((index + 1) % 20),
                    GraphEdgeData::default(),
                )
            })
            .collect();
        // Chunks of 8: three node commits, three edge commits. Stop after
        // the fourth, mid-edges — nodes complete, a prefix of the streets.
        let cut = graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 4)
            .expect("the first four chunks commit");
        assert_eq!(cut.commits(), 4);
        let pending = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(pending.import_pending(), "cut short: the watermark stays");
        assert_eq!((pending.node_count(), pending.edge_count()), (20, 8));
        assert_eq!(
            pending.updated_version(),
            cut.last_commit().expect("commits").version()
        );

        // The graph is usable meanwhile, and an ordinary write keeps the mark.
        let write = graph
            .upsert_node(&city, node(99), GraphNodeData::default())
            .expect("a write during a pending import");
        let still = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(still.import_pending());
        assert_eq!(still.node_count(), 21);
        assert_eq!(still.updated_version(), write.commit().version());

        // Re-running the same payload finishes it: every row an upsert, so
        // nothing doubles, and the last chunk clears the mark.
        let done = graph
            .bulk_insert(&city, &nodes, &edges, Some(8))
            .expect("re-run completes");
        assert_eq!(done.commits(), 6);
        let finished = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(
            !finished.import_pending(),
            "the re-run cleared the watermark"
        );
        assert_eq!((finished.node_count(), finished.edge_count()), (21, 20));
        assert_eq!(
            graph
                .list_edges(&city, None, 100)
                .expect("edges")
                .edges()
                .len(),
            20
        );

        // History keeps the truth of each moment.
        let at_cut = graph
            .graph_info_at_version(&city, cut.last_commit().expect("commits").version())
            .expect("historical")
            .expect("existed");
        assert!(at_cut.import_pending());
        assert_eq!((at_cut.node_count(), at_cut.edge_count()), (20, 8));
    }

    #[test]
    fn an_interrupted_import_stays_pending_until_the_same_payload_is_rerun() {
        exercise(
            &Database::open_cache(CacheOpenOptions::new())
                .expect("cache")
                .into_database(),
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise(
            &Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable")
                .into_database(),
        );
    }

    type Edge = (GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData);

    /// A ring of `count` nodes and `count` edges, chunked into `>1` commits at
    /// `chunk_size` 8 (three node chunks, three edge chunks for `count == 20`).
    fn ring(count: usize) -> (Vec<(GraphNodeId, GraphNodeData)>, Vec<Edge>) {
        let street = GraphEdgeType::new("street").expect("type");
        let nodes = (0..count)
            .map(|index| (node(index), GraphNodeData::default()))
            .collect();
        let edges = (0..count)
            .map(|index| {
                (
                    node(index),
                    street.clone(),
                    node((index + 1) % count),
                    GraphEdgeData::default(),
                )
            })
            .collect();
        (nodes, edges)
    }

    fn cache() -> Database {
        Database::open_cache(CacheOpenOptions::new())
            .expect("cache")
            .into_database()
    }

    /// A cut inside the node loop (before any edge chunk) leaves the graph
    /// pending with the node prefix landed and no edges — the node loop's own
    /// interruption guard, which a cut mid-edges never exercises.
    #[test]
    fn a_cut_within_the_node_loop_is_pending_with_no_edges_started() {
        let database = cache();
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let (nodes, edges) = ring(20);
        // Chunks of 8: stop after the second node chunk — two of three node
        // chunks landed, the edge loop never entered.
        let cut = graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 2)
            .expect("first two node chunks commit");
        assert_eq!(cut.commits(), 2);
        let pending = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(
            pending.import_pending(),
            "cut mid-nodes: the watermark is set"
        );
        assert_eq!(
            (pending.node_count(), pending.edge_count()),
            (16, 0),
            "two node chunks of eight, no edges"
        );
    }

    /// A cut *at* the last chunk (its `Some(false)` already written) leaves the
    /// mark clear — an interruption there is indistinguishable from completion.
    #[test]
    fn a_cut_at_the_last_chunk_leaves_the_mark_clear() {
        let database = cache();
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let (nodes, edges) = ring(20); // six chunks at size 8
        let cut = graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 6)
            .expect("all six chunks commit");
        assert_eq!(cut.commits(), 6);
        let info = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(
            !info.import_pending(),
            "the last chunk wrote a clear mark before the stop"
        );
        assert_eq!((info.node_count(), info.edge_count()), (20, 20));
    }

    /// `max_commits == 0` commits nothing and marks nothing — the seam stops
    /// before the first chunk, the state a crash before it lands leaves.
    #[test]
    fn a_zero_commit_limit_commits_and_marks_nothing() {
        let database = cache();
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let (nodes, edges) = ring(20);
        let cut = graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 0)
            .expect("zero chunks commit");
        assert_eq!(cut.commits(), 0);
        assert!(cut.last_commit().is_none());
        let info = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(
            !info.import_pending(),
            "nothing was imported, nothing marked"
        );
        assert_eq!((info.node_count(), info.edge_count()), (0, 0));
    }

    /// Re-running the interrupted payload at a *different* `chunk_size` still
    /// completes it and clears the mark: the watermark is not tied to a
    /// chunking, only to some multi-commit import's last chunk landing.
    #[test]
    fn a_rerun_at_a_different_chunk_size_completes_and_clears() {
        let database = cache();
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let (nodes, edges) = ring(20);
        graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 4)
            .expect("cut mid-edges");
        assert!(graph
            .graph_info(&city)
            .expect("reads")
            .expect("exists")
            .import_pending());
        // Re-run the same payload with a coarser chunk_size (five chunks, not
        // six). Every row an upsert, so nothing doubles.
        let done = graph
            .bulk_insert(&city, &nodes, &edges, Some(5))
            .expect("re-run completes");
        assert!(done.commits() > 1);
        let info = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(!info.import_pending(), "the re-run's last chunk cleared it");
        assert_eq!((info.node_count(), info.edge_count()), (20, 20));
    }

    /// An edges-only import spanning chunks marks the graph too — the shared
    /// `chunk_index`/`total_chunks` spans both loops, so a cut inside the edge
    /// loop of an all-edges payload is pending with the edge prefix landed.
    #[test]
    fn an_edges_only_import_spanning_chunks_is_pending_when_cut() {
        let database = cache();
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let (nodes, edges) = ring(20);
        // Land the endpoints first, as a finished nodes-only import — never
        // pending (one loop, last chunk clears).
        let nodes_only = graph
            .bulk_insert(&city, &nodes, &[], Some(8))
            .expect("nodes imported");
        assert!(nodes_only.commits() > 1);
        assert!(!graph
            .graph_info(&city)
            .expect("reads")
            .expect("exists")
            .import_pending());
        // Now an edges-only import (three edge chunks), cut after the first.
        let cut = graph
            .bulk_insert_interrupted_for_test(&city, &[], &edges, Some(8), 1)
            .expect("first edge chunk commits");
        assert_eq!(cut.commits(), 1);
        let info = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(
            info.import_pending(),
            "an edges-only import spans commits too"
        );
        assert_eq!((info.node_count(), info.edge_count()), (20, 8));
    }

    /// A fork mid-import copies the watermark to the child; the child clears it
    /// only by its own re-run, and the parent's mark is untouched by that.
    #[test]
    fn a_fork_mid_import_leaves_the_child_pending_and_a_child_rerun_clears_only_the_child() {
        let mut database = cache();
        let (nodes, edges) = ring(20);
        let city = GraphName::new("city").expect("graph");
        {
            let mut graph = database
                .graph(
                    BranchName::new("default").expect("branch"),
                    ProductSpace::new("default").expect("space"),
                )
                .expect("service");
            graph.create_graph(city.clone()).expect("created");
            graph
                .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 4)
                .expect("cut mid-edges");
        }
        database
            .branches()
            .expect("branch service")
            .fork_current(
                &BranchName::new("default").expect("branch"),
                BranchName::new("child").expect("branch"),
            )
            .expect("fork");
        {
            let child = database
                .graph(
                    BranchName::new("child").expect("branch"),
                    ProductSpace::new("default").expect("space"),
                )
                .expect("service");
            assert!(
                child
                    .graph_info(&city)
                    .expect("reads")
                    .expect("exists")
                    .import_pending(),
                "the child inherits the pending mark"
            );
        }
        {
            let mut child = database
                .graph(
                    BranchName::new("child").expect("branch"),
                    ProductSpace::new("default").expect("space"),
                )
                .expect("service");
            child
                .bulk_insert(&city, &nodes, &edges, Some(8))
                .expect("child re-run completes");
            assert!(
                !child
                    .graph_info(&city)
                    .expect("reads")
                    .expect("exists")
                    .import_pending(),
                "the child's re-run cleared its own mark"
            );
        }
        let parent = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        assert!(
            parent
                .graph_info(&city)
                .expect("reads")
                .expect("exists")
                .import_pending(),
            "the parent's mark is untouched by the child's re-run"
        );
    }

    /// Deleting a graph with a pending import and recreating it under the same
    /// name yields a fresh graph — never pending, whatever the old one carried.
    #[test]
    fn delete_then_create_after_a_cut_yields_a_fresh_graph_not_pending() {
        let database = cache();
        let mut graph = database
            .graph(
                BranchName::new("default").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("service");
        let city = GraphName::new("city").expect("graph");
        graph.create_graph(city.clone()).expect("created");
        let (nodes, edges) = ring(20);
        graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 4)
            .expect("cut mid-edges");
        assert!(graph
            .graph_info(&city)
            .expect("reads")
            .expect("exists")
            .import_pending());
        graph.delete_graph(&city, true).expect("deleted");
        graph.create_graph(city.clone()).expect("recreated");
        let info = graph.graph_info(&city).expect("reads").expect("exists");
        assert!(!info.import_pending(), "a fresh graph is never pending");
        assert_eq!((info.node_count(), info.edge_count()), (0, 0));
    }
}
