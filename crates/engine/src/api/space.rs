//! Branch-local product space API.

use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::BranchName;
use crate::commit::CommitOutcome;
use crate::control::{space as control_space, ControlPlane};
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{
    decode_vector_index_manifest_key, encode_event_meta_space_prefix, encode_event_space_prefix,
    encode_event_type_index_space_prefix, encode_graph_binding_space_prefix,
    encode_graph_edge_space_prefix, encode_graph_metadata_prefix, encode_graph_node_space_prefix,
    encode_graph_ontology_space_prefix, encode_graph_reverse_edge_space_prefix,
    encode_graph_type_index_space_prefix, encode_json_index_entry_space_prefix,
    encode_json_index_meta_prefix, encode_json_space_prefix, encode_kv_space_prefix,
    encode_vector_collection_prefix, encode_vector_space_prefix, vector_index_manifest_prefix,
    CommitPlan, ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
};

const SPACE_DELETE_MUTATION_LIMIT: usize = 10_000;

/// Outcome returned after creating a product space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpaceCreateOutcome {
    space: ProductSpace,
    created: bool,
    commit: Option<CommitOutcome>,
}

impl SpaceCreateOutcome {
    pub(crate) const fn new(
        space: ProductSpace,
        created: bool,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            space,
            created,
            commit,
        }
    }

    #[must_use]
    /// Returns the product space.
    pub const fn space(&self) -> &ProductSpace {
        &self.space
    }

    #[must_use]
    /// Returns true when a new catalog entry was created.
    pub const fn created(&self) -> bool {
        self.created
    }

    #[must_use]
    /// Returns the commit outcome when the catalog changed.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }

    #[must_use]
    /// Returns the commit version when the catalog changed.
    pub const fn version(&self) -> Option<CommitVersion> {
        match self.commit {
            Some(commit) => Some(commit.version()),
            None => None,
        }
    }

    #[must_use]
    /// Returns the commit timestamp when the catalog changed.
    pub const fn timestamp(&self) -> Option<Timestamp> {
        match self.commit {
            Some(commit) => Some(commit.timestamp()),
            None => None,
        }
    }
}

/// Outcome returned after deleting a product space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpaceDeleteOutcome {
    space: ProductSpace,
    deleted: bool,
    force: bool,
    deleted_rows: u64,
    commit: Option<CommitOutcome>,
}

impl SpaceDeleteOutcome {
    pub(crate) const fn new(
        space: ProductSpace,
        deleted: bool,
        force: bool,
        deleted_rows: u64,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            space,
            deleted,
            force,
            deleted_rows,
            commit,
        }
    }

    #[must_use]
    /// Returns the product space.
    pub const fn space(&self) -> &ProductSpace {
        &self.space
    }

    #[must_use]
    /// Returns true when the catalog entry was deleted.
    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    #[must_use]
    /// Returns whether forced deletion was requested.
    pub const fn force(&self) -> bool {
        self.force
    }

    #[must_use]
    /// Returns the number of visible space rows tombstoned, including primitive index/control rows.
    pub const fn deleted_rows(&self) -> u64 {
        self.deleted_rows
    }

    #[must_use]
    /// Returns the commit outcome when the catalog changed.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }

    #[must_use]
    /// Returns the commit version when the catalog changed.
    pub const fn version(&self) -> Option<CommitVersion> {
        match self.commit {
            Some(commit) => Some(commit.version()),
            None => None,
        }
    }

    #[must_use]
    /// Returns the commit timestamp when the catalog changed.
    pub const fn timestamp(&self) -> Option<Timestamp> {
        match self.commit {
            Some(commit) => Some(commit.timestamp()),
            None => None,
        }
    }
}

/// Visible usage for one product space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpaceUsageSummary {
    space: ProductSpace,
    kv_count: u64,
    json_count: u64,
    vector_collection_count: u64,
    vector_entry_count: u64,
    event_count: u64,
    graph_count: u64,
    graph_node_count: u64,
    graph_edge_count: u64,
}

impl SpaceUsageSummary {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        space: ProductSpace,
        kv_count: u64,
        json_count: u64,
        vector_collection_count: u64,
        vector_entry_count: u64,
        event_count: u64,
        graph_count: u64,
        graph_node_count: u64,
        graph_edge_count: u64,
    ) -> Self {
        Self {
            space,
            kv_count,
            json_count,
            vector_collection_count,
            vector_entry_count,
            event_count,
            graph_count,
            graph_node_count,
            graph_edge_count,
        }
    }

    #[must_use]
    /// Returns the product space.
    pub const fn space(&self) -> &ProductSpace {
        &self.space
    }

    #[must_use]
    /// Returns the visible KV row count.
    pub const fn kv_count(&self) -> u64 {
        self.kv_count
    }

    #[must_use]
    /// Returns the visible JSON document count.
    pub const fn json_count(&self) -> u64 {
        self.json_count
    }

    #[must_use]
    /// Returns the visible vector collection count.
    pub const fn vector_collection_count(&self) -> u64 {
        self.vector_collection_count
    }

    #[must_use]
    /// Returns the visible vector entry count.
    pub const fn vector_entry_count(&self) -> u64 {
        self.vector_entry_count
    }

    #[must_use]
    /// Returns the visible event count.
    pub const fn event_count(&self) -> u64 {
        self.event_count
    }

    #[must_use]
    /// Returns the visible graph count.
    pub const fn graph_count(&self) -> u64 {
        self.graph_count
    }

    #[must_use]
    /// Returns the visible graph node count.
    pub const fn graph_node_count(&self) -> u64 {
        self.graph_node_count
    }

    #[must_use]
    /// Returns the visible graph edge count.
    pub const fn graph_edge_count(&self) -> u64 {
        self.graph_edge_count
    }
}

/// Service for branch-local product space operations.
pub struct SpaceService<'a> {
    persistence: &'a mut StoragePersistence,
    control: &'a mut ControlPlane,
    branch: BranchName,
}

impl<'a> SpaceService<'a> {
    pub(crate) const fn new(
        persistence: &'a mut StoragePersistence,
        control: &'a mut ControlPlane,
        branch: BranchName,
    ) -> Self {
        Self {
            persistence,
            control,
            branch,
        }
    }

    /// Lists registered product spaces on this branch.
    pub fn list(&mut self) -> Result<Vec<ProductSpace>, EngineError> {
        let record = self.branch_record()?;
        control_space::registered_spaces(self.persistence, &record)
    }

    /// Creates a product space catalog entry.
    pub fn create(&mut self, space: ProductSpace) -> Result<SpaceCreateOutcome, EngineError> {
        let record = self.branch_record()?;
        let mutations = control_space::registration_mutations(self.persistence, &record, &space)?;
        if mutations.is_empty() {
            return Ok(SpaceCreateOutcome::new(space, false, None));
        }
        let commit = self
            .persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                mutations,
                Some(record.generation()),
            ))?
            .with_counts(1, 0);
        Ok(SpaceCreateOutcome::new(space, true, Some(commit)))
    }

    /// Returns whether a product space is registered on this branch.
    pub fn exists(&mut self, space: &ProductSpace) -> Result<bool, EngineError> {
        let record = self.branch_record()?;
        control_space::space_exists(self.persistence, &record, space)
    }

    /// Returns visible usage for a registered or valid product space.
    pub fn usage(&mut self, space: &ProductSpace) -> Result<SpaceUsageSummary, EngineError> {
        let record = self.branch_record()?;
        self.usage_for_record(&record, space)
    }

    /// Deletes a product space catalog entry and, when forced, its visible data rows.
    pub fn delete(
        &mut self,
        space: &ProductSpace,
        force: bool,
    ) -> Result<SpaceDeleteOutcome, EngineError> {
        if space.as_str() == control_space::DEFAULT_SPACE {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.space_delete_default",
                "default product space cannot be deleted",
            ));
        }

        let record = self.branch_record()?;
        let Some(catalog_mutations) =
            control_space::deletion_mutations(self.persistence, &record, space)?
        else {
            return Ok(SpaceDeleteOutcome::new(
                space.clone(),
                false,
                force,
                0,
                None,
            ));
        };

        let data_mutations = self.delete_mutations_for_space(&record, space)?;
        let deleted_rows = u64::try_from(data_mutations.len()).unwrap_or(u64::MAX);
        if deleted_rows > 0 && !force {
            return Err(EngineError::conflict(
                "failed_precondition.engine.space_not_empty",
                format!(
                    "product space `{}` contains visible data; retry with force=true to delete it",
                    space.as_str()
                ),
            ));
        }

        let mutation_count = data_mutations
            .len()
            .checked_add(catalog_mutations.len())
            .ok_or_else(|| {
                EngineError::invalid_input(
                    "invalid_argument.engine.space_delete_too_large",
                    "space delete mutation count overflowed",
                )
            })?;
        if mutation_count > SPACE_DELETE_MUTATION_LIMIT {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.space_delete_too_large",
                format!(
                    "space delete would require {mutation_count} mutations; maximum is {SPACE_DELETE_MUTATION_LIMIT}"
                ),
            ));
        }

        let mut mutations = data_mutations;
        mutations.extend(catalog_mutations);
        let commit = self
            .persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                mutations,
                Some(record.generation()),
            ))?
            .with_counts(0, usize::try_from(deleted_rows).unwrap_or(usize::MAX));
        Ok(SpaceDeleteOutcome::new(
            space.clone(),
            true,
            force,
            deleted_rows,
            Some(commit),
        ))
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

    fn usage_for_record(
        &mut self,
        record: &BranchCatalogRecord,
        space: &ProductSpace,
    ) -> Result<SpaceUsageSummary, EngineError> {
        Ok(SpaceUsageSummary::new(
            space.clone(),
            self.visible_count(record, RowClass::Kv, encode_kv_space_prefix(space))?,
            self.visible_count(record, RowClass::Json, encode_json_space_prefix(space))?,
            self.visible_count(
                record,
                RowClass::VectorCollection,
                encode_vector_collection_prefix(space),
            )?,
            self.visible_count(record, RowClass::Vector, encode_vector_space_prefix(space))?,
            self.visible_count(record, RowClass::Event, encode_event_space_prefix(space))?,
            self.visible_count(
                record,
                RowClass::GraphMetadata,
                encode_graph_metadata_prefix(space),
            )?,
            self.visible_count(
                record,
                RowClass::GraphNode,
                encode_graph_node_space_prefix(space),
            )?,
            self.visible_count(
                record,
                RowClass::GraphEdge,
                encode_graph_edge_space_prefix(space),
            )?,
        ))
    }

    fn visible_count(
        &mut self,
        record: &BranchCatalogRecord,
        row_class: RowClass,
        prefix: Vec<u8>,
    ) -> Result<u64, EngineError> {
        let count = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                row_class,
                prefix,
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    fn delete_mutations_for_space(
        &mut self,
        record: &BranchCatalogRecord,
        space: &ProductSpace,
    ) -> Result<Vec<RowMutation>, EngineError> {
        let mut mutations = Vec::new();
        for (row_class, prefix) in data_delete_prefixes(space) {
            let rows = self.persistence.scan_prefix(
                record.storage_branch_id(),
                row_class,
                prefix,
                ReadSelector::Latest,
                None,
            )?;
            for row in rows.into_iter().filter(|row| !row.is_tombstone()) {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    row_class,
                    row.key().to_vec(),
                )));
            }
        }
        let rows = self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::SpaceControl,
            vector_index_manifest_prefix(),
            ReadSelector::Latest,
            None,
        )?;
        for row in rows.into_iter().filter(|row| !row.is_tombstone()) {
            let Ok((manifest_space, _collection)) = decode_vector_index_manifest_key(row.key())
            else {
                continue;
            };
            if &manifest_space == space {
                mutations.push(RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::SpaceControl,
                    row.key().to_vec(),
                )));
            }
        }
        Ok(mutations)
    }
}

fn data_delete_prefixes(space: &ProductSpace) -> Vec<(RowClass, Vec<u8>)> {
    vec![
        (RowClass::Kv, encode_kv_space_prefix(space)),
        (RowClass::Json, encode_json_space_prefix(space)),
        (RowClass::JsonIndex, encode_json_index_meta_prefix(space)),
        (
            RowClass::JsonIndex,
            encode_json_index_entry_space_prefix(space),
        ),
        (
            RowClass::VectorCollection,
            encode_vector_collection_prefix(space),
        ),
        (RowClass::Vector, encode_vector_space_prefix(space)),
        (RowClass::Event, encode_event_space_prefix(space)),
        (
            RowClass::EventMetadata,
            encode_event_meta_space_prefix(space),
        ),
        (
            RowClass::EventIndex,
            encode_event_type_index_space_prefix(space),
        ),
        (RowClass::GraphMetadata, encode_graph_metadata_prefix(space)),
        (RowClass::GraphNode, encode_graph_node_space_prefix(space)),
        (RowClass::GraphEdge, encode_graph_edge_space_prefix(space)),
        (
            RowClass::GraphReverseEdge,
            encode_graph_reverse_edge_space_prefix(space),
        ),
        (
            RowClass::GraphBindingIndex,
            encode_graph_binding_space_prefix(space),
        ),
        (
            RowClass::GraphOntology,
            encode_graph_ontology_space_prefix(space),
        ),
        (
            RowClass::GraphTypeIndex,
            encode_graph_type_index_space_prefix(space),
        ),
    ]
}
