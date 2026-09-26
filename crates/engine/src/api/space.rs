//! Branch-local product space API.

use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::BranchName;
use crate::commit::CommitOutcome;
use crate::control::{space as control_space, ControlPlane};
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{
    encode_event_space_prefix, encode_graph_edge_space_prefix, encode_graph_metadata_prefix,
    encode_graph_node_space_prefix, encode_json_space_prefix, encode_kv_space_prefix,
    encode_vector_collection_prefix, encode_vector_space_prefix, CommitPlan, ReadSelector,
    RowClass, StoragePersistence,
};

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

    /// Deletes a product space catalog entry and, when forced, its visible
    /// data rows.
    ///
    /// A space whose rows fit one commit goes in the single commit it always
    /// did. A larger one is unregistered by its first commit — absent to
    /// `list`, `exists` and every writer from then on — and its rows are
    /// then swept in commits the storage budget admits, the marked catalog
    /// row last (#3574, the sibling of `delete_graph`'s #3477). A deletion
    /// interrupted mid-sweep is finished by the next `delete` or `create` of
    /// the name, or by the first write that would register it.
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
        let Some(remaining) = control_space::index_without(self.persistence, &record, space)?
        else {
            // Not registered. #3574: an interrupted deletion leaves a marked
            // row and its rows behind; the space is already gone to every
            // observer, so finish the sweep and report it deleted.
            let outcome =
                match control_space::finish_pending_deletion(self.persistence, &record, space)? {
                    Some(last) => {
                        let deleted_rows = u64::try_from(last.delete_count()).unwrap_or(u64::MAX);
                        SpaceDeleteOutcome::new(
                            space.clone(),
                            true,
                            force,
                            deleted_rows,
                            Some(last),
                        )
                    }
                    None => SpaceDeleteOutcome::new(space.clone(), false, force, 0, None),
                };
            return Ok(outcome);
        };

        let rows = control_space::space_row_tombstones(self.persistence, &record, space)?;
        let deleted_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        if deleted_rows > 0 && !force {
            return Err(EngineError::conflict(
                "failed_precondition.engine.space_not_empty",
                format!(
                    "product space `{}` contains visible data; retry with force=true to delete it",
                    space.as_str()
                ),
            ));
        }

        if rows.len() < control_space::DELETE_CHUNK_ROWS {
            // Small enough for one commit: the catalog and the rows go together.
            let mut mutations = rows;
            mutations.extend(control_space::unregister_mutations(
                &record, space, &remaining, false,
            )?);
            let commit = self
                .persistence
                .commit(&CommitPlan::new(
                    record.storage_branch_id(),
                    mutations,
                    Some(record.generation()),
                ))?
                .with_counts(0, usize::try_from(deleted_rows).unwrap_or(usize::MAX));
            return Ok(SpaceDeleteOutcome::new(
                space.clone(),
                true,
                force,
                deleted_rows,
                Some(commit),
            ));
        }
        // #3574: too many rows for the storage commit budget. Mark first —
        // from this commit the space is unregistered — then sweep the rows
        // in commits the budget admits, and tombstone the marked row last.
        // An interruption leaves a marked row, which the next `delete`,
        // `create` or registering write of this name finishes.
        self.persistence.commit(&CommitPlan::new(
            record.storage_branch_id(),
            control_space::unregister_mutations(&record, space, &remaining, true)?,
            Some(record.generation()),
        ))?;
        let last = control_space::sweep_space_rows(self.persistence, &record, space, rows)?;
        Ok(SpaceDeleteOutcome::new(
            space.clone(),
            true,
            force,
            deleted_rows,
            Some(last),
        ))
    }

    /// Performs only the first commit of a chunked deletion — the mark — and
    /// stops, leaving the space exactly as a crash between the mark and the
    /// sweep would: unregistered, its rows still stored, its deletion waiting
    /// for the next `delete`, `create` or registering write of its name.
    #[cfg(any(test, feature = "testkit"))]
    pub fn begin_space_delete_for_test(
        &mut self,
        space: &ProductSpace,
    ) -> Result<CommitOutcome, EngineError> {
        let record = self.branch_record()?;
        let remaining = control_space::index_without(self.persistence, &record, space)?
            .ok_or_else(|| {
                EngineError::invalid_input(
                    "invalid_argument.engine.space_catalog",
                    "space is not registered",
                )
            })?;
        self.persistence.commit(&CommitPlan::new(
            record.storage_branch_id(),
            control_space::unregister_mutations(&record, space, &remaining, true)?,
            Some(record.generation()),
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
}

#[cfg(test)]
mod tests {
    use crate::branch::BranchName;
    use crate::data::kv::{KvKey, KvValue, ProductSpace};
    use crate::{CacheOpenOptions, Database, DurableLocalOpenOptions};

    fn key(index: usize) -> KvKey {
        KvKey::new(format!("k{index:05}")).expect("key")
    }

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).expect("branch")
    }

    fn space(name: &str) -> ProductSpace {
        ProductSpace::new(name).expect("space")
    }

    /// Registers `name` on the default branch and fills it with `count` keys
    /// in commits under the storage budget — more than one chunk's worth.
    fn populate(database: &mut Database, name: &str, count: usize) -> ProductSpace {
        let target = space(name);
        database
            .spaces(branch("default"))
            .expect("space service")
            .create(target.clone())
            .expect("space created");
        for start in (0..count).step_by(1_000) {
            database
                .kv(branch("default"), target.clone())
                .expect("kv service")
                .put_batch(
                    (start..(start + 1_000).min(count))
                        .map(|index| (key(index), KvValue::new(b"v".to_vec()))),
                )
                .expect("bounded put");
        }
        target
    }

    fn registered(database: &mut Database, branch_name: &str, target: &ProductSpace) -> bool {
        database
            .spaces(branch(branch_name))
            .expect("space service")
            .exists(target)
            .expect("exists reads")
    }

    fn kv_count(database: &mut Database, branch_name: &str, target: &ProductSpace) -> u64 {
        database
            .kv(branch(branch_name), target.clone())
            .expect("kv service")
            .count(None)
            .expect("count reads")
    }

    /// #3574: the mark alone unregisters a space, and every path that could
    /// re-register or re-delete the name finishes the interrupted sweep
    /// first — `delete`, `create`, a registering write — on the branch that
    /// carries the mark and, after a fork, independently on each branch.
    fn exercise(mut database: Database) {
        // Resume by delete: no force needed for a space already gone to every
        // observer; the rows it swept are reported; nothing remains.
        let by_delete = populate(&mut database, "by-delete", 3_000);
        let mark = database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&by_delete)
            .expect("mark commits");
        assert!(!registered(&mut database, "default", &by_delete));
        let listed = database
            .spaces(branch("default"))
            .expect("space service")
            .list()
            .expect("list reads");
        assert!(!listed.contains(&by_delete));
        assert!(
            listed.contains(&space("default")),
            "the default space survives the mark"
        );
        let outcome = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&by_delete, false)
            .expect("a marked space's deletion resumes");
        assert!(outcome.deleted());
        assert_eq!(outcome.deleted_rows(), 3_000);
        assert!(outcome.version().expect("commits") > mark.version());
        assert_eq!(kv_count(&mut database, "default", &by_delete), 0);
        let again = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&by_delete, true)
            .expect("nothing pending is not an error");
        assert!(!again.deleted());
        assert!(again.commit().is_none());

        // Resume by create: the old rows are swept before the name is reborn.
        let by_create = populate(&mut database, "by-create", 2_500);
        database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&by_create)
            .expect("mark commits");
        let created = database
            .spaces(branch("default"))
            .expect("space service")
            .create(by_create.clone())
            .expect("create resumes the sweep");
        assert!(created.created());
        assert!(registered(&mut database, "default", &by_create));
        assert_eq!(kv_count(&mut database, "default", &by_create), 0);

        // Resume by a registering write: the write lands in a clean space.
        let by_write = populate(&mut database, "by-write", 2_500);
        database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&by_write)
            .expect("mark commits");
        database
            .kv(branch("default"), by_write.clone())
            .expect("kv service")
            .put(key(1), KvValue::new(b"new".to_vec()))
            .expect("a registering write resumes the sweep");
        assert!(registered(&mut database, "default", &by_write));
        assert_eq!(kv_count(&mut database, "default", &by_write), 1);
        let mut kv = database
            .kv(branch("default"), by_write.clone())
            .expect("kv service");
        assert_eq!(
            kv.get(&key(1)).expect("get").expect("present").as_bytes(),
            b"new"
        );
        assert!(kv.get(&key(2)).expect("get").is_none());

        // COW-003: a fork mid-sweep inherits the mark; each branch finishes
        // its own copy, and neither finishes the other's.
        let forked = populate(&mut database, "forked", 2_500);
        database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&forked)
            .expect("mark commits");
        database
            .branches()
            .expect("branch service")
            .fork_current(&branch("default"), branch("child"))
            .expect("fork succeeds");
        assert!(!registered(&mut database, "child", &forked));
        let child_outcome = database
            .spaces(branch("child"))
            .expect("space service")
            .delete(&forked, false)
            .expect("the child resumes its copy");
        assert!(child_outcome.deleted());
        assert_eq!(child_outcome.deleted_rows(), 2_500);
        assert_eq!(kv_count(&mut database, "child", &forked), 0);
        let parent_outcome = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&forked, false)
            .expect("the parent resumes its own copy");
        assert!(parent_outcome.deleted());
        assert_eq!(parent_outcome.deleted_rows(), 2_500);
        assert_eq!(kv_count(&mut database, "default", &forked), 0);
    }

    /// A promotion re-registering a space the target holds a crash-interrupted
    /// deletion for finishes that sweep only once it has decided to commit: a
    /// preview and a refused promotion leave the sweep owed in full (rule 20),
    /// and a committed one leaves the target's space holding exactly what the
    /// source promoted.
    fn exercise_promotion(mut database: Database) {
        use crate::api::branch::PromotionStrategy;

        let shared = populate(&mut database, "shared", 2_500);
        database
            .branches()
            .expect("branch service")
            .fork_current(&branch("default"), branch("child"))
            .expect("fork succeeds");
        database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&shared)
            .expect("mark commits on the target");
        database
            .kv(branch("child"), shared.clone())
            .expect("kv service")
            .put(key(90_000), KvValue::new(b"from-child".to_vec()))
            .expect("child writes into its copy");
        // An unrelated conflict: both sides wrote the same default-space key
        // after the fork.
        for (branch_name, value) in [("default", b"p".as_slice()), ("child", b"c".as_slice())] {
            database
                .kv(branch(branch_name), space("default"))
                .expect("kv service")
                .put(key(1), KvValue::new(value.to_vec()))
                .expect("conflicting write");
        }

        // A preview writes nothing.
        database
            .branches()
            .expect("branch service")
            .preview(
                &branch("child"),
                &branch("default"),
                PromotionStrategy::Strict,
            )
            .expect("preview succeeds");
        // A refused promotion writes nothing.
        let refused = database
            .branches()
            .expect("branch service")
            .promote(
                &branch("child"),
                &branch("default"),
                PromotionStrategy::Strict,
            )
            .expect_err("a strict promotion with a conflict is refused");
        assert_eq!(refused.code(), "conflict.engine.promotion");
        assert!(!registered(&mut database, "default", &shared));
        // Every row of the interrupted deletion is still owed on the target.
        let owed = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&shared, false)
            .expect("the owed sweep is still there to finish");
        assert!(owed.deleted());
        assert_eq!(
            owed.deleted_rows(),
            2_500,
            "neither preview nor refusal swept"
        );

        // Re-arm the mark and promote under SourceWins: the sweep runs, then
        // the promotion re-registers the space with the source's rows only.
        let shared = populate(&mut database, "shared", 2_500);
        database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&shared)
            .expect("mark commits on the target");
        let outcome = database
            .branches()
            .expect("branch service")
            .promote(
                &branch("child"),
                &branch("default"),
                PromotionStrategy::SourceWins,
            )
            .expect("promotion commits");
        assert!(outcome.target_version().is_some());
        assert!(registered(&mut database, "default", &shared));
        let mut kv = database
            .kv(branch("default"), shared.clone())
            .expect("kv service");
        assert_eq!(
            kv.get(&key(90_000))
                .expect("get")
                .expect("promoted")
                .as_bytes(),
            b"from-child"
        );
        assert!(
            kv.get(&key(0)).expect("get").is_none(),
            "a row of the deleted incarnation does not resurface"
        );
        let after = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&shared, true)
            .expect("delete succeeds");
        assert_eq!(after.deleted_rows(), 1, "only the promoted row was there");
    }

    #[test]
    fn a_promotion_finishes_a_pending_space_deletion_only_when_it_commits() {
        exercise_promotion(
            Database::open_cache(CacheOpenOptions::new())
                .expect("cache opens")
                .into_database(),
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise_promotion(
            Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable opens")
                .into_database(),
        );
    }

    /// The mark is durable: a database closed between the mark and the sweep
    /// reopens with the space unregistered and its deletion owed, and the
    /// next delete finishes it.
    #[test]
    fn a_reopened_database_finishes_an_interrupted_space_deletion() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let target = {
            let mut database = Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable opens")
                .into_database();
            let target = populate(&mut database, "interrupted", 3_000);
            database
                .spaces(branch("default"))
                .expect("space service")
                .begin_space_delete_for_test(&target)
                .expect("mark commits");
            database.close().expect("close succeeds");
            target
        };
        let mut reopened = Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
            .expect("reopen succeeds")
            .into_database();
        assert!(!registered(&mut reopened, "default", &target));
        let outcome = reopened
            .spaces(branch("default"))
            .expect("space service")
            .delete(&target, false)
            .expect("the reopened database finishes the sweep");
        assert!(outcome.deleted());
        assert_eq!(outcome.deleted_rows(), 3_000);
        assert_eq!(kv_count(&mut reopened, "default", &target), 0);
    }

    #[test]
    fn a_marked_space_is_unregistered_and_its_deletion_resumes() {
        exercise(
            Database::open_cache(CacheOpenOptions::new())
                .expect("cache opens")
                .into_database(),
        );
        let tempdir = tempfile::tempdir().expect("tempdir");
        exercise(
            Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
                .expect("durable opens")
                .into_database(),
        );
    }
    /// The vector index manifests a space's collections own live under the
    /// space-control class, keyed by space rather than under the space's own
    /// prefixes; a force-delete sweeps them with the rest and counts them.
    #[test]
    fn a_force_delete_sweeps_the_space_vector_index_manifests() {
        use crate::persistence::{
            decode_vector_index_manifest_key, vector_index_manifest_prefix, ReadSelector, RowClass,
        };
        use crate::{VectorCollectionName, VectorConfig, VectorDistanceMetric};

        let mut database = Database::open_cache(CacheOpenOptions::new())
            .expect("cache opens")
            .into_database();
        // Two spaces with a manifest each: the deleted one loses its own, the
        // other keeps its own.
        let tenant = space("tenant");
        let other = space("other");
        let docs = VectorCollectionName::new("docs").expect("collection name");
        for owner in [&tenant, &other] {
            database
                .spaces(branch("default"))
                .expect("space service")
                .create(owner.clone())
                .expect("space created");
            database
                .vector(branch("default"), owner.clone())
                .expect("vector service")
                .create_collection(
                    docs.clone(),
                    VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("config"),
                )
                .expect("collection created");
            database
                .vector(branch("default"), owner.clone())
                .expect("vector service")
                .seed_empty_index_manifest_for_test(&docs)
                .expect("manifest seeded");
        }

        let live_manifests = |database: &mut Database, owner: &ProductSpace| -> usize {
            let spaces = database.spaces(branch("default")).expect("space service");
            let record = spaces.branch_record().expect("branch record");
            spaces
                .persistence
                .scan_prefix(
                    record.storage_branch_id(),
                    RowClass::SpaceControl,
                    vector_index_manifest_prefix(),
                    ReadSelector::Latest,
                    None,
                )
                .expect("manifest scan")
                .into_iter()
                .filter(|row| !row.is_tombstone())
                .filter(|row| {
                    decode_vector_index_manifest_key(row.key())
                        .is_ok_and(|(manifest_owner, _)| &manifest_owner == owner)
                })
                .count()
        };
        assert_eq!(live_manifests(&mut database, &tenant), 1);
        assert_eq!(live_manifests(&mut database, &other), 1);

        let outcome = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&tenant, true)
            .expect("forced delete succeeds");
        assert!(outcome.deleted());
        assert_eq!(
            outcome.deleted_rows(),
            2,
            "the collection row and its manifest"
        );
        assert_eq!(
            live_manifests(&mut database, &tenant),
            0,
            "the manifest was swept"
        );
        assert_eq!(
            live_manifests(&mut database, &other),
            1,
            "another space's manifest is not"
        );
    }
}
