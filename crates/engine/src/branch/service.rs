//! Product branch service.

use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::{
    BranchCatalogRecord, BranchMergeRecord, BranchOperationKind, BranchParentRecord, BranchStatus,
};
use crate::control::ControlPlane;
use crate::data::vector::{decode_vector_index_manifest, encode_vector_index_manifest};
use crate::diagnostics::EngineError;
use crate::persistence::{
    decode_vector_index_manifest_key, vector_index_manifest_key, vector_index_manifest_prefix,
    CommitPlan, PersistenceBranchCleanup, PersistenceBranchOutcome, PersistenceBranchStatus,
    PersistenceBranchSummary, ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
};

use super::BranchName;
use crate::api::{
    BranchCleanupSummary, BranchComparison, BranchCreateOutcome, BranchDeleteOutcome,
    BranchPreview, BranchStateSelector, BranchSummary, ComparedCapability, ConflictKind,
    PromotionOutcome, PromotionStrategy,
};

/// Service for product branch operations.
pub struct BranchService<'a> {
    persistence: &'a mut StoragePersistence,
    control: &'a mut ControlPlane,
}

impl<'a> BranchService<'a> {
    pub(crate) const fn new(
        persistence: &'a mut StoragePersistence,
        control: &'a mut ControlPlane,
    ) -> Self {
        Self {
            persistence,
            control,
        }
    }

    /// Lists active product branches.
    pub fn list(&self) -> Result<Vec<BranchSummary>, EngineError> {
        self.control.require_healthy()?;
        Ok(self
            .control
            .list_branches()
            .into_iter()
            .map(|record| BranchSummary::from_catalog(&record))
            .collect())
    }

    /// Looks up an active product branch by name.
    pub fn get(&self, name: &BranchName) -> Result<BranchSummary, EngineError> {
        self.control.require_healthy()?;
        let record = self.control.lookup_branch(name).ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{name}` does not exist"),
            )
        })?;
        Ok(BranchSummary::from_catalog(record))
    }

    /// Creates an empty root product branch.
    pub fn create(&mut self, name: BranchName) -> Result<BranchCreateOutcome, EngineError> {
        self.control.require_healthy()?;
        self.reject_duplicate_active(&name)?;
        let generation = self.control.next_generation_for_name(&name);
        let record = BranchCatalogRecord::root(name, generation);
        self.reject_aliasing_storage_branch(&record)?;

        ControlPlane::begin_branch_operation(
            self.persistence,
            &record,
            BranchOperationKind::CreateOrFork,
        )?;
        let outcome = match self
            .persistence
            .create_branch(record.storage_branch_id(), generation)
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(self.clear_pending_after_storage_error(&record, error));
            }
        };
        let record = record.with_storage_facts(
            outcome.branch().generation(),
            branch_status(outcome.branch()),
            outcome.branch().created_at(),
            outcome.branch().deleted_at(),
            outcome.branch().state_revision(),
        );
        self.persist_catalog_record(record.clone())?;
        Ok(BranchCreateOutcome::new(BranchSummary::from_catalog(
            &record,
        )))
    }

    /// Forks a product branch from the current source branch head.
    pub fn fork_current(
        &mut self,
        source: &BranchName,
        name: BranchName,
    ) -> Result<BranchCreateOutcome, EngineError> {
        self.control.require_healthy()?;
        self.reject_duplicate_active(&name)?;
        let source_record = self.control.lookup_branch(source).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("source branch `{source}` does not exist"),
            )
        })?;
        let generation = self.control.next_generation_for_name(&name);
        let placeholder_parent = BranchParentRecord::new(
            source_record.name().clone(),
            source_record.branch_id(),
            source_record.generation(),
            CommitVersion::ZERO,
            None,
        );
        let record = BranchCatalogRecord::forked(name, generation, placeholder_parent);
        let storage_branch_id = record.storage_branch_id();
        self.reject_aliasing_storage_branch(&record)?;

        ControlPlane::begin_branch_operation(
            self.persistence,
            &record,
            BranchOperationKind::CreateOrFork,
        )?;
        // #2521: no silent fallback to `create_branch` on a fork error — that
        // fabricated an EMPTY, unparented child (fork_version 0) whenever
        // fork-source history resolution failed, turning a recoverable
        // condition into silent data loss. Storage now forks a genuinely
        // history-less source at version zero itself (parent linkage intact);
        // every other error surfaces.
        let fork_outcome = match self.persistence.fork_branch_current(
            storage_branch_id,
            source_record.storage_branch_id(),
            generation,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(self.clear_pending_after_storage_error(&record, error));
            }
        };
        let outcome = match self.persistence.describe_branch(storage_branch_id) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(self.roll_back_forked_branch(&record, error));
            }
        };
        let parent = BranchParentRecord::new(
            source_record.name().clone(),
            source_record.branch_id(),
            source_record.generation(),
            fork_outcome
                .fork_version()
                .or_else(|| {
                    outcome
                        .parent()
                        .map(crate::persistence::PersistenceBranchParent::fork_version)
                })
                .unwrap_or(CommitVersion::ZERO),
            None,
        );
        let record = BranchCatalogRecord::forked(record.name().clone(), generation, parent.clone())
            .with_storage_facts(
                outcome.generation(),
                branch_status(outcome),
                outcome.created_at(),
                outcome.deleted_at(),
                outcome.state_revision(),
            );
        if let Err(error) = self.materialize_vector_index_manifests_for_fork(
            &source_record,
            &record,
            parent.fork_version(),
            parent.fork_timestamp(),
        ) {
            return Err(self.roll_back_forked_branch(&record, error));
        }
        self.persist_catalog_record(record.clone())?;

        Ok(BranchCreateOutcome::new(BranchSummary::from_catalog(
            &record,
        )))
    }

    /// Forks a product branch from a retained source version.
    pub fn fork_at_version(
        &mut self,
        source: &BranchName,
        name: BranchName,
        version: CommitVersion,
    ) -> Result<BranchCreateOutcome, EngineError> {
        self.fork_with(
            source,
            name,
            |persistence, branch_id, source_id, generation| {
                persistence.fork_branch_at_version(branch_id, source_id, version, generation)
            },
        )
    }

    /// Forks a product branch from a retained source timestamp.
    pub fn fork_at_timestamp(
        &mut self,
        source: &BranchName,
        name: BranchName,
        timestamp: Timestamp,
    ) -> Result<BranchCreateOutcome, EngineError> {
        self.fork_with(
            source,
            name,
            |persistence, branch_id, source_id, generation| {
                persistence.fork_branch_at_timestamp(branch_id, source_id, timestamp, generation)
            },
        )
    }

    /// Compares two branches, reporting the authored entities that differ,
    /// grouped by capability and space. The comparison is directional from
    /// `branch_a` to `branch_b`. Derived rows are omitted by default.
    pub fn compare(
        &mut self,
        branch_a: &BranchName,
        branch_b: &BranchName,
        selector: BranchStateSelector,
    ) -> Result<BranchComparison, EngineError> {
        self.control.require_healthy()?;
        let record_a = self
            .control
            .lookup_branch(branch_a)
            .cloned()
            .ok_or_else(|| {
                EngineError::not_found(
                    "not_found.engine.branch",
                    format!("branch `{branch_a}` does not exist"),
                )
            })?;
        let record_b = self
            .control
            .lookup_branch(branch_b)
            .cloned()
            .ok_or_else(|| {
                EngineError::not_found(
                    "not_found.engine.branch",
                    format!("branch `{branch_b}` does not exist"),
                )
            })?;
        super::compare::compare_records(self.persistence, &record_a, &record_b, selector)
    }

    /// Previews promoting `source` into `target`: derives the branch point from
    /// lineage, runs a three-way comparison, and reports the conflicts a
    /// promotion would hit, without mutating either branch.
    pub fn preview(
        &mut self,
        source: &BranchName,
        target: &BranchName,
        strategy: PromotionStrategy,
    ) -> Result<BranchPreview, EngineError> {
        self.control.require_healthy()?;
        let source_record = self.control.lookup_branch(source).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{source}` does not exist"),
            )
        })?;
        let target_record = self.control.lookup_branch(target).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{target}` does not exist"),
            )
        })?;
        super::preview::preview_branches(self.persistence, &source_record, &target_record, strategy)
    }

    /// Promotes `source` into `target`: derives the branch point from lineage,
    /// runs a three-way merge, and applies the source's changes onto the target
    /// as a single atomic commit. The source branch is never modified.
    ///
    /// `Strict` refuses with `conflict.engine.promotion` and zero target
    /// mutation when any conflict exists; `SourceWins` applies the source value
    /// or tombstone for each conflict and reports what it overwrote or deleted.
    /// A clean promotion that applies nothing leaves the target unchanged and
    /// writes no commit.
    pub fn promote(
        &mut self,
        source: &BranchName,
        target: &BranchName,
        strategy: PromotionStrategy,
    ) -> Result<PromotionOutcome, EngineError> {
        self.control.require_healthy()?;
        let source_record = self.control.lookup_branch(source).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{source}` does not exist"),
            )
        })?;
        let target_record = self.control.lookup_branch(target).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{target}` does not exist"),
            )
        })?;

        let plan = super::promote::plan_promotion(
            self.persistence,
            &source_record,
            &target_record,
            strategy,
        )?;

        // Structural conflicts (e.g. an incompatible vector collection) cannot be
        // merged by any strategy, so they refuse the promotion even under
        // SourceWins; ordinary conflicts refuse only under Strict.
        let has_structural = plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind() == ConflictKind::IncompatibleCollection);
        if has_structural || (strategy == PromotionStrategy::Strict && !plan.conflicts.is_empty()) {
            return Err(EngineError::conflict(
                "conflict.engine.promotion",
                format!(
                    "promotion of `{source}` into `{target}` refused: {} conflict(s)",
                    plan.conflicts.len()
                ),
            ));
        }

        let (target_version, target_timestamp) = if plan.mutations.is_empty() {
            (None, None)
        } else {
            let (version, timestamp) =
                self.commit_promotion_with_lineage(&source_record, &target_record, plan.mutations)?;
            (Some(version), Some(timestamp))
        };

        // The capabilities actually carried in — their derived state is what the
        // promotion dispositioned.
        let mut promoted: Vec<ComparedCapability> = Vec::new();
        for entity in plan.applied.iter().chain(plan.deleted.iter()) {
            if !promoted.contains(&entity.capability()) {
                promoted.push(entity.capability());
            }
        }
        let coverage = super::preview::branch_workflow_coverage(
            self.persistence,
            &source_record,
            &target_record,
            &promoted,
        )?;

        Ok(PromotionOutcome::new(
            source_record.name().clone(),
            target_record.name().clone(),
            plan.branch_point,
            strategy,
            target_version,
            target_timestamp,
            plan.applied,
            plan.deleted,
            plan.conflicts,
            coverage,
        ))
    }

    /// Commits a promotion's data mutations into `target` and publishes the
    /// authoritative merge lineage, bracketed by a recoverable intent so a crash
    /// in the data-commit → edge-publish window is reconciled on reopen. Returns
    /// the committed target version and timestamp.
    fn commit_promotion_with_lineage(
        &mut self,
        source_record: &BranchCatalogRecord,
        target_record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
    ) -> Result<(CommitVersion, Timestamp), EngineError> {
        // Capture the target's timeline head before mutating, so reopen recovery
        // can tell whether the data commit landed after a crash.
        let (baseline, _) = self
            .persistence
            .branch_timeline_head(target_record.storage_branch_id())?;
        let baseline = baseline.unwrap_or(CommitVersion::ZERO);
        // The source's timeline head is the frontier being merged and the true
        // base for any later repeated promotion; record it so `resolve_base_point`
        // diffs against source-at-merge, not the target's post-merge commit (which
        // includes target-only rows and would re-surface them as source deletions).
        let (source_head, _) = self
            .persistence
            .branch_timeline_head(source_record.storage_branch_id())?;
        let source_head = source_head.unwrap_or(CommitVersion::ZERO);
        // Recoverable promotion intent (contract §Promotion rule 7): the target
        // record carrying the source lineage and the baseline (as the placeholder
        // merge version), written before any target mutation so a crash in the
        // data-commit → edge-publish window is completed or rolled back on reopen.
        // The edge publish clears it.
        let intent = target_record
            .clone()
            .with_merge_parent(BranchMergeRecord::new(
                source_record.name().clone(),
                source_record.branch_id(),
                source_record.generation(),
                baseline,
                None,
                Some(source_head),
            ));
        ControlPlane::begin_branch_operation(
            self.persistence,
            &intent,
            BranchOperationKind::Promote,
        )?;

        let outcome = match self.persistence.commit(&CommitPlan::new(
            target_record.storage_branch_id(),
            mutations,
            Some(target_record.generation()),
        )) {
            Ok(outcome) => outcome,
            Err(error) => return Err(self.clear_pending_after_storage_error(&intent, error)),
        };
        // Publish the authoritative promotion lineage with the real target commit
        // version, atomically clearing the pending intent.
        let merge = BranchMergeRecord::new(
            source_record.name().clone(),
            source_record.branch_id(),
            source_record.generation(),
            outcome.version(),
            Some(outcome.timestamp()),
            Some(source_head),
        );
        self.persist_catalog_record(target_record.clone().with_merge_parent(merge))?;
        Ok((outcome.version(), outcome.timestamp()))
    }

    /// Deletes an active product branch.
    pub fn delete(&mut self, name: &BranchName) -> Result<BranchDeleteOutcome, EngineError> {
        self.control.require_healthy()?;
        if name == self.control.default_branch() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.branch_delete",
                "default branch cannot be deleted",
            ));
        }
        if self.control.active_branch_count() <= 1 {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.branch_delete",
                "delete would remove the last active branch",
            ));
        }
        let record = self.control.lookup_branch(name).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{name}` does not exist"),
            )
        })?;

        ControlPlane::begin_branch_operation(
            self.persistence,
            &record,
            BranchOperationKind::Delete,
        )?;
        let outcome = match self
            .persistence
            .delete_branch(record.storage_branch_id(), record.generation())
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(self.clear_pending_after_storage_error(&record, error));
            }
        };
        let branch = outcome.branch();
        let deleted = record.with_storage_facts(
            branch.generation(),
            branch_status(branch),
            branch.created_at(),
            branch.deleted_at(),
            branch.state_revision(),
        );
        self.persist_catalog_record(deleted.clone())?;
        Ok(BranchDeleteOutcome::new(
            BranchSummary::from_catalog(&deleted),
            outcome.generation_before(),
            outcome.generation_after(),
            outcome.cleanup().map(cleanup_summary),
        ))
    }

    fn fork_with(
        &mut self,
        source: &BranchName,
        name: BranchName,
        fork: impl FnOnce(
            &mut StoragePersistence,
            strata_core::BranchId,
            strata_core::BranchId,
            u64,
        ) -> Result<PersistenceBranchOutcome, EngineError>,
    ) -> Result<BranchCreateOutcome, EngineError> {
        self.control.require_healthy()?;
        self.reject_duplicate_active(&name)?;
        let source_record = self.control.lookup_branch(source).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("source branch `{source}` does not exist"),
            )
        })?;
        let generation = self.control.next_generation_for_name(&name);
        let pending = BranchCatalogRecord::root(name.clone(), generation);
        let storage_branch_id = pending.storage_branch_id();
        self.reject_aliasing_storage_branch(&pending)?;
        ControlPlane::begin_branch_operation(
            self.persistence,
            &pending,
            BranchOperationKind::CreateOrFork,
        )?;
        let outcome = match fork(
            self.persistence,
            storage_branch_id,
            source_record.storage_branch_id(),
            generation,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(self.clear_pending_after_storage_error(&pending, error));
            }
        };
        let branch = outcome.branch();
        let parent = BranchParentRecord::new(
            source_record.name().clone(),
            source_record.branch_id(),
            source_record.generation(),
            outcome
                .fork_version()
                .or_else(|| {
                    branch
                        .parent()
                        .map(crate::persistence::PersistenceBranchParent::fork_version)
                })
                .unwrap_or(CommitVersion::ZERO),
            outcome.fork_timestamp(),
        );
        let record = BranchCatalogRecord::forked(name, generation, parent.clone())
            .with_storage_facts(
                branch.generation(),
                branch_status(branch),
                branch.created_at(),
                branch.deleted_at(),
                branch.state_revision(),
            );
        if let Err(error) = self.materialize_vector_index_manifests_for_fork(
            &source_record,
            &record,
            parent.fork_version(),
            parent.fork_timestamp(),
        ) {
            return Err(self.roll_back_forked_branch(&record, error));
        }
        self.persist_catalog_record(record.clone())?;
        Ok(BranchCreateOutcome::new(BranchSummary::from_catalog(
            &record,
        )))
    }

    /// Rejects a new branch whose derived storage identity collides with an
    /// existing branch of a different name (finding U8), before any durable
    /// state is written.
    fn reject_aliasing_storage_branch(
        &self,
        record: &BranchCatalogRecord,
    ) -> Result<(), EngineError> {
        if let Some(existing) = self
            .control
            .find_aliasing_storage_branch(record.name(), record.storage_branch_id())
        {
            return Err(EngineError::conflict(
                "already_exists.engine.branch",
                format!(
                    "branch `{}` derives the same storage identity as existing branch `{}`",
                    record.name(),
                    existing.name()
                ),
            ));
        }
        Ok(())
    }

    fn reject_duplicate_active(&self, name: &BranchName) -> Result<(), EngineError> {
        if self.control.contains_branch(name) {
            return Err(EngineError::conflict(
                "already_exists.engine.branch",
                format!("branch `{name}` already exists"),
            ));
        }
        Ok(())
    }

    fn clear_pending_after_storage_error(
        &mut self,
        record: &BranchCatalogRecord,
        original: EngineError,
    ) -> EngineError {
        match ControlPlane::clear_pending_branch_operation(self.persistence, record) {
            Ok(()) => original,
            Err(error) => {
                self.control
                    .fail_closed_after_branch_operation_error(&error);
                error
            }
        }
    }

    /// Rolls back a fork/create that failed AFTER its storage branch was already
    /// durably created, so no orphaned storage branch is left behind with no
    /// catalog row (finding U12).
    ///
    /// The storage branch is deleted (tombstoned) and a matching deleted catalog
    /// record is published, which both removes the orphan and advances the
    /// name's generation past the storage tombstone — storage's recreate guard
    /// requires a strictly higher generation, so without this the name would be
    /// permanently poisoned (a clean retry would fail). The deleted record is
    /// keyed by name, so a successful retry overwrites it. If the storage
    /// rollback itself fails, fail closed and leave the pending marker for reopen
    /// recovery to reconcile.
    fn roll_back_forked_branch(
        &mut self,
        record: &BranchCatalogRecord,
        original: EngineError,
    ) -> EngineError {
        let deleted = match self
            .persistence
            .delete_branch(record.storage_branch_id(), record.generation())
        {
            Ok(outcome) => {
                let branch = outcome.branch();
                record.clone().with_storage_facts(
                    branch.generation(),
                    branch_status(branch),
                    branch.created_at(),
                    branch.deleted_at(),
                    branch.state_revision(),
                )
            }
            Err(error) => {
                self.control
                    .fail_closed_after_branch_operation_error(&error);
                return error;
            }
        };
        // persist_catalog_record writes the (deleted) record and clears the
        // pending marker in one commit, and fails closed on its own error.
        match self.persist_catalog_record(deleted) {
            Ok(()) => original,
            Err(error) => error,
        }
    }

    fn persist_catalog_record(&mut self, record: BranchCatalogRecord) -> Result<(), EngineError> {
        match self.control.persist_branch_record(self.persistence, record) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.control
                    .fail_closed_after_branch_operation_error(&error);
                Err(error)
            }
        }
    }

    fn materialize_vector_index_manifests_for_fork(
        &mut self,
        source_record: &BranchCatalogRecord,
        child_record: &BranchCatalogRecord,
        fork_version: CommitVersion,
        fork_timestamp: Option<Timestamp>,
    ) -> Result<(), EngineError> {
        if fork_version == CommitVersion::ZERO {
            return Ok(());
        }
        let selector = fork_timestamp.map_or(ReadSelector::AtVersion(fork_version), |timestamp| {
            ReadSelector::AtTimestamp(timestamp)
        });
        let mut mutations = self.vector_index_manifest_fork_mutations(
            source_record,
            child_record,
            fork_version,
            selector,
        )?;
        if mutations.is_empty() && selector != ReadSelector::Latest {
            mutations = self.vector_index_manifest_fork_mutations(
                source_record,
                child_record,
                fork_version,
                ReadSelector::Latest,
            )?;
        }
        if mutations.is_empty() {
            return Ok(());
        }
        self.persistence.commit(&CommitPlan::new(
            child_record.storage_branch_id(),
            mutations,
            Some(child_record.generation()),
        ))?;
        Ok(())
    }

    fn vector_index_manifest_fork_mutations(
        &mut self,
        source_record: &BranchCatalogRecord,
        child_record: &BranchCatalogRecord,
        fork_version: CommitVersion,
        selector: ReadSelector,
    ) -> Result<Vec<RowMutation>, EngineError> {
        let rows = self.persistence.scan_prefix(
            source_record.storage_branch_id(),
            RowClass::SpaceControl,
            vector_index_manifest_prefix(),
            selector,
            None,
        )?;
        let mut mutations = Vec::new();
        for row in rows {
            if row.is_tombstone() {
                continue;
            }
            let Ok((space, collection)) = decode_vector_index_manifest_key(row.key()) else {
                continue;
            };
            let Some(value) = row.value() else {
                continue;
            };
            let Ok(manifest) = decode_vector_index_manifest(value) else {
                continue;
            };
            if !manifest.matches_branch_key(
                source_record.storage_branch_id(),
                source_record.generation(),
                &space,
                &collection,
            ) {
                continue;
            }
            let child_manifest = manifest.materialize_for_child_fork(
                child_record.storage_branch_id(),
                child_record.generation(),
                fork_version,
            );
            let bytes = encode_vector_index_manifest(&child_manifest)?;
            mutations.push(RowMutation::put(
                RowAddress::new(
                    child_record.storage_branch_id(),
                    RowClass::SpaceControl,
                    vector_index_manifest_key(&space, &collection),
                ),
                bytes,
            ));
        }
        Ok(mutations)
    }
}

const fn branch_status(summary: PersistenceBranchSummary) -> BranchStatus {
    match summary.status() {
        PersistenceBranchStatus::Active => BranchStatus::Active,
        PersistenceBranchStatus::Deleted => BranchStatus::Deleted,
    }
}

const fn cleanup_summary(cleanup: PersistenceBranchCleanup) -> BranchCleanupSummary {
    BranchCleanupSummary::new(
        cleanup.removed_refs(),
        cleanup.releasable_tables(),
        cleanup.protected_tables(),
    )
}

#[cfg(test)]
mod tests {
    use super::BranchService;
    use crate::branch::catalog::{BranchCatalogRecord, BranchStatus, DEFAULT_BRANCH_GENERATION};
    use crate::branch::BranchName;
    use crate::control::bootstrap_or_load;
    use crate::diagnostics::{EngineError, EngineErrorClass};
    use crate::persistence::{PersistenceOpenTarget, StoragePersistence};
    use strata_core::BranchId;

    #[test]
    fn branch_create_failure_after_pending_does_not_activate_catalog_entry() {
        let (mut persistence, summary) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let mut control = bootstrap_or_load(&mut persistence, summary.created(), None)
            .expect("bootstrap succeeds");
        let feature = BranchName::new("feature").expect("valid branch");
        control.insert_branch(BranchCatalogRecord::new(
            BranchName::default_branch(),
            BranchId::from_bytes([0x44; BranchId::BYTE_LEN]),
            BranchId::from_bytes([0x44; BranchId::BYTE_LEN]),
            DEFAULT_BRANCH_GENERATION,
            BranchStatus::Active,
            None,
            None,
            None,
            0,
        ));

        let error = BranchService::new(&mut persistence, &mut control)
            .fork_current(&BranchName::default_branch(), feature.clone())
            .expect_err("preexisting lower branch must fail");
        assert_eq!(error.class(), EngineErrorClass::NotFound);
        assert_eq!(error.code(), "not_found.engine.persistence");
        assert!(!control.contains_branch(&feature));

        let loaded = bootstrap_or_load(&mut persistence, false, None)
            .expect("control plane reloads cleanly");
        assert!(!loaded.contains_branch(&feature));
    }

    #[test]
    fn fork_empty_source_fallback_failure_clears_pending_catalog_entry() {
        let (mut persistence, summary) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let mut control = bootstrap_or_load(&mut persistence, summary.created(), None)
            .expect("bootstrap succeeds");
        let feature = BranchName::new("feature").expect("valid branch");
        let feature_record = BranchCatalogRecord::root(feature.clone(), DEFAULT_BRANCH_GENERATION);
        persistence
            .create_branch(
                feature_record.storage_branch_id(),
                feature_record.generation(),
            )
            .expect("lower branch is preexisting");

        let error = BranchService::new(&mut persistence, &mut control)
            .fork_current(&BranchName::default_branch(), feature.clone())
            .expect_err("fallback create fails");
        assert_eq!(error.class(), EngineErrorClass::Conflict);
        assert_eq!(error.code(), "already_exists.engine.persistence");
        assert!(!control.contains_branch(&feature));

        let loaded = bootstrap_or_load(&mut persistence, false, None)
            .expect("control plane reloads cleanly");
        assert!(!loaded.contains_branch(&feature));
    }

    #[test]
    fn branch_service_refuses_work_after_control_plane_failure() {
        let (mut persistence, summary) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let mut control = bootstrap_or_load(&mut persistence, summary.created(), None)
            .expect("bootstrap succeeds");
        let cause = EngineError::corruption(
            "data_loss.engine.branch_catalog",
            "catalog update failed after branch operation",
        );
        control.fail_closed_after_branch_operation_error(&cause);

        let error = BranchService::new(&mut persistence, &mut control)
            .list()
            .expect_err("failed control plane rejects list");
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "unavailable.engine.control_plane");
    }
}
