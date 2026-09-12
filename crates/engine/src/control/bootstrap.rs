//! Control-plane bootstrap and load.

use std::collections::{BTreeMap, BTreeSet};

use crate::api::{ControlDiagnostics, ControlHealthStatus, SpaceCatalogDiagnostics};
use crate::branch::catalog::{
    BranchCatalogRecord, BranchMergeRecord, BranchOperationKind, BranchStatus,
    DEFAULT_BRANCH_GENERATION, SYSTEM_BRANCH_ID,
};
use crate::branch::BranchName;
use crate::diagnostics::{EngineError, EngineErrorClass};
use crate::persistence::{
    branch_catalog_key, branch_default_key, branch_index_key, branch_pending_index_key,
    branch_pending_key, capability_registry_key, database_identity_key,
    local_instance_identity_key, migration_registry_key, storage_registry_key, CommitPlan,
    ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
};

use super::records::{
    decode_branch_index, decode_branch_record, decode_capability_registry,
    decode_database_identity, decode_default_branch, decode_local_instance_identity,
    decode_migration_registry, decode_pending_branch_index, decode_pending_branch_record,
    decode_storage_registry, encode_branch_index, encode_branch_record, encode_capability_registry,
    encode_database_identity, encode_default_branch, encode_local_instance_identity,
    encode_migration_registry, encode_pending_branch_index, encode_pending_branch_record,
    encode_storage_registry, DatabaseIdentityRecord,
};

#[derive(Clone, Debug)]
pub(crate) struct ControlPlane {
    default_branch: BranchName,
    branches: BTreeMap<BranchName, BranchCatalogRecord>,
    terminal_error: Option<EngineError>,
}

impl ControlPlane {
    pub(crate) fn require_healthy(&self) -> Result<(), EngineError> {
        match &self.terminal_error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn fail_closed_after_branch_operation_error(&mut self, error: &EngineError) {
        self.terminal_error = Some(EngineError::control_plane_unavailable(format!(
            "engine control plane is unavailable after branch operation catalog update failed: {}",
            error.code()
        )));
    }

    pub(crate) fn default_branch(&self) -> &BranchName {
        &self.default_branch
    }

    pub(crate) fn list_branches(&self) -> Vec<BranchCatalogRecord> {
        self.branches
            .values()
            .filter(|record| record.is_active())
            .cloned()
            .collect()
    }

    pub(crate) fn lookup_branch(&self, name: &BranchName) -> Option<&BranchCatalogRecord> {
        self.branches.get(name).filter(|record| record.is_active())
    }

    pub(crate) fn lookup_any_branch(&self, name: &BranchName) -> Option<&BranchCatalogRecord> {
        self.branches.get(name)
    }

    pub(crate) fn contains_branch(&self, name: &BranchName) -> bool {
        self.lookup_branch(name).is_some()
    }

    /// Finds an existing branch (any status) that shares `storage_branch_id` but
    /// has a different name. Distinct product branch names normally derive
    /// distinct storage ids, but a UUID-form name derives its literal bytes
    /// while ordinary names hash to a `UUIDv5`, so two different names can alias
    /// the same storage branch. Detecting the collision lets branch creation
    /// reject it with a structured error instead of failing later with a raw
    /// storage generation conflict (finding U8).
    pub(crate) fn find_aliasing_storage_branch(
        &self,
        name: &BranchName,
        storage_branch_id: strata_core::BranchId,
    ) -> Option<&BranchCatalogRecord> {
        self.branches
            .values()
            .find(|record| record.name() != name && record.storage_branch_id() == storage_branch_id)
    }

    pub(crate) fn active_branch_count(&self) -> usize {
        self.branches
            .values()
            .filter(|record| record.is_active())
            .count()
    }

    pub(crate) fn diagnostics(
        &self,
        persistence: &mut StoragePersistence,
        branch: Option<&BranchName>,
    ) -> ControlDiagnostics {
        if self.terminal_error.is_some() {
            return ControlDiagnostics::new(
                ControlHealthStatus::Unavailable,
                ControlHealthStatus::Unavailable,
                ControlHealthStatus::Unavailable,
                self.default_branch.clone(),
                self.active_branch_count(),
                branch.map(|branch| {
                    SpaceCatalogDiagnostics::new(
                        branch.clone(),
                        ControlHealthStatus::Unavailable,
                        None,
                    )
                }),
            );
        }

        let space_catalog = branch.map(|branch| {
            let Some(record) = self.lookup_branch(branch) else {
                return SpaceCatalogDiagnostics::new(
                    branch.clone(),
                    ControlHealthStatus::Missing,
                    None,
                );
            };
            match super::space::validate_required_space_rows_and_count(persistence, record) {
                Ok(count) => SpaceCatalogDiagnostics::new(
                    branch.clone(),
                    ControlHealthStatus::Healthy,
                    Some(count),
                ),
                Err(error) if error.class() == EngineErrorClass::Corruption => {
                    SpaceCatalogDiagnostics::new(branch.clone(), ControlHealthStatus::Corrupt, None)
                }
                Err(_) => SpaceCatalogDiagnostics::new(
                    branch.clone(),
                    ControlHealthStatus::Unavailable,
                    None,
                ),
            }
        });

        ControlDiagnostics::new(
            ControlHealthStatus::Healthy,
            ControlHealthStatus::Healthy,
            ControlHealthStatus::Healthy,
            self.default_branch.clone(),
            self.active_branch_count(),
            space_catalog,
        )
    }

    pub(crate) fn next_generation_for_name(&self, name: &BranchName) -> u64 {
        self.lookup_any_branch(name)
            .map_or(DEFAULT_BRANCH_GENERATION, |record| record.generation() + 1)
    }

    pub(crate) fn insert_branch(&mut self, record: BranchCatalogRecord) {
        self.branches.insert(record.name().clone(), record);
    }

    pub(crate) fn begin_branch_operation(
        persistence: &mut StoragePersistence,
        record: &BranchCatalogRecord,
        kind: BranchOperationKind,
    ) -> Result<(), EngineError> {
        let names = [record.name().clone()];
        let mutations = vec![
            RowMutation::put(
                control_address(RowClass::BranchControl, branch_pending_index_key()),
                encode_pending_branch_index(&names)?,
            ),
            RowMutation::put(
                control_address(
                    RowClass::BranchControl,
                    branch_pending_key(record.name().as_str()),
                ),
                encode_pending_branch_record(record, kind),
            ),
        ];
        persistence.commit(&CommitPlan::new(SYSTEM_BRANCH_ID, mutations, None))?;
        Ok(())
    }

    pub(crate) fn clear_pending_branch_operation(
        persistence: &mut StoragePersistence,
        record: &BranchCatalogRecord,
    ) -> Result<(), EngineError> {
        let mutations = vec![
            RowMutation::put(
                control_address(RowClass::BranchControl, branch_pending_index_key()),
                encode_pending_branch_index(&[])?,
            ),
            RowMutation::delete(control_address(
                RowClass::BranchControl,
                branch_pending_key(record.name().as_str()),
            )),
        ];
        persistence.commit(&CommitPlan::new(SYSTEM_BRANCH_ID, mutations, None))?;
        Ok(())
    }

    pub(crate) fn persist_branch_record(
        &mut self,
        persistence: &mut StoragePersistence,
        record: BranchCatalogRecord,
    ) -> Result<(), EngineError> {
        if record.is_active() {
            super::space::seed_required_space_rows(persistence, &record)?;
        }

        let mut names: Vec<_> = self.branches.keys().cloned().collect();
        names.push(record.name().clone());
        names.sort();
        names.dedup();

        let mutations = vec![
            RowMutation::put(
                control_address(
                    RowClass::BranchControl,
                    branch_catalog_key(record.name().as_str()),
                ),
                encode_branch_record(&record),
            ),
            RowMutation::put(
                control_address(RowClass::BranchControl, branch_index_key()),
                encode_branch_index(&names)?,
            ),
            RowMutation::put(
                control_address(RowClass::BranchControl, branch_pending_index_key()),
                encode_pending_branch_index(&[])?,
            ),
            RowMutation::delete(control_address(
                RowClass::BranchControl,
                branch_pending_key(record.name().as_str()),
            )),
        ];
        persistence.commit(&CommitPlan::new(SYSTEM_BRANCH_ID, mutations, None))?;
        self.insert_branch(record);
        Ok(())
    }

    pub(crate) fn space_registration_mutations(
        persistence: &StoragePersistence,
        record: &BranchCatalogRecord,
        space: &crate::data::kv::ProductSpace,
    ) -> Result<Vec<RowMutation>, EngineError> {
        super::space::registration_mutations(persistence, record, space)
    }
}

pub(crate) fn bootstrap_or_load(
    persistence: &mut StoragePersistence,
    created: bool,
    requested_default: Option<BranchName>,
) -> Result<ControlPlane, EngineError> {
    if created {
        bootstrap_new_database(
            persistence,
            requested_default.unwrap_or_else(BranchName::default_branch),
        )
    } else {
        load_existing_database(persistence, requested_default.as_ref())
    }
}

fn bootstrap_new_database(
    persistence: &mut StoragePersistence,
    default_branch: BranchName,
) -> Result<ControlPlane, EngineError> {
    persistence.create_system_branch_for_new_database()?;

    let default_record = if default_branch == BranchName::default_branch() {
        BranchCatalogRecord::default_record()
    } else {
        BranchCatalogRecord::root(default_branch, DEFAULT_BRANCH_GENERATION)
    };
    persistence.ensure_branch_created(
        default_record.storage_branch_id(),
        DEFAULT_BRANCH_GENERATION,
    )?;
    let names = [default_record.name().clone()];
    let mutations = vec![
        RowMutation::put(
            control_address(RowClass::DatasetIdentity, database_identity_key()),
            encode_database_identity(&DatabaseIdentityRecord::current()),
        ),
        RowMutation::put(
            control_address(RowClass::DatasetIdentity, local_instance_identity_key()),
            encode_local_instance_identity(&DatabaseIdentityRecord::current()),
        ),
        RowMutation::put(
            control_address(RowClass::Registry, storage_registry_key()),
            encode_storage_registry(),
        ),
        RowMutation::put(
            control_address(RowClass::Registry, capability_registry_key()),
            encode_capability_registry(),
        ),
        RowMutation::put(
            control_address(RowClass::Registry, migration_registry_key()),
            encode_migration_registry(),
        ),
        RowMutation::put(
            control_address(RowClass::BranchControl, branch_index_key()),
            encode_branch_index(&names)?,
        ),
        RowMutation::put(
            control_address(RowClass::BranchControl, branch_default_key()),
            encode_default_branch(default_record.name()),
        ),
        RowMutation::put(
            control_address(RowClass::BranchControl, branch_pending_index_key()),
            encode_pending_branch_index(&[])?,
        ),
        RowMutation::put(
            control_address(
                RowClass::BranchControl,
                branch_catalog_key(default_record.name().as_str()),
            ),
            encode_branch_record(&default_record),
        ),
    ];
    super::space::seed_required_space_rows(persistence, &default_record)?;
    persistence.commit(&CommitPlan::new(SYSTEM_BRANCH_ID, mutations, None))?;

    Ok(ControlPlane {
        default_branch: default_record.name().clone(),
        branches: BTreeMap::from([(default_record.name().clone(), default_record)]),
        terminal_error: None,
    })
}

fn load_existing_database(
    persistence: &mut StoragePersistence,
    requested_default: Option<&BranchName>,
) -> Result<ControlPlane, EngineError> {
    let identity = read_required(
        persistence,
        RowClass::DatasetIdentity,
        database_identity_key(),
    )?;
    decode_database_identity(&identity)?;

    let local_identity = read_required(
        persistence,
        RowClass::DatasetIdentity,
        local_instance_identity_key(),
    )?;
    decode_local_instance_identity(&local_identity)?;

    let registry = read_required(persistence, RowClass::Registry, storage_registry_key())?;
    decode_storage_registry(&registry)?;

    let capabilities = read_required(persistence, RowClass::Registry, capability_registry_key())?;
    decode_capability_registry(&capabilities)?;

    let migrations = read_required(persistence, RowClass::Registry, migration_registry_key())?;
    decode_migration_registry(&migrations)?;

    let default_row = read_required(persistence, RowClass::BranchControl, branch_default_key())?;
    let default_branch = decode_default_branch(&default_row)?;
    if requested_default.is_some_and(|requested| requested != &default_branch) {
        return Err(EngineError::incompatible_layout(
            "failed_precondition.engine.default_branch",
            "requested default branch does not match the persisted database default",
        ));
    }

    let pending = read_required(
        persistence,
        RowClass::BranchControl,
        branch_pending_index_key(),
    )?;
    let pending_names = decode_pending_branch_index(&pending)?;
    if !pending_names.is_empty() {
        recover_pending_branch_operations(persistence, &pending_names)?;
    }

    let branch_index = read_required(persistence, RowClass::BranchControl, branch_index_key())?;
    let branch_names = decode_branch_index(&branch_index)?;
    if branch_names.is_empty() {
        return Err(EngineError::corruption(
            "data_loss.engine.branch_catalog",
            "branch catalog index is empty",
        ));
    }

    let mut branches = BTreeMap::new();
    for name in branch_names {
        if branches.contains_key(&name) {
            return Err(EngineError::corruption(
                "data_loss.engine.branch_catalog",
                "branch catalog index contains a duplicate branch name",
            ));
        }
        let row = read_required(
            persistence,
            RowClass::BranchControl,
            branch_catalog_key(name.as_str()),
        )?;
        let record = decode_branch_record(&row)?;
        if record.name() != &name {
            return Err(EngineError::corruption(
                "data_loss.engine.branch_catalog",
                "branch catalog row name does not match its index entry",
            ));
        }
        if record.is_active() && !persistence.branch_exists(record.storage_branch_id())? {
            return Err(EngineError::corruption(
                "data_loss.engine.branch_catalog",
                "branch catalog references a missing storage branch",
            ));
        }
        if record.is_active() {
            super::space::validate_required_space_rows(persistence, &record)?;
        }
        branches.insert(record.name().clone(), record);
    }

    if !branches
        .get(&default_branch)
        .is_some_and(BranchCatalogRecord::is_active)
    {
        return Err(EngineError::corruption(
            "data_loss.engine.branch_catalog",
            "branch catalog is missing the default branch",
        ));
    }

    Ok(ControlPlane {
        default_branch,
        branches,
        terminal_error: None,
    })
}

fn read_required(
    persistence: &mut StoragePersistence,
    row_class: RowClass,
    key: Vec<u8>,
) -> Result<Vec<u8>, EngineError> {
    match persistence.read(control_address(row_class, key), ReadSelector::Latest) {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(EngineError::corruption(
            "data_loss.engine.control_plane_missing",
            "required control-plane row is missing",
        )),
        Err(error) if error.class() == EngineErrorClass::NotFound => Err(EngineError::corruption(
            "data_loss.engine.control_plane_missing",
            "required control-plane storage branch is missing",
        )),
        Err(error) => Err(error),
    }
}

fn control_address(row_class: RowClass, key: Vec<u8>) -> RowAddress {
    RowAddress::new(SYSTEM_BRANCH_ID, row_class, key)
}

/// Recovers branch operations interrupted between their pending-marker commit
/// and their catalog-activation commit, restoring an openable, consistent state
/// instead of failing the whole database closed (finding F2).
///
/// The branch-operation contract's Publication rule requires recovery to either
/// finish publishing the branch-control rows or clean up the destination
/// storage state. The pending marker records the intended branch; whether that
/// branch is already published (present in the catalog index) distinguishes an
/// interrupted delete from an interrupted create/fork, which the pending record
/// alone cannot (it carries no operation kind — this also resolves the
/// misleading create-specific diagnostic of finding U17).
fn recover_pending_branch_operations(
    persistence: &mut StoragePersistence,
    pending_names: &[BranchName],
) -> Result<(), EngineError> {
    let branch_index = read_required(persistence, RowClass::BranchControl, branch_index_key())?;
    let published: BTreeSet<BranchName> = decode_branch_index(&branch_index)?.into_iter().collect();

    // The protocol clears a pending marker before starting the next operation,
    // so at most one name is normally present; handle any number defensively.
    for name in pending_names {
        let row = read_required(
            persistence,
            RowClass::BranchControl,
            branch_pending_key(name.as_str()),
        )?;
        let (kind, pending) = decode_pending_branch_record(&row)?;
        recover_one_pending_branch_operation(persistence, kind, &pending, &published)?;
    }
    Ok(())
}

fn recover_one_pending_branch_operation(
    persistence: &mut StoragePersistence,
    kind: BranchOperationKind,
    pending: &BranchCatalogRecord,
    published: &BTreeSet<BranchName>,
) -> Result<(), EngineError> {
    // A promotion mutates an already-published, existing target branch, so the
    // create/fork/delete inference below (which keys on published-membership and
    // storage existence) cannot recognise it — it must be routed by its kind.
    if let BranchOperationKind::Promote = kind {
        return recover_pending_promotion(persistence, pending);
    }
    let storage_branch_id = pending.storage_branch_id();
    if published.contains(pending.name()) {
        // Interrupted delete: the branch is still published as active because
        // its catalog-activation commit never ran.
        if persistence.branch_exists(storage_branch_id)? {
            // The storage delete never happened — abandon the delete, leaving
            // the branch active; just clear the marker.
            clear_pending_branch_marker(persistence, pending.name(), None)
        } else {
            // The storage delete completed but the catalog was not updated —
            // finalize it so the catalog no longer references missing storage.
            let deleted = pending.clone().with_storage_facts(
                pending.generation(),
                BranchStatus::Deleted,
                pending.created_at(),
                pending.deleted_at(),
                pending.state_revision(),
            );
            clear_pending_branch_marker(persistence, pending.name(), Some(&deleted))
        }
    } else {
        // Interrupted create or fork: the branch was never published. Roll back
        // the half-created storage branch (if any) so it cannot leak, then clear
        // the marker. The branch never becomes visible.
        if persistence.branch_exists(storage_branch_id)? {
            persistence.delete_branch(storage_branch_id, pending.generation())?;
        }
        clear_pending_branch_marker(persistence, pending.name(), None)
    }
}

/// Reconciles an interrupted promotion (M12D2). The pending record IS the
/// target branch record; its `merge_parent` carries the source facts and, as the
/// placeholder `merged_at`, the target's pre-promote baseline version.
///
/// A promotion commits target data, then publishes the merge edge. Recovery
/// learns which side of that window the crash landed on by comparing the
/// baseline to the target's current timeline head: a higher head means the data
/// commit landed, so the merge edge is finalized with the real commit version
/// (auto complete-forward); an unchanged head means nothing was applied, so the
/// marker is cleared and the target is left as it was (roll back).
fn recover_pending_promotion(
    persistence: &mut StoragePersistence,
    pending: &BranchCatalogRecord,
) -> Result<(), EngineError> {
    let Some(intent) = pending.merge_parent() else {
        // A promotion intent must carry its source lineage; without it there is
        // nothing to finalize. Clear the marker rather than fail recovery.
        return clear_pending_branch_marker(persistence, pending.name(), None);
    };
    let baseline = intent.merged_at();
    let (latest_version, latest_timestamp) =
        persistence.branch_timeline_head(pending.storage_branch_id())?;
    match latest_version {
        Some(latest) if latest > baseline => {
            let completed = pending.clone().with_merge_parent(BranchMergeRecord::new(
                intent.source_name().clone(),
                intent.source_branch_id(),
                intent.source_generation(),
                latest,
                latest_timestamp,
                intent.source_merged_version(),
            ));
            clear_pending_branch_marker(persistence, pending.name(), Some(&completed))
        }
        _ => clear_pending_branch_marker(persistence, pending.name(), None),
    }
}

/// Clears the pending marker for `name` (empty pending index + delete the
/// pending record). When `catalog_record` is provided, it is published in the
/// same atomic commit — used to finalize an interrupted delete.
fn clear_pending_branch_marker(
    persistence: &mut StoragePersistence,
    name: &BranchName,
    catalog_record: Option<&BranchCatalogRecord>,
) -> Result<(), EngineError> {
    let mut mutations = vec![
        RowMutation::put(
            control_address(RowClass::BranchControl, branch_pending_index_key()),
            encode_pending_branch_index(&[])?,
        ),
        RowMutation::delete(control_address(
            RowClass::BranchControl,
            branch_pending_key(name.as_str()),
        )),
    ];
    if let Some(record) = catalog_record {
        mutations.push(RowMutation::put(
            control_address(
                RowClass::BranchControl,
                branch_catalog_key(record.name().as_str()),
            ),
            encode_branch_record(record),
        ));
    }
    persistence.commit(&CommitPlan::new(SYSTEM_BRANCH_ID, mutations, None))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{bootstrap_new_database, control_address, load_existing_database, ControlPlane};
    use crate::api::ControlHealthStatus;
    use crate::branch::catalog::{
        BranchCatalogRecord, BranchMergeRecord, BranchOperationKind, SYSTEM_BRANCH_ID,
    };
    use crate::branch::BranchName;
    use crate::control::records::{
        decode_pending_branch_index, encode_branch_index, encode_branch_record,
        encode_default_branch, encode_migration_registry, encode_reserved_system_space,
    };
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{
        branch_catalog_key, branch_default_key, branch_index_key, branch_pending_index_key,
        capability_registry_key, database_identity_key, local_instance_identity_key,
        migration_registry_key, reserved_space_key, storage_registry_key, CommitPlan,
        PersistenceOpenTarget, ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
    };
    use strata_core::{BranchId, CommitVersion};

    fn pending_names(persistence: &mut StoragePersistence) -> Vec<BranchName> {
        let row = persistence
            .read(
                control_address(RowClass::BranchControl, branch_pending_index_key()),
                ReadSelector::Latest,
            )
            .expect("read pending index")
            .expect("pending index present");
        decode_pending_branch_index(&row).expect("decode pending index")
    }

    /// Publishes an active branch through the control plane so delete-recovery
    /// tests have a real published branch to interrupt.
    fn publish_branch(
        control: &mut ControlPlane,
        persistence: &mut StoragePersistence,
        name: &str,
    ) -> BranchCatalogRecord {
        let record = BranchCatalogRecord::root(BranchName::new(name).expect("valid branch"), 1);
        let outcome = persistence
            .create_branch(record.storage_branch_id(), record.generation())
            .expect("storage branch created");
        let branch = outcome.branch();
        let record = record.with_storage_facts(
            branch.generation(),
            super::BranchStatus::Active,
            branch.created_at(),
            branch.deleted_at(),
            branch.state_revision(),
        );
        control
            .persist_branch_record(persistence, record.clone())
            .expect("branch published");
        record
    }

    #[test]
    fn interrupted_create_recovers_by_clearing_the_pending_marker() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let record =
            BranchCatalogRecord::root(BranchName::new("feature").expect("valid branch"), 1);

        // Crash after the pending marker but before catalog activation, with no
        // storage branch created yet.
        ControlPlane::begin_branch_operation(
            &mut persistence,
            &record,
            BranchOperationKind::CreateOrFork,
        )
        .expect("pending row writes");

        // Recovery un-bricks the database: load succeeds, the branch is absent,
        // and the pending marker is cleared.
        let control =
            load_existing_database(&mut persistence, None).expect("recovery opens the database");
        assert!(control
            .lookup_branch(&BranchName::new("feature").expect("valid branch"))
            .is_none());
        assert!(pending_names(&mut persistence).is_empty());
    }

    #[test]
    fn interrupted_create_rolls_back_an_orphaned_storage_branch() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let record =
            BranchCatalogRecord::root(BranchName::new("feature").expect("valid branch"), 1);

        // Crash after the storage branch was created but before catalog
        // activation.
        ControlPlane::begin_branch_operation(
            &mut persistence,
            &record,
            BranchOperationKind::CreateOrFork,
        )
        .expect("pending row writes");
        persistence
            .create_branch(record.storage_branch_id(), record.generation())
            .expect("storage branch created");
        assert!(persistence
            .branch_exists(record.storage_branch_id())
            .expect("exists check"));

        let control =
            load_existing_database(&mut persistence, None).expect("recovery opens the database");
        assert!(control
            .lookup_branch(&BranchName::new("feature").expect("valid branch"))
            .is_none());
        // The orphaned storage branch was rolled back so it cannot leak.
        assert!(!persistence
            .branch_exists(record.storage_branch_id())
            .expect("exists check"));
        assert!(pending_names(&mut persistence).is_empty());
    }

    #[test]
    fn interrupted_delete_with_present_storage_abandons_the_delete() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let mut control =
            load_existing_database(&mut persistence, None).expect("initial load succeeds");
        let feature = publish_branch(&mut control, &mut persistence, "feature");

        // Crash after the delete's pending marker but before catalog activation;
        // the storage branch was not yet deleted.
        ControlPlane::begin_branch_operation(
            &mut persistence,
            &feature,
            BranchOperationKind::Delete,
        )
        .expect("pending row writes");

        // Recovery abandons the delete: the branch stays active.
        let control =
            load_existing_database(&mut persistence, None).expect("recovery opens the database");
        assert!(control
            .lookup_branch(&BranchName::new("feature").expect("valid branch"))
            .is_some());
        assert!(pending_names(&mut persistence).is_empty());
    }

    #[test]
    fn interrupted_delete_with_missing_storage_finalizes_the_delete() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let mut control =
            load_existing_database(&mut persistence, None).expect("initial load succeeds");
        let feature = publish_branch(&mut control, &mut persistence, "feature");

        // Crash after the storage delete completed but before catalog activation.
        ControlPlane::begin_branch_operation(
            &mut persistence,
            &feature,
            BranchOperationKind::Delete,
        )
        .expect("pending row writes");
        persistence
            .delete_branch(feature.storage_branch_id(), feature.generation())
            .expect("storage branch deleted");

        // Recovery finalizes the delete: load succeeds and the branch is no
        // longer active (rather than dangling against missing storage).
        let control =
            load_existing_database(&mut persistence, None).expect("recovery opens the database");
        assert!(control
            .lookup_branch(&BranchName::new("feature").expect("valid branch"))
            .is_none());
        assert!(pending_names(&mut persistence).is_empty());
    }

    #[test]
    fn missing_database_identity_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(
            &mut persistence,
            RowClass::DatasetIdentity,
            database_identity_key(),
        );

        let error =
            load_existing_database(&mut persistence, None).expect_err("missing identity fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn corrupt_database_identity_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        put_control_row(
            &mut persistence,
            RowClass::DatasetIdentity,
            database_identity_key(),
            vec![0xff],
        );

        let error =
            load_existing_database(&mut persistence, None).expect_err("corrupt identity fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane");
    }

    #[test]
    fn missing_local_instance_identity_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(
            &mut persistence,
            RowClass::DatasetIdentity,
            local_instance_identity_key(),
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("missing local identity fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn corrupt_local_instance_identity_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        put_control_row(
            &mut persistence,
            RowClass::DatasetIdentity,
            local_instance_identity_key(),
            vec![0xff],
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("corrupt local identity fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane");
    }

    #[test]
    fn missing_storage_registry_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(&mut persistence, RowClass::Registry, storage_registry_key());

        let error =
            load_existing_database(&mut persistence, None).expect_err("missing registry fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn corrupt_storage_registry_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        put_control_row(
            &mut persistence,
            RowClass::Registry,
            storage_registry_key(),
            vec![0xff],
        );

        let error =
            load_existing_database(&mut persistence, None).expect_err("corrupt registry fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane");
    }

    #[test]
    fn missing_capability_registry_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(
            &mut persistence,
            RowClass::Registry,
            capability_registry_key(),
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("missing capability registry fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn missing_migration_registry_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(
            &mut persistence,
            RowClass::Registry,
            migration_registry_key(),
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("missing migration registry fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn unsupported_migration_registry_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let mut migration = encode_migration_registry();
        let offset = b"strata.engine.migrations".len() + 2;
        migration[offset..offset + 2].copy_from_slice(&2_u16.to_be_bytes());
        put_control_row(
            &mut persistence,
            RowClass::Registry,
            migration_registry_key(),
            migration,
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("unsupported migration registry fails");
        assert_eq!(error.class(), EngineErrorClass::IncompatibleLayout);
        assert_eq!(
            error.code(),
            "failed_precondition.engine.migration_registry"
        );
    }

    #[test]
    fn missing_default_branch_row_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(
            &mut persistence,
            RowClass::BranchControl,
            branch_default_key(),
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("missing default branch row fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn missing_default_branch_catalog_record_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        delete_control_row(
            &mut persistence,
            RowClass::BranchControl,
            branch_catalog_key(BranchName::default_branch().as_str()),
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("missing default branch catalog fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane_missing");
    }

    #[test]
    fn default_branch_missing_from_branch_index_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let feature = BranchName::new("feature").expect("valid branch");
        put_control_row(
            &mut persistence,
            RowClass::BranchControl,
            branch_default_key(),
            encode_default_branch(&feature),
        );

        let error = load_existing_database(&mut persistence, None)
            .expect_err("missing default branch catalog entry fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.branch_catalog");
    }

    #[test]
    fn diagnostics_report_corrupt_space_catalog_without_exposing_rows() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let control = bootstrap_new_database(&mut persistence, BranchName::default_branch())
            .expect("bootstrap succeeds");
        let mut reserved = encode_reserved_system_space();
        *reserved.last_mut().expect("reserved flag exists") = 1;
        let default_record = control
            .lookup_branch(&BranchName::default_branch())
            .expect("default branch loaded")
            .clone();
        persistence
            .commit(&CommitPlan::new(
                default_record.storage_branch_id(),
                vec![RowMutation::put(
                    crate::persistence::RowAddress::new(
                        default_record.storage_branch_id(),
                        RowClass::SpaceControl,
                        reserved_space_key(crate::control::space::SYSTEM_SPACE),
                    ),
                    reserved,
                )],
                Some(default_record.generation()),
            ))
            .expect("reserved space row corrupts");

        let diagnostics =
            control.diagnostics(&mut persistence, Some(&BranchName::default_branch()));
        let space = diagnostics
            .space_catalog()
            .expect("space diagnostics present");
        assert_eq!(space.status(), ControlHealthStatus::Corrupt);
        assert_eq!(space.space_count(), None);
    }

    #[test]
    fn corrupt_branch_catalog_row_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        persistence
            .commit(&CommitPlan::new(
                SYSTEM_BRANCH_ID,
                vec![RowMutation::put(
                    control_address(
                        RowClass::BranchControl,
                        branch_catalog_key(BranchName::default_branch().as_str()),
                    ),
                    vec![0xff],
                )],
                None,
            ))
            .expect("corrupt catalog row writes");

        let error =
            load_existing_database(&mut persistence, None).expect_err("corrupt catalog row fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.control_plane");
    }

    #[test]
    fn duplicate_branch_index_entries_fail_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let default = BranchName::new("default").expect("valid branch");
        let names = [default.clone(), default];
        persistence
            .commit(&CommitPlan::new(
                SYSTEM_BRANCH_ID,
                vec![RowMutation::put(
                    control_address(RowClass::BranchControl, branch_index_key()),
                    encode_branch_index(&names).expect("index encodes"),
                )],
                None,
            ))
            .expect("corrupt index writes");

        let error =
            load_existing_database(&mut persistence, None).expect_err("duplicate index fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.branch_catalog");
    }

    #[test]
    fn branch_index_record_name_mismatch_fails_closed_on_load() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let default = BranchCatalogRecord::default_record();
        let feature = BranchName::new("feature").expect("valid branch");
        let names = [default.name().clone(), feature.clone()];
        persistence
            .commit(&CommitPlan::new(
                SYSTEM_BRANCH_ID,
                vec![
                    RowMutation::put(
                        control_address(RowClass::BranchControl, branch_index_key()),
                        encode_branch_index(&names).expect("index encodes"),
                    ),
                    RowMutation::put(
                        control_address(
                            RowClass::BranchControl,
                            branch_catalog_key(feature.as_str()),
                        ),
                        encode_branch_record(&default),
                    ),
                ],
                None,
            ))
            .expect("corrupt catalog writes");

        let error = load_existing_database(&mut persistence, None).expect_err("mismatch fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.branch_catalog");
    }

    fn bootstrap_default(persistence: &mut StoragePersistence) {
        bootstrap_new_database(persistence, BranchName::default_branch())
            .expect("bootstrap succeeds");
    }

    fn delete_control_row(persistence: &mut StoragePersistence, row_class: RowClass, key: Vec<u8>) {
        persistence
            .commit(&CommitPlan::new(
                SYSTEM_BRANCH_ID,
                vec![RowMutation::delete(control_address(row_class, key))],
                None,
            ))
            .expect("control row delete writes");
    }

    fn put_control_row(
        persistence: &mut StoragePersistence,
        row_class: RowClass,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        persistence
            .commit(&CommitPlan::new(
                SYSTEM_BRANCH_ID,
                vec![RowMutation::put(control_address(row_class, key), value)],
                None,
            ))
            .expect("control row put writes");
    }

    /// Commits one KV row to a branch's storage timeline, returning the version,
    /// so promotion-recovery tests can move the target's timeline head.
    fn commit_target_data(
        persistence: &mut StoragePersistence,
        target: &BranchCatalogRecord,
        key: &[u8],
        value: &[u8],
    ) -> CommitVersion {
        persistence
            .commit(&CommitPlan::new(
                target.storage_branch_id(),
                vec![RowMutation::put(
                    RowAddress::new(target.storage_branch_id(), RowClass::Kv, key.to_vec()),
                    value.to_vec(),
                )],
                None,
            ))
            .expect("target data commits")
            .version()
    }

    /// Writes a Promote intent for `target` carrying the source lineage and the
    /// given baseline (the pre-promote timeline head), as `promote` does.
    fn begin_promote_intent(
        persistence: &mut StoragePersistence,
        target: &BranchCatalogRecord,
        baseline: CommitVersion,
    ) {
        let intent = target.clone().with_merge_parent(BranchMergeRecord::new(
            BranchName::new("feature").expect("valid branch"),
            BranchId::from_bytes([0x2a; BranchId::BYTE_LEN]),
            1,
            baseline,
            None,
            Some(baseline),
        ));
        ControlPlane::begin_branch_operation(persistence, &intent, BranchOperationKind::Promote)
            .expect("promote intent writes");
    }

    #[test]
    fn interrupted_promotion_with_committed_data_finalizes_the_merge_edge() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let control =
            load_existing_database(&mut persistence, None).expect("initial load succeeds");
        let target = control
            .lookup_branch(&BranchName::new("default").expect("valid branch"))
            .cloned()
            .expect("default branch");

        // Baseline: the target's timeline head before the promotion.
        let baseline = commit_target_data(&mut persistence, &target, b"k", b"v0");
        begin_promote_intent(&mut persistence, &target, baseline);
        // The data commit landed (head advances past the baseline) before a crash.
        let merged = commit_target_data(&mut persistence, &target, b"k", b"v1");
        assert!(merged > baseline);

        // Recovery finalizes the merge edge with the real committed version.
        let control =
            load_existing_database(&mut persistence, None).expect("recovery opens the database");
        let recovered = control
            .lookup_branch(&BranchName::new("default").expect("valid branch"))
            .expect("default branch");
        let edge = recovered
            .merge_parent()
            .expect("recovery finalized the merge edge");
        assert_eq!(edge.source_name().as_str(), "feature");
        assert_eq!(edge.merged_at(), merged);
        // The source frontier recorded in the intent survives finalization.
        assert_eq!(edge.source_merged_version(), Some(baseline));
        assert!(pending_names(&mut persistence).is_empty());
    }

    #[test]
    fn interrupted_promotion_without_committed_data_rolls_back() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        bootstrap_default(&mut persistence);
        let control =
            load_existing_database(&mut persistence, None).expect("initial load succeeds");
        let target = control
            .lookup_branch(&BranchName::new("default").expect("valid branch"))
            .cloned()
            .expect("default branch");

        // Baseline is a real version; the data commit never lands, so the head
        // stays equal to the baseline (exercises the `latest > baseline` boundary).
        let baseline = commit_target_data(&mut persistence, &target, b"k", b"v0");
        begin_promote_intent(&mut persistence, &target, baseline);

        let control =
            load_existing_database(&mut persistence, None).expect("recovery opens the database");
        let recovered = control
            .lookup_branch(&BranchName::new("default").expect("valid branch"))
            .expect("default branch");
        assert!(
            recovered.merge_parent().is_none(),
            "an unpromoted target must not record a merge edge"
        );
        assert!(pending_names(&mut persistence).is_empty());
    }
}
