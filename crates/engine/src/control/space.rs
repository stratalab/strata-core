//! Branch-local core space control rows.

use strata_core::BranchId;

use crate::branch::catalog::BranchCatalogRecord;
use crate::commit::CommitOutcome;
use crate::data::kv::ProductSpace;
use crate::diagnostics::{EngineError, EngineErrorClass};
use crate::persistence::{
    decode_vector_index_manifest_key, encode_event_meta_space_prefix, encode_event_space_prefix,
    encode_event_type_index_space_prefix, encode_graph_binding_space_prefix,
    encode_graph_edge_space_prefix, encode_graph_metadata_prefix, encode_graph_node_space_prefix,
    encode_graph_ontology_space_prefix, encode_graph_reverse_edge_space_prefix,
    encode_graph_type_index_space_prefix, encode_json_index_entry_space_prefix,
    encode_json_index_meta_prefix, encode_json_space_prefix, encode_kv_space_prefix,
    encode_vector_collection_prefix, encode_vector_space_prefix, reserved_space_key,
    space_catalog_key, space_index_key, vector_index_manifest_prefix, CommitPlan, ReadSelector,
    RowAddress, RowClass, RowMutation, StoragePersistence,
};

use super::records::{
    decode_reserved_system_space, decode_space_catalog_record, decode_space_index,
    encode_reserved_system_space, encode_space_index, encode_space_record,
    encode_space_record_deleting,
};

/// Rows tombstoned per sweep commit of a chunked deletion (#3477 graphs,
/// #3574 spaces): half the storage layer's default per-commit mutation
/// budget (4096), so a chunk fits beside whatever else a commit carries.
pub(crate) const DELETE_CHUNK_ROWS: usize = 2048;

pub(crate) const DEFAULT_SPACE: &str = "default";
pub(crate) const SYSTEM_SPACE: &str = "_system_";

pub(crate) fn seed_required_space_rows(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
) -> Result<(), EngineError> {
    let mutations = required_space_mutations(persistence, record)?;
    if mutations.is_empty() {
        return Ok(());
    }
    persistence.commit(&CommitPlan::new(
        record.storage_branch_id(),
        mutations,
        Some(record.generation()),
    ))?;
    Ok(())
}

pub(crate) fn registration_mutations(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<Vec<RowMutation>, EngineError> {
    let mut spaces = read_required_space_index(persistence, record)?;
    if spaces.iter().any(|existing| existing == space) {
        validate_space_catalog_row(persistence, record, space)?;
        return Ok(Vec::new());
    }
    // #3574: a name whose last deletion was interrupted still owns rows
    // behind a marked catalog row. Finish that sweep before registering, so
    // no row of the old space surfaces under the new one.
    finish_pending_deletion(persistence, record, space)?;

    spaces.push(space.clone());
    spaces.sort();
    spaces.dedup();
    Ok(vec![
        RowMutation::put(
            space_address(record, space_index_key()),
            encode_space_index(&spaces)?,
        ),
        RowMutation::put(
            space_address(record, space_catalog_key(space.as_str())),
            encode_space_record(space),
        ),
    ])
}

/// Reconciles `record`'s space index toward a source branch in one atomic batch:
/// registers every `to_add` space it lacks and tombstones every `to_remove` space
/// it still has, rewriting the index a single time (so neither direction clobbers
/// the other) plus one catalog row put per addition and one delete per removal.
/// `to_add` and `to_remove` are disjoint by construction on the promotion path.
/// Removing the default space is rejected as corruption; returns an empty vec
/// when nothing changes.
pub(crate) fn registration_and_deletion_mutations(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
    to_add: &[ProductSpace],
    to_remove: &[ProductSpace],
) -> Result<Vec<RowMutation>, EngineError> {
    let mut spaces = read_required_space_index(persistence, record)?;
    let added: Vec<ProductSpace> = to_add
        .iter()
        .filter(|candidate| !spaces.iter().any(|existing| existing == *candidate))
        .cloned()
        .collect();
    let removed: Vec<ProductSpace> = to_remove
        .iter()
        .filter(|candidate| spaces.iter().any(|existing| existing == *candidate))
        .cloned()
        .collect();
    if added.is_empty() && removed.is_empty() {
        return Ok(Vec::new());
    }
    // Additions and removals are disjoint by construction (a removed space is
    // absent from the source, an added one present), so a single index rewrite
    // carries both without one clobbering the other.
    if removed.iter().any(|space| space.as_str() == DEFAULT_SPACE) {
        return Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "space deletion would remove the default space",
        ));
    }
    spaces.extend(added.iter().cloned());
    spaces.retain(|existing| !removed.iter().any(|space| space == existing));
    spaces.sort();
    spaces.dedup();
    let mut mutations = vec![RowMutation::put(
        space_address(record, space_index_key()),
        encode_space_index(&spaces)?,
    )];
    for space in &added {
        mutations.push(RowMutation::put(
            space_address(record, space_catalog_key(space.as_str())),
            encode_space_record(space),
        ));
    }
    for space in &removed {
        mutations.push(RowMutation::delete(space_address(
            record,
            space_catalog_key(space.as_str()),
        )));
    }
    Ok(mutations)
}

pub(crate) fn registered_spaces(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
) -> Result<Vec<ProductSpace>, EngineError> {
    let spaces = read_required_space_index(persistence, record)?;
    for space in &spaces {
        validate_space_catalog_row(persistence, record, space)?;
    }
    Ok(spaces)
}

/// Reads the registered space index of `storage_branch` at `selector` — the base
/// leg of a promotion's space three-way. Returns an empty set when no index row
/// exists at that version (a branch point predating any space carries none), so a
/// spaceless base is simply "nothing to reconcile"; a genuinely missing base
/// branch (never expected — the base is always an ancestor) surfaces as an error.
pub(crate) fn read_space_index_at(
    persistence: &mut StoragePersistence,
    storage_branch: BranchId,
    selector: ReadSelector,
) -> Result<Vec<ProductSpace>, EngineError> {
    let address = RowAddress::new(storage_branch, RowClass::SpaceControl, space_index_key());
    match persistence.read(address, selector)? {
        Some(bytes) => decode_space_index(&bytes),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn space_exists(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<bool, EngineError> {
    Ok(registered_spaces(persistence, record)?
        .iter()
        .any(|existing| existing == space))
}

/// The space index without `space`, every listed row validated first, or
/// `None` when `space` is not registered. Refuses to drop the default space.
pub(crate) fn index_without(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<Option<Vec<ProductSpace>>, EngineError> {
    let mut spaces = read_required_space_index(persistence, record)?;
    for existing in &spaces {
        validate_space_catalog_row(persistence, record, existing)?;
    }
    let Some(position) = spaces.iter().position(|existing| existing == space) else {
        return Ok(None);
    };
    spaces.remove(position);
    if !spaces
        .iter()
        .any(|existing| existing.as_str() == DEFAULT_SPACE)
    {
        return Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "space deletion would remove the default space",
        ));
    }
    Ok(Some(spaces))
}

/// The two catalog mutations that unregister `space`: the index rewritten
/// as `remaining`, and its catalog row either tombstoned (a deletion that
/// fits one commit) or, with `marked`, rewritten with the deleting mark
/// (#3574) so the rows can be swept afterwards and the row tombstoned last.
pub(crate) fn unregister_mutations(
    record: &BranchCatalogRecord,
    space: &ProductSpace,
    remaining: &[ProductSpace],
    marked: bool,
) -> Result<Vec<RowMutation>, EngineError> {
    let catalog = space_address(record, space_catalog_key(space.as_str()));
    let catalog_row = if marked {
        RowMutation::put(catalog, encode_space_record_deleting(space))
    } else {
        RowMutation::delete(catalog)
    };
    Ok(vec![
        RowMutation::put(
            space_address(record, space_index_key()),
            encode_space_index(remaining)?,
        ),
        catalog_row,
    ])
}

/// Every data row `space` owns, as tombstones, in the order a sweep removes
/// them (#3574): the rows another capability's readers gate on first (graph
/// metadata, vector collections), then the rest. Rows already tombstoned
/// are not seen, so the list is exactly what remains — which is what makes
/// a sweep resumable.
pub(crate) fn space_row_tombstones(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<Vec<RowMutation>, EngineError> {
    let branch_id = record.storage_branch_id();
    let mut mutations = Vec::new();
    for (row_class, prefix) in data_delete_prefixes(space) {
        let rows =
            persistence.scan_prefix(branch_id, row_class, prefix, ReadSelector::Latest, None)?;
        for row in rows.into_iter().filter(|row| !row.is_tombstone()) {
            mutations.push(RowMutation::delete(RowAddress::new(
                branch_id,
                row_class,
                row.key().to_vec(),
            )));
        }
    }
    let rows = persistence.scan_prefix(
        branch_id,
        RowClass::SpaceControl,
        vector_index_manifest_prefix(),
        ReadSelector::Latest,
        None,
    )?;
    for row in rows.into_iter().filter(|row| !row.is_tombstone()) {
        let Ok((manifest_space, _collection)) = decode_vector_index_manifest_key(row.key()) else {
            continue;
        };
        if &manifest_space == space {
            mutations.push(RowMutation::delete(RowAddress::new(
                branch_id,
                RowClass::SpaceControl,
                row.key().to_vec(),
            )));
        }
    }
    Ok(mutations)
}

fn data_delete_prefixes(space: &ProductSpace) -> Vec<(RowClass, Vec<u8>)> {
    vec![
        (RowClass::GraphMetadata, encode_graph_metadata_prefix(space)),
        (
            RowClass::VectorCollection,
            encode_vector_collection_prefix(space),
        ),
        (RowClass::Kv, encode_kv_space_prefix(space)),
        (RowClass::Json, encode_json_space_prefix(space)),
        (RowClass::JsonIndex, encode_json_index_meta_prefix(space)),
        (
            RowClass::JsonIndex,
            encode_json_index_entry_space_prefix(space),
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

/// Tombstones `rows` in commits of at most [`DELETE_CHUNK_ROWS`], then the
/// marked catalog row in a commit of its own, so the row outlives every row
/// it describes and a crash at any point leaves a resumable deletion, never
/// an orphaned one. The returned outcome is the final commit carrying the
/// data-row count of the whole sweep, so the acknowledgement reads like a
/// single-commit deletion's.
pub(crate) fn sweep_space_rows(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
    mut rows: Vec<RowMutation>,
) -> Result<CommitOutcome, EngineError> {
    let deleted = rows.len();
    while !rows.is_empty() {
        let rest = rows.split_off(rows.len().min(DELETE_CHUNK_ROWS));
        commit_rows(persistence, record, rows)?;
        rows = rest;
    }
    let last = commit_rows(
        persistence,
        record,
        vec![RowMutation::delete(space_address(
            record,
            space_catalog_key(space.as_str()),
        ))],
    )?;
    Ok(last.with_counts(0, deleted))
}

/// Finishes the deletion of a space whose catalog row carries the mark
/// (#3574): whatever rows remain, then the row. `None` when no deletion is
/// pending — the row is absent, or registered and unmarked.
pub(crate) fn finish_pending_deletion(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<Option<CommitOutcome>, EngineError> {
    if !pending_deletion(persistence, record, space)? {
        return Ok(None);
    }
    let rows = space_row_tombstones(persistence, record, space)?;
    Ok(Some(sweep_space_rows(persistence, record, space, rows)?))
}

/// The spaces among `candidates` whose catalog row on `record` carries the
/// deleting mark (#3574): names an interrupted deletion still owns rows
/// under. Read-only — a promotion plans with this and sweeps only once it
/// has decided to commit, so a refused promotion or a preview writes nothing.
pub(crate) fn pending_deletions_among(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    candidates: &[ProductSpace],
) -> Result<Vec<ProductSpace>, EngineError> {
    let mut pending = Vec::new();
    for space in candidates {
        if pending_deletion(persistence, record, space)? {
            pending.push(space.clone());
        }
    }
    Ok(pending)
}

/// Whether `space`'s catalog row carries the deleting mark.
fn pending_deletion(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<bool, EngineError> {
    let address = space_address(record, space_catalog_key(space.as_str()));
    match persistence.read(address, ReadSelector::Latest)? {
        Some(bytes) => Ok(decode_space_catalog_record(&bytes)?.deleting()),
        None => Ok(false),
    }
}

fn commit_rows(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    mutations: Vec<RowMutation>,
) -> Result<CommitOutcome, EngineError> {
    persistence.commit(&CommitPlan::new(
        record.storage_branch_id(),
        mutations,
        Some(record.generation()),
    ))
}

pub(crate) fn validate_required_space_rows(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
) -> Result<(), EngineError> {
    validate_required_space_rows_and_count(persistence, record).map(|_| ())
}

pub(crate) fn validate_required_space_rows_and_count(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
) -> Result<usize, EngineError> {
    let spaces = read_required_space_index(persistence, record)?;
    let default = default_space()?;
    if !spaces.iter().any(|space| space == &default) {
        return Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "space catalog is missing the default space",
        ));
    }
    for space in &spaces {
        validate_space_catalog_row(persistence, record, space)?;
    }
    let reserved = read_required(
        persistence,
        &space_address(record, reserved_space_key(SYSTEM_SPACE)),
    )?;
    decode_reserved_system_space(&reserved)?;
    Ok(spaces.len())
}

fn required_space_mutations(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
) -> Result<Vec<RowMutation>, EngineError> {
    let default = default_space()?;
    let mut spaces = match persistence.read(
        space_address(record, space_index_key()),
        ReadSelector::Latest,
    )? {
        Some(bytes) => decode_space_index(&bytes)?,
        None => Vec::new(),
    };
    if !spaces.iter().any(|space| space == &default) {
        spaces.push(default.clone());
    }
    spaces.sort();
    spaces.dedup();
    let mutations = vec![
        RowMutation::put(
            space_address(record, space_index_key()),
            encode_space_index(&spaces)?,
        ),
        RowMutation::put(
            space_address(record, space_catalog_key(default.as_str())),
            encode_space_record(&default),
        ),
        RowMutation::put(
            space_address(record, reserved_space_key(SYSTEM_SPACE)),
            encode_reserved_system_space(),
        ),
    ];
    Ok(mutations)
}

fn read_required_space_index(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
) -> Result<Vec<ProductSpace>, EngineError> {
    let bytes = read_required(persistence, &space_address(record, space_index_key()))?;
    decode_space_index(&bytes)
}

fn validate_space_catalog_row(
    persistence: &StoragePersistence,
    record: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<(), EngineError> {
    let bytes = read_required(
        persistence,
        &space_address(record, space_catalog_key(space.as_str())),
    )?;
    let decoded = decode_space_catalog_record(&bytes)?;
    if decoded.space() != space {
        return Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "space catalog row name does not match its index entry",
        ));
    }
    if decoded.deleting() {
        return Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "space catalog row is marked deleting but still listed in the index",
        ));
    }
    Ok(())
}

fn read_required(
    persistence: &StoragePersistence,
    address: &RowAddress,
) -> Result<Vec<u8>, EngineError> {
    match persistence.read(address.clone(), ReadSelector::Latest) {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "required branch-local space control row is missing",
        )),
        Err(error) if error.class() == EngineErrorClass::NotFound => Err(EngineError::corruption(
            "data_loss.engine.space_catalog",
            "branch-local space control storage branch is missing",
        )),
        Err(error) => Err(error),
    }
}

fn default_space() -> Result<ProductSpace, EngineError> {
    ProductSpace::new(DEFAULT_SPACE)
}

fn space_address(record: &BranchCatalogRecord, key: Vec<u8>) -> RowAddress {
    RowAddress::new(record.storage_branch_id(), RowClass::SpaceControl, key)
}

#[cfg(test)]
mod tests {
    use super::{
        read_space_index_at, registered_spaces, registration_and_deletion_mutations,
        registration_mutations, seed_required_space_rows, validate_required_space_rows,
        DEFAULT_SPACE, SYSTEM_SPACE,
    };
    use crate::branch::catalog::BranchCatalogRecord;
    use crate::branch::BranchName;
    use crate::control::records::{
        encode_reserved_system_space, encode_space_index, encode_space_record,
    };
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{
        reserved_space_key, space_catalog_key, space_index_key, CommitPlan, PersistenceOpenTarget,
        ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
    };

    #[test]
    fn required_space_rows_seed_and_validate() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");

        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        validate_required_space_rows(&mut persistence, &record).expect("space rows validate");
    }

    #[test]
    fn registration_mutations_add_user_space_once() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let tenant = ProductSpace::new("tenant").expect("valid space");

        let mutations = registration_mutations(&persistence, &record, &tenant)
            .expect("tenant registration mutations");
        assert_eq!(mutations.len(), 2);

        persistence
            .commit(&crate::persistence::CommitPlan::new(
                record.storage_branch_id(),
                mutations,
                Some(record.generation()),
            ))
            .expect("space registration commits");
        let mutations = registration_mutations(&persistence, &record, &tenant)
            .expect("tenant already registered");
        assert!(mutations.is_empty());
    }

    #[test]
    fn missing_required_space_rows_fail_closed() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");

        let error = validate_required_space_rows(&mut persistence, &record)
            .expect_err("missing space rows fail");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    #[test]
    fn missing_registered_space_catalog_row_fails_closed() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let tenant = ProductSpace::new("tenant").expect("valid space");
        let mutations = registration_mutations(&persistence, &record, &tenant)
            .expect("tenant registration mutations");
        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                mutations,
                Some(record.generation()),
            ))
            .expect("space registration commits");

        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                vec![RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::SpaceControl,
                    space_catalog_key(tenant.as_str()),
                ))],
                Some(record.generation()),
            ))
            .expect("space catalog row deletes");

        let error = validate_required_space_rows(&mut persistence, &record)
            .expect_err("missing registered space row fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    #[test]
    fn mismatched_registered_space_catalog_row_fails_closed() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let tenant = ProductSpace::new("tenant").expect("valid space");
        let other = ProductSpace::new("other").expect("valid space");
        let mutations = registration_mutations(&persistence, &record, &tenant)
            .expect("tenant registration mutations");
        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                mutations,
                Some(record.generation()),
            ))
            .expect("space registration commits");

        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                vec![RowMutation::put(
                    RowAddress::new(
                        record.storage_branch_id(),
                        RowClass::SpaceControl,
                        space_catalog_key(tenant.as_str()),
                    ),
                    encode_space_record(&other),
                )],
                Some(record.generation()),
            ))
            .expect("space catalog row corrupts");

        let error = validate_required_space_rows(&mut persistence, &record)
            .expect_err("mismatched registered space row fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    #[test]
    fn missing_default_space_index_entry_fails_closed() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");

        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                vec![RowMutation::put(
                    RowAddress::new(
                        record.storage_branch_id(),
                        RowClass::SpaceControl,
                        space_index_key(),
                    ),
                    encode_space_index(&[]).expect("empty index encodes"),
                )],
                Some(record.generation()),
            ))
            .expect("space index corrupts");

        let error = validate_required_space_rows(&mut persistence, &record)
            .expect_err("missing default space fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    #[test]
    fn missing_reserved_system_space_fact_fails_closed() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");

        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                vec![RowMutation::delete(RowAddress::new(
                    record.storage_branch_id(),
                    RowClass::SpaceControl,
                    reserved_space_key(SYSTEM_SPACE),
                ))],
                Some(record.generation()),
            ))
            .expect("reserved space row deletes");

        let error = validate_required_space_rows(&mut persistence, &record)
            .expect_err("missing reserved space fact fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    #[test]
    fn user_managed_reserved_system_space_fact_fails_closed() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let mut reserved = encode_reserved_system_space();
        *reserved.last_mut().expect("reserved flag exists") = 1;

        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                vec![RowMutation::put(
                    RowAddress::new(
                        record.storage_branch_id(),
                        RowClass::SpaceControl,
                        reserved_space_key(SYSTEM_SPACE),
                    ),
                    reserved,
                )],
                Some(record.generation()),
            ))
            .expect("reserved space row corrupts");

        let error = validate_required_space_rows(&mut persistence, &record)
            .expect_err("user-managed reserved space fact fails");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    #[test]
    fn reserved_space_names_are_documented_constants() {
        assert_eq!(DEFAULT_SPACE, "default");
        assert_eq!(SYSTEM_SPACE, "_system_");
    }

    #[test]
    fn registration_and_deletion_reconciles_adds_and_removes_in_one_index() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let keep = ProductSpace::new("keep").expect("valid space");
        let drop = ProductSpace::new("drop").expect("valid space");
        let add = ProductSpace::new("add").expect("valid space");
        // Register `keep` and `drop` up front.
        let seed = registration_and_deletion_mutations(
            &mut persistence,
            &record,
            &[keep.clone(), drop.clone()],
            &[],
        )
        .expect("seed spaces");
        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                seed,
                Some(record.generation()),
            ))
            .expect("seed commits");

        // Add `add` and remove `drop` in one reconciliation. A correct final
        // index proves the single rewrite carried both directions (two clobbering
        // rewrites would lose one).
        let mutations = registration_and_deletion_mutations(
            &mut persistence,
            &record,
            std::slice::from_ref(&add),
            std::slice::from_ref(&drop),
        )
        .expect("reconcile");
        persistence
            .commit(&CommitPlan::new(
                record.storage_branch_id(),
                mutations,
                Some(record.generation()),
            ))
            .expect("reconcile commits");

        let spaces = registered_spaces(&mut persistence, &record).expect("read spaces");
        assert!(spaces.iter().any(|s| s == &add), "add is registered");
        assert!(spaces.iter().any(|s| s == &keep), "keep is retained");
        assert!(!spaces.iter().any(|s| s == &drop), "drop is removed");
        assert!(
            spaces.iter().any(|s| s.as_str() == DEFAULT_SPACE),
            "default is retained"
        );
    }

    #[test]
    fn registration_and_deletion_is_a_noop_when_nothing_changes() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let absent = ProductSpace::new("absent").expect("valid space");
        // Adding an already-present default and removing a space the target never
        // had produces no mutations.
        let default = ProductSpace::new(DEFAULT_SPACE).expect("valid space");
        let mutations =
            registration_and_deletion_mutations(&mut persistence, &record, &[default], &[absent])
                .expect("reconcile");
        assert!(mutations.is_empty());
    }

    #[test]
    fn read_space_index_at_reads_seeded_spaces_and_empty_when_absent() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        // A branch with no seeded index reads back as empty (no index row).
        let empty = read_space_index_at(
            &mut persistence,
            record.storage_branch_id(),
            ReadSelector::Latest,
        )
        .expect("read empty index");
        assert!(
            empty.is_empty(),
            "an unseeded branch has no registered spaces"
        );

        // After seeding, the index reads back with the default space.
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let seeded = read_space_index_at(
            &mut persistence,
            record.storage_branch_id(),
            ReadSelector::Latest,
        )
        .expect("read seeded index");
        assert!(
            seeded.iter().any(|s| s.as_str() == DEFAULT_SPACE),
            "the seeded index contains the default space"
        );
    }

    #[test]
    fn registration_and_deletion_refuses_to_remove_the_default_space() {
        let (mut persistence, _) =
            StoragePersistence::open(PersistenceOpenTarget::Cache).expect("cache opens");
        let record = create_branch(&mut persistence, "space-test");
        seed_required_space_rows(&mut persistence, &record).expect("space rows seed");
        let default = ProductSpace::new(DEFAULT_SPACE).expect("valid space");
        let error = registration_and_deletion_mutations(&mut persistence, &record, &[], &[default])
            .expect_err("removing the default space is refused");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.space_catalog");
    }

    fn create_branch(persistence: &mut StoragePersistence, name: &str) -> BranchCatalogRecord {
        let record = BranchCatalogRecord::root(BranchName::new(name).expect("valid branch"), 1);
        persistence
            .create_branch(record.storage_branch_id(), record.generation())
            .expect("storage branch created");
        record
    }
}
