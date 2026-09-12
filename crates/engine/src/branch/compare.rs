//! Branch comparison — the engine workflow behind `BranchService::compare`.
//!
//! Compare is a read-only workflow that diffs two branches' authored entities,
//! grouped by capability and space (contract §Compare And Preview, conformance
//! #4). It owns the orchestration the capability adapters deliberately do not:
//! it enumerates each branch's rows through persistence at a read selector and
//! feeds them to the adapters, which interpret them into comparable entities.
//! Derived rows are omitted by default (only `Authored` capabilities compare).

use std::collections::BTreeMap;

use crate::api::{BranchComparison, BranchStateSelector, ComparedEntity, SpaceComparison};
use crate::branch::adapter::{CapabilityBranchAdapter, ComparableEntity, DerivedDisposition};
use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::preview::capability_adapters;
use crate::control::space::registered_spaces;
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{ReadSelector, StoragePersistence};

/// Maps the public branch-state selector to the internal storage read selector.
const fn read_selector(selector: BranchStateSelector) -> ReadSelector {
    match selector {
        BranchStateSelector::Current => ReadSelector::Latest,
        BranchStateSelector::AtTimestamp(timestamp) => ReadSelector::AtTimestamp(timestamp),
    }
}

/// The present (non-tombstone) entities of one capability in one space at the
/// selected branch state, keyed by identity for set difference.
fn present_entities(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
    adapter: &dyn CapabilityBranchAdapter,
    space: &ProductSpace,
    selector: ReadSelector,
) -> Result<BTreeMap<Vec<u8>, ComparableEntity>, EngineError> {
    let rows = persistence.scan_prefix(
        record.storage_branch_id(),
        adapter.row_class(),
        adapter.space_prefix(space),
        selector,
        None,
    )?;
    let mut present = BTreeMap::new();
    for row in &rows {
        let entity = adapter.interpret_row(space, row)?;
        if entity.is_tombstone() {
            continue;
        }
        present.insert(entity.identity().to_vec(), entity);
    }
    Ok(present)
}

/// Compare two resolved branches, grouped by capability and space. The caller
/// resolves branch names to catalog records so the control-plane borrow is
/// released before the persistence scans run.
pub(crate) fn compare_records(
    persistence: &mut StoragePersistence,
    record_a: &BranchCatalogRecord,
    record_b: &BranchCatalogRecord,
    selector: BranchStateSelector,
) -> Result<BranchComparison, EngineError> {
    let read = read_selector(selector);
    let mut spaces = registered_spaces(persistence, record_a)?;
    for space in registered_spaces(persistence, record_b)? {
        if !spaces.contains(&space) {
            spaces.push(space);
        }
    }
    spaces.sort();

    let adapters = capability_adapters();
    let mut comparisons = Vec::new();
    for space in &spaces {
        for (capability, adapter) in &adapters {
            if adapter.derived_disposition() != DerivedDisposition::Authored {
                continue;
            }
            let side_a = present_entities(persistence, record_a, adapter.as_ref(), space, read)?;
            let side_b = present_entities(persistence, record_b, adapter.as_ref(), space, read)?;

            let mut added = Vec::new();
            let mut modified = Vec::new();
            for (identity, entity_b) in &side_b {
                match side_a.get(identity) {
                    None => added.push(ComparedEntity::new(identity.clone(), entity_b.version())),
                    Some(entity_a) if entity_a.summary() != entity_b.summary() => {
                        modified.push(ComparedEntity::new(identity.clone(), entity_b.version()));
                    }
                    Some(_) => {}
                }
            }
            let removed = side_a
                .iter()
                .filter(|(identity, _)| !side_b.contains_key(*identity))
                .map(|(identity, entity_a)| {
                    ComparedEntity::new(identity.clone(), entity_a.version())
                })
                .collect::<Vec<_>>();

            if !(added.is_empty() && removed.is_empty() && modified.is_empty()) {
                comparisons.push(SpaceComparison::new(
                    space.clone(),
                    *capability,
                    added,
                    removed,
                    modified,
                ));
            }
        }
    }

    Ok(BranchComparison::new(
        record_a.name().clone(),
        record_b.name().clone(),
        comparisons,
    ))
}

#[cfg(test)]
mod tests {
    use super::read_selector;

    use strata_core::Timestamp;

    use crate::api::BranchStateSelector;
    use crate::persistence::ReadSelector;

    #[test]
    fn selector_maps_to_the_matching_read_selector() {
        assert_eq!(
            read_selector(BranchStateSelector::Current),
            ReadSelector::Latest
        );
        assert_eq!(
            read_selector(BranchStateSelector::AtTimestamp(Timestamp::from_micros(42))),
            ReadSelector::AtTimestamp(Timestamp::from_micros(42))
        );
    }
}
