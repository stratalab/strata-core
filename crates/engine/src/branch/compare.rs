//! Branch comparison — the engine workflow behind `BranchService::compare`.
//!
//! Compare is a read-only workflow that diffs two branches' authored entities,
//! grouped by capability and space (contract §Compare And Preview, conformance
//! #4). It owns the orchestration the capability adapters deliberately do not:
//! it enumerates each branch's rows through persistence at a read selector and
//! feeds them to the adapters, which interpret them into comparable entities.
//! Derived rows are omitted by default (only `Authored` capabilities compare).

use std::collections::{BTreeMap, BTreeSet};

use crate::api::{BranchComparison, BranchStateSelector, ComparedEntity, SpaceComparison};
use crate::branch::adapter::{CapabilityBranchAdapter, ComparableEntity, DerivedDisposition};
use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::preview::capability_adapters;
use crate::control::space::{pending_deletion_at, registered_spaces};
use crate::data::graph::{marked_graphs, GraphName};
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{PersistenceReadRow, ReadSelector, StoragePersistence};

/// Maps the public branch-state selector to the internal storage read selector.
const fn read_selector(selector: BranchStateSelector) -> ReadSelector {
    match selector {
        BranchStateSelector::Current => ReadSelector::Latest,
        BranchStateSelector::AtTimestamp(timestamp) => ReadSelector::AtTimestamp(timestamp),
    }
}

/// The present (non-tombstone) entities of one capability in one space at the
/// selected branch state, keyed by identity for set difference.
/// What a deletion under way hides from one branch's view of one space at
/// the compared version (#3575): the whole space, when its catalog row is
/// marked (#3574), or the graphs whose metadata row is marked (#3477). Rows
/// behind either mark are absent to every observer, so they compare as
/// absent — a graph or space mid-sweep diffs exactly as a deleted one.
struct HiddenByDeletion {
    space: bool,
    graphs: BTreeSet<GraphName>,
}

impl HiddenByDeletion {
    fn read(
        persistence: &StoragePersistence,
        record: &BranchCatalogRecord,
        space: &ProductSpace,
        selector: ReadSelector,
    ) -> Result<Self, EngineError> {
        let space_marked = pending_deletion_at(persistence, record, space, selector)?;
        let graphs = if space_marked {
            BTreeSet::new()
        } else {
            marked_graphs(persistence, record, space, selector)?
        };
        Ok(Self {
            space: space_marked,
            graphs,
        })
    }

    fn hides(
        &self,
        adapter: &dyn CapabilityBranchAdapter,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<bool, EngineError> {
        if self.space {
            return Ok(true);
        }
        if self.graphs.is_empty() {
            return Ok(false);
        }
        Ok(adapter
            .graph_of(space, row)?
            .is_some_and(|graph| self.graphs.contains(&graph)))
    }
}

fn present_entities(
    persistence: &mut StoragePersistence,
    record: &BranchCatalogRecord,
    adapter: &dyn CapabilityBranchAdapter,
    space: &ProductSpace,
    selector: ReadSelector,
    hidden: &HiddenByDeletion,
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
        if hidden.hides(adapter, space, row)? {
            continue;
        }
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
        let hidden_a = HiddenByDeletion::read(persistence, record_a, space, read)?;
        let hidden_b = HiddenByDeletion::read(persistence, record_b, space, read)?;
        for (capability, adapter) in &adapters {
            if adapter.derived_disposition() != DerivedDisposition::Authored {
                continue;
            }
            let side_a = present_entities(
                persistence,
                record_a,
                adapter.as_ref(),
                space,
                read,
                &hidden_a,
            )?;
            let side_b = present_entities(
                persistence,
                record_b,
                adapter.as_ref(),
                space,
                read,
                &hidden_b,
            )?;

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
    /// #3575: a graph mid-deletion on one branch compares exactly as a deleted
    /// one — its metadata, nodes, edges and ontology are absent on that side —
    /// and a space mid-deletion on one branch compares as absent in full.
    // One scenario seeds every graph row class plus a KV space so the four
    // adapters and the space mark are exercised against one comparison.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn a_graph_or_space_mid_deletion_compares_as_absent() {
        use crate::api::{BranchStateSelector, ComparedCapability};
        use crate::branch::BranchName;
        use crate::data::kv::{KvKey, KvValue, ProductSpace};
        use crate::{
            CacheOpenOptions, Database, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData,
            GraphNodeId, GraphObjectTypeDef, GraphTypeName,
        };

        let branch = |name: &str| BranchName::new(name).expect("branch");
        let space = |name: &str| ProductSpace::new(name).expect("space");
        let node = |id: &str| GraphNodeId::new(id).expect("node");
        let mut database = Database::open_cache(CacheOpenOptions::new())
            .expect("cache opens")
            .into_database();

        // A graph with every row class, and a bystander graph, on the default
        // branch; a KV-only space `tenant` beside them.
        let doomed = GraphName::new("doomed").expect("graph");
        let kept = GraphName::new("kept").expect("graph");
        {
            let mut graph = database
                .graph(branch("default"), space("default"))
                .expect("graph service");
            for name in [&doomed, &kept] {
                graph.create_graph(name.clone()).expect("created");
                graph
                    .upsert_node(name, node("a"), GraphNodeData::default())
                    .expect("node");
                graph
                    .upsert_node(name, node("b"), GraphNodeData::default())
                    .expect("node");
                graph
                    .upsert_edge(
                        name,
                        node("a"),
                        GraphEdgeType::new("road").expect("type"),
                        node("b"),
                        GraphEdgeData::default(),
                    )
                    .expect("edge");
            }
            graph
                .define_object_type(
                    &doomed,
                    GraphObjectTypeDef::new(GraphTypeName::new("Place").expect("type"), [])
                        .expect("type def"),
                )
                .expect("ontology row");
        }
        // A KV row beside the graphs: a non-graph row in a space with a marked
        // graph is still compared (the adapters outside graphs name no graph).
        database
            .kv(branch("default"), space("default"))
            .expect("kv service")
            .put(
                KvKey::new("beside").expect("key"),
                KvValue::new(b"v1".to_vec()),
            )
            .expect("put");
        database
            .spaces(branch("default"))
            .expect("space service")
            .create(space("tenant"))
            .expect("space created");
        database
            .kv(branch("default"), space("tenant"))
            .expect("kv service")
            .put(KvKey::new("k").expect("key"), KvValue::new(b"v".to_vec()))
            .expect("put");
        database
            .branches()
            .expect("branch service")
            .fork_current(&branch("default"), branch("child"))
            .expect("fork");

        database
            .kv(branch("child"), space("default"))
            .expect("kv service")
            .put(
                KvKey::new("beside").expect("key"),
                KvValue::new(b"v2".to_vec()),
            )
            .expect("the child changes the KV row");

        // Marks on the default branch only.
        database
            .graph(branch("default"), space("default"))
            .expect("graph service")
            .begin_graph_delete_for_test(&doomed)
            .expect("graph mark commits");
        database
            .spaces(branch("default"))
            .expect("space service")
            .begin_space_delete_for_test(&space("tenant"))
            .expect("space mark commits");

        let comparison = database
            .branches()
            .expect("branch service")
            .compare(
                &branch("default"),
                &branch("child"),
                BranchStateSelector::Current,
            )
            .expect("compare succeeds");
        let added = |capability: ComparedCapability, space_name: &str| -> usize {
            comparison
                .comparisons()
                .iter()
                .find(|entry| {
                    entry.capability() == capability && entry.space().as_str() == space_name
                })
                .map_or(0, |entry| entry.added().len())
        };
        let removed = comparison
            .comparisons()
            .iter()
            .map(|entry| entry.removed().len())
            .sum::<usize>();
        assert_eq!(removed, 0, "the child removed nothing");
        let modified: Vec<(ComparedCapability, String)> = comparison
            .comparisons()
            .iter()
            .flat_map(|entry| {
                entry
                    .modified()
                    .iter()
                    .map(move |_| (entry.capability(), entry.space().as_str().to_owned()))
            })
            .collect();
        assert_eq!(
            modified,
            vec![(ComparedCapability::Kv, "default".to_owned())],
            "the KV row beside the marked graph is still compared, and nothing else moved"
        );
        // The doomed graph is absent on the marked side, so every one of its
        // rows is "added" on the child: 1 metadata, 2 nodes, 1 edge, 1 ontology.
        assert_eq!(added(ComparedCapability::GraphMetadata, "default"), 1);
        assert_eq!(added(ComparedCapability::GraphNode, "default"), 2);
        assert_eq!(added(ComparedCapability::GraphEdge, "default"), 1);
        assert_eq!(added(ComparedCapability::GraphOntology, "default"), 1);
        // The marked space is absent in full on the marked side.
        assert_eq!(added(ComparedCapability::Kv, "tenant"), 1);

        // The bystander graph shows nowhere: its rows are identical.
        let all_identities: Vec<Vec<u8>> = comparison
            .comparisons()
            .iter()
            .flat_map(|entry| entry.added().iter().map(|e| e.identity().to_vec()))
            .collect();
        assert!(
            all_identities
                .iter()
                .all(|identity| !identity.windows(4).any(|w| w == b"kept")),
            "the kept graph is not in the diff: {all_identities:?}"
        );

        // Symmetry (#3575): the marks hide on branch_b as well. Reverse the
        // direction so `default` is branch_b — its doomed graph and marked space
        // are absent on the right, so they read as `removed`, mirror-image of
        // the `added` counts above. This exercises the `hidden_b` side, which
        // the forward comparison leaves in its "nothing hidden" form.
        let reversed = database
            .branches()
            .expect("branch service")
            .compare(
                &branch("child"),
                &branch("default"),
                BranchStateSelector::Current,
            )
            .expect("compare succeeds");
        let removed = |capability: ComparedCapability, space_name: &str| -> usize {
            reversed
                .comparisons()
                .iter()
                .find(|entry| {
                    entry.capability() == capability && entry.space().as_str() == space_name
                })
                .map_or(0, |entry| entry.removed().len())
        };
        let added = reversed
            .comparisons()
            .iter()
            .map(|entry| entry.added().len())
            .sum::<usize>();
        assert_eq!(added, 0, "the child added nothing");
        let modified: Vec<(ComparedCapability, String)> = reversed
            .comparisons()
            .iter()
            .flat_map(|entry| {
                entry
                    .modified()
                    .iter()
                    .map(move |_| (entry.capability(), entry.space().as_str().to_owned()))
            })
            .collect();
        assert_eq!(
            modified,
            vec![(ComparedCapability::Kv, "default".to_owned())],
            "the KV row beside the marked graph is compared in this direction too"
        );
        assert_eq!(removed(ComparedCapability::GraphMetadata, "default"), 1);
        assert_eq!(removed(ComparedCapability::GraphNode, "default"), 2);
        assert_eq!(removed(ComparedCapability::GraphEdge, "default"), 1);
        assert_eq!(removed(ComparedCapability::GraphOntology, "default"), 1);
        assert_eq!(removed(ComparedCapability::Kv, "tenant"), 1);
    }
}
