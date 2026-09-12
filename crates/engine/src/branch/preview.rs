//! Branch preview promotion — the engine workflow behind `BranchService::preview`.
//!
//! Preview derives the branch point from lineage, runs a three-way comparison
//! (branch point → source, branch point → target), and reports the conflicts a
//! promotion would hit — without mutating either branch (contract §Preview
//! Promotion, conformance #5).
//!
//! M12C1 supports a direct fork lineage: one branch forked from the other. The
//! branch point is the ancestor's state at the fork. Sibling, transitive, and
//! unrelated lineages are rejected here and land in a follow-on; per the
//! contract, callers may never inject a synthetic branch point.

use std::collections::{BTreeMap, BTreeSet};

use strata_core::{BranchId, CommitVersion, Timestamp};

use crate::api::{
    BranchPreview, BranchWorkflowCoverage, ComparedCapability, ConflictKind,
    ConflictStrategyResult, DerivedStateDisposition, DerivedStateReport, PreviewConflict,
    PromotionStrategy,
};
use crate::branch::adapter::{CapabilityBranchAdapter, DerivedDisposition, EntitySummary};
use crate::branch::catalog::BranchCatalogRecord;
use crate::control::space::{read_space_index_at, registered_spaces};
use crate::data::event::EventBranchAdapter;
use crate::data::graph::{
    GraphEdgeBranchAdapter, GraphMetadataBranchAdapter, GraphNodeBranchAdapter,
    GraphOntologyBranchAdapter,
};
use crate::data::json::JsonBranchAdapter;
use crate::data::kv::{KvBranchAdapter, ProductSpace};
use crate::data::vector::{
    plan_collection_promotion, VectorBranchAdapter, VectorCollectionBranchAdapter,
};
use crate::diagnostics::EngineError;
use crate::persistence::{ReadSelector, StoragePersistence};

/// The authored capabilities a branch workflow enumerates, in report order.
/// Comparison covers all of them; promotion additionally filters to those whose
/// adapter reports `supports_promotion` (see `three_way`).
const AUTHORED_CAPABILITIES: [ComparedCapability; 9] = [
    ComparedCapability::Kv,
    ComparedCapability::Json,
    ComparedCapability::Vector,
    ComparedCapability::VectorCollection,
    ComparedCapability::Event,
    ComparedCapability::GraphMetadata,
    ComparedCapability::GraphNode,
    ComparedCapability::GraphEdge,
    ComparedCapability::GraphOntology,
];

/// The branch adapter for one capability. Single source of truth for the
/// capability→adapter mapping, shared by compare, preview, and promotion.
pub(crate) fn adapter_for(capability: ComparedCapability) -> Box<dyn CapabilityBranchAdapter> {
    match capability {
        ComparedCapability::Kv => Box::new(KvBranchAdapter),
        ComparedCapability::Json => Box::new(JsonBranchAdapter),
        ComparedCapability::Vector => Box::new(VectorBranchAdapter),
        ComparedCapability::VectorCollection => Box::new(VectorCollectionBranchAdapter),
        ComparedCapability::Event => Box::new(EventBranchAdapter),
        ComparedCapability::GraphMetadata => Box::new(GraphMetadataBranchAdapter),
        ComparedCapability::GraphNode => Box::new(GraphNodeBranchAdapter),
        ComparedCapability::GraphEdge => Box::new(GraphEdgeBranchAdapter),
        ComparedCapability::GraphOntology => Box::new(GraphOntologyBranchAdapter),
    }
}

/// The authored capabilities in report order, each with its adapter. The single
/// registry shared by compare, preview, and promotion — register a capability
/// once here and all three cover it.
pub(crate) fn capability_adapters() -> Vec<(ComparedCapability, Box<dyn CapabilityBranchAdapter>)> {
    AUTHORED_CAPABILITIES
        .iter()
        .map(|&capability| (capability, adapter_for(capability)))
        .collect()
}

/// Splits the authored capabilities into those a promotion carries (promotable)
/// and those it does not (compare-only), in report order (contract §Promotion
/// rule 9 / §Preview rule 4).
fn capability_coverage() -> (Vec<ComparedCapability>, Vec<ComparedCapability>) {
    let mut covered = Vec::new();
    let mut unsupported = Vec::new();
    for &capability in &AUTHORED_CAPABILITIES {
        if adapter_for(capability).supports_promotion() {
            covered.push(capability);
        } else {
            unsupported.push(capability);
        }
    }
    (covered, unsupported)
}

/// The derived-state disposition a promotion carrying `promoted` capabilities
/// triggers (contract §Promotion rule 9 / §Preview rule 5). JSON secondary index
/// rows are not maintained by the document carry, so a promoted JSON capability
/// leaves them stale; vector search stays correct via its query-time
/// full-collection fallback, so its index needs no rebuild.
fn derived_state_reports(promoted: &[ComparedCapability]) -> Vec<DerivedStateReport> {
    let mut reports = Vec::new();
    if promoted.contains(&ComparedCapability::Json) {
        reports.push(DerivedStateReport::new(
            ComparedCapability::Json,
            DerivedStateDisposition::RebuildRequired,
        ));
    }
    if promoted.contains(&ComparedCapability::Vector) {
        reports.push(DerivedStateReport::new(
            ComparedCapability::Vector,
            DerivedStateDisposition::Current,
        ));
    }
    reports
}

/// Assembles the coverage facts a promotion outcome and its preview share: the
/// spaces spanned (source ∪ target), the static promotable/compare-only capability
/// split, and the derived-state disposition for the `promoted` capabilities.
pub(crate) fn branch_workflow_coverage(
    persistence: &mut StoragePersistence,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
    promoted: &[ComparedCapability],
) -> Result<BranchWorkflowCoverage, EngineError> {
    let (capabilities_covered, capabilities_unsupported) = capability_coverage();
    let mut spaces_covered = registered_spaces(persistence, source)?;
    for space in registered_spaces(persistence, target)? {
        if !spaces_covered.contains(&space) {
            spaces_covered.push(space);
        }
    }
    spaces_covered.sort();
    Ok(BranchWorkflowCoverage {
        spaces_covered,
        capabilities_covered,
        capabilities_unsupported,
        derived_state: derived_state_reports(promoted),
    })
}

/// The branch point of a direct fork lineage: the ancestor's storage branch and
/// the read selector that reproduces its state at the fork.
struct BasePoint {
    storage_branch_id: BranchId,
    selector: ReadSelector,
    version: CommitVersion,
}

/// Derives the branch point for a direct fork lineage (M12C1): the target
/// forked from the source, or the source forked from the target, with an
/// intact generation edge. Any other relationship is rejected.
fn resolve_base_point(
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
) -> Result<BasePoint, EngineError> {
    // A prior promotion of `source` into `target` advances the branch point past
    // the fork: the target's post-merge state already incorporates the source, so
    // a repeated promotion diffed against the fork would re-surface already-merged
    // changes as false conflicts. The recorded merge edge is a valid branch-point
    // source (contract §Branch Point). The base is the SOURCE's frontier at that
    // merge (the LCA of source-now and target-now), read by version on the
    // source's own timeline — NOT the target's post-merge commit, which also
    // holds target-only rows and would re-surface them as spurious source
    // deletions on the next promote.
    if let Some(merge) = target.merge_parent() {
        if merge.source_branch_id() == source.branch_id()
            && merge.source_generation() == source.generation()
        {
            if let Some(source_version) = merge.source_merged_version() {
                return Ok(base_point_from(source, source_version, None));
            }
            // A merge edge written before the source frontier was recorded falls
            // through to the fork base — non-destructive (it may re-surface
            // already-merged changes, but never deletes target-only rows).
        }
    }
    if let Some(parent) = target.parent() {
        if parent.branch_id() == source.branch_id() && parent.generation() == source.generation() {
            return Ok(base_point_from(
                source,
                parent.fork_version(),
                parent.fork_timestamp(),
            ));
        }
    }
    if let Some(parent) = source.parent() {
        if parent.branch_id() == target.branch_id() && parent.generation() == target.generation() {
            return Ok(base_point_from(
                target,
                parent.fork_version(),
                parent.fork_timestamp(),
            ));
        }
    }
    Err(EngineError::invalid_input(
        "invalid_argument.engine.branch_point",
        "no branch point: the two branches are not in a direct fork lineage",
    ))
}

/// The promotion base point as a `(storage_branch, selector)` pair — the read
/// coordinates for any base-leg three-way (spaces, vector collections) that
/// needs to see the branch state at the point the two branches diverged.
pub(crate) fn base_point_for(
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
) -> Result<(BranchId, ReadSelector), EngineError> {
    let base = resolve_base_point(source, target)?;
    Ok((base.storage_branch_id, base.selector))
}

/// The spaces registered at the promotion base point — the third leg of the
/// space three-way. It distinguishes a source-side deletion (present in the
/// base, gone from the source) from a space the target merely added (absent
/// from the base), so promotion never removes a genuinely target-only space.
pub(crate) fn base_registered_spaces(
    persistence: &mut StoragePersistence,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
) -> Result<Vec<ProductSpace>, EngineError> {
    let base = resolve_base_point(source, target)?;
    read_space_index_at(persistence, base.storage_branch_id, base.selector)
}

fn base_point_from(
    ancestor: &BranchCatalogRecord,
    fork_version: CommitVersion,
    fork_timestamp: Option<Timestamp>,
) -> BasePoint {
    let selector = fork_timestamp.map_or(
        ReadSelector::AtVersion(fork_version),
        ReadSelector::AtTimestamp,
    );
    BasePoint {
        storage_branch_id: ancestor.storage_branch_id(),
        selector,
        version: fork_version,
    }
}

/// Every entity of one capability in one space at a branch state, keyed by
/// identity, including tombstones (as `EntitySummary::Absent`) so a three-way
/// diff can see deletions.
fn entity_states(
    persistence: &mut StoragePersistence,
    storage_branch_id: BranchId,
    adapter: &dyn CapabilityBranchAdapter,
    space: &ProductSpace,
    selector: ReadSelector,
) -> Result<BTreeMap<Vec<u8>, EntitySummary>, EngineError> {
    let rows = persistence.scan_prefix(
        storage_branch_id,
        adapter.row_class(),
        adapter.space_prefix(space),
        selector,
        None,
    )?;
    let mut states = BTreeMap::new();
    for row in &rows {
        let entity = adapter.interpret_row(space, row)?;
        states.insert(entity.identity().to_vec(), entity.summary().clone());
    }
    Ok(states)
}

pub(crate) fn value_of(summary: Option<&EntitySummary>) -> Option<Vec<u8>> {
    match summary {
        Some(EntitySummary::Present(bytes)) => Some(bytes.clone()),
        _ => None,
    }
}

/// Whether a side's state differs from the branch-point state for an entity. An
/// entity absent from a side's map is absent on that side (its inherited rows
/// are visible in the scan, so a missing key is a genuine absence).
pub(crate) fn changed(side: Option<&EntitySummary>, base: Option<&EntitySummary>) -> bool {
    normalized(side) != normalized(base)
}

pub(crate) fn normalized(summary: Option<&EntitySummary>) -> &EntitySummary {
    summary.unwrap_or(&EntitySummary::Absent)
}

/// One entity's three-way state across a promotion's branch point, source, and
/// target — emitted for every identity that changed on at least one side since
/// the branch point. Shared by preview (which reports conflicts) and promotion
/// (which turns source changes into target mutations).
pub(crate) struct ThreeWayEntity {
    pub(crate) capability: ComparedCapability,
    pub(crate) space: ProductSpace,
    pub(crate) identity: Vec<u8>,
    pub(crate) base: Option<EntitySummary>,
    pub(crate) source: Option<EntitySummary>,
    pub(crate) target: Option<EntitySummary>,
}

/// Runs the three-way scan (branch point → source, branch point → target) over
/// every authored capability and space, returning the branch-point version and,
/// for each identity that changed on at least one side, its three summaries.
///
/// The branch point is derived from lineage (a direct fork edge in M12C1);
/// unrelated branches are rejected with `invalid_argument.engine.branch_point`.
pub(crate) fn three_way(
    persistence: &mut StoragePersistence,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
) -> Result<(CommitVersion, Vec<ThreeWayEntity>), EngineError> {
    let base = resolve_base_point(source, target)?;

    let mut spaces = registered_spaces(persistence, source)?;
    for space in registered_spaces(persistence, target)? {
        if !spaces.contains(&space) {
            spaces.push(space);
        }
    }
    spaces.sort();

    let adapters = capability_adapters();
    let mut entities = Vec::new();
    for space in &spaces {
        for (capability, adapter) in &adapters {
            // The three-way scan drives promotion and its preview, so it covers
            // only capabilities that support promotion. Comparison (compare.rs)
            // uses the same registry but includes every authored capability, so
            // compare-only capabilities such as event streams still diff.
            if adapter.derived_disposition() != DerivedDisposition::Authored
                || !adapter.supports_promotion()
            {
                continue;
            }
            let base_states = entity_states(
                persistence,
                base.storage_branch_id,
                adapter.as_ref(),
                space,
                base.selector,
            )?;
            let source_states = entity_states(
                persistence,
                source.storage_branch_id(),
                adapter.as_ref(),
                space,
                ReadSelector::Latest,
            )?;
            let target_states = entity_states(
                persistence,
                target.storage_branch_id(),
                adapter.as_ref(),
                space,
                ReadSelector::Latest,
            )?;

            let mut identities: BTreeSet<&Vec<u8>> = BTreeSet::new();
            identities.extend(source_states.keys());
            identities.extend(target_states.keys());
            identities.extend(base_states.keys());

            for identity in identities {
                let base_value = base_states.get(identity);
                let source_value = source_states.get(identity);
                let target_value = target_states.get(identity);

                if !(changed(source_value, base_value) || changed(target_value, base_value)) {
                    continue;
                }

                entities.push(ThreeWayEntity {
                    capability: *capability,
                    space: space.clone(),
                    identity: identity.clone(),
                    base: base_value.cloned(),
                    source: source_value.cloned(),
                    target: target_value.cloned(),
                });
            }
        }
    }

    Ok((base.version, entities))
}

/// Previews promoting `source` into `target`: derives the branch point and runs
/// the three-way comparison, reporting conflicts without mutating either branch.
pub(crate) fn preview_branches(
    persistence: &mut StoragePersistence,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
    strategy: PromotionStrategy,
) -> Result<BranchPreview, EngineError> {
    let strategy_result = match strategy {
        PromotionStrategy::Strict => ConflictStrategyResult::Refused,
        PromotionStrategy::SourceWins => ConflictStrategyResult::SourceWins,
    };
    let (branch_point, entities) = three_way(persistence, source, target)?;

    let mut conflicts = Vec::new();
    let mut promoted = Vec::new();
    for entity in &entities {
        let source_value = entity.source.as_ref();
        let target_value = entity.target.as_ref();
        let base_value = entity.base.as_ref();

        // A source-side change is what a promotion would carry — the capabilities
        // whose derived state it would disposition. Duplicates are harmless:
        // `branch_workflow_coverage` tests membership, not multiplicity.
        if changed(source_value, base_value) {
            promoted.push(entity.capability);
        }

        if !(changed(source_value, base_value) && changed(target_value, base_value)) {
            continue;
        }
        if normalized(source_value) == normalized(target_value) {
            continue; // both sides converged on the same change
        }

        let source_present = matches!(entity.source, Some(EntitySummary::Present(_)));
        let target_present = matches!(entity.target, Some(EntitySummary::Present(_)));
        let kind = if source_present && target_present {
            ConflictKind::ValueDivergence
        } else {
            ConflictKind::ModifyDeleteDivergence
        };

        conflicts.push(PreviewConflict::new(
            entity.capability,
            entity.space.clone(),
            entity.identity.clone(),
            value_of(source_value),
            value_of(target_value),
            kind,
            strategy_result,
        ));
    }

    // Fold in the collection-config conflicts a promotion would hit (incompatible
    // dimension/metric, or a source-side delete of a collection the target
    // reshaped) — the generic three-way scans only vector data rows, not the
    // config metadata, so without this a preview reports clean on a promotion the
    // service would refuse. Read-only: the carry mutations are discarded.
    let source_spaces = registered_spaces(persistence, source)?;
    let (_, collection_conflicts) =
        plan_collection_promotion(persistence, source, target, &source_spaces, strategy_result)?;
    conflicts.extend(collection_conflicts);

    let coverage = branch_workflow_coverage(persistence, source, target, &promoted)?;
    Ok(BranchPreview::new(
        source.name().clone(),
        target.name().clone(),
        branch_point,
        strategy,
        conflicts,
        coverage,
    ))
}
