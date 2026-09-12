//! Branch promotion (merge) — the engine planner behind `BranchService::promote`.
//!
//! Promotion applies a source branch's completed changes into a target branch as
//! a single atomic commit (contract §Promotion). This module derives the plan:
//! it reuses the three-way scan behind preview, then turns every source-side
//! change since the branch point into a target mutation, resolving conflicts per
//! the selected strategy. It does not commit — the branch service coordinates
//! the single target commit and records the promotion lineage.
//!
//! M12D1 covers KV and JSON in cache/durable happy paths; the recoverable
//! workflow intent and crash/reopen recovery land in M12D2.

use std::collections::BTreeSet;

use strata_core::CommitVersion;

use crate::api::{
    ComparedCapability, ConflictKind, ConflictStrategyResult, PreviewConflict, PromotedEntity,
    PromotionStrategy,
};
use crate::branch::adapter::EntitySummary;
use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::preview::{
    adapter_for, base_registered_spaces, changed, normalized, three_way, value_of,
};
use crate::control::space::{registered_spaces, registration_and_deletion_mutations};
use crate::data::kv::ProductSpace;
use crate::data::vector::plan_collection_promotion;
use crate::diagnostics::EngineError;
use crate::persistence::{
    encode_event_space_prefix, encode_graph_edge_space_prefix, encode_graph_metadata_prefix,
    encode_graph_node_space_prefix, ReadSelector, RowClass, RowMutation, StoragePersistence,
};

/// The mutations and reporting a promotion of `source` into `target` would
/// produce. `mutations` apply every winning source change onto the target in one
/// commit; `applied`/`deleted` report them for the outcome; `conflicts` records
/// entities that diverged on both sides (empty when the merge is clean).
pub(crate) struct PromotionPlan {
    pub(crate) branch_point: CommitVersion,
    pub(crate) mutations: Vec<RowMutation>,
    pub(crate) applied: Vec<PromotedEntity>,
    pub(crate) deleted: Vec<PromotedEntity>,
    pub(crate) conflicts: Vec<PreviewConflict>,
}

/// Plans promoting `source` into `target` under `strategy`, without mutating
/// either branch. The caller decides whether to commit: a strict strategy
/// refuses when `conflicts` is non-empty; otherwise `mutations` are the target
/// write-set (empty when the merge is a no-op).
pub(crate) fn plan_promotion(
    persistence: &mut StoragePersistence,
    source: &BranchCatalogRecord,
    target: &BranchCatalogRecord,
    strategy: PromotionStrategy,
) -> Result<PromotionPlan, EngineError> {
    let strategy_result = match strategy {
        PromotionStrategy::Strict => ConflictStrategyResult::Refused,
        PromotionStrategy::SourceWins => ConflictStrategyResult::SourceWins,
    };
    let (branch_point, entities) = three_way(persistence, source, target)?;
    let target_branch = target.storage_branch_id();

    let mut mutations = Vec::new();
    let mut applied = Vec::new();
    let mut deleted = Vec::new();
    let mut conflicts = Vec::new();
    // Spaces that still hold at least one live row on the target after this
    // promotion. Such a space must keep its registration even if the source
    // deleted it, so a source-side space deletion never orphans a target-only
    // row (data readable but outside the registered catalog).
    let mut retained_spaces: BTreeSet<ProductSpace> = BTreeSet::new();

    for entity in entities {
        let source_value = entity.source.as_ref();
        let target_value = entity.target.as_ref();
        let base_value = entity.base.as_ref();

        // A row's post-promotion presence on the target is the source's resolved
        // state where the source changed it, otherwise the target's own state.
        let post_present = if changed(source_value, base_value) {
            matches!(entity.source, Some(EntitySummary::Present(_)))
        } else {
            matches!(entity.target, Some(EntitySummary::Present(_)))
        };
        if post_present {
            retained_spaces.insert(entity.space.clone());
        }

        // Only source-side changes propagate; a target-only change is kept.
        if !changed(source_value, base_value) {
            continue;
        }
        // Source and target already agree (both converged on the same change) —
        // nothing to write. Keeps a clean promotion a no-op with no commit.
        if normalized(source_value) == normalized(target_value) {
            continue;
        }

        // Both sides changed this identity to different states: a conflict.
        if changed(target_value, base_value) {
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

        // Apply the source's resolved state onto the target.
        let resolved = normalized(source_value);
        let adapter = adapter_for(entity.capability);
        mutations.push(adapter.write_mutation(
            target_branch,
            &entity.space,
            &entity.identity,
            resolved,
        ));
        record_applied_or_deleted(
            &mut applied,
            &mut deleted,
            entity.capability,
            &entity,
            resolved,
        );
    }

    // Carry source-only spaces so promoted rows land in a space the target's
    // catalog registers, rather than orphaned outside it — a visible data change
    // must carry the branch-control metadata that explains it (contract Binding
    // Decision #8). Symmetrically, remove spaces the source deleted (present in
    // the base, gone from the source) so their stale registration does not linger
    // on the target; a space the target merely added is absent from the base and
    // is never touched (mirroring the data three-way, which keeps target-only
    // rows). Both directions commit atomically with the data mutations, so a
    // strict refusal (which never commits the plan) leaves the target untouched.
    let source_spaces = registered_spaces(persistence, source)?;
    let base_spaces = base_registered_spaces(persistence, source, target)?;
    // A space present in the base but gone from the source was deleted there —
    // remove it from the target, UNLESS the target still holds live state in it.
    // That state is either a promotable row the three-way kept (`retained_spaces`)
    // or an event/graph row the promotable three-way never scans
    // (`space_has_unpromoted_target_rows`). Deregistering a space that still holds
    // any of these would orphan it outside the catalog. (The mutation builder
    // further narrows this to spaces the target actually registers.)
    let mut deleted_spaces: Vec<ProductSpace> = Vec::new();
    for space in base_spaces {
        if source_spaces.iter().any(|existing| existing == &space)
            || retained_spaces.contains(&space)
            || space_has_unpromoted_target_rows(persistence, target, &space)?
        {
            continue;
        }
        deleted_spaces.push(space);
    }
    mutations.extend(registration_and_deletion_mutations(
        persistence,
        target,
        &source_spaces,
        &deleted_spaces,
    )?);

    // Carry vector collection configs so promoted vectors are usable on the
    // target rather than orphaned behind a missing collection config (contract
    // Vector minimum). An incompatible dimension/metric surfaces as a structural
    // conflict that refuses the promotion under every strategy (see the service).
    let (collection_mutations, collection_conflicts) =
        plan_collection_promotion(persistence, source, target, &source_spaces, strategy_result)?;
    mutations.extend(collection_mutations);
    conflicts.extend(collection_conflicts);

    Ok(PromotionPlan {
        branch_point,
        mutations,
        applied,
        deleted,
        conflicts,
    })
}

/// Whether the `target` holds any live row in `space` for a capability the
/// promotion's data three-way does not scan — event or graph rows. Those
/// capabilities are not promotable (`supports_promotion() == false`), so a
/// promotion never carries or deletes their rows: any live event/graph row on the
/// target is genuinely surviving state, invisible to `retained_spaces` (which is
/// built only from promotable KV/JSON/Vector entities). Without this, a source-side
/// space deletion would deregister the space and orphan that state outside the
/// catalog.
///
/// Vector-collection metadata is deliberately NOT checked here: the promotion DOES
/// delete collection configs (`plan_collection_promotion`), so a live config is not
/// necessarily surviving; a source-deleted collection's config must not keep the
/// space alive (that would regress the source-deleted-space cleanup). The
/// target-only-collection-in-a-deregistered-space case is tracked separately.
fn space_has_unpromoted_target_rows(
    persistence: &mut StoragePersistence,
    target: &BranchCatalogRecord,
    space: &ProductSpace,
) -> Result<bool, EngineError> {
    let branch = target.storage_branch_id();
    let classes = [
        (RowClass::Event, encode_event_space_prefix(space)),
        (RowClass::GraphMetadata, encode_graph_metadata_prefix(space)),
        (RowClass::GraphNode, encode_graph_node_space_prefix(space)),
        (RowClass::GraphEdge, encode_graph_edge_space_prefix(space)),
    ];
    for (class, prefix) in classes {
        let rows = persistence.scan_prefix(branch, class, prefix, ReadSelector::Latest, None)?;
        if rows.iter().any(|row| !row.is_tombstone()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn record_applied_or_deleted(
    applied: &mut Vec<PromotedEntity>,
    deleted: &mut Vec<PromotedEntity>,
    capability: ComparedCapability,
    entity: &crate::branch::preview::ThreeWayEntity,
    resolved: &EntitySummary,
) {
    match resolved {
        EntitySummary::Present(value) => applied.push(PromotedEntity::new(
            capability,
            entity.space.clone(),
            entity.identity.clone(),
            Some(value.clone()),
        )),
        EntitySummary::Absent => deleted.push(PromotedEntity::new(
            capability,
            entity.space.clone(),
            entity.identity.clone(),
            None,
        )),
    }
}
