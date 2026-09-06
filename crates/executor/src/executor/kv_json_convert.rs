use super::{
    output_json_index_type, BatchExistsItemResult, BatchGetItemResult, BatchItem, BatchItemResult,
    BranchCleanupItem, BranchCleanupSummary, BranchComparisonItem, BranchItem, BranchMergeItem,
    BranchParentItem, BranchPreviewItem, BranchStatus, BranchSummary, Bytes, CommitDurability,
    CommitOutcome, CommitReceipt, CommitVersion, ComparedCapability, ComparedEntityItem,
    ConflictKind, ConflictStrategyResult, DerivedStateDisposition, DerivedStateReportItem,
    EngineBranchComparison, EngineBranchPreview, EngineBranchStatus, EngineComparedCapability,
    EngineComparedEntity, EngineConflictKind, EngineConflictStrategyResult,
    EngineDerivedStateDisposition, EngineDerivedStateReport, EngineJsonIndexDefinition,
    EngineJsonSample, EngineJsonValue, EngineJsonVersionedValue, EnginePreviewConflict,
    EnginePromotedEntity, EnginePromotionOutcome, EnginePromotionStrategy, EngineSpaceComparison,
    ExecutorError, HistoryItem, HistoryResult, JsonBatchGetItemResult, JsonBatchItemResult,
    JsonHistory, JsonHistoryItem, JsonHistoryRow, JsonIndexDefinition, JsonListPage,
    JsonSampleItem, JsonSampleRow, KvHistory, KvHistoryRow, KvKey, KvSample, KvScanRow,
    KvVersionedValue, MutationEffect, MutationEffectKind, Output, OutputJsonVersionedValue,
    PageInfo, PreviewConflictItem, ProductSpace, PromotedEntityItem, PromotionOutcomeItem,
    PromotionStrategy, SampleItem, ScanItem, SpaceComparisonItem, Timestamp, VersionedValue,
};

pub(super) fn bytes_from_key(key: &KvKey) -> Bytes {
    Bytes::from(key.as_bytes())
}

pub(super) fn branch_item(summary: &BranchSummary) -> BranchItem {
    BranchItem::new(
        summary.name().as_str().to_owned(),
        summary.branch_id().to_string(),
        summary.generation(),
        branch_status(summary.status()),
        summary.parent().map(|parent| {
            BranchParentItem::new(
                parent.name().as_str().to_owned(),
                parent.branch_id().to_string(),
                parent.generation(),
                parent.fork_version().as_u64(),
                parent.fork_timestamp().map(Timestamp::as_micros),
            )
        }),
        summary.merge_parent().map(|merge| {
            BranchMergeItem::new(
                merge.source_name().as_str().to_owned(),
                merge.source_branch_id().to_string(),
                merge.source_generation(),
                merge.merged_at().as_u64(),
                merge.merged_timestamp().map(Timestamp::as_micros),
            )
        }),
        summary.created_at().map(CommitVersion::as_u64),
        summary.deleted_at().map(CommitVersion::as_u64),
        summary.state_revision(),
    )
}

pub(super) const fn branch_status(status: EngineBranchStatus) -> BranchStatus {
    match status {
        EngineBranchStatus::Active => BranchStatus::Active,
        EngineBranchStatus::Deleted => BranchStatus::Deleted,
    }
}

pub(super) fn branch_cleanup_item(cleanup: BranchCleanupSummary) -> BranchCleanupItem {
    BranchCleanupItem::new(
        usize_to_u64(cleanup.removed_refs()),
        usize_to_u64(cleanup.releasable_tables()),
        usize_to_u64(cleanup.protected_tables()),
    )
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    // `usize` only exceeds `u64` on a >64-bit target, which Strata does not
    // support; saturating to `u64::MAX` keeps counts monotonic on the
    // theoretical overflow instead of panicking.
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn commit_receipt(outcome: CommitOutcome) -> CommitReceipt {
    CommitReceipt::new(
        outcome.version().as_u64(),
        outcome.timestamp().as_micros(),
        commit_durability(outcome.durability()),
        usize_to_u64(outcome.put_count()),
        usize_to_u64(outcome.delete_count()),
    )
    .with_committed_at(outcome.committed_at().map(Timestamp::as_micros))
}

const fn commit_durability(durability: strata_engine::CommitDurability) -> CommitDurability {
    match durability {
        strata_engine::CommitDurability::NotDurable => CommitDurability::NotDurable,
        strata_engine::CommitDurability::Standard => CommitDurability::Standard,
        strata_engine::CommitDurability::Always => CommitDurability::Always,
        _ => CommitDurability::Uncertain,
    }
}

pub(super) fn upsert_effect(existed: bool) -> MutationEffect {
    if existed {
        MutationEffect::updated()
    } else {
        MutationEffect::created()
    }
}

pub(super) fn update_effect(updated: bool) -> MutationEffect {
    if updated {
        MutationEffect::updated()
    } else {
        MutationEffect::not_found()
    }
}

pub(super) fn create_effect(created: bool) -> MutationEffect {
    if created {
        MutationEffect::created()
    } else {
        MutationEffect::new(false, MutationEffectKind::Unchanged, true, 0)
    }
}

pub(super) fn delete_effect(deleted: bool) -> MutationEffect {
    if deleted {
        MutationEffect::deleted()
    } else {
        MutationEffect::not_found()
    }
}

pub(super) fn bulk_delete_effect(deleted_count: u64) -> MutationEffect {
    if deleted_count == 0 {
        MutationEffect::not_found()
    } else {
        MutationEffect::new(true, MutationEffectKind::Deleted, true, deleted_count)
    }
}

pub(super) fn write_output(key: Bytes, effect: MutationEffect, outcome: CommitOutcome) -> Output {
    Output::WriteResult {
        key,
        effect,
        commit: commit_receipt(outcome),
    }
}

pub(super) fn delete_output(key: Bytes, deleted: bool, outcome: Option<CommitOutcome>) -> Output {
    Output::DeleteResult {
        key,
        effect: delete_effect(deleted),
        commit: outcome.map(commit_receipt),
    }
}

pub(super) fn batch_item_result(
    index: u64,
    key: Bytes,
    effect: MutationEffect,
    outcome: Option<CommitOutcome>,
) -> BatchItem<BatchItemResult> {
    BatchItem::ok(
        index,
        effect.applied(),
        Some(effect),
        outcome.map(commit_receipt),
        BatchItemResult::new(key),
    )
}

pub(super) fn batch_item_failed(
    index: u64,
    key: Bytes,
    error: ExecutorError,
) -> BatchItem<BatchItemResult> {
    BatchItem::failed(index, Some(BatchItemResult::new(key)), error.into_status())
}

pub(super) fn batch_get_result(
    index: u64,
    key: Bytes,
    value: Option<KvVersionedValue>,
) -> BatchItem<BatchGetItemResult> {
    match value {
        Some(value) => BatchItem::ok(
            index,
            false,
            None,
            None,
            BatchGetItemResult::new(
                key,
                Some(Bytes::from(value.value().as_bytes())),
                Some(value.version().as_u64()),
                Some(value.timestamp().as_micros()),
            ),
        ),
        None => BatchItem::miss(index, BatchGetItemResult::not_found(key)),
    }
}

pub(super) fn batch_get_failed(
    index: u64,
    key: Bytes,
    error: ExecutorError,
) -> BatchItem<BatchGetItemResult> {
    BatchItem::failed(
        index,
        Some(BatchGetItemResult::not_found(key)),
        error.into_status(),
    )
}

pub(super) fn batch_exists_item(
    index: u64,
    key: Bytes,
    exists: bool,
) -> BatchItem<BatchExistsItemResult> {
    BatchItem::ok(
        index,
        false,
        None,
        None,
        BatchExistsItemResult::new(key, exists),
    )
}

pub(super) fn batch_exists_failed(
    index: u64,
    key: Bytes,
    error: ExecutorError,
) -> BatchItem<BatchExistsItemResult> {
    BatchItem::failed(
        index,
        Some(BatchExistsItemResult::new(key, false)),
        error.into_status(),
    )
}

pub(super) fn versioned_value(value: &KvVersionedValue) -> VersionedValue {
    VersionedValue::new(
        Bytes::from(value.value().as_bytes()),
        value.version().as_u64(),
        value.timestamp().as_micros(),
    )
}

pub(super) fn history_result(history: &KvHistory) -> HistoryResult {
    HistoryResult::new(history.rows().iter().map(history_item).collect())
}

pub(super) fn history_item(row: &KvHistoryRow) -> HistoryItem {
    HistoryItem::new(
        row.value().map(|value| Bytes::from(value.as_bytes())),
        row.is_tombstone(),
        row.version().as_u64(),
        row.timestamp().as_micros(),
    )
    .with_committed_at(row.committed_at().map(Timestamp::as_micros))
}

pub(super) fn scan_item(row: &KvScanRow) -> ScanItem {
    ScanItem::new(
        bytes_from_key(row.key()),
        Bytes::from(row.value().as_bytes()),
        row.version().as_u64(),
        row.timestamp().as_micros(),
    )
}

pub(super) fn sample_item(row: &KvScanRow) -> SampleItem {
    SampleItem::new(
        bytes_from_key(row.key()),
        Bytes::from(row.value().as_bytes()),
        row.version().as_u64(),
        row.timestamp().as_micros(),
    )
}

pub(super) fn sample_output(sample: &KvSample) -> Output {
    Output::SampleResult {
        total_count: sample.total_count(),
        items: sample.rows().iter().map(sample_item).collect(),
        page: PageInfo::terminal(),
    }
}

pub(super) fn json_write_output(
    key: &str,
    effect: MutationEffect,
    outcome: CommitOutcome,
) -> Output {
    Output::JsonWriteResult {
        key: key.to_owned(),
        effect,
        commit: commit_receipt(outcome),
    }
}

pub(super) fn json_delete_output(
    key: &str,
    deleted: bool,
    outcome: Option<CommitOutcome>,
) -> Output {
    Output::JsonDeleteResult {
        key: key.to_owned(),
        effect: delete_effect(deleted),
        commit: outcome.map(commit_receipt),
    }
}

pub(super) fn json_value_output(value: EngineJsonValue) -> serde_json::Value {
    value.into_inner()
}

pub(super) fn json_versioned_value(value: &EngineJsonVersionedValue) -> OutputJsonVersionedValue {
    OutputJsonVersionedValue::new(
        value.value().clone().into_inner(),
        value.version().as_u64(),
        value.timestamp().as_micros(),
        value.document_version(),
    )
}

pub(super) fn json_history_items(history: &JsonHistory) -> Vec<JsonHistoryItem> {
    history.rows().iter().map(json_history_item).collect()
}

pub(super) fn json_history_item(row: &JsonHistoryRow) -> JsonHistoryItem {
    JsonHistoryItem::new(
        row.value().map(|value| value.clone().into_inner()),
        row.version().as_u64(),
        row.timestamp().as_micros(),
        row.document_version(),
        row.is_tombstone(),
    )
    .with_committed_at(row.committed_at().map(Timestamp::as_micros))
}

pub(super) fn json_batch_item_result(
    index: u64,
    effect: MutationEffect,
    outcome: Option<CommitOutcome>,
    document_version: Option<u64>,
) -> BatchItem<JsonBatchItemResult> {
    BatchItem::ok(
        index,
        effect.applied(),
        Some(effect),
        outcome.map(commit_receipt),
        JsonBatchItemResult::new(document_version),
    )
}

pub(super) fn json_batch_item_failed(
    index: u64,
    error: ExecutorError,
) -> BatchItem<JsonBatchItemResult> {
    BatchItem::failed(
        index,
        Some(JsonBatchItemResult::new(None)),
        error.into_status(),
    )
}

pub(super) fn json_batch_get_result(
    index: u64,
    value: Option<EngineJsonVersionedValue>,
) -> BatchItem<JsonBatchGetItemResult> {
    match value {
        Some(value) => BatchItem::ok(
            index,
            false,
            None,
            None,
            JsonBatchGetItemResult::new(
                Some(value.value().clone().into_inner()),
                Some(value.version().as_u64()),
                Some(value.timestamp().as_micros()),
                Some(value.document_version()),
            ),
        ),
        None => BatchItem::miss(index, JsonBatchGetItemResult::not_found()),
    }
}

pub(super) fn json_batch_get_failed(
    index: u64,
    error: ExecutorError,
) -> BatchItem<JsonBatchGetItemResult> {
    BatchItem::failed(
        index,
        Some(JsonBatchGetItemResult::not_found()),
        error.into_status(),
    )
}

pub(super) fn json_list_output(page: &JsonListPage) -> Output {
    Output::JsonListResult {
        items: page
            .document_ids()
            .iter()
            .map(|id| id.as_str().to_owned())
            .collect(),
        page: PageInfo::new(
            page.has_more(),
            page.cursor().map(|cursor| cursor.as_str().to_owned()),
        ),
    }
}

pub(super) fn json_sample_output(sample: &EngineJsonSample) -> Output {
    Output::JsonSampleResult {
        total_count: sample.total_count(),
        items: sample.rows().iter().map(json_sample_item).collect(),
        page: PageInfo::terminal(),
    }
}

pub(super) fn json_sample_item(row: &JsonSampleRow) -> JsonSampleItem {
    JsonSampleItem::new(
        row.document_id().as_str().to_owned(),
        row.value().clone().into_inner(),
        row.version().as_u64(),
        row.timestamp().as_micros(),
        row.document_version(),
    )
}

pub(super) fn json_index_definition(definition: &EngineJsonIndexDefinition) -> JsonIndexDefinition {
    JsonIndexDefinition::new(
        definition.name().as_str().to_owned(),
        definition.space().as_str().to_owned(),
        definition.field_path().to_string(),
        output_json_index_type(definition.index_type()),
        definition.created_version(),
        definition.created_timestamp(),
    )
}

pub(super) fn branch_comparison(comparison: &EngineBranchComparison) -> BranchComparisonItem {
    let spaces = comparison
        .comparisons()
        .iter()
        .map(|space: &EngineSpaceComparison| {
            SpaceComparisonItem::new(
                space.space().as_str().to_owned(),
                compared_capability(space.capability()),
                space.added().iter().map(compared_entity).collect(),
                space.removed().iter().map(compared_entity).collect(),
                space.modified().iter().map(compared_entity).collect(),
            )
        })
        .collect();
    BranchComparisonItem::new(
        comparison.branch_a().as_str().to_owned(),
        comparison.branch_b().as_str().to_owned(),
        spaces,
    )
}

fn compared_entity(entity: &EngineComparedEntity) -> ComparedEntityItem {
    ComparedEntityItem::new(Bytes::from(entity.identity()), entity.version().as_u64())
}

const fn compared_capability(capability: EngineComparedCapability) -> ComparedCapability {
    match capability {
        EngineComparedCapability::KeyValue => ComparedCapability::KeyValue,
        EngineComparedCapability::Json => ComparedCapability::Json,
        EngineComparedCapability::Vector => ComparedCapability::Vector,
        EngineComparedCapability::VectorCollection => ComparedCapability::VectorCollection,
        EngineComparedCapability::Event => ComparedCapability::Event,
        EngineComparedCapability::GraphMetadata => ComparedCapability::GraphMetadata,
        EngineComparedCapability::GraphNode => ComparedCapability::GraphNode,
        EngineComparedCapability::GraphEdge => ComparedCapability::GraphEdge,
        EngineComparedCapability::GraphOntology => ComparedCapability::GraphOntology,
    }
}

pub(super) fn branch_promotion(outcome: &EnginePromotionOutcome) -> PromotionOutcomeItem {
    PromotionOutcomeItem::new(
        outcome.source().as_str().to_owned(),
        outcome.target().as_str().to_owned(),
        outcome.branch_point().as_u64(),
        wire_promotion_strategy(outcome.strategy()),
        outcome.target_version().map(CommitVersion::as_u64),
        outcome.target_timestamp().map(Timestamp::as_micros),
        outcome.applied().iter().map(promoted_entity).collect(),
        outcome.deleted().iter().map(promoted_entity).collect(),
        outcome.conflicts().iter().map(preview_conflict).collect(),
        spaces_covered(outcome.spaces_covered()),
        capabilities(outcome.capabilities_covered()),
        capabilities(outcome.capabilities_unsupported()),
        outcome
            .derived_state()
            .iter()
            .map(derived_state_report)
            .collect(),
    )
}

pub(super) fn branch_preview(preview: &EngineBranchPreview) -> BranchPreviewItem {
    BranchPreviewItem::new(
        preview.source().as_str().to_owned(),
        preview.target().as_str().to_owned(),
        preview.branch_point().as_u64(),
        wire_promotion_strategy(preview.strategy()),
        preview.conflicts().iter().map(preview_conflict).collect(),
        spaces_covered(preview.spaces_covered()),
        capabilities(preview.capabilities_covered()),
        capabilities(preview.capabilities_unsupported()),
        preview
            .derived_state()
            .iter()
            .map(derived_state_report)
            .collect(),
    )
}

fn spaces_covered(spaces: &[ProductSpace]) -> Vec<String> {
    spaces
        .iter()
        .map(|space| space.as_str().to_owned())
        .collect()
}

fn capabilities(capabilities: &[EngineComparedCapability]) -> Vec<ComparedCapability> {
    capabilities
        .iter()
        .map(|capability| compared_capability(*capability))
        .collect()
}

fn derived_state_report(report: &EngineDerivedStateReport) -> DerivedStateReportItem {
    DerivedStateReportItem::new(
        compared_capability(report.capability()),
        wire_derived_disposition(report.disposition()),
    )
}

const fn wire_derived_disposition(
    disposition: EngineDerivedStateDisposition,
) -> DerivedStateDisposition {
    match disposition {
        EngineDerivedStateDisposition::Current => DerivedStateDisposition::Current,
        EngineDerivedStateDisposition::RebuildRequired => DerivedStateDisposition::RebuildRequired,
    }
}

fn promoted_entity(entity: &EnginePromotedEntity) -> PromotedEntityItem {
    PromotedEntityItem::new(
        compared_capability(entity.capability()),
        entity.space().as_str().to_owned(),
        Bytes::from(entity.identity()),
        entity.value().map(Bytes::from),
    )
}

fn preview_conflict(conflict: &EnginePreviewConflict) -> PreviewConflictItem {
    PreviewConflictItem::new(
        compared_capability(conflict.capability()),
        conflict.space().as_str().to_owned(),
        Bytes::from(conflict.identity()),
        conflict.source_value().map(Bytes::from),
        conflict.target_value().map(Bytes::from),
        wire_conflict_kind(conflict.kind()),
        wire_conflict_strategy_result(conflict.strategy_result()),
    )
}

/// Maps the wire promotion strategy onto the engine strategy (command → engine).
pub(super) const fn engine_promotion_strategy(
    strategy: PromotionStrategy,
) -> EnginePromotionStrategy {
    match strategy {
        PromotionStrategy::Strict => EnginePromotionStrategy::Strict,
        PromotionStrategy::SourceWins => EnginePromotionStrategy::SourceWins,
    }
}

const fn wire_promotion_strategy(strategy: EnginePromotionStrategy) -> PromotionStrategy {
    match strategy {
        EnginePromotionStrategy::Strict => PromotionStrategy::Strict,
        EnginePromotionStrategy::SourceWins => PromotionStrategy::SourceWins,
    }
}

const fn wire_conflict_kind(kind: EngineConflictKind) -> ConflictKind {
    match kind {
        EngineConflictKind::ValueDivergence => ConflictKind::ValueDivergence,
        EngineConflictKind::ModifyDeleteDivergence => ConflictKind::ModifyDeleteDivergence,
        EngineConflictKind::IncompatibleCollection => ConflictKind::IncompatibleCollection,
    }
}

const fn wire_conflict_strategy_result(
    result: EngineConflictStrategyResult,
) -> ConflictStrategyResult {
    match result {
        EngineConflictStrategyResult::Refused => ConflictStrategyResult::Refused,
        EngineConflictStrategyResult::SourceWins => ConflictStrategyResult::SourceWins,
    }
}

#[cfg(test)]
mod tests {
    use strata_engine::{
        BranchName, CacheOpenOptions, Database, DatabaseOpenOutcome, KvKey, KvValue, ProductSpace,
        PromotionStrategy,
    };

    use super::branch_promotion;
    use crate::types::ComparedCapability;

    #[test]
    fn incompatible_collection_conflict_kind_maps_to_the_wire() {
        assert_eq!(
            super::wire_conflict_kind(super::EngineConflictKind::IncompatibleCollection),
            crate::types::ConflictKind::IncompatibleCollection
        );
    }

    #[test]
    fn branch_promotion_carries_coverage_onto_the_wire_item() {
        let mut database = Database::open_cache(CacheOpenOptions::new())
            .map(DatabaseOpenOutcome::into_database)
            .expect("cache open");
        database
            .branches()
            .expect("branch service")
            .fork_current(
                &BranchName::new("default").expect("branch"),
                BranchName::new("feature").expect("branch"),
            )
            .expect("fork");
        database
            .kv(
                BranchName::new("feature").expect("branch"),
                ProductSpace::new("default").expect("space"),
            )
            .expect("kv opens")
            .put(KvKey::new(b"k").expect("key"), KvValue::new(b"v"))
            .expect("put");
        let outcome = database
            .branches()
            .expect("branch service")
            .promote(
                &BranchName::new("feature").expect("branch"),
                &BranchName::new("default").expect("branch"),
                PromotionStrategy::Strict,
            )
            .expect("promote");

        // The converter must carry the engine outcome's coverage onto the wire
        // item; a dropped field mapping surfaces here.
        let item = branch_promotion(&outcome);
        assert!(item.spaces_covered().iter().any(|space| space == "default"));
        assert!(item
            .capabilities_covered()
            .contains(&ComparedCapability::KeyValue));
        assert!(item
            .capabilities_unsupported()
            .contains(&ComparedCapability::Event));
        assert!(item.target_version().is_some());
        assert!(item.target_timestamp().is_some());
    }

    #[test]
    fn branch_item_carries_promotion_lineage_onto_the_wire() {
        let mut database = Database::open_cache(CacheOpenOptions::new())
            .map(DatabaseOpenOutcome::into_database)
            .expect("cache open");
        let default = || BranchName::new("default").expect("branch");
        let feature = || BranchName::new("feature").expect("branch");

        // Fork, drop, and re-fork `feature` so its generation is 2 — the wire item
        // must carry the real generation, not a trivial constant.
        database
            .branches()
            .expect("branch service")
            .fork_current(&default(), feature())
            .expect("fork gen1");
        database
            .branches()
            .expect("branch service")
            .delete(&feature())
            .expect("delete gen1");
        database
            .branches()
            .expect("branch service")
            .fork_current(&default(), feature())
            .expect("fork gen2");
        database
            .kv(feature(), ProductSpace::new("default").expect("space"))
            .expect("kv opens")
            .put(KvKey::new(b"k").expect("key"), KvValue::new(b"v"))
            .expect("put");
        database
            .branches()
            .expect("branch service")
            .promote(&feature(), &default(), PromotionStrategy::Strict)
            .expect("promote");

        // The target summary records the merge edge; the converter must carry every
        // field faithfully onto the wire item, not drop or flatten it.
        let feature_summary = database
            .branches()
            .expect("branch service")
            .get(&feature())
            .expect("source summary");
        let summary = database
            .branches()
            .expect("branch service")
            .get(&default())
            .expect("target summary");
        let engine_merge = summary
            .merge_parent()
            .expect("engine records the merge edge")
            .clone();
        assert_eq!(
            engine_merge.source_generation(),
            2,
            "the re-forked feature is generation 2"
        );

        let item = super::branch_item(&summary);
        let merge = item
            .merge_parent()
            .expect("promotion lineage present on the wire item");
        assert_eq!(merge.source_name(), "feature");
        assert_eq!(merge.source_generation(), feature_summary.generation());
        assert_eq!(
            merge.source_branch_id(),
            feature_summary.branch_id().to_string()
        );
        assert_eq!(merge.merged_at(), engine_merge.merged_at().as_u64());
        assert_eq!(
            merge.merged_timestamp(),
            engine_merge
                .merged_timestamp()
                .map(super::Timestamp::as_micros)
        );
    }
}
