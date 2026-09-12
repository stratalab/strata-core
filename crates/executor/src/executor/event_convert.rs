use super::{
    commit_receipt, engine_error_status, BatchEventEntry, BatchItem, CommitOutcome,
    EngineEventAppendOutcome, EngineEventBatchAppendEntry, EngineEventBatchAppendItemOutcome,
    EngineEventChainVerification, EngineEventPayload, EngineEventRangeDirection,
    EngineEventRangePage, EngineEventSequence, EngineEventType, EngineEventVersionedRecord,
    EventBatchAppendItemResult, EventData, EventRangeDirection, EventVersionedData, ExecutorError,
    MutationEffect, Output, OutputEventChainVerification, PageInfo,
};

pub(super) fn engine_event_type(event_type: String) -> Result<EngineEventType, ExecutorError> {
    EngineEventType::new(event_type).map_err(ExecutorError::from)
}

pub(super) fn optional_engine_event_type(
    event_type: Option<String>,
) -> Result<Option<EngineEventType>, ExecutorError> {
    event_type.map(engine_event_type).transpose()
}

pub(super) fn event_payload(
    payload: serde_json::Value,
) -> Result<EngineEventPayload, ExecutorError> {
    EngineEventPayload::new(payload).map_err(ExecutorError::from)
}

pub(super) const fn event_sequence(sequence: u64) -> EngineEventSequence {
    EngineEventSequence::new(sequence)
}

pub(super) fn event_batch_entry(entry: BatchEventEntry) -> EngineEventBatchAppendEntry {
    let (event_type, payload) = entry.into_parts();
    EngineEventBatchAppendEntry::from_raw(event_type, payload)
}

pub(super) const fn engine_event_direction(
    direction: EventRangeDirection,
) -> EngineEventRangeDirection {
    match direction {
        EventRangeDirection::Forward => EngineEventRangeDirection::Forward,
        EventRangeDirection::Reverse => EngineEventRangeDirection::Reverse,
    }
}

pub(super) fn event_append_output(outcome: &EngineEventAppendOutcome) -> Output {
    Output::EventAppendResult {
        sequence: outcome.sequence().as_u64(),
        event_type: outcome.event_type().as_str().to_owned(),
        effect: MutationEffect::created(),
        commit: commit_receipt(outcome.commit()),
    }
}

pub(super) fn event_records(records: &[EngineEventVersionedRecord]) -> Vec<EventVersionedData> {
    records.iter().map(event_versioned_data).collect()
}

pub(super) fn event_versioned_data(record: &EngineEventVersionedRecord) -> EventVersionedData {
    EventVersionedData::new(
        event_data(record),
        record.version().as_u64(),
        record.commit_timestamp().as_micros(),
    )
}

pub(super) fn event_data(record: &EngineEventVersionedRecord) -> EventData {
    EventData::new(
        record.sequence().as_u64(),
        record.event_type().as_str().to_owned(),
        record.payload().as_inner().clone(),
        record.timestamp().as_micros(),
        hash_hex(record.previous_hash()),
        hash_hex(record.hash()),
    )
}

pub(super) fn hash_hex(hash: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(hash.len().saturating_mul(2));
    for byte in hash {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(super) fn event_range_output(page: &EngineEventRangePage) -> Output {
    Output::EventRangeResult {
        items: event_records(page.events()),
        page: PageInfo::new(
            page.has_more(),
            page.cursor().map(EngineEventSequence::as_u64),
        ),
    }
}

pub(super) fn event_batch_append_item_result(
    index: u64,
    item: &EngineEventBatchAppendItemOutcome,
    batch_commit: Option<CommitOutcome>,
) -> BatchItem<EventBatchAppendItemResult> {
    if let Some(error) = item.error_status() {
        return BatchItem::failed(
            index,
            Some(EventBatchAppendItemResult::new(None, None)),
            engine_error_status(error),
        );
    }
    let commit = item
        .sequence()
        .and_then(|_| batch_commit.map(commit_receipt));
    let effect = item.sequence().map(|_| MutationEffect::created());
    BatchItem::ok(
        index,
        effect.is_some_and(|value| value.applied()),
        effect,
        commit,
        EventBatchAppendItemResult::new(
            item.sequence().map(EngineEventSequence::as_u64),
            item.event_type()
                .map(|event_type| event_type.as_str().to_owned()),
        ),
    )
}

pub(super) fn event_chain_verification(
    verification: &EngineEventChainVerification,
) -> OutputEventChainVerification {
    OutputEventChainVerification::new(
        verification.is_valid(),
        verification.length(),
        verification
            .first_invalid()
            .map(EngineEventSequence::as_u64),
        verification.error_message().map(str::to_owned),
    )
}
