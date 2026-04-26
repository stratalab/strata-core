use std::sync::Arc;

use crate::bridge::{
    extract_version, require_branch_exists, to_core_branch_id, validate_value, Primitives,
};
use crate::convert::convert_result;
use crate::handlers::embed_runtime;
use crate::{BatchItemResult, BranchId, Output, Result, ScanDirection, VersionedValue};

pub(crate) fn batch_append(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::BatchEventEntry>,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    if entries.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let mut results = vec![
        BatchItemResult {
            version: None,
            error: None,
        };
        entries.len()
    ];
    let mut valid_entries = Vec::with_capacity(entries.len());

    for (index, entry) in entries.into_iter().enumerate() {
        if let Err(error) = validate_value(&entry.payload, &primitives.limits) {
            results[index].error = Some(error.to_string());
            continue;
        }
        valid_entries.push((index, entry.event_type, entry.payload));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    let embed_data: Vec<(usize, Option<String>)> = valid_entries
        .iter()
        .map(|(index, _, payload)| (*index, embed_runtime::extract_text(payload)))
        .collect();

    let engine_entries: Vec<_> = valid_entries
        .iter()
        .map(|(_, event_type, payload)| (event_type.clone(), payload.clone()))
        .collect();
    let engine_results = convert_result(primitives.event.batch_append(
        &branch_id,
        &space,
        engine_entries,
    ))?;

    for (engine_index, (result_index, _, _)) in valid_entries.iter().enumerate() {
        match &engine_results[engine_index] {
            Ok(version) => {
                let sequence = extract_version(version);
                results[*result_index].version = Some(sequence);
                if let Some(text) = embed_data[engine_index].1.as_deref() {
                    let event_key = sequence.to_string();
                    embed_runtime::maybe_embed_text(
                        primitives,
                        branch_id,
                        &space,
                        embed_runtime::SHADOW_EVENT,
                        &event_key,
                        text,
                        strata_core::EntityRef::event(branch_id, &space, sequence),
                    );
                }
            }
            Err(error) => results[*result_index].error = Some(error.clone()),
        }
    }

    Ok(Output::BatchResults(results))
}

pub(crate) fn append(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    event_type: String,
    payload: strata_core::Value,
) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;
    convert_result(validate_value(&payload, &primitives.limits))?;
    let text = embed_runtime::extract_text(&payload);
    let version = convert_result(primitives.event.append(
        &branch_id,
        &space,
        &event_type,
        payload,
    ))?;
    let sequence = extract_version(&version);
    if let Some(text) = text.as_deref() {
        let event_key = sequence.to_string();
        embed_runtime::maybe_embed_text(
            primitives,
            branch_id,
            &space,
            embed_runtime::SHADOW_EVENT,
            &event_key,
            text,
            strata_core::EntityRef::event(branch_id, &space, sequence),
        );
    }
    Ok(Output::EventAppendResult {
        sequence,
        event_type,
    })
}

pub(crate) fn get(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    sequence: u64,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let event = match as_of {
        Some(as_of) => convert_result(primitives.event.get(&branch_id, &space, sequence))?
            .and_then(|event| {
                if event.value.timestamp.as_micros() <= as_of {
                    Some(event)
                } else {
                    None
                }
            }),
        None => convert_result(primitives.event.get(&branch_id, &space, sequence))?,
    };

    Ok(Output::MaybeVersioned(event.map(|event| VersionedValue {
        value: event.value.payload,
        version: extract_version(&event.version),
        timestamp: event.value.timestamp.into(),
    })))
}

pub(crate) fn get_by_type(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    event_type: String,
    limit: Option<u64>,
    after_sequence: Option<u64>,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let events = match as_of {
        Some(as_of) => convert_result(primitives.event.get_by_type_at(
            &branch_id,
            &space,
            &event_type,
            as_of,
        ))?,
        None => convert_result(primitives.event.get_by_type(
            &branch_id,
            &space,
            &event_type,
            after_sequence,
            limit.map(|v| v as usize),
        ))?,
    };

    let versioned = events
        .into_iter()
        .filter(|event| after_sequence.is_none_or(|after| extract_version(&event.version) > after))
        .take(limit.unwrap_or(u64::MAX) as usize)
        .map(|event| VersionedValue {
            value: event.value.payload,
            version: extract_version(&event.version),
            timestamp: event.value.timestamp.into(),
        })
        .collect();

    Ok(Output::VersionedValues(versioned))
}

pub(crate) fn len(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let count = match as_of {
        Some(as_of) => convert_result(primitives.event.len_at(&branch_id, &space, as_of))?,
        None => convert_result(primitives.event.len(&branch_id, &space))?,
    };
    Ok(Output::Uint(count))
}

pub(crate) fn range(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    start_seq: u64,
    end_seq: Option<u64>,
    limit: Option<u64>,
    direction: ScanDirection,
    event_type: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let limit = limit.map(|v| v as usize);
    let reverse = matches!(direction, ScanDirection::Reverse);
    let fetch_limit = limit.map(|v| v.saturating_add(1));

    let mut events = convert_result(primitives.event.range(
        &branch_id,
        &space,
        start_seq,
        end_seq,
        fetch_limit,
        reverse,
        event_type.as_deref(),
    ))?;

    let has_more = if let Some(limit) = limit {
        if events.len() > limit {
            events.truncate(limit);
            true
        } else {
            false
        }
    } else {
        false
    };

    let next_cursor = if has_more {
        events.last().map(|event| {
            let seq = extract_version(&event.version);
            if reverse {
                format!("seq:{}", seq.saturating_sub(1))
            } else {
                format!("seq:{}", seq + 1)
            }
        })
    } else {
        None
    };

    Ok(Output::EventRangeResult {
        events: events
            .into_iter()
            .map(|event| VersionedValue {
                value: event.value.payload,
                version: extract_version(&event.version),
                timestamp: event.value.timestamp.into(),
            })
            .collect(),
        has_more,
        next_cursor,
    })
}

pub(crate) fn range_by_time(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    start_ts: u64,
    end_ts: Option<u64>,
    limit: Option<u64>,
    direction: ScanDirection,
    event_type: Option<String>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let limit = limit.map(|v| v as usize);
    let reverse = matches!(direction, ScanDirection::Reverse);
    let fetch_limit = limit.map(|v| v.saturating_add(1));

    let mut events = convert_result(primitives.event.range_by_time(
        &branch_id,
        &space,
        start_ts,
        end_ts,
        fetch_limit,
        reverse,
        event_type.as_deref(),
    ))?;

    let has_more = if let Some(limit) = limit {
        if events.len() > limit {
            events.truncate(limit);
            true
        } else {
            false
        }
    } else {
        false
    };

    let next_cursor = if has_more {
        events.last().map(|event| {
            let ts: u64 = event.value.timestamp.into();
            let seq = extract_version(&event.version);
            format!("ts:{}:seq:{}", ts, seq)
        })
    } else {
        None
    };

    Ok(Output::EventRangeResult {
        events: events
            .into_iter()
            .map(|event| VersionedValue {
                value: event.value.payload,
                version: extract_version(&event.version),
                timestamp: event.value.timestamp.into(),
            })
            .collect(),
        has_more,
        next_cursor,
    })
}

pub(crate) fn list_types(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let types = match as_of {
        Some(as_of) => convert_result(primitives.event.list_types_at(&branch_id, &space, as_of))?,
        None => convert_result(primitives.event.list_types(&branch_id, &space))?,
    };
    Ok(Output::Keys(types))
}

pub(crate) fn list(
    primitives: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    event_type: Option<String>,
    limit: Option<u64>,
    as_of: Option<u64>,
) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;
    let events = convert_result(primitives.event.list_at(
        &branch_id,
        &space,
        event_type.as_deref(),
        as_of.unwrap_or(u64::MAX),
        limit.map(|v| v as usize),
    ))?;

    Ok(Output::VersionedValues(
        events
            .into_iter()
            .map(|event| VersionedValue {
                value: event.payload,
                version: event.sequence,
                timestamp: event.timestamp.into(),
            })
            .collect(),
    ))
}
