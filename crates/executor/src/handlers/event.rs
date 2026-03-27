//! Event command handlers (4 MVP).
//!
//! MVP: append, read, get_by_type, len

use std::sync::Arc;

use crate::bridge::{self, validate_value, Primitives};
use crate::convert::convert_result;
use crate::types::{BranchId, VersionedValue};
use crate::{Output, Result};

use super::require_branch_exists;

// =============================================================================
// Individual Handlers (4 MVP)
// =============================================================================

/// Handle EventAppend command.
pub fn event_append(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    event_type: String,
    payload: strata_core::Value,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    convert_result(validate_value(&payload, &p.limits))?;

    // Extract text before payload is consumed
    let text = super::embed_hook::extract_text(&payload);

    let version = convert_result(
        p.event
            .append(&core_branch_id, &space, &event_type, payload),
    )?;

    // Best-effort auto-embed after successful write
    let sequence = bridge::extract_version(&version);
    if let Some(ref text) = text {
        let event_key = sequence.to_string();
        super::embed_hook::maybe_embed_text(
            p,
            core_branch_id,
            &space,
            super::embed_hook::SHADOW_EVENT,
            &event_key,
            text,
            strata_core::EntityRef::event(core_branch_id, sequence),
        );
    }

    Ok(Output::EventAppendResult {
        sequence,
        event_type,
    })
}

/// Handle EventGet command.
pub fn event_get(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    sequence: u64,
) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let event = convert_result(p.event.get(&core_branch_id, &space, sequence))?;

    let result = event.map(|e| VersionedValue {
        value: e.value.payload,
        version: bridge::extract_version(&e.version),
        timestamp: e.value.timestamp.into(),
    });

    Ok(Output::MaybeVersioned(result))
}

/// Handle EventGet with as_of timestamp (time-travel read).
///
/// Returns the event at the given sequence number only if it existed at or
/// before the given timestamp. Events are immutable, so this checks whether
/// the event's timestamp <= as_of_ts.
pub fn event_get_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    sequence: u64,
    as_of_ts: u64,
) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let event = convert_result(p.event.get(&core_branch_id, &space, sequence))?;

    let result = event.and_then(|e| {
        if e.value.timestamp.as_micros() <= as_of_ts {
            Some(VersionedValue {
                value: e.value.payload,
                version: bridge::extract_version(&e.version),
                timestamp: e.value.timestamp.into(),
            })
        } else {
            None // Event was appended after as_of_ts
        }
    });

    Ok(Output::MaybeVersioned(result))
}

/// Handle EventGetByType command.
pub fn event_get_by_type(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    event_type: String,
    limit: Option<u64>,
    after_sequence: Option<u64>,
) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let events = convert_result(p.event.get_by_type(
        &core_branch_id,
        &space,
        &event_type,
        after_sequence,
        limit.map(|l| l as usize),
    ))
    .map_err(|e| enrich_event_error(p, &core_branch_id, &space, e))?;

    let versioned: Vec<VersionedValue> = events
        .into_iter()
        .map(|e| VersionedValue {
            value: e.value.payload.clone(),
            version: bridge::extract_version(&e.version),
            timestamp: e.value.timestamp.into(),
        })
        .collect();

    Ok(Output::VersionedValues(versioned))
}

/// Handle EventGetByType with as_of timestamp (time-travel read).
///
/// Returns only events whose timestamp <= as_of_ts.
/// Timestamp filtering is pushed down to the engine level for efficiency.
pub fn event_get_by_type_at(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    event_type: String,
    as_of_ts: u64,
) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let events =
        convert_result(
            p.event
                .get_by_type_at(&core_branch_id, &space, &event_type, as_of_ts),
        )
        .map_err(|e| enrich_event_error(p, &core_branch_id, &space, e))?;

    let versioned: Vec<VersionedValue> = events
        .into_iter()
        .map(|e| VersionedValue {
            value: e.value.payload.clone(),
            version: bridge::extract_version(&e.version),
            timestamp: e.value.timestamp.into(),
        })
        .collect();

    Ok(Output::VersionedValues(versioned))
}

/// Handle EventBatchAppend command.
///
/// Pre-validates all entries, passes valid ones to the engine, and merges
/// validation errors with engine results into `Vec<BatchItemResult>`.
pub fn event_batch_append(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    entries: Vec<crate::types::BatchEventEntry>,
) -> Result<Output> {
    require_branch_exists(p, &branch)?;
    let core_branch_id = bridge::to_core_branch_id(&branch)?;

    if entries.is_empty() {
        return Ok(Output::BatchResults(Vec::new()));
    }

    let n = entries.len();
    let mut results: Vec<crate::types::BatchItemResult> = vec![
        crate::types::BatchItemResult {
            version: None,
            error: None,
        };
        n
    ];

    // Pre-validate value sizes at executor level
    let mut valid_entries: Vec<(usize, String, strata_core::Value)> = Vec::with_capacity(n);
    for (i, entry) in entries.into_iter().enumerate() {
        if let Err(e) = validate_value(&entry.payload, &p.limits) {
            results[i].error = Some(e.to_string());
            continue;
        }
        valid_entries.push((i, entry.event_type, entry.payload));
    }

    if valid_entries.is_empty() {
        return Ok(Output::BatchResults(results));
    }

    // Extract text for embed hooks
    let embed_data: Vec<(usize, Option<String>)> = valid_entries
        .iter()
        .map(|(idx, _, payload)| {
            let text = super::embed_hook::extract_text(payload);
            (*idx, text)
        })
        .collect();

    // Build engine entries
    let engine_entries: Vec<(String, strata_core::Value)> = valid_entries
        .into_iter()
        .map(|(_, event_type, payload)| (event_type, payload))
        .collect();

    let engine_results = convert_result(p.event.batch_append(
        &core_branch_id,
        &space,
        engine_entries,
    ))?;

    // Merge engine results
    for (j, (orig_idx, _)) in embed_data.iter().enumerate() {
        match &engine_results[j] {
            Ok(version) => {
                let seq = bridge::extract_version(version);
                results[*orig_idx].version = Some(seq);

                // Fire embed hook for successful items
                if let Some(ref text) = embed_data[j].1 {
                    let event_key = seq.to_string();
                    super::embed_hook::maybe_embed_text(
                        p,
                        core_branch_id,
                        &space,
                        super::embed_hook::SHADOW_EVENT,
                        &event_key,
                        text,
                        strata_core::EntityRef::event(core_branch_id, seq),
                    );
                }
            }
            Err(e) => {
                results[*orig_idx].error = Some(e.clone());
            }
        }
    }

    Ok(Output::BatchResults(results))
}

/// Handle EventRange command — sequence-based range query with pagination.
pub fn event_range(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    start_seq: u64,
    end_seq: Option<u64>,
    limit: Option<u64>,
    reverse: bool,
    event_type: Option<String>,
) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let limit_usize = limit.map(|l| l as usize);

    // Request one extra to detect has_more (saturating to avoid overflow)
    let fetch_limit = limit_usize.map(|l| l.saturating_add(1));

    let mut events = convert_result(p.event.range(
        &core_branch_id,
        &space,
        start_seq,
        end_seq,
        fetch_limit,
        reverse,
        event_type.as_deref(),
    ))?;

    let has_more = if let Some(l) = limit_usize {
        if events.len() > l {
            events.truncate(l);
            true
        } else {
            false
        }
    } else {
        false
    };

    // Build cursor from the last returned event's sequence
    let next_cursor = if has_more {
        events.last().map(|e| {
            let seq = bridge::extract_version(&e.version);
            if reverse {
                // Next page starts before this sequence
                format!("seq:{}", seq.saturating_sub(1))
            } else {
                // Next page starts after this sequence
                format!("seq:{}", seq + 1)
            }
        })
    } else {
        None
    };

    let versioned: Vec<VersionedValue> = events
        .into_iter()
        .map(|e| VersionedValue {
            value: e.value.payload,
            version: bridge::extract_version(&e.version),
            timestamp: e.value.timestamp.into(),
        })
        .collect();

    Ok(Output::EventRangeResult {
        events: versioned,
        has_more,
        next_cursor,
    })
}

/// Handle EventRangeByTime command — timestamp-based range query with pagination.
pub fn event_range_by_time(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    start_ts: u64,
    end_ts: Option<u64>,
    limit: Option<u64>,
    reverse: bool,
    event_type: Option<String>,
) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let limit_usize = limit.map(|l| l as usize);

    // Request one extra to detect has_more (saturating to avoid overflow)
    let fetch_limit = limit_usize.map(|l| l.saturating_add(1));

    let mut events = convert_result(p.event.range_by_time(
        &core_branch_id,
        &space,
        start_ts,
        end_ts,
        fetch_limit,
        reverse,
        event_type.as_deref(),
    ))?;

    let has_more = if let Some(l) = limit_usize {
        if events.len() > l {
            events.truncate(l);
            true
        } else {
            false
        }
    } else {
        false
    };

    // Build cursor from the last returned event's timestamp
    let next_cursor = if has_more {
        events.last().map(|e| {
            let ts: u64 = e.value.timestamp.into();
            let seq = bridge::extract_version(&e.version);
            if reverse {
                // Next page: events before this timestamp
                format!("ts:{}:seq:{}", ts, seq)
            } else {
                // Next page: events after this timestamp
                format!("ts:{}:seq:{}", ts, seq)
            }
        })
    } else {
        None
    };

    let versioned: Vec<VersionedValue> = events
        .into_iter()
        .map(|e| VersionedValue {
            value: e.value.payload,
            version: bridge::extract_version(&e.version),
            timestamp: e.value.timestamp.into(),
        })
        .collect();

    Ok(Output::EventRangeResult {
        events: versioned,
        has_more,
        next_cursor,
    })
}

/// Handle EventListTypes command.
pub fn event_list_types(p: &Arc<Primitives>, branch: BranchId, space: String) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let types = convert_result(p.event.list_types(&core_branch_id, &space))?;
    Ok(Output::Keys(types))
}

/// Enrich a StreamNotFound error with fuzzy-match suggestions on event types.
fn enrich_event_error(
    p: &Arc<Primitives>,
    branch_id: &strata_core::types::BranchId,
    space: &str,
    err: crate::Error,
) -> crate::Error {
    match err {
        crate::Error::StreamNotFound { stream, hint: None } => {
            let candidates = p.event.list_types(branch_id, space).unwrap_or_default();
            let hint = crate::suggest::format_hint("event types", &candidates, &stream, 2);
            crate::Error::StreamNotFound { stream, hint }
        }
        other => other,
    }
}

/// Handle EventLen command.
pub fn event_len(p: &Arc<Primitives>, branch: BranchId, space: String) -> Result<Output> {
    let core_branch_id = bridge::to_core_branch_id(&branch)?;
    let count = convert_result(p.event.len(&core_branch_id, &space))?;
    Ok(Output::Uint(count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::Version;

    #[test]
    fn test_bridge_extract_version() {
        assert_eq!(bridge::extract_version(&Version::Sequence(42)), 42);
    }
}
