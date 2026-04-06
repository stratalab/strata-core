//! Event log operations (4 MVP).
//!
//! MVP: append, read, get_by_type, len

use super::Strata;
use crate::types::*;
use crate::{Command, Error, Output, Result, Value};

impl Strata {
    // =========================================================================
    // Event Operations (4 MVP)
    // =========================================================================

    /// Append an event to the log.
    pub fn event_append(&self, event_type: &str, payload: Value) -> Result<u64> {
        match self.execute_cmd(Command::EventAppend {
            branch: self.branch_id(),
            space: self.space_id(),
            event_type: event_type.to_string(),
            payload,
        })? {
            Output::EventAppendResult { sequence, .. } => Ok(sequence),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventAppend".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Read a specific event by sequence number.
    pub fn event_get(&self, sequence: u64) -> Result<Option<VersionedValue>> {
        match self.execute_cmd(Command::EventGet {
            branch: self.branch_id(),
            space: self.space_id(),
            sequence,
            as_of: None,
        })? {
            Output::MaybeVersioned(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Read all events of a specific type.
    pub fn event_get_by_type(&self, event_type: &str) -> Result<Vec<VersionedValue>> {
        match self.execute_cmd(Command::EventGetByType {
            branch: self.branch_id(),
            space: self.space_id(),
            event_type: event_type.to_string(),
            limit: None,
            after_sequence: None,
            as_of: None,
        })? {
            Output::VersionedValues(events) => Ok(events),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGetByType".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get the total count of events in the log.
    pub fn event_len(&self) -> Result<u64> {
        match self.execute_cmd(Command::EventLen {
            branch: self.branch_id(),
            space: self.space_id(),
            as_of: None,
        })? {
            Output::Uint(len) => Ok(len),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventLen".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get the event log length as of a past timestamp.
    ///
    /// Returns `next_sequence` from the `EventLogMeta` snapshot visible at
    /// `as_of_ts` (microseconds since epoch). This counts events committed at
    /// or before that timestamp.
    pub fn event_len_at(&self, as_of_ts: u64) -> Result<u64> {
        match self.execute_cmd(Command::EventLen {
            branch: self.branch_id(),
            space: self.space_id(),
            as_of: Some(as_of_ts),
        })? {
            Output::Uint(len) => Ok(len),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventLen (as_of)".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Event Time-Travel Variants
    // =========================================================================

    /// Read a specific event by sequence number at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn event_get_at(
        &self,
        sequence: u64,
        as_of: Option<u64>,
    ) -> Result<Option<VersionedValue>> {
        match self.execute_cmd(Command::EventGet {
            branch: self.branch_id(),
            space: self.space_id(),
            sequence,
            as_of,
        })? {
            Output::MaybeVersioned(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Event Batch Operations
    // =========================================================================

    /// Batch append multiple events in a single transaction.
    ///
    /// Returns per-item results positionally mapped to the input entries.
    pub fn event_batch_append(
        &self,
        entries: Vec<crate::types::BatchEventEntry>,
    ) -> Result<Vec<crate::types::BatchItemResult>> {
        match self.execute_cmd(Command::EventBatchAppend {
            branch: self.branch_id(),
            space: self.space_id(),
            entries,
        })? {
            Output::BatchResults(results) => Ok(results),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventBatchAppend".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Event Extended Variants
    // =========================================================================

    /// Read events of a specific type with limit, pagination, and time-travel options.
    pub fn event_get_by_type_with_options(
        &self,
        event_type: &str,
        limit: Option<u64>,
        after_sequence: Option<u64>,
        as_of: Option<u64>,
    ) -> Result<Vec<VersionedValue>> {
        match self.execute_cmd(Command::EventGetByType {
            branch: self.branch_id(),
            space: self.space_id(),
            event_type: event_type.to_string(),
            limit,
            after_sequence,
            as_of,
        })? {
            Output::VersionedValues(events) => Ok(events),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGetByType".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Range Queries
    // =========================================================================

    /// Query events by sequence range with pagination.
    ///
    /// Returns events in `[start_seq, end_seq)` with optional count limit and direction.
    /// The result includes a cursor for fetching the next page.
    pub fn event_range(
        &self,
        start_seq: u64,
        end_seq: Option<u64>,
        limit: Option<u64>,
        direction: ScanDirection,
        event_type: Option<&str>,
    ) -> Result<(Vec<VersionedValue>, bool, Option<String>)> {
        match self.execute_cmd(Command::EventRange {
            branch: self.branch_id(),
            space: self.space_id(),
            start_seq,
            end_seq,
            limit,
            direction,
            event_type: event_type.map(|s| s.to_string()),
        })? {
            Output::EventRangeResult {
                events,
                has_more,
                next_cursor,
            } => Ok((events, has_more, next_cursor)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventRange".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Query events by timestamp range with pagination.
    ///
    /// Returns events whose timestamp is in `[start_ts, end_ts]` (inclusive, microseconds).
    /// The result includes a cursor for fetching the next page.
    pub fn event_range_by_time(
        &self,
        start_ts: u64,
        end_ts: Option<u64>,
        limit: Option<u64>,
        direction: ScanDirection,
        event_type: Option<&str>,
    ) -> Result<(Vec<VersionedValue>, bool, Option<String>)> {
        match self.execute_cmd(Command::EventRangeByTime {
            branch: self.branch_id(),
            space: self.space_id(),
            start_ts,
            end_ts,
            limit,
            direction,
            event_type: event_type.map(|s| s.to_string()),
        })? {
            Output::EventRangeResult {
                events,
                has_more,
                next_cursor,
            } => Ok((events, has_more, next_cursor)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventRangeByTime".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List all known event types in the stream.
    pub fn event_list_types(&self) -> Result<Vec<String>> {
        match self.execute_cmd(Command::EventListTypes {
            branch: self.branch_id(),
            space: self.space_id(),
            as_of: None,
        })? {
            Output::Keys(types) => Ok(types),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventListTypes".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List event types visible as of a past timestamp.
    ///
    /// Returns event types whose first event was appended at or before
    /// `as_of_ts` (microseconds since epoch).
    pub fn event_list_types_at(&self, as_of_ts: u64) -> Result<Vec<String>> {
        match self.execute_cmd(Command::EventListTypes {
            branch: self.branch_id(),
            space: self.space_id(),
            as_of: Some(as_of_ts),
        })? {
            Output::Keys(types) => Ok(types),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventListTypes (as_of)".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List events (optionally filtered by type), optionally as of a past
    /// timestamp.
    ///
    /// When `as_of_ts` is `None`, returns all events in the log (still
    /// optionally filtered by `event_type` and bounded by `limit`).
    /// When `as_of_ts` is `Some(ts)`, only events whose `event.timestamp <= ts`
    /// are returned.
    pub fn event_list(
        &self,
        event_type: Option<&str>,
        limit: Option<u64>,
        as_of_ts: Option<u64>,
    ) -> Result<Vec<crate::types::VersionedValue>> {
        match self.execute_cmd(Command::EventList {
            branch: self.branch_id(),
            space: self.space_id(),
            event_type: event_type.map(|s| s.to_string()),
            limit,
            as_of: as_of_ts,
        })? {
            Output::VersionedValues(events) => Ok(events),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventList".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }
}
