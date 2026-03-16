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
        match self.executor.execute(Command::EventAppend {
            branch: self.branch_id(),
            space: self.space_id(),
            event_type: event_type.to_string(),
            payload,
        })? {
            Output::EventAppendResult { sequence, .. } => Ok(sequence),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventAppend".into(),
            }),
        }
    }

    /// Read a specific event by sequence number.
    pub fn event_get(&self, sequence: u64) -> Result<Option<VersionedValue>> {
        match self.executor.execute(Command::EventGet {
            branch: self.branch_id(),
            space: self.space_id(),
            sequence,
            as_of: None,
        })? {
            Output::MaybeVersioned(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGet".into(),
            }),
        }
    }

    /// Read all events of a specific type.
    pub fn event_get_by_type(&self, event_type: &str) -> Result<Vec<VersionedValue>> {
        match self.executor.execute(Command::EventGetByType {
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
            }),
        }
    }

    /// Get the total count of events in the log.
    pub fn event_len(&self) -> Result<u64> {
        match self.executor.execute(Command::EventLen {
            branch: self.branch_id(),
            space: self.space_id(),
        })? {
            Output::Uint(len) => Ok(len),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventLen".into(),
            }),
        }
    }

    // =========================================================================
    // Event as_of Variant
    // =========================================================================

    /// Read a specific event by sequence number at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn event_get_as_of(
        &self,
        sequence: u64,
        as_of: Option<u64>,
    ) -> Result<Option<VersionedValue>> {
        match self.executor.execute(Command::EventGet {
            branch: self.branch_id(),
            space: self.space_id(),
            sequence,
            as_of,
        })? {
            Output::MaybeVersioned(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGet".into(),
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
        match self.executor.execute(Command::EventBatchAppend {
            branch: self.branch_id(),
            space: self.space_id(),
            entries,
        })? {
            Output::BatchResults(results) => Ok(results),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventBatchAppend".into(),
            }),
        }
    }

    // =========================================================================
    // Event Extended Variants
    // =========================================================================

    /// Read events of a specific type with limit and pagination options.
    pub fn event_get_by_type_with_options(
        &self,
        event_type: &str,
        limit: Option<u64>,
        after_sequence: Option<u64>,
    ) -> Result<Vec<VersionedValue>> {
        match self.executor.execute(Command::EventGetByType {
            branch: self.branch_id(),
            space: self.space_id(),
            event_type: event_type.to_string(),
            limit,
            after_sequence,
            as_of: None,
        })? {
            Output::VersionedValues(events) => Ok(events),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EventGetByType".into(),
            }),
        }
    }
}
