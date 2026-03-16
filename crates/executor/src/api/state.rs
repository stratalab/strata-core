//! State cell operations.

use super::Strata;
use crate::{Command, Error, Output, Result, Value};

impl Strata {
    // =========================================================================
    // State Operations
    // =========================================================================

    /// Set a state cell value (unconditional write).
    pub fn state_set(&self, cell: &str, value: impl Into<Value>) -> Result<u64> {
        match self.executor.execute(Command::StateSet {
            branch: self.branch_id(),
            space: self.space_id(),
            cell: cell.to_string(),
            value: value.into(),
        })? {
            Output::WriteResult { version, .. } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateSet".into(),
            }),
        }
    }

    /// Read a state cell value.
    pub fn state_get(&self, cell: &str) -> Result<Option<Value>> {
        match self.executor.execute(Command::StateGet {
            branch: self.branch_id(),
            space: self.space_id(),
            cell: cell.to_string(),
            as_of: None,
        })? {
            Output::MaybeVersioned(v) => Ok(v.map(|vv| vv.value)),
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateGet".into(),
            }),
        }
    }

    /// Get the full version history for a state cell.
    ///
    /// Returns all versions of the cell, newest first, or None if the cell
    /// doesn't exist.
    pub fn state_getv(&self, cell: &str) -> Result<Option<Vec<crate::types::VersionedValue>>> {
        match self.executor.execute(Command::StateGetv {
            branch: self.branch_id(),
            space: self.space_id(),
            as_of: None,
            cell: cell.to_string(),
        })? {
            Output::VersionHistory(h) => Ok(h),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateGetv".into(),
            }),
        }
    }

    /// Compare-and-swap on a state cell.
    pub fn state_cas(
        &self,
        cell: &str,
        expected_counter: Option<u64>,
        value: impl Into<Value>,
    ) -> Result<Option<u64>> {
        match self.executor.execute(Command::StateCas {
            branch: self.branch_id(),
            space: self.space_id(),
            cell: cell.to_string(),
            expected_counter,
            value: value.into(),
        })? {
            Output::StateCasResult {
                success, version, ..
            } => {
                if success {
                    Ok(version)
                } else {
                    Ok(None)
                }
            }
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateCas".into(),
            }),
        }
    }

    /// Initialize a state cell (only if it doesn't exist).
    pub fn state_init(&self, cell: &str, value: impl Into<Value>) -> Result<u64> {
        match self.executor.execute(Command::StateInit {
            branch: self.branch_id(),
            space: self.space_id(),
            cell: cell.to_string(),
            value: value.into(),
        })? {
            Output::WriteResult { version, .. } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateInit".into(),
            }),
        }
    }

    /// Delete a state cell.
    ///
    /// Returns `true` if the cell existed and was deleted, `false` if it didn't exist.
    pub fn state_delete(&self, cell: &str) -> Result<bool> {
        match self.executor.execute(Command::StateDelete {
            branch: self.branch_id(),
            space: self.space_id(),
            cell: cell.to_string(),
        })? {
            Output::DeleteResult { deleted, .. } => Ok(deleted),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateDelete".into(),
            }),
        }
    }

    /// List state cell names with optional prefix filter.
    ///
    /// Returns all cell names, optionally filtered by prefix.
    pub fn state_list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        match self.executor.execute(Command::StateList {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
            as_of: None,
        })? {
            Output::Keys(keys) => Ok(keys),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateList".into(),
            }),
        }
    }

    // =========================================================================
    // State as_of Variant
    // =========================================================================

    /// Read a state cell value at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn state_get_as_of(&self, cell: &str, as_of: Option<u64>) -> Result<Option<Value>> {
        match self.executor.execute(Command::StateGet {
            branch: self.branch_id(),
            space: self.space_id(),
            cell: cell.to_string(),
            as_of,
        })? {
            Output::MaybeVersioned(v) => Ok(v.map(|vv| vv.value)),
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateGet".into(),
            }),
        }
    }

    /// List state cell names at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn state_list_as_of(
        &self,
        prefix: Option<&str>,
        as_of: Option<u64>,
    ) -> Result<Vec<String>> {
        match self.executor.execute(Command::StateList {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
            as_of,
        })? {
            Output::Keys(keys) => Ok(keys),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateList".into(),
            }),
        }
    }

    // =========================================================================
    // State Batch Operations
    // =========================================================================

    /// Batch set multiple state cells in a single transaction.
    ///
    /// Returns per-item results positionally mapped to the input entries.
    pub fn state_batch_set(
        &self,
        entries: Vec<crate::types::BatchStateEntry>,
    ) -> Result<Vec<crate::types::BatchItemResult>> {
        match self.executor.execute(Command::StateBatchSet {
            branch: self.branch_id(),
            space: self.space_id(),
            entries,
        })? {
            Output::BatchResults(results) => Ok(results),
            _ => Err(Error::Internal {
                reason: "Unexpected output for StateBatchSet".into(),
            }),
        }
    }
}
