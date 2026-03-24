//! Internal API for the `_system_` branch.
//!
//! Access via `db.system_branch()`. Provides the same primitives (KV, JSON,
//! events) routed to the `_system_` branch, bypassing the user-facing
//! guard that normally rejects access to reserved branches.
//!
//! This is a capability-based design: having the handle *is* the authorization.

use strata_engine::branch_dag::SYSTEM_BRANCH;

use crate::ipc::Backend;
use crate::types::{BranchId, VersionedValue};
use crate::{Command, Error, Output, Result, Value};

/// Handle for internal operations on the `_system_` branch.
///
/// Obtained via [`Strata::system_branch()`](super::Strata::system_branch).
/// All operations are pre-bound to the `_system_` branch and `default` space.
///
/// This handle bypasses the `reject_system_branch` guard that prevents
/// user-facing APIs from accessing reserved branches. It is intended for
/// internal consumers (e.g., strata-ai) that need to store private workspace
/// data alongside the user's database.
pub struct SystemBranch<'a> {
    backend: &'a Backend,
}

impl<'a> SystemBranch<'a> {
    pub(crate) fn new(backend: &'a Backend) -> Self {
        Self { backend }
    }

    fn branch_id(&self) -> Option<BranchId> {
        Some(BranchId::from(SYSTEM_BRANCH))
    }

    fn space_id(&self) -> Option<String> {
        Some("default".to_string())
    }

    // =========================================================================
    // KV Operations
    // =========================================================================

    /// Put a value in the system branch KV store.
    pub fn kv_put(&self, key: &str, value: impl Into<Value>) -> Result<u64> {
        match self.backend.execute_internal(Command::KvPut {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            value: value.into(),
        })? {
            Output::WriteResult { version, .. } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvPut".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get a value from the system branch KV store.
    pub fn kv_get(&self, key: &str) -> Result<Option<Value>> {
        match self.backend.execute_internal(Command::KvGet {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            as_of: None,
        })? {
            Output::MaybeVersioned(v) => Ok(v.map(|vv| vv.value)),
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a key from the system branch KV store.
    pub fn kv_delete(&self, key: &str) -> Result<bool> {
        match self.backend.execute_internal(Command::KvDelete {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
        })? {
            Output::DeleteResult { deleted, .. } => Ok(deleted),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List keys in the system branch KV store.
    pub fn kv_list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        match self.backend.execute_internal(Command::KvList {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
            cursor: None,
            limit: None,
            as_of: None,
        })? {
            Output::Keys(keys) => Ok(keys),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvList".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // JSON Operations
    // =========================================================================

    /// Set a JSON value at a path in the system branch.
    ///
    /// Use `"$"` as the path to set the entire document.
    pub fn json_set(&self, key: &str, path: &str, value: impl Into<Value>) -> Result<u64> {
        match self.backend.execute_internal(Command::JsonSet {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            path: path.to_string(),
            value: value.into(),
        })? {
            Output::WriteResult { version, .. } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for JsonSet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get a JSON value at a path from the system branch.
    ///
    /// Use `"$"` as the path to get the entire document.
    pub fn json_get(&self, key: &str, path: &str) -> Result<Option<Value>> {
        match self.backend.execute_internal(Command::JsonGet {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            path: path.to_string(),
            as_of: None,
        })? {
            Output::MaybeVersioned(v) => Ok(v.map(|vv| vv.value)),
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for JsonGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a JSON value at a path from the system branch.
    pub fn json_delete(&self, key: &str, path: &str) -> Result<u64> {
        match self.backend.execute_internal(Command::JsonDelete {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            path: path.to_string(),
        })? {
            Output::Uint(count) => Ok(count),
            _ => Err(Error::Internal {
                reason: "Unexpected output for JsonDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Event Operations
    // =========================================================================

    /// Append an event to the system branch event log.
    pub fn event_append(&self, event_type: &str, payload: Value) -> Result<u64> {
        match self.backend.execute_internal(Command::EventAppend {
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

    /// Read an event by sequence number from the system branch.
    pub fn event_get(&self, sequence: u64) -> Result<Option<VersionedValue>> {
        match self.backend.execute_internal(Command::EventGet {
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
}
