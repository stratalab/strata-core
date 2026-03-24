//! Branch lifecycle operations.
//!
//! Branch operations: create, get, list, exists, delete.

use super::Strata;
use crate::types::*;
use crate::{Command, Error, Output, Result, Value};

impl Strata {
    // =========================================================================
    // Branch Operations (5 MVP)
    // =========================================================================

    /// Create a new branch.
    ///
    /// # Arguments
    /// - `branch_id`: Optional user-provided name. If None, a UUID is generated.
    /// - `metadata`: Optional metadata (ignored in MVP).
    ///
    /// # Returns
    /// Tuple of (BranchInfo, version).
    pub fn branch_create(
        &self,
        branch_id: Option<String>,
        metadata: Option<Value>,
    ) -> Result<(BranchInfo, u64)> {
        match self.execute_cmd(Command::BranchCreate {
            branch_id,
            metadata,
        })? {
            Output::BranchWithVersion { info, version } => Ok((info, version)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchCreate".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get branch info.
    ///
    /// # Returns
    /// `Some(VersionedBranchInfo)` if the branch exists, `None` otherwise.
    pub fn branch_get(&self, name: &str) -> Result<Option<VersionedBranchInfo>> {
        match self.execute_cmd(Command::BranchGet {
            branch: BranchId::from(name),
        })? {
            Output::MaybeBranchInfo(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List all branches.
    ///
    /// # Arguments
    /// - `state`: Optional status filter (ignored in MVP).
    /// - `limit`: Optional maximum number of branches to return.
    /// - `offset`: Optional offset (ignored in MVP).
    pub fn branch_list(
        &self,
        state: Option<BranchStatus>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<Vec<VersionedBranchInfo>> {
        match self.execute_cmd(Command::BranchList {
            state,
            limit,
            offset,
        })? {
            Output::BranchInfoList(branches) => Ok(branches),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchList".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Check if a branch exists.
    pub fn branch_exists(&self, name: &str) -> Result<bool> {
        match self.execute_cmd(Command::BranchExists {
            branch: BranchId::from(name),
        })? {
            Output::Bool(exists) => Ok(exists),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchExists".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a branch and all its data (cascading delete).
    ///
    /// This deletes:
    /// - The branch metadata
    /// - All branch-scoped data (KV, Events, States, JSON, Vectors)
    ///
    /// USE WITH CAUTION - this is irreversible!
    pub fn branch_delete(&self, name: &str) -> Result<()> {
        match self.execute_cmd(Command::BranchDelete {
            branch: BranchId::from(name),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }
}
