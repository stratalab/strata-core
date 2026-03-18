//! Space operations.

use super::Strata;
use crate::{Command, Error, Output, Result};

impl Strata {
    // =========================================================================
    // Space Operations
    // =========================================================================

    /// List all spaces in the current branch.
    pub fn space_list(&self) -> Result<Vec<String>> {
        match self.execute_cmd(Command::SpaceList {
            branch: self.branch_id(),
        })? {
            Output::SpaceList(spaces) => Ok(spaces),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceList".into(),
            }),
        }
    }

    /// Create a space in the current branch.
    pub fn space_create(&self, name: &str) -> Result<()> {
        match self.execute_cmd(Command::SpaceCreate {
            branch: self.branch_id(),
            space: name.to_string(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceCreate".into(),
            }),
        }
    }

    /// Delete a space from the current branch.
    ///
    /// If `force` is true, deletes even if the space is non-empty.
    pub fn space_delete(&self, name: &str, force: bool) -> Result<()> {
        match self.execute_cmd(Command::SpaceDelete {
            branch: self.branch_id(),
            space: name.to_string(),
            force,
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceDelete".into(),
            }),
        }
    }

    /// Check if a space exists in the current branch.
    pub fn space_exists(&self, name: &str) -> Result<bool> {
        match self.execute_cmd(Command::SpaceExists {
            branch: self.branch_id(),
            space: name.to_string(),
        })? {
            Output::Bool(exists) => Ok(exists),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceExists".into(),
            }),
        }
    }
}
