//! Branch management power API.
//!
//! Access via `db.branches()` for advanced branch operations including
//! fork, diff, and merge.
//!
//! # Example
//!
//! ```text
//! use strata_executor::Strata;
//!
//! let db = Strata::open("/path/to/data")?;
//!
//! // List all branches
//! for branch in db.branches().list()? {
//!     println!("Branch: {}", branch);
//! }
//!
//! // Create a new branch
//! db.branches().create("experiment-1")?;
//!
//! // Fork a branch (copies all data)
//! db.branches().fork("main", "experiment-2")?;
//!
//! // Diff two branches
//! let diff = db.branches().diff("main", "experiment-2")?;
//!
//! // Merge branches
//! use strata_engine::MergeStrategy;
//! db.branches().merge("experiment-2", "main", MergeStrategy::LastWriterWins)?;
//! ```

use crate::types::BranchId;
use crate::{Command, Error, Executor, Output, Result};
use strata_engine::branch_ops::{BranchDiffResult, ForkInfo, MergeInfo, MergeStrategy};

/// Handle for branch management operations.
///
/// Obtained via [`Strata::branches()`]. Provides the "power API" for branch
/// management including listing, creating, deleting, forking, diffing, and merging.
pub struct Branches<'a> {
    executor: &'a Executor,
}

impl<'a> Branches<'a> {
    pub(crate) fn new(executor: &'a Executor) -> Self {
        Self { executor }
    }

    /// List all branch names.
    pub fn list(&self) -> Result<Vec<String>> {
        match self.executor.execute(Command::BranchList {
            state: None,
            limit: None,
            offset: None,
        })? {
            Output::BranchInfoList(branches) => {
                Ok(branches.into_iter().map(|r| r.info.id.0).collect())
            }
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchList".into(),
            }),
        }
    }

    /// Check if a branch exists.
    pub fn exists(&self, name: &str) -> Result<bool> {
        match self.executor.execute(Command::BranchExists {
            branch: BranchId::from(name),
        })? {
            Output::Bool(exists) => Ok(exists),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchExists".into(),
            }),
        }
    }

    /// Create a new empty branch.
    pub fn create(&self, name: &str) -> Result<()> {
        match self.executor.execute(Command::BranchCreate {
            branch_id: Some(name.to_string()),
            metadata: None,
        })? {
            Output::BranchWithVersion { .. } => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchCreate".into(),
            }),
        }
    }

    /// Delete a branch and all its data.
    pub fn delete(&self, name: &str) -> Result<()> {
        if name == "default" {
            return Err(Error::ConstraintViolation {
                reason: "Cannot delete the default branch".into(),
            });
        }

        match self.executor.execute(Command::BranchDelete {
            branch: BranchId::from(name),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchDelete".into(),
            }),
        }
    }

    /// Fork a branch, creating a copy with all its data.
    pub fn fork(&self, source: &str, destination: &str) -> Result<ForkInfo> {
        match self.executor.execute(Command::BranchFork {
            source: source.to_string(),
            destination: destination.to_string(),
        })? {
            Output::BranchForked(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchFork".into(),
            }),
        }
    }

    /// Compare two branches and return their differences.
    pub fn diff(&self, branch_a: &str, branch_b: &str) -> Result<BranchDiffResult> {
        match self.executor.execute(Command::BranchDiff {
            branch_a: branch_a.to_string(),
            branch_b: branch_b.to_string(),
        })? {
            Output::BranchDiff(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchDiff".into(),
            }),
        }
    }

    /// Merge data from source branch into target branch.
    pub fn merge(&self, source: &str, target: &str, strategy: MergeStrategy) -> Result<MergeInfo> {
        match self.executor.execute(Command::BranchMerge {
            source: source.to_string(),
            target: target.to_string(),
            strategy,
        })? {
            Output::BranchMerged(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchMerge".into(),
            }),
        }
    }
}
