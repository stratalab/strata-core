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

use crate::ipc::Backend;
use crate::types::BranchId;
use crate::{Command, Error, Output, Result};
use strata_engine::branch_ops::{BranchDiffResult, ForkInfo, MergeInfo, MergeStrategy};

/// Handle for branch management operations.
///
/// Obtained via [`Strata::branches()`]. Provides the "power API" for branch
/// management including listing, creating, deleting, forking, diffing, and merging.
pub struct Branches<'a> {
    backend: &'a Backend,
}

impl<'a> Branches<'a> {
    pub(crate) fn new(backend: &'a Backend) -> Self {
        Self { backend }
    }

    /// List all branch names.
    pub fn list(&self) -> Result<Vec<String>> {
        match self.backend.execute(Command::BranchList {
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
        match self.backend.execute(Command::BranchExists {
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
        match self.backend.execute(Command::BranchCreate {
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

        match self.backend.execute(Command::BranchDelete {
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
        match self.backend.execute(Command::BranchFork {
            source: source.to_string(),
            destination: destination.to_string(),
            message: None,
            creator: None,
        })? {
            Output::BranchForked(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchFork".into(),
            }),
        }
    }

    /// Fork a branch with optional message and creator.
    pub fn fork_with_options(
        &self,
        source: &str,
        destination: &str,
        message: Option<String>,
        creator: Option<String>,
    ) -> Result<ForkInfo> {
        match self.backend.execute(Command::BranchFork {
            source: source.to_string(),
            destination: destination.to_string(),
            message,
            creator,
        })? {
            Output::BranchForked(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchFork".into(),
            }),
        }
    }

    /// Compare two branches and return their differences.
    pub fn diff(&self, branch_a: &str, branch_b: &str) -> Result<BranchDiffResult> {
        match self.backend.execute(Command::BranchDiff {
            branch_a: branch_a.to_string(),
            branch_b: branch_b.to_string(),
            filter_primitives: None,
            filter_spaces: None,
            as_of: None,
        })? {
            Output::BranchDiff(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchDiff".into(),
            }),
        }
    }

    /// Compare two branches with filtering and point-in-time options.
    pub fn diff_with_options(
        &self,
        branch_a: &str,
        branch_b: &str,
        options: strata_engine::branch_ops::DiffOptions,
    ) -> Result<BranchDiffResult> {
        let has_filter = options.filter.is_some();
        let (filter_primitives, filter_spaces) = if has_filter {
            let f = options.filter.unwrap_or_default();
            (f.primitives, f.spaces)
        } else {
            (None, None)
        };
        match self.backend.execute(Command::BranchDiff {
            branch_a: branch_a.to_string(),
            branch_b: branch_b.to_string(),
            filter_primitives,
            filter_spaces,
            as_of: options.as_of,
        })? {
            Output::BranchDiff(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchDiff".into(),
            }),
        }
    }

    /// Merge data from source branch into target branch.
    pub fn merge(&self, source: &str, target: &str, strategy: MergeStrategy) -> Result<MergeInfo> {
        match self.backend.execute(Command::BranchMerge {
            source: source.to_string(),
            target: target.to_string(),
            strategy,
            message: None,
            creator: None,
        })? {
            Output::BranchMerged(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchMerge".into(),
            }),
        }
    }

    /// Merge branches with optional message and creator.
    pub fn merge_with_options(
        &self,
        source: &str,
        target: &str,
        strategy: MergeStrategy,
        message: Option<String>,
        creator: Option<String>,
    ) -> Result<MergeInfo> {
        match self.backend.execute(Command::BranchMerge {
            source: source.to_string(),
            target: target.to_string(),
            strategy,
            message,
            creator,
        })? {
            Output::BranchMerged(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchMerge".into(),
            }),
        }
    }
}
