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
use strata_engine::branch_ops::{
    BranchDiffResult, CherryPickFilter, CherryPickInfo, ForkInfo, MergeInfo, MergeStrategy,
    NoteInfo, RevertInfo, TagInfo, ThreeWayDiffResult,
};
use strata_engine::MergeBaseInfo;

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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
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
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Compute three-way diff between two branches.
    ///
    /// Returns the raw 14-case classification for each key without applying
    /// any merge strategy. Useful for previewing changes before merging.
    pub fn diff_three_way(&self, branch_a: &str, branch_b: &str) -> Result<ThreeWayDiffResult> {
        match self.backend.execute(Command::BranchDiffThreeWay {
            branch_a: branch_a.to_string(),
            branch_b: branch_b.to_string(),
        })? {
            Output::ThreeWayDiff(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchDiffThreeWay".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get the merge base for two branches.
    ///
    /// Returns `None` if the branches have no fork or merge relationship.
    pub fn merge_base(&self, branch_a: &str, branch_b: &str) -> Result<Option<MergeBaseInfo>> {
        match self.backend.execute(Command::BranchMergeBase {
            branch_a: branch_a.to_string(),
            branch_b: branch_b.to_string(),
        })? {
            Output::MergeBaseInfo(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchMergeBase".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Revert
    // =========================================================================

    /// Revert a version range on a branch.
    ///
    /// Restores keys to their state before the range, preserving any changes
    /// made after the range.
    pub fn revert(&self, branch: &str, from_version: u64, to_version: u64) -> Result<RevertInfo> {
        match self.backend.execute(Command::BranchRevert {
            branch: branch.to_string(),
            from_version,
            to_version,
        })? {
            Output::BranchReverted(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchRevert".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Cherry-Pick
    // =========================================================================

    /// Cherry-pick specific keys from source branch to target branch.
    pub fn cherry_pick(
        &self,
        source: &str,
        target: &str,
        keys: &[(String, String)],
    ) -> Result<CherryPickInfo> {
        match self.backend.execute(Command::BranchCherryPick {
            source: source.to_string(),
            target: target.to_string(),
            keys: Some(keys.to_vec()),
            filter_spaces: None,
            filter_keys: None,
            filter_primitives: None,
        })? {
            Output::BranchCherryPicked(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchCherryPick".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Cherry-pick from a three-way diff with filter criteria.
    pub fn cherry_pick_filtered(
        &self,
        source: &str,
        target: &str,
        filter: CherryPickFilter,
    ) -> Result<CherryPickInfo> {
        match self.backend.execute(Command::BranchCherryPick {
            source: source.to_string(),
            target: target.to_string(),
            keys: None,
            filter_spaces: filter.spaces,
            filter_keys: filter.keys,
            filter_primitives: filter.primitives,
        })? {
            Output::BranchCherryPicked(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchCherryPick".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Tags
    // =========================================================================

    /// Create a tag on a branch at the current version.
    pub fn create_tag(&self, branch: &str, name: &str, message: Option<&str>) -> Result<TagInfo> {
        match self.backend.execute(Command::TagCreate {
            branch: branch.to_string(),
            name: name.to_string(),
            version: None,
            message: message.map(|s| s.to_string()),
            creator: None,
        })? {
            Output::TagCreated(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for TagCreate".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Create a tag on a branch at a specific version.
    pub fn create_tag_at_version(
        &self,
        branch: &str,
        name: &str,
        version: u64,
        message: Option<&str>,
    ) -> Result<TagInfo> {
        match self.backend.execute(Command::TagCreate {
            branch: branch.to_string(),
            name: name.to_string(),
            version: Some(version),
            message: message.map(|s| s.to_string()),
            creator: None,
        })? {
            Output::TagCreated(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for TagCreate".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a tag. Returns `true` if the tag existed.
    pub fn delete_tag(&self, branch: &str, name: &str) -> Result<bool> {
        match self.backend.execute(Command::TagDelete {
            branch: branch.to_string(),
            name: name.to_string(),
        })? {
            Output::Bool(deleted) => Ok(deleted),
            _ => Err(Error::Internal {
                reason: "Unexpected output for TagDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List all tags on a branch.
    pub fn list_tags(&self, branch: &str) -> Result<Vec<TagInfo>> {
        match self.backend.execute(Command::TagList {
            branch: branch.to_string(),
        })? {
            Output::TagList(tags) => Ok(tags),
            _ => Err(Error::Internal {
                reason: "Unexpected output for TagList".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Resolve a tag to its version info.
    pub fn resolve_tag(&self, branch: &str, name: &str) -> Result<Option<TagInfo>> {
        match self.backend.execute(Command::TagResolve {
            branch: branch.to_string(),
            name: name.to_string(),
        })? {
            Output::MaybeTag(tag) => Ok(tag),
            _ => Err(Error::Internal {
                reason: "Unexpected output for TagResolve".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Notes
    // =========================================================================

    /// Add a note to a specific version of a branch.
    pub fn add_note(
        &self,
        branch: &str,
        version: u64,
        message: &str,
        author: Option<&str>,
    ) -> Result<NoteInfo> {
        match self.backend.execute(Command::NoteAdd {
            branch: branch.to_string(),
            version,
            message: message.to_string(),
            author: author.map(|s| s.to_string()),
            metadata: None,
        })? {
            Output::NoteAdded(note) => Ok(note),
            _ => Err(Error::Internal {
                reason: "Unexpected output for NoteAdd".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get notes for a branch, optionally filtered by version.
    pub fn get_notes(&self, branch: &str, version: Option<u64>) -> Result<Vec<NoteInfo>> {
        match self.backend.execute(Command::NoteGet {
            branch: branch.to_string(),
            version,
        })? {
            Output::NoteList(notes) => Ok(notes),
            _ => Err(Error::Internal {
                reason: "Unexpected output for NoteGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a note at a specific version. Returns `true` if the note existed.
    pub fn delete_note(&self, branch: &str, version: u64) -> Result<bool> {
        match self.backend.execute(Command::NoteDelete {
            branch: branch.to_string(),
            version,
        })? {
            Output::Bool(deleted) => Ok(deleted),
            _ => Err(Error::Internal {
                reason: "Unexpected output for NoteDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Audit Log
    // =========================================================================

    /// Query the branch audit log.
    ///
    /// Returns recent branch lifecycle events (create, fork, merge, delete, tag,
    /// note) in chronological order from the `_system_` branch.
    pub fn log(&self, limit: Option<u64>) -> Result<Vec<strata_core::Value>> {
        let mut all_events = Vec::new();
        for event_type in &[
            "branch.create",
            "branch.fork",
            "branch.merge",
            "branch.delete",
            "branch.revert",
            "branch.cherry_pick",
            "branch.tag",
            "branch.note",
        ] {
            // Use execute_internal to bypass the _system_ branch guard.
            if let Ok(Output::VersionedValues(events)) =
                self.backend.execute_internal(Command::EventGetByType {
                    branch: Some(BranchId::from("_system_")),
                    space: Some("default".to_string()),
                    event_type: event_type.to_string(),
                    limit,
                    after_sequence: None,
                    as_of: None,
                })
            {
                for event in events {
                    all_events.push(event.value);
                }
            }
        }
        Ok(all_events)
    }
}
