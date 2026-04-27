//! Branch DAG types and constants.
//!
//! These are the fundamental types for the branch lifecycle tracking system.
//! The types live in core because they're used across multiple crates (engine,
//! graph, executor) without creating circular dependencies.

use crate::id::CommitVersion;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Reserved branch name for system-internal data.
pub const SYSTEM_BRANCH: &str = "_system_";

/// Graph name for the branch DAG on the `_system_` branch.
pub const BRANCH_DAG_GRAPH: &str = "_branch_dag";

/// Returns `true` if the given name starts with the reserved `_system` prefix.
pub fn is_system_branch(name: &str) -> bool {
    name.starts_with("_system")
}

/// Status of a branch in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DagBranchStatus {
    /// Branch is active and writable.
    Active,
    /// Branch is archived (read-only).
    Archived,
    /// Branch has been merged into another branch.
    Merged,
    /// Branch has been deleted.
    Deleted,
}

impl DagBranchStatus {
    /// Returns `true` if this status allows writes.
    pub fn is_writable(&self) -> bool {
        matches!(self, DagBranchStatus::Active)
    }
    /// String representation of this status.
    pub fn as_str(&self) -> &'static str {
        match self {
            DagBranchStatus::Active => "active",
            DagBranchStatus::Archived => "archived",
            DagBranchStatus::Merged => "merged",
            DagBranchStatus::Deleted => "deleted",
        }
    }
    /// Parse a status string. Returns `None` for unknown values.
    pub fn parse_str(s: &str) -> Option<Self> {
        match s {
            "active" => Some(DagBranchStatus::Active),
            "archived" => Some(DagBranchStatus::Archived),
            "merged" => Some(DagBranchStatus::Merged),
            "deleted" => Some(DagBranchStatus::Deleted),
            _ => None,
        }
    }
}

impl fmt::Display for DagBranchStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Unique identifier for a DAG event (fork or merge).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DagEventId(String);

impl DagEventId {
    /// Create a new fork event ID.
    pub fn new_fork() -> Self {
        Self(format!("fork:{}", uuid::Uuid::new_v4()))
    }
    /// Create a new merge event ID.
    pub fn new_merge() -> Self {
        Self(format!("merge:{}", uuid::Uuid::new_v4()))
    }
    /// Create a new revert event ID.
    pub fn new_revert() -> Self {
        Self(format!("revert:{}", uuid::Uuid::new_v4()))
    }
    /// Create a new cherry-pick event ID.
    pub fn new_cherry_pick() -> Self {
        Self(format!("cherry_pick:{}", uuid::Uuid::new_v4()))
    }
    /// Create a `DagEventId` from an existing string (e.g. loaded from storage).
    pub fn from_string(s: String) -> Self {
        Self(s)
    }
    /// The string representation of this event ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Returns `true` if this is a fork event.
    pub fn is_fork(&self) -> bool {
        self.0.starts_with("fork:")
    }
    /// Returns `true` if this is a merge event.
    pub fn is_merge(&self) -> bool {
        self.0.starts_with("merge:")
    }
    /// Returns `true` if this is a revert event.
    pub fn is_revert(&self) -> bool {
        self.0.starts_with("revert:")
    }
    /// Returns `true` if this is a cherry-pick event.
    pub fn is_cherry_pick(&self) -> bool {
        self.0.starts_with("cherry_pick:")
    }
}

impl fmt::Display for DagEventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Record of a fork event in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkRecord {
    /// Unique event identifier.
    pub event_id: DagEventId,
    /// Parent branch name.
    pub parent: String,
    /// Child (forked) branch name.
    pub child: String,
    /// Timestamp in microseconds.
    pub timestamp: u64,
    /// MVCC snapshot version at the time of fork.
    pub fork_version: Option<u64>,
    /// Optional descriptive message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

/// Record of a merge event in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeRecord {
    /// Unique event identifier.
    pub event_id: DagEventId,
    /// Source branch name.
    pub source: String,
    /// Target branch name.
    pub target: String,
    /// Timestamp in microseconds.
    pub timestamp: u64,
    /// MVCC snapshot version at the time of merge (for three-way merge).
    #[serde(default)]
    pub merge_version: Option<u64>,
    /// Merge strategy used.
    pub strategy: Option<String>,
    /// Number of keys applied.
    pub keys_applied: Option<u64>,
    /// Number of spaces merged.
    pub spaces_merged: Option<u64>,
    /// Number of conflicts encountered.
    pub conflicts: Option<u64>,
    /// Optional descriptive message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

/// Record of a revert event in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RevertRecord {
    /// Unique event identifier.
    pub event_id: DagEventId,
    /// Branch that was reverted.
    pub branch: String,
    /// Start of the reverted version range (inclusive).
    pub from_version: CommitVersion,
    /// End of the reverted version range (inclusive).
    pub to_version: CommitVersion,
    /// MVCC version of the revert transaction itself.
    #[serde(default)]
    pub revert_version: Option<CommitVersion>,
    /// Number of keys reverted.
    pub keys_reverted: u64,
    /// Timestamp in microseconds.
    pub timestamp: u64,
    /// Optional descriptive message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

/// Record of a cherry-pick event in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CherryPickRecord {
    /// Unique event identifier.
    pub event_id: DagEventId,
    /// Source branch that the changes were picked from.
    pub source: String,
    /// Target branch that received the picked changes.
    pub target: String,
    /// MVCC version of the cherry-pick transaction.
    #[serde(default)]
    pub cherry_pick_version: Option<u64>,
    /// Number of keys written by the cherry-pick.
    pub keys_applied: u64,
    /// Number of keys deleted by the cherry-pick.
    pub keys_deleted: u64,
    /// Timestamp in microseconds.
    pub timestamp: u64,
}

/// Full information about a branch in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DagBranchInfo {
    /// Branch name.
    pub name: String,
    /// Current status.
    pub status: DagBranchStatus,
    /// Creation timestamp in microseconds.
    pub created_at: Option<u64>,
    /// Last update timestamp in microseconds.
    pub updated_at: Option<u64>,
    /// Optional descriptive message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
    /// Fork origin, if this branch was forked.
    pub forked_from: Option<ForkRecord>,
    /// Merge events where this branch was the source.
    pub merges: Vec<MergeRecord>,
    /// Names of branches forked from this one.
    pub children: Vec<String>,
}
