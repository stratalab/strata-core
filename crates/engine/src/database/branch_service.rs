//! BranchService: Canonical branch operation facade.
//!
//! `BranchService` is the single canonical path for all branch operations.
//! It wraps existing `branch_ops` functions and adds:
//!
//! - Branch name validation
//! - DAG integration via `BranchDagHook`
//! - Observer notification via `BranchOpObserver`
//! - Capability gating for DAG-required operations
//!
//! ## Usage
//!
//! ```text
//! let branches = db.branches();
//! branches.create("feature-x")?;
//! branches.fork("main", "feature-x")?;
//! branches.merge("feature-x", "main", MergeOptions::default())?;
//! ```

use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_core::{StrataError, StrataResult};

use crate::branch_ops::{
    self, BranchDiffResult, CherryPickInfo, DiffOptions, ForkInfo, MergeInfo, MergeStrategy,
    RevertInfo, TagInfo, ThreeWayDiffResult,
};
use crate::database::branch_mutation::BranchMutation;
use crate::database::dag_hook::{BranchDagError, DagEvent, DagHookSlot};
use crate::database::observers::{BranchOpEvent, BranchOpKind, BranchOpObserverRegistry};
use crate::database::Database;
use crate::primitives::branch::{resolve_branch_name, BranchIndex, BranchMetadata};

// =============================================================================
// Merge Options
// =============================================================================

/// Options for branch merge operations.
#[derive(Debug, Clone)]
pub struct MergeOptions {
    /// Conflict resolution strategy.
    pub strategy: MergeStrategy,
    /// Optional merge base override (branch_id, version).
    pub merge_base: Option<(BranchId, u64)>,
    /// Optional commit message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

impl Default for MergeOptions {
    fn default() -> Self {
        Self {
            strategy: MergeStrategy::LastWriterWins,
            merge_base: None,
            message: None,
            creator: None,
        }
    }
}

impl MergeOptions {
    /// Create options with a specific strategy.
    pub fn with_strategy(strategy: MergeStrategy) -> Self {
        Self {
            strategy,
            ..Default::default()
        }
    }

    /// Set the commit message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the creator identifier.
    pub fn with_creator(mut self, creator: impl Into<String>) -> Self {
        self.creator = Some(creator.into());
        self
    }

    /// Set the merge base override.
    pub fn with_merge_base(mut self, branch_id: BranchId, version: u64) -> Self {
        self.merge_base = Some((branch_id, version));
        self
    }
}

/// Options for branch fork operations.
#[derive(Debug, Clone, Default)]
pub struct ForkOptions {
    /// Optional commit message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

impl ForkOptions {
    /// Set the commit message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the creator identifier.
    pub fn with_creator(mut self, creator: impl Into<String>) -> Self {
        self.creator = Some(creator.into());
        self
    }
}

// =============================================================================
// Branch Validation
// =============================================================================

/// Reserved branch name prefix for system branches.
const SYSTEM_PREFIX: &str = "_";

/// Maximum branch name length.
const MAX_BRANCH_NAME_LEN: usize = 255;

/// Validate a branch name.
fn validate_branch_name(name: &str) -> StrataResult<()> {
    if name.is_empty() {
        return Err(StrataError::invalid_input("branch name cannot be empty"));
    }

    if name.trim().is_empty() {
        return Err(StrataError::invalid_input(
            "branch name cannot be whitespace-only",
        ));
    }

    if name.len() > MAX_BRANCH_NAME_LEN {
        return Err(StrataError::invalid_input(format!(
            "branch name exceeds maximum length of {} characters",
            MAX_BRANCH_NAME_LEN
        )));
    }

    if name.starts_with(SYSTEM_PREFIX) {
        return Err(StrataError::invalid_input(format!(
            "branch name cannot start with '{}' (reserved for system)",
            SYSTEM_PREFIX
        )));
    }

    if name.chars().any(|c| c.is_control()) {
        return Err(StrataError::invalid_input(
            "branch name cannot contain control characters",
        ));
    }

    Ok(())
}

// =============================================================================
// BranchService
// =============================================================================

/// Canonical branch operation facade.
///
/// Obtained via `db.branches()`. Provides all branch operations with
/// integrated validation, DAG recording, and observer notification.
pub struct BranchService {
    db: Arc<Database>,
}

impl BranchService {
    /// Create a new BranchService for the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    // =========================================================================
    // Core Operations (DAG optional)
    // =========================================================================

    /// Create a new branch.
    ///
    /// Emits a DAG create event if a DAG hook is installed.
    pub fn create(&self, name: &str) -> StrataResult<BranchMetadata> {
        validate_branch_name(name)?;

        let mut mutation = BranchMutation::new(&self.db);
        let branch_id = resolve_branch_name(name);

        let index = BranchIndex::new(self.db.clone());
        let versioned = index.create_branch(name)?;

        // Rollback: delete the branch on DAG failure
        mutation.on_rollback_delete_branch(name, false);

        // Record to DAG if hook installed (optional, not required)
        let event = DagEvent::create(branch_id, name);
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        mutation.commit(BranchOpEvent::create(branch_id, name));

        Ok(versioned.value)
    }

    /// Delete a branch.
    ///
    /// Emits a DAG delete event if a DAG hook is installed.
    pub fn delete(&self, name: &str) -> StrataResult<()> {
        if name.starts_with(SYSTEM_PREFIX) {
            return Err(StrataError::invalid_input(
                "cannot delete system branches",
            ));
        }

        let mut mutation = BranchMutation::new(&self.db);
        let branch_id = resolve_branch_name(name);

        let index = BranchIndex::new(self.db.clone());
        index.delete_branch(name)?;

        // Record to DAG if hook installed (optional, not required)
        // Note: no rollback for delete - branch is already gone
        let event = DagEvent::delete(branch_id, name);
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        mutation.commit(BranchOpEvent::delete(branch_id, name));

        Ok(())
    }

    /// Check if a branch exists.
    pub fn exists(&self, name: &str) -> StrataResult<bool> {
        let index = BranchIndex::new(self.db.clone());
        index.exists(name)
    }

    /// Get branch metadata.
    pub fn info(&self, name: &str) -> StrataResult<Option<BranchMetadata>> {
        let index = BranchIndex::new(self.db.clone());
        index.get_branch(name).map(|opt| opt.map(|v| v.value))
    }

    /// List all branch names.
    pub fn list(&self) -> StrataResult<Vec<String>> {
        let index = BranchIndex::new(self.db.clone());
        index.list_branches()
    }

    /// Diff two branches.
    pub fn diff(&self, source: &str, target: &str) -> StrataResult<BranchDiffResult> {
        branch_ops::diff_branches(&self.db, source, target)
    }

    /// Diff two branches with options.
    pub fn diff_with_options(
        &self,
        source: &str,
        target: &str,
        options: DiffOptions,
    ) -> StrataResult<BranchDiffResult> {
        branch_ops::diff_branches_with_options(&self.db, source, target, options)
    }

    /// Three-way diff between branches.
    pub fn diff3(
        &self,
        source: &str,
        target: &str,
        merge_base: Option<(BranchId, u64)>,
    ) -> StrataResult<ThreeWayDiffResult> {
        branch_ops::diff_three_way(&self.db, source, target, merge_base)
    }

    // =========================================================================
    // DAG-Required Operations
    // =========================================================================

    /// Fork a branch.
    pub fn fork(&self, source: &str, destination: &str) -> StrataResult<ForkInfo> {
        self.fork_with_options(source, destination, ForkOptions::default())
    }

    /// Fork a branch with options.
    ///
    /// Uses `BranchMutation` for atomicity: if DAG recording fails, the branch
    /// fork is rolled back (the new branch is deleted).
    ///
    /// Requires a DAG hook to be installed — forks without DAG recording would
    /// create orphan branches with no lineage tracking.
    pub fn fork_with_options(
        &self,
        source: &str,
        destination: &str,
        options: ForkOptions,
    ) -> StrataResult<ForkInfo> {
        validate_branch_name(destination)?;

        // Create mutation context for atomicity
        let mut mutation = BranchMutation::new(&self.db);

        // Fork requires DAG — without it, lineage is lost
        mutation.require_dag_hook("fork")?;

        // Execute the fork
        let info = branch_ops::fork_branch_with_metadata(
            &self.db,
            source,
            destination,
            options.message.as_deref(),
            options.creator.as_deref(),
        )?;

        // Register rollback: if DAG write fails, delete the forked branch
        // We set clear_storage=true because fork_branch creates storage state
        mutation.on_rollback_delete_branch(destination, true);

        // Record to DAG — if this fails, rollback is executed
        if let Some(fork_version) = info.fork_version {
            let source_id = resolve_branch_name(source);
            let dest_id = resolve_branch_name(destination);
            let mut event = DagEvent::fork(
                dest_id,
                destination,
                source_id,
                source,
                CommitVersion(fork_version),
            );
            if let Some(msg) = &options.message {
                event = event.with_message(msg.clone());
            }
            if let Some(creator) = &options.creator {
                event = event.with_creator(creator.clone());
            }
            mutation.record_dag_event(&event)?;
        }

        // Commit: fires observers, clears rollback actions
        if let Some(fork_version) = info.fork_version {
            let observer_event = BranchOpEvent::fork(
                resolve_branch_name(destination),
                destination,
                resolve_branch_name(source),
                CommitVersion(fork_version),
            );
            mutation.commit(observer_event);
        } else {
            mutation.commit_silent();
        }

        Ok(info)
    }

    /// Merge one branch into another.
    pub fn merge(&self, source: &str, target: &str) -> StrataResult<MergeInfo> {
        self.merge_with_options(source, target, MergeOptions::default())
    }

    /// Merge one branch into another with options.
    ///
    /// Uses `BranchMutation` for atomicity: DAG failures propagate as errors.
    /// Note: merge rollback is not implemented (the merge data is already
    /// committed). If DAG write fails, the merge data remains but the error
    /// is surfaced to the caller.
    ///
    /// Requires a DAG hook to be installed — merges without DAG recording would
    /// lose merge provenance and break merge-base computation.
    pub fn merge_with_options(
        &self,
        source: &str,
        target: &str,
        options: MergeOptions,
    ) -> StrataResult<MergeInfo> {
        // Create mutation context for atomicity
        // Note: we don't register rollback for merge because it's complex
        // (would need to revert all the changes). The mutation still ensures
        // DAG failures propagate and observers only fire on success.
        let mut mutation = BranchMutation::new(&self.db);

        // Merge requires DAG — without it, merge provenance is lost
        mutation.require_dag_hook("merge")?;

        let info = branch_ops::merge_branches_with_metadata(
            &self.db,
            source,
            target,
            options.strategy,
            options.merge_base,
            options.message.as_deref(),
            options.creator.as_deref(),
        )?;

        // Record to DAG — failures propagate
        let source_id = resolve_branch_name(source);
        let target_id = resolve_branch_name(target);
        let commit_version = info
            .merge_version
            .map(CommitVersion)
            .unwrap_or(CommitVersion::ZERO);
        let mut event = DagEvent::merge(target_id, target, source_id, source, commit_version);
        if let Some(msg) = &options.message {
            event = event.with_message(msg.clone());
        }
        if let Some(creator) = &options.creator {
            event = event.with_creator(creator.clone());
        }
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        let observer_event = BranchOpEvent {
            kind: BranchOpKind::Merge,
            branch_id: target_id,
            branch_name: Some(target.to_string()),
            source_branch_id: Some(source_id),
            commit_version: info.merge_version.map(CommitVersion),
            message: options.message.clone(),
            creator: options.creator.clone(),
        };
        mutation.commit(observer_event);

        Ok(info)
    }

    /// Revert a version range on a branch.
    ///
    /// Uses `BranchMutation` for atomicity. Emits a DAG revert event.
    ///
    /// Requires a DAG hook to be installed — reverts without DAG recording
    /// would lose revert provenance and break history queries.
    pub fn revert(
        &self,
        branch: &str,
        from_version: CommitVersion,
        to_version: CommitVersion,
    ) -> StrataResult<RevertInfo> {
        let mut mutation = BranchMutation::new(&self.db);

        // Revert requires DAG — without it, revert history is lost
        mutation.require_dag_hook("revert")?;

        let branch_id = resolve_branch_name(branch);

        // Execute the revert
        let info = branch_ops::revert_version_range(&self.db, branch, from_version, to_version)?;

        // Record to DAG if hook installed (optional)
        // Note: revert has no source branch, just a version range
        let event = DagEvent::revert(
            branch_id,
            branch,
            from_version,
            to_version,
            info.revert_version.unwrap_or(CommitVersion(0)),
        );
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        let observer_event = BranchOpEvent {
            kind: BranchOpKind::Revert,
            branch_id,
            branch_name: Some(branch.to_string()),
            source_branch_id: None,
            commit_version: info.revert_version,
            message: None,
            creator: None,
        };
        mutation.commit(observer_event);

        Ok(info)
    }

    /// Cherry-pick specific keys from one branch to another.
    ///
    /// Uses `BranchMutation` for atomicity. Emits a DAG cherry-pick event.
    ///
    /// Requires a DAG hook to be installed — cherry-picks without DAG recording
    /// would lose cherry-pick provenance and break history queries.
    pub fn cherry_pick(
        &self,
        source: &str,
        target: &str,
        keys: &[(String, String)],
    ) -> StrataResult<CherryPickInfo> {
        let mut mutation = BranchMutation::new(&self.db);

        // Cherry-pick requires DAG — without it, cherry-pick history is lost
        mutation.require_dag_hook("cherry_pick")?;

        let source_id = resolve_branch_name(source);
        let target_id = resolve_branch_name(target);

        // Execute the cherry-pick
        let info = branch_ops::cherry_pick_keys(&self.db, source, target, keys)?;

        // Record to DAG if hook installed (optional)
        let event = DagEvent::cherry_pick(
            target_id,
            target,
            source_id,
            source,
            CommitVersion(info.cherry_pick_version.unwrap_or(0)),
        );
        mutation.record_dag_event(&event)?;

        // Commit: fires observers
        let observer_event = BranchOpEvent {
            kind: BranchOpKind::CherryPick,
            branch_id: target_id,
            branch_name: Some(target.to_string()),
            source_branch_id: Some(source_id),
            commit_version: info.cherry_pick_version.map(CommitVersion),
            message: None,
            creator: None,
        };
        mutation.commit(observer_event);

        Ok(info)
    }

    // =========================================================================
    // DAG Queries
    // =========================================================================

    /// Find the merge base (common ancestor) of two branches.
    pub fn merge_base(&self, branch_a: &str, branch_b: &str) -> StrataResult<Option<(BranchId, CommitVersion)>> {
        let hook = self.dag_hook().require("merge_base").map_err(dag_to_strata)?;
        let id_a = resolve_branch_name(branch_a);
        let id_b = resolve_branch_name(branch_b);

        match hook.find_merge_base(&id_a, &id_b) {
            Ok(Some(result)) => Ok(Some((result.branch_id, result.commit_version))),
            Ok(None) => Ok(None),
            Err(e) => Err(dag_to_strata(e)),
        }
    }

    /// Get the history log for a branch.
    pub fn log(&self, branch: &str, limit: usize) -> StrataResult<Vec<crate::database::dag_hook::DagEvent>> {
        let hook = self.dag_hook().require("log").map_err(dag_to_strata)?;
        let branch_id = resolve_branch_name(branch);
        hook.log(&branch_id, limit).map_err(dag_to_strata)
    }

    /// Get the ancestry chain for a branch.
    pub fn ancestors(&self, branch: &str) -> StrataResult<Vec<crate::database::dag_hook::AncestryEntry>> {
        let hook = self.dag_hook().require("ancestors").map_err(dag_to_strata)?;
        let branch_id = resolve_branch_name(branch);
        hook.ancestors(&branch_id).map_err(dag_to_strata)
    }

    // =========================================================================
    // Tagging
    // =========================================================================

    /// Create a tag on a branch.
    pub fn tag(
        &self,
        branch: &str,
        name: &str,
        version: Option<u64>,
        message: Option<&str>,
    ) -> StrataResult<TagInfo> {
        branch_ops::create_tag(&self.db, branch, name, version, message, None)
    }

    /// Delete a tag from a branch.
    pub fn untag(&self, branch: &str, name: &str) -> StrataResult<bool> {
        branch_ops::delete_tag(&self.db, branch, name)
    }

    /// List all tags on a branch.
    pub fn list_tags(&self, branch: &str) -> StrataResult<Vec<TagInfo>> {
        branch_ops::list_tags(&self.db, branch)
    }

    /// Resolve a tag to its version.
    pub fn resolve_tag(&self, branch: &str, name: &str) -> StrataResult<Option<TagInfo>> {
        branch_ops::resolve_tag(&self.db, branch, name)
    }

    // =========================================================================
    // Internal Helpers
    // =========================================================================

    fn dag_hook(&self) -> &DagHookSlot {
        self.db.dag_hook()
    }

    fn branch_op_registry(&self) -> &BranchOpObserverRegistry {
        self.db.branch_op_observers()
    }

    fn notify_branch_op(&self, event: BranchOpEvent) {
        self.branch_op_registry().notify(&event);
    }
}

fn dag_to_strata(e: BranchDagError) -> StrataError {
    StrataError::invalid_input(e.message)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_branch_name() {
        // Valid names
        assert!(validate_branch_name("main").is_ok());
        assert!(validate_branch_name("feature-x").is_ok());
        assert!(validate_branch_name("feature/foo").is_ok());
        assert!(validate_branch_name("a").is_ok());

        // Invalid: empty
        assert!(validate_branch_name("").is_err());

        // Invalid: whitespace-only
        assert!(validate_branch_name("   ").is_err());
        assert!(validate_branch_name("\t").is_err());

        // Invalid: system prefix
        assert!(validate_branch_name("_system").is_err());
        assert!(validate_branch_name("_").is_err());

        // Invalid: too long
        assert!(validate_branch_name(&"x".repeat(256)).is_err());
        assert!(validate_branch_name(&"x".repeat(255)).is_ok());

        // Invalid: control characters
        assert!(validate_branch_name("foo\nbar").is_err());
        assert!(validate_branch_name("foo\x00bar").is_err());
    }

    #[test]
    fn test_merge_options() {
        let opts = MergeOptions::with_strategy(MergeStrategy::Strict)
            .with_message("Merge feature")
            .with_creator("alice");

        assert_eq!(opts.strategy, MergeStrategy::Strict);
        assert_eq!(opts.message, Some("Merge feature".to_string()));
    }
}
