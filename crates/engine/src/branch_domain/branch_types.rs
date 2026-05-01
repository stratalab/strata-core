//! Branch lifecycle types.
//!
//! This module defines the branch lifecycle types for durability and replay.
//! These are distinct from the branch management types in `primitives::branch::index`.

use serde::{Deserialize, Serialize};
use strata_core::{BranchId, Timestamp};

/// Branch lifecycle status for durability and replay.
///
/// This enum represents the lifecycle states relevant to durability:
/// - Active: Branch in progress (`begin_branch` called, `end_branch` not yet called)
/// - Completed: Branch finished normally (`end_branch` called)
/// - Orphaned: Branch was never ended (crash without end_branch marker)
/// - NotFound: Branch doesn't exist in the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BranchStatus {
    /// Branch is active (`begin_branch` called, `end_branch` not yet called)
    Active,
    /// Branch completed normally (`end_branch` called)
    Completed,
    /// Branch was never ended (orphaned - no end_branch marker in WAL)
    Orphaned,
    /// Branch doesn't exist
    NotFound,
}

impl BranchStatus {
    /// Check if branch is still active.
    pub fn is_active(&self) -> bool {
        matches!(self, BranchStatus::Active)
    }

    /// Check if branch is completed.
    pub fn is_completed(&self) -> bool {
        matches!(self, BranchStatus::Completed)
    }

    /// Check if branch is orphaned.
    pub fn is_orphaned(&self) -> bool {
        matches!(self, BranchStatus::Orphaned)
    }

    /// Check if branch exists (any status except NotFound).
    pub fn exists(&self) -> bool {
        !matches!(self, BranchStatus::NotFound)
    }

    /// Get string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            BranchStatus::Active => "Active",
            BranchStatus::Completed => "Completed",
            BranchStatus::Orphaned => "Orphaned",
            BranchStatus::NotFound => "NotFound",
        }
    }
}

impl std::fmt::Display for BranchStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Branch metadata for replay and recovery.
///
/// Contains all information needed to replay a branch and track its lifecycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchMetadata {
    /// Branch ID.
    pub branch_id: BranchId,
    /// Current status.
    pub status: BranchStatus,
    /// When branch started (microseconds since epoch).
    pub started_at: Timestamp,
    /// When branch ended (if completed).
    pub ended_at: Option<Timestamp>,
    /// Number of events in this branch.
    pub event_count: u64,
    /// WAL offset where branch began.
    pub begin_wal_offset: u64,
    /// WAL offset where branch ended (if completed).
    pub end_wal_offset: Option<u64>,
}

impl BranchMetadata {
    /// Create metadata for a new branch.
    pub fn new(branch_id: BranchId, started_at: Timestamp, begin_wal_offset: u64) -> Self {
        BranchMetadata {
            branch_id,
            status: BranchStatus::Active,
            started_at,
            ended_at: None,
            event_count: 0,
            begin_wal_offset,
            end_wal_offset: None,
        }
    }

    /// Mark branch as completed.
    pub fn complete(&mut self, ended_at: Timestamp, end_wal_offset: u64) {
        self.status = BranchStatus::Completed;
        self.ended_at = Some(ended_at);
        self.end_wal_offset = Some(end_wal_offset);
    }

    /// Mark branch as orphaned.
    pub fn mark_orphaned(&mut self) {
        self.status = BranchStatus::Orphaned;
    }

    /// Duration in microseconds (if completed).
    pub fn duration_micros(&self) -> Option<u64> {
        self.ended_at
            .map(|e| e.as_micros().saturating_sub(self.started_at.as_micros()))
    }

    /// Increment event count.
    pub fn increment_event_count(&mut self) {
        self.event_count += 1;
    }
}

/// Event offsets for a branch (for O(branch size) replay).
///
/// Maps a branch to its event offsets in the EventLog,
/// enabling efficient replay without scanning the entire log.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BranchEventOffsets {
    /// WAL offsets of events belonging to this branch.
    pub offsets: Vec<u64>,
}

impl BranchEventOffsets {
    /// Create new empty offsets.
    pub fn new() -> Self {
        BranchEventOffsets {
            offsets: Vec::new(),
        }
    }

    /// Add an offset.
    pub fn push(&mut self, offset: u64) {
        debug_assert!(
            self.offsets.last().map_or(true, |&last| offset >= last),
            "BranchEventOffsets: new offset {} is less than last offset {:?}",
            offset,
            self.offsets.last()
        );
        self.offsets.push(offset);
    }

    /// Get all offsets.
    pub fn as_slice(&self) -> &[u64] {
        &self.offsets
    }

    /// Get number of offsets.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_status_transitions_work() {
        let branch_id = BranchId::new();
        let mut meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        assert_eq!(meta.status, BranchStatus::Active);
        assert!(meta.status.is_active());
        assert!(meta.status.exists());

        meta.complete(Timestamp::from(2000), 100);
        assert_eq!(meta.status, BranchStatus::Completed);
        assert!(meta.status.is_completed());
        assert_eq!(meta.duration_micros(), Some(1000));
        assert_eq!(meta.end_wal_offset, Some(100));
    }

    #[test]
    fn branch_status_orphaned_works() {
        let branch_id = BranchId::new();
        let mut meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        assert_eq!(meta.status, BranchStatus::Active);

        meta.mark_orphaned();
        assert_eq!(meta.status, BranchStatus::Orphaned);
        assert!(meta.status.is_orphaned());
        assert!(meta.status.exists());
    }

    #[test]
    fn branch_status_not_found_is_nonexistent() {
        let status = BranchStatus::NotFound;
        assert!(!status.exists());
        assert!(!status.is_active());
        assert!(!status.is_completed());
        assert!(!status.is_orphaned());
    }

    #[test]
    fn branch_status_strings_are_stable() {
        assert_eq!(BranchStatus::Active.as_str(), "Active");
        assert_eq!(BranchStatus::Completed.as_str(), "Completed");
        assert_eq!(BranchStatus::Orphaned.as_str(), "Orphaned");
        assert_eq!(BranchStatus::NotFound.as_str(), "NotFound");
        assert_eq!(format!("{}", BranchStatus::Active), "Active");
        assert_eq!(format!("{}", BranchStatus::Completed), "Completed");
    }

    #[test]
    fn branch_metadata_round_trips_through_serde() {
        let branch_id = BranchId::new();
        let meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 50);

        let json = serde_json::to_string(&meta).unwrap();
        let restored: BranchMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(meta, restored);
    }

    #[test]
    fn branch_event_offsets_behave_monotonically() {
        let mut offsets = BranchEventOffsets::new();
        assert!(offsets.is_empty());
        assert_eq!(offsets.len(), 0);

        offsets.push(100);
        offsets.push(200);
        offsets.push(300);

        assert!(!offsets.is_empty());
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets.as_slice(), &[100, 200, 300]);
    }

    #[test]
    fn branch_metadata_event_count_increments() {
        let branch_id = BranchId::new();
        let mut meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        assert_eq!(meta.event_count, 0);

        meta.increment_event_count();
        meta.increment_event_count();
        meta.increment_event_count();

        assert_eq!(meta.event_count, 3);
    }

    #[test]
    fn active_branch_has_no_duration() {
        let branch_id = BranchId::new();
        let meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        assert!(meta.duration_micros().is_none());
    }

    #[test]
    fn branch_event_offsets_round_trip_through_serde() {
        let mut offsets = BranchEventOffsets::new();
        offsets.push(10);
        offsets.push(20);

        let json = serde_json::to_string(&offsets).unwrap();
        let restored: BranchEventOffsets = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.as_slice(), &[10, 20]);
    }

    #[test]
    fn branch_event_offsets_default_is_empty() {
        let offsets = BranchEventOffsets::default();
        assert!(offsets.is_empty());
        assert_eq!(offsets.len(), 0);
    }

    #[test]
    fn branch_status_round_trips_through_serde() {
        for status in [
            BranchStatus::Active,
            BranchStatus::Completed,
            BranchStatus::Orphaned,
            BranchStatus::NotFound,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let restored: BranchStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, restored);
        }
    }

    #[test]
    fn branch_metadata_double_completion_overwrites_prior_end_state() {
        let branch_id = BranchId::new();
        let mut meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        meta.complete(Timestamp::from(2000), 100);
        meta.complete(Timestamp::from(3000), 200);

        assert_eq!(meta.status, BranchStatus::Completed);
        assert_eq!(meta.ended_at, Some(Timestamp::from(3000)));
        assert_eq!(meta.end_wal_offset, Some(200));
    }

    #[test]
    fn branch_metadata_can_be_marked_orphaned_after_completion() {
        let branch_id = BranchId::new();
        let mut meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        meta.complete(Timestamp::from(2000), 100);

        meta.mark_orphaned();

        assert_eq!(meta.status, BranchStatus::Orphaned);
    }

    #[test]
    fn branch_metadata_duration_saturates_when_end_precedes_start() {
        let branch_id = BranchId::new();
        let mut meta = BranchMetadata::new(branch_id, Timestamp::from(2000), 0);
        meta.complete(Timestamp::from(1000), 50);

        assert_eq!(meta.duration_micros(), Some(0));
    }

    #[test]
    fn branch_status_hashes_all_variants_distinctly() {
        use std::collections::HashSet;

        let statuses = [
            BranchStatus::Active,
            BranchStatus::Completed,
            BranchStatus::Orphaned,
            BranchStatus::NotFound,
        ];
        let set: HashSet<BranchStatus> = statuses.iter().copied().collect();
        assert_eq!(set.len(), 4, "all BranchStatus variants must hash uniquely");
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "new offset")]
    fn branch_event_offsets_debug_rejects_non_monotonic_pushes() {
        let mut offsets = BranchEventOffsets::new();
        offsets.push(300);
        offsets.push(100);
    }
}
