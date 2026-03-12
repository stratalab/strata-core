//! Branch Lifecycle and Deterministic Replay
//!
//! This module implements branch lifecycle management and deterministic replay.
//!
//! ## Replay Invariants (P1-P6)
//!
//! | # | Invariant | Meaning |
//! |---|-----------|---------|
//! | P1 | Pure function | Over (Snapshot, WAL, EventLog) |
//! | P2 | Side-effect free | Does not mutate canonical store |
//! | P3 | Derived view | Not a new source of truth |
//! | P4 | Does not persist | Unless explicitly materialized |
//! | P5 | Deterministic | Same inputs = Same view |
//! | P6 | Idempotent | Running twice produces identical view |
//!
//! **CRITICAL**: Replay NEVER writes to the canonical store.
//! ReadOnlyView is derived, not authoritative.
//!
//! ## Stories Implemented
//!
//! - begin_branch() - Creates branch metadata, writes WAL entry
//! - end_branch() - Marks branch completed, writes WAL entry
//! - BranchIndex - Event offset tracking for O(branch size) replay
//! - replay_branch() - Returns ReadOnlyView
//! - diff_branches() - Key-level comparison
//! - Orphaned branch detection

use std::collections::HashMap;
use strata_core::branch_types::{BranchEventOffsets, BranchMetadata, BranchStatus};
use strata_core::types::{BranchId, Key};
use strata_core::value::Value;
use strata_core::PrimitiveType;
use strata_core::{EntityRef, StrataError};
use thiserror::Error;

// ============================================================================
// Branch Errors
// ============================================================================

/// Errors related to branch lifecycle operations
#[derive(Debug, Error)]
pub enum BranchError {
    /// Branch already exists
    #[error("Branch already exists: {0}")]
    AlreadyExists(BranchId),

    /// Branch not found
    #[error("Branch not found: {0}")]
    NotFound(BranchId),

    /// Branch is not active
    #[error("Branch not active: {0}")]
    NotActive(BranchId),

    /// WAL error
    #[error("WAL error: {0}")]
    Wal(String),

    /// Storage error (preserves original error for chain)
    #[error("Storage error: {0}")]
    Storage(#[from] StrataError),
}

// Conversion to StrataError
impl From<BranchError> for StrataError {
    fn from(e: BranchError) -> Self {
        match e {
            BranchError::AlreadyExists(branch_id) => StrataError::InvalidOperation {
                entity_ref: EntityRef::branch(branch_id),
                reason: format!("Branch '{}' already exists", branch_id),
            },
            BranchError::NotFound(branch_id) => StrataError::BranchNotFound { branch_id },
            BranchError::NotActive(branch_id) => StrataError::InvalidOperation {
                entity_ref: EntityRef::branch(branch_id),
                reason: "Branch is not active".to_string(),
            },
            BranchError::Wal(msg) => StrataError::Storage {
                message: format!("WAL error: {}", msg),
                source: None,
            },
            BranchError::Storage(e) => e, // Preserve original error
        }
    }
}

// ============================================================================
// Branch Index
// ============================================================================

/// Branch index for tracking branches and their events
///
/// Maps branches to their metadata and event offsets for O(branch size) replay.
#[derive(Debug, Default)]
pub struct BranchIndex {
    /// Branch metadata by branch ID
    branches: HashMap<BranchId, BranchMetadata>,
    /// Event offsets by branch ID (for O(branch size) replay)
    branch_events: HashMap<BranchId, BranchEventOffsets>,
}

impl BranchIndex {
    /// Create a new empty branch index
    pub fn new() -> Self {
        BranchIndex {
            branches: HashMap::new(),
            branch_events: HashMap::new(),
        }
    }

    /// Insert a new branch
    pub fn insert(&mut self, branch_id: BranchId, metadata: BranchMetadata) {
        self.branches.insert(branch_id, metadata);
        self.branch_events
            .insert(branch_id, BranchEventOffsets::new());
    }

    /// Check if a branch exists
    pub fn exists(&self, branch_id: BranchId) -> bool {
        self.branches.contains_key(&branch_id)
    }

    /// Get branch metadata
    pub fn get(&self, branch_id: BranchId) -> Option<&BranchMetadata> {
        self.branches.get(&branch_id)
    }

    /// Get mutable branch metadata
    pub fn get_mut(&mut self, branch_id: BranchId) -> Option<&mut BranchMetadata> {
        self.branches.get_mut(&branch_id)
    }

    /// Record an event offset for a branch
    pub fn record_event(&mut self, branch_id: BranchId, offset: u64) {
        if let Some(offsets) = self.branch_events.get_mut(&branch_id) {
            offsets.push(offset);
        }
        if let Some(meta) = self.branches.get_mut(&branch_id) {
            meta.increment_event_count();
        }
    }

    /// Get event offsets for a branch (for O(branch size) replay)
    pub fn get_event_offsets(&self, branch_id: BranchId) -> Option<&[u64]> {
        self.branch_events.get(&branch_id).map(|o| o.as_slice())
    }

    /// List all branches
    pub fn list(&self) -> Vec<&BranchMetadata> {
        self.branches.values().collect()
    }

    /// List all branch IDs
    pub fn list_branch_ids(&self) -> Vec<BranchId> {
        self.branches.keys().copied().collect()
    }

    /// Get branch status
    pub fn status(&self, branch_id: BranchId) -> BranchStatus {
        match self.branches.get(&branch_id) {
            Some(meta) => meta.status,
            None => BranchStatus::NotFound,
        }
    }

    /// Find branches that are still active (potential orphans after crash)
    pub fn find_active(&self) -> Vec<BranchId> {
        self.branches
            .iter()
            .filter(|(_, meta)| meta.status.is_active())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Mark branches as orphaned
    pub fn mark_orphaned(&mut self, branch_ids: &[BranchId]) {
        for branch_id in branch_ids {
            if let Some(meta) = self.branches.get_mut(branch_id) {
                meta.mark_orphaned();
            }
        }
    }

    /// Count branches by status
    pub fn count_by_status(&self) -> HashMap<BranchStatus, usize> {
        let mut counts = HashMap::new();
        for meta in self.branches.values() {
            *counts.entry(meta.status).or_insert(0) += 1;
        }
        counts
    }
}

// ============================================================================
// Read-Only View
// ============================================================================

/// Read-only view from replay
///
/// This is a derived view, NOT a new source of truth.
/// It does NOT persist and does NOT mutate the canonical store.
///
/// ## Replay Invariants
///
/// - P1: Pure function over (Snapshot, WAL, EventLog)
/// - P2: Side-effect free (does not mutate canonical store)
/// - P3: Derived view (not authoritative)
/// - P4: Does not persist (unless explicitly materialized)
/// - P5: Deterministic (same inputs = same view)
/// - P6: Idempotent (running twice produces identical view)
#[derive(Debug, Clone)]
pub struct ReadOnlyView {
    /// Branch this view is for
    pub branch_id: BranchId,
    /// KV state at branch end
    kv_state: HashMap<Key, Value>,
    /// Events during branch (simplified as key-value pairs)
    events: Vec<(String, Value)>,
    /// Number of operations in this view
    operation_count: u64,
}

impl ReadOnlyView {
    /// Create a new empty read-only view
    pub fn new(branch_id: BranchId) -> Self {
        ReadOnlyView {
            branch_id,
            kv_state: HashMap::new(),
            events: Vec::new(),
            operation_count: 0,
        }
    }

    /// Get a KV value
    pub fn get_kv(&self, key: &Key) -> Option<&Value> {
        self.kv_state.get(key)
    }

    /// Check if a KV key exists
    pub fn contains_kv(&self, key: &Key) -> bool {
        self.kv_state.contains_key(key)
    }

    /// List all KV keys
    pub fn kv_keys(&self) -> impl Iterator<Item = &Key> {
        self.kv_state.keys()
    }

    /// Get all KV entries
    pub fn kv_entries(&self) -> impl Iterator<Item = (&Key, &Value)> {
        self.kv_state.iter()
    }

    /// Get events
    pub fn events(&self) -> &[(String, Value)] {
        &self.events
    }

    /// Get event count
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Get operation count
    pub fn operation_count(&self) -> u64 {
        self.operation_count
    }

    /// Get number of KV entries
    pub fn kv_count(&self) -> usize {
        self.kv_state.len()
    }

    // Methods for building the view during replay
    // These are used by the replay implementation and tests

    /// Apply a KV put operation
    pub fn apply_kv_put(&mut self, key: Key, value: Value) {
        self.kv_state.insert(key, value);
        self.operation_count += 1;
    }

    /// Apply a KV delete operation
    pub fn apply_kv_delete(&mut self, key: &Key) {
        self.kv_state.remove(key);
        self.operation_count += 1;
    }

    /// Append an event
    pub fn append_event(&mut self, event_type: String, data: Value) {
        self.events.push((event_type, data));
        self.operation_count += 1;
    }
}

// ============================================================================
// Branch Diff
// ============================================================================

/// A single diff entry
#[derive(Debug, Clone)]
pub struct DiffEntry {
    /// Key that changed
    pub key: String,
    /// Primitive type
    pub primitive: PrimitiveType,
    /// Value in branch A (if present)
    pub value_a: Option<String>,
    /// Value in branch B (if present)
    pub value_b: Option<String>,
}

impl DiffEntry {
    /// Create a new diff entry for an added key
    pub fn added(key: String, primitive: PrimitiveType, value: String) -> Self {
        DiffEntry {
            key,
            primitive,
            value_a: None,
            value_b: Some(value),
        }
    }

    /// Create a new diff entry for a removed key
    pub fn removed(key: String, primitive: PrimitiveType, value: String) -> Self {
        DiffEntry {
            key,
            primitive,
            value_a: Some(value),
            value_b: None,
        }
    }

    /// Create a new diff entry for a modified key
    pub fn modified(
        key: String,
        primitive: PrimitiveType,
        old_value: String,
        new_value: String,
    ) -> Self {
        DiffEntry {
            key,
            primitive,
            value_a: Some(old_value),
            value_b: Some(new_value),
        }
    }
}

/// Diff between two branches at key level
#[derive(Debug, Clone)]
pub struct BranchDiff {
    /// Branch A (base)
    pub branch_a: BranchId,
    /// Branch B (comparison)
    pub branch_b: BranchId,
    /// Keys added in B (not in A)
    pub added: Vec<DiffEntry>,
    /// Keys removed in B (in A but not B)
    pub removed: Vec<DiffEntry>,
    /// Keys modified (different values)
    pub modified: Vec<DiffEntry>,
}

impl BranchDiff {
    /// Create a new empty diff
    pub fn new(branch_a: BranchId, branch_b: BranchId) -> Self {
        BranchDiff {
            branch_a,
            branch_b,
            added: Vec::new(),
            removed: Vec::new(),
            modified: Vec::new(),
        }
    }

    /// Check if there are any differences
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.modified.is_empty()
    }

    /// Total number of changes
    pub fn total_changes(&self) -> usize {
        self.added.len() + self.removed.len() + self.modified.len()
    }

    /// Get a summary string
    pub fn summary(&self) -> String {
        format!(
            "+{} -{} ~{} (total: {})",
            self.added.len(),
            self.removed.len(),
            self.modified.len(),
            self.total_changes()
        )
    }
}

/// Compare two ReadOnlyViews and produce a diff
pub fn diff_views(view_a: &ReadOnlyView, view_b: &ReadOnlyView) -> BranchDiff {
    let mut diff = BranchDiff::new(view_a.branch_id, view_b.branch_id);

    // Compare KV state
    diff_kv_maps(&view_a.kv_state, &view_b.kv_state, &mut diff);

    // Compare events (by count since events are append-only)
    if view_a.events.len() != view_b.events.len() {
        let a_count = view_a.events.len();
        let b_count = view_b.events.len();

        if b_count > a_count {
            // Events added in B
            for (event_type, data) in &view_b.events[a_count..] {
                diff.added.push(DiffEntry::added(
                    event_type.clone(),
                    PrimitiveType::Event,
                    format!("{:?}", data),
                ));
            }
        } else {
            // Events removed (shouldn't happen in normal operation, but detect it)
            for (event_type, data) in &view_a.events[b_count..] {
                diff.removed.push(DiffEntry::removed(
                    event_type.clone(),
                    PrimitiveType::Event,
                    format!("{:?}", data),
                ));
            }
        }
    }

    diff
}

fn diff_kv_maps(map_a: &HashMap<Key, Value>, map_b: &HashMap<Key, Value>, diff: &mut BranchDiff) {
    // Added: in B but not A
    for (key, value_b) in map_b {
        if !map_a.contains_key(key) {
            let key_str = key
                .user_key_string()
                .unwrap_or_else(|| format!("{:?}", key.user_key));
            diff.added.push(DiffEntry::added(
                key_str,
                PrimitiveType::Kv,
                format!("{:?}", value_b),
            ));
        }
    }

    // Removed: in A but not B
    for (key, value_a) in map_a {
        if !map_b.contains_key(key) {
            let key_str = key
                .user_key_string()
                .unwrap_or_else(|| format!("{:?}", key.user_key));
            diff.removed.push(DiffEntry::removed(
                key_str,
                PrimitiveType::Kv,
                format!("{:?}", value_a),
            ));
        }
    }

    // Modified: in both but different
    for (key, value_a) in map_a {
        if let Some(value_b) = map_b.get(key) {
            if value_a != value_b {
                let key_str = key
                    .user_key_string()
                    .unwrap_or_else(|| format!("{:?}", key.user_key));
                diff.modified.push(DiffEntry::modified(
                    key_str,
                    PrimitiveType::Kv,
                    format!("{:?}", value_a),
                    format!("{:?}", value_b),
                ));
            }
        }
    }
}

// ============================================================================
// Replay Error
// ============================================================================

/// Errors during replay
#[derive(Debug, Error)]
pub enum ReplayError {
    /// Branch not found
    #[error("Branch not found: {0}")]
    BranchNotFound(BranchId),

    /// Event log error
    #[error("Event log error: {0}")]
    EventLog(String),

    /// WAL error
    #[error("WAL error: {0}")]
    Wal(String),

    /// Invalid operation during replay
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::types::Namespace;
    use strata_core::Timestamp;

    fn test_namespace() -> Arc<Namespace> {
        Arc::new(Namespace::for_branch(BranchId::new()))
    }

    // ========== BranchIndex Tests ==========

    #[test]
    fn test_branch_index_new() {
        let index = BranchIndex::new();
        assert!(index.list().is_empty());
    }

    #[test]
    fn test_branch_index_insert_and_get() {
        let mut index = BranchIndex::new();
        let branch_id = BranchId::new();
        let metadata = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);

        index.insert(branch_id, metadata.clone());

        assert!(index.exists(branch_id));
        let retrieved = index.get(branch_id).unwrap();
        assert_eq!(retrieved.branch_id, branch_id);
        assert_eq!(retrieved.status, BranchStatus::Active);
    }

    #[test]
    fn test_branch_index_status() {
        let mut index = BranchIndex::new();
        let branch_id = BranchId::new();

        // Non-existent branch
        assert_eq!(index.status(branch_id), BranchStatus::NotFound);

        // Insert branch
        let metadata = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
        index.insert(branch_id, metadata);

        assert_eq!(index.status(branch_id), BranchStatus::Active);
    }

    #[test]
    fn test_branch_index_record_event() {
        let mut index = BranchIndex::new();
        let branch_id = BranchId::new();
        let metadata = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);

        index.insert(branch_id, metadata);
        index.record_event(branch_id, 100);
        index.record_event(branch_id, 200);
        index.record_event(branch_id, 300);

        let offsets = index.get_event_offsets(branch_id).unwrap();
        assert_eq!(offsets, &[100, 200, 300]);

        let meta = index.get(branch_id).unwrap();
        assert_eq!(meta.event_count, 3);
    }

    #[test]
    fn test_branch_index_find_active() {
        let mut index = BranchIndex::new();

        let run1 = BranchId::new();
        let run2 = BranchId::new();
        let run3 = BranchId::new();

        index.insert(run1, BranchMetadata::new(run1, Timestamp::from(1000), 0));
        index.insert(run2, BranchMetadata::new(run2, Timestamp::from(2000), 100));
        index.insert(run3, BranchMetadata::new(run3, Timestamp::from(3000), 200));

        // Complete branch2
        index.get_mut(run2).unwrap().complete(Timestamp::from(2500), 150);

        let active = index.find_active();
        assert_eq!(active.len(), 2);
        assert!(active.contains(&run1));
        assert!(active.contains(&run3));
        assert!(!active.contains(&run2));
    }

    #[test]
    fn test_branch_index_mark_orphaned() {
        let mut index = BranchIndex::new();

        let run1 = BranchId::new();
        let run2 = BranchId::new();

        index.insert(run1, BranchMetadata::new(run1, Timestamp::from(1000), 0));
        index.insert(run2, BranchMetadata::new(run2, Timestamp::from(2000), 100));

        index.mark_orphaned(&[run1]);

        assert_eq!(index.status(run1), BranchStatus::Orphaned);
        assert_eq!(index.status(run2), BranchStatus::Active);
    }

    // ========== ReadOnlyView Tests ==========

    #[test]
    fn test_read_only_view_new() {
        let branch_id = BranchId::new();
        let view = ReadOnlyView::new(branch_id);

        assert_eq!(view.branch_id, branch_id);
        assert_eq!(view.kv_count(), 0);
        assert_eq!(view.event_count(), 0);
        assert_eq!(view.operation_count(), 0);
    }

    #[test]
    fn test_read_only_view_kv_operations() {
        let branch_id = BranchId::new();
        let ns = test_namespace();
        let mut view = ReadOnlyView::new(branch_id);

        let key = Key::new_kv(ns.clone(), "test-key");
        let value = Value::String("test-value".into());

        // Apply put
        view.apply_kv_put(key.clone(), value.clone());
        assert_eq!(view.get_kv(&key), Some(&value));
        assert!(view.contains_kv(&key));
        assert_eq!(view.kv_count(), 1);
        assert_eq!(view.operation_count(), 1);

        // Apply delete
        view.apply_kv_delete(&key);
        assert_eq!(view.get_kv(&key), None);
        assert!(!view.contains_kv(&key));
        assert_eq!(view.kv_count(), 0);
        assert_eq!(view.operation_count(), 2);
    }

    #[test]
    fn test_read_only_view_events() {
        let branch_id = BranchId::new();
        let mut view = ReadOnlyView::new(branch_id);

        view.append_event("UserCreated".into(), Value::String("alice".into()));
        view.append_event("UserUpdated".into(), Value::String("bob".into()));

        assert_eq!(view.event_count(), 2);
        assert_eq!(view.events()[0].0, "UserCreated");
        assert_eq!(view.events()[1].0, "UserUpdated");
    }

    // ========== BranchDiff Tests ==========

    #[test]
    fn test_branch_diff_empty() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        let view_a = ReadOnlyView::new(branch_a);
        let view_b = ReadOnlyView::new(branch_b);

        let diff = diff_views(&view_a, &view_b);
        assert!(diff.is_empty());
        assert_eq!(diff.total_changes(), 0);
    }

    #[test]
    fn test_branch_diff_added() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns = test_namespace();

        let view_a = ReadOnlyView::new(branch_a);

        let mut view_b = ReadOnlyView::new(branch_b);
        view_b.apply_kv_put(Key::new_kv(ns.clone(), "new-key"), Value::Int(42));

        let diff = diff_views(&view_a, &view_b);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.removed.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.added[0].key, "new-key");
    }

    #[test]
    fn test_branch_diff_removed() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns = test_namespace();

        let mut view_a = ReadOnlyView::new(branch_a);
        view_a.apply_kv_put(Key::new_kv(ns.clone(), "old-key"), Value::Int(42));

        let view_b = ReadOnlyView::new(branch_b);

        let diff = diff_views(&view_a, &view_b);
        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.removed[0].key, "old-key");
    }

    #[test]
    fn test_branch_diff_modified() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns = test_namespace();

        let key = Key::new_kv(ns.clone(), "shared-key");

        let mut view_a = ReadOnlyView::new(branch_a);
        view_a.apply_kv_put(key.clone(), Value::Int(1));

        let mut view_b = ReadOnlyView::new(branch_b);
        view_b.apply_kv_put(key.clone(), Value::Int(2));

        let diff = diff_views(&view_a, &view_b);
        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.removed.len(), 0);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].key, "shared-key");
    }

    #[test]
    fn test_branch_diff_summary() {
        let diff = BranchDiff {
            branch_a: BranchId::new(),
            branch_b: BranchId::new(),
            added: vec![DiffEntry::added("a".into(), PrimitiveType::Kv, "1".into())],
            removed: vec![
                DiffEntry::removed("b".into(), PrimitiveType::Kv, "2".into()),
                DiffEntry::removed("c".into(), PrimitiveType::Kv, "3".into()),
            ],
            modified: vec![DiffEntry::modified(
                "d".into(),
                PrimitiveType::Kv,
                "4".into(),
                "5".into(),
            )],
        };

        assert_eq!(diff.summary(), "+1 -2 ~1 (total: 4)");
    }

    // ========== Orphaned Branch Detection Tests ==========

    #[test]
    fn test_orphaned_detection() {
        let mut index = BranchIndex::new();

        // Create some branches
        let run1 = BranchId::new();
        let run2 = BranchId::new();
        let run3 = BranchId::new();

        index.insert(run1, BranchMetadata::new(run1, Timestamp::from(1000), 0));
        index.insert(run2, BranchMetadata::new(run2, Timestamp::from(2000), 100));
        index.insert(run3, BranchMetadata::new(run3, Timestamp::from(3000), 200));

        // Complete branch2 properly
        index.get_mut(run2).unwrap().complete(Timestamp::from(2500), 150);

        // Simulate crash - run1 and run3 are still active
        let active = index.find_active();
        assert_eq!(active.len(), 2);

        // Mark them as orphaned
        index.mark_orphaned(&active);

        // Verify
        assert_eq!(index.status(run1), BranchStatus::Orphaned);
        assert_eq!(index.status(run2), BranchStatus::Completed);
        assert_eq!(index.status(run3), BranchStatus::Orphaned);
    }

    #[test]
    fn test_count_by_status() {
        let mut index = BranchIndex::new();

        // Create runs with different states
        for _ in 0..3 {
            let branch_id = BranchId::new();
            index.insert(branch_id, BranchMetadata::new(branch_id, Timestamp::from(1000), 0));
        }

        for _ in 0..2 {
            let branch_id = BranchId::new();
            let mut meta = BranchMetadata::new(branch_id, Timestamp::from(1000), 0);
            meta.complete(Timestamp::from(2000), 100);
            index.insert(branch_id, meta);
        }

        let counts = index.count_by_status();
        assert_eq!(counts.get(&BranchStatus::Active), Some(&3));
        assert_eq!(counts.get(&BranchStatus::Completed), Some(&2));
    }

    // ========== Replay Invariant Tests ==========
    // These tests verify the documented invariants P1-P6

    /// P5: Deterministic - Same inputs = Same view
    /// Running replay with the same operations should produce identical views
    #[test]
    fn test_replay_invariant_p5_deterministic() {
        let branch_id = BranchId::new();
        let ns = test_namespace();

        // Define a sequence of operations
        let operations: Vec<(&str, Value)> = vec![
            ("key1", Value::Int(100)),
            ("key2", Value::String("hello".into())),
            ("key3", Value::Float(3.14)),
        ];

        // Create first view
        let mut view1 = ReadOnlyView::new(branch_id);
        for (key, value) in &operations {
            view1.apply_kv_put(Key::new_kv(ns.clone(), key), value.clone());
        }
        view1.append_event("TestEvent".into(), Value::Int(1));

        // Create second view with same operations
        let mut view2 = ReadOnlyView::new(branch_id);
        for (key, value) in &operations {
            view2.apply_kv_put(Key::new_kv(ns.clone(), key), value.clone());
        }
        view2.append_event("TestEvent".into(), Value::Int(1));

        // Views should be identical
        assert_eq!(view1.kv_count(), view2.kv_count());
        assert_eq!(view1.event_count(), view2.event_count());
        assert_eq!(view1.operation_count(), view2.operation_count());

        // Every key in view1 should have the same value in view2
        for (key, value) in view1.kv_entries() {
            let value2 = view2.get_kv(key);
            assert_eq!(Some(value), value2, "Values differ for key {:?}", key);
        }

        // Diff should be empty
        let diff = diff_views(&view1, &view2);
        assert!(
            diff.is_empty(),
            "Deterministic replay should produce identical views"
        );
    }

    /// P5: Deterministic - Order of operations matters
    /// Different operation orders should produce different views
    #[test]
    fn test_replay_invariant_p5_order_matters() {
        let branch_id = BranchId::new();
        let ns = test_namespace();
        let key = Key::new_kv(ns.clone(), "counter");

        // View 1: put 1, then put 2 (final value = 2)
        let mut view1 = ReadOnlyView::new(branch_id);
        view1.apply_kv_put(key.clone(), Value::Int(1));
        view1.apply_kv_put(key.clone(), Value::Int(2));

        // View 2: put 2, then put 1 (final value = 1)
        let mut view2 = ReadOnlyView::new(branch_id);
        view2.apply_kv_put(key.clone(), Value::Int(2));
        view2.apply_kv_put(key.clone(), Value::Int(1));

        // Final values should differ
        assert_eq!(view1.get_kv(&key), Some(&Value::Int(2)));
        assert_eq!(view2.get_kv(&key), Some(&Value::Int(1)));

        // Diff should show modification
        let diff = diff_views(&view1, &view2);
        assert!(!diff.is_empty());
        assert_eq!(diff.modified.len(), 1);
    }

    /// P6: Idempotent - Running twice produces identical view
    /// Applying the same operation sequence twice should give same result
    #[test]
    fn test_replay_invariant_p6_idempotent() {
        let branch_id = BranchId::new();
        let ns = test_namespace();

        // Function to build a view from operations
        fn build_view(branch_id: BranchId, ns: &Arc<Namespace>) -> ReadOnlyView {
            let mut view = ReadOnlyView::new(branch_id);
            view.apply_kv_put(Key::new_kv(ns.clone(), "a"), Value::Int(1));
            view.apply_kv_put(Key::new_kv(ns.clone(), "b"), Value::Int(2));
            view.apply_kv_delete(&Key::new_kv(ns.clone(), "a"));
            view.apply_kv_put(Key::new_kv(ns.clone(), "c"), Value::Int(3));
            view.append_event("E1".into(), Value::Null);
            view.append_event("E2".into(), Value::Null);
            view
        }

        // Run twice
        let view1 = build_view(branch_id, &ns);
        let view2 = build_view(branch_id, &ns);

        // Should be identical
        assert_eq!(view1.kv_count(), view2.kv_count());
        assert_eq!(view1.event_count(), view2.event_count());

        let diff = diff_views(&view1, &view2);
        assert!(
            diff.is_empty(),
            "Idempotent replay should produce identical views"
        );
    }

    /// P2: Side-effect free - ReadOnlyView operations don't affect external state
    /// This is a structural test - we verify the view is self-contained
    #[test]
    fn test_replay_invariant_p2_self_contained() {
        let branch_id = BranchId::new();
        let ns = test_namespace();

        // Create a view and modify it
        let mut view = ReadOnlyView::new(branch_id);
        let key = Key::new_kv(ns.clone(), "test");

        // The view should be completely self-contained
        view.apply_kv_put(key.clone(), Value::Int(42));

        // Create another view - it should be independent
        let view2 = ReadOnlyView::new(branch_id);

        // view2 should not see view's changes (they're independent)
        assert!(view.contains_kv(&key));
        assert!(!view2.contains_kv(&key));
    }

    /// P3: Derived view - Not a source of truth
    /// Views are snapshots, not live data
    #[test]
    fn test_replay_invariant_p3_derived_view() {
        let branch_id = BranchId::new();
        let ns = test_namespace();
        let key = Key::new_kv(ns.clone(), "test");

        // Create a view
        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(key.clone(), Value::Int(1));

        // Clone the view
        let view_clone = view.clone();

        // Modify original
        view.apply_kv_put(key.clone(), Value::Int(2));

        // Clone should retain original value (it's a snapshot)
        assert_eq!(view.get_kv(&key), Some(&Value::Int(2)));
        assert_eq!(view_clone.get_kv(&key), Some(&Value::Int(1)));
    }

    // ========== Additional Diff Tests for Robustness ==========

    #[test]
    fn test_diff_complex_scenario() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();
        let ns = test_namespace();

        let mut view_a = ReadOnlyView::new(branch_a);
        view_a.apply_kv_put(Key::new_kv(ns.clone(), "shared"), Value::Int(1));
        view_a.apply_kv_put(Key::new_kv(ns.clone(), "only_a"), Value::Int(2));
        view_a.apply_kv_put(Key::new_kv(ns.clone(), "modified"), Value::Int(10));
        view_a.append_event("E1".into(), Value::Null);

        let mut view_b = ReadOnlyView::new(branch_b);
        view_b.apply_kv_put(Key::new_kv(ns.clone(), "shared"), Value::Int(1)); // Same
        view_b.apply_kv_put(Key::new_kv(ns.clone(), "only_b"), Value::Int(3)); // Added
        view_b.apply_kv_put(Key::new_kv(ns.clone(), "modified"), Value::Int(20)); // Modified
        view_b.append_event("E1".into(), Value::Null);
        view_b.append_event("E2".into(), Value::Null); // Added event

        let diff = diff_views(&view_a, &view_b);

        // Verify counts
        assert_eq!(diff.added.len(), 2, "Should have 2 additions (only_b + E2)");
        assert_eq!(diff.removed.len(), 1, "Should have 1 removal (only_a)");
        assert_eq!(
            diff.modified.len(),
            1,
            "Should have 1 modification (modified)"
        );

        // Verify specific entries
        assert!(diff.added.iter().any(|e| e.key == "only_b"));
        assert!(diff.removed.iter().any(|e| e.key == "only_a"));
        assert!(diff.modified.iter().any(|e| e.key == "modified"));
    }

    #[test]
    fn test_diff_event_count_difference() {
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        let mut view_a = ReadOnlyView::new(branch_a);
        view_a.append_event("E1".into(), Value::Int(1));
        view_a.append_event("E2".into(), Value::Int(2));
        view_a.append_event("E3".into(), Value::Int(3));

        let mut view_b = ReadOnlyView::new(branch_b);
        view_b.append_event("E1".into(), Value::Int(1));

        let diff = diff_views(&view_a, &view_b);

        // B has fewer events than A - should show as removed
        assert_eq!(diff.removed.len(), 2);
        assert!(diff
            .removed
            .iter()
            .all(|e| e.primitive == PrimitiveType::Event));
    }

    #[test]
    fn test_branch_index_list_branch_ids() {
        let mut index = BranchIndex::new();

        let run1 = BranchId::new();
        let run2 = BranchId::new();
        let run3 = BranchId::new();

        index.insert(run1, BranchMetadata::new(run1, Timestamp::from(1000), 0));
        index.insert(run2, BranchMetadata::new(run2, Timestamp::from(2000), 100));
        index.insert(run3, BranchMetadata::new(run3, Timestamp::from(3000), 200));

        let ids = index.list_branch_ids();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&run1));
        assert!(ids.contains(&run2));
        assert!(ids.contains(&run3));
    }

    #[test]
    fn test_read_only_view_kv_keys_iterator() {
        let branch_id = BranchId::new();
        let ns = test_namespace();

        let mut view = ReadOnlyView::new(branch_id);
        view.apply_kv_put(Key::new_kv(ns.clone(), "a"), Value::Int(1));
        view.apply_kv_put(Key::new_kv(ns.clone(), "b"), Value::Int(2));
        view.apply_kv_put(Key::new_kv(ns.clone(), "c"), Value::Int(3));

        let keys: Vec<_> = view.kv_keys().collect();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_branch_error_conversions() {
        // Test From<BranchError> for StrataError
        let error = BranchError::AlreadyExists(BranchId::new());
        let strata_error: StrataError = error.into();
        assert!(matches!(strata_error, StrataError::InvalidOperation { .. }));

        let error = BranchError::NotFound(BranchId::new());
        let strata_error: StrataError = error.into();
        assert!(matches!(strata_error, StrataError::BranchNotFound { .. }));

        let error = BranchError::NotActive(BranchId::new());
        let strata_error: StrataError = error.into();
        assert!(matches!(strata_error, StrataError::InvalidOperation { .. }));

        let error = BranchError::Wal("test".to_string());
        let strata_error: StrataError = error.into();
        assert!(matches!(strata_error, StrataError::Storage { .. }));

        let storage_err = StrataError::Storage {
            message: "test".to_string(),
            source: None,
        };
        let error = BranchError::Storage(storage_err);
        let strata_error: StrataError = error.into();
        assert!(matches!(strata_error, StrataError::Storage { .. }));
    }

    #[test]
    fn test_replay_error_display() {
        let error = ReplayError::BranchNotFound(BranchId::new());
        let msg = error.to_string();
        assert!(msg.contains("Branch not found"));

        let error = ReplayError::EventLog("test error".to_string());
        let msg = error.to_string();
        assert!(msg.contains("Event log error"));
        assert!(msg.contains("test error"));

        let error = ReplayError::Wal("wal error".to_string());
        let msg = error.to_string();
        assert!(msg.contains("WAL error"));

        let error = ReplayError::InvalidOperation("invalid".to_string());
        let msg = error.to_string();
        assert!(msg.contains("Invalid operation"));
    }

    #[test]
    fn test_diff_entry_constructors() {
        let added = DiffEntry::added("key".into(), PrimitiveType::Kv, "value".into());
        assert!(added.value_a.is_none());
        assert!(added.value_b.is_some());

        let removed = DiffEntry::removed("key".into(), PrimitiveType::Kv, "value".into());
        assert!(removed.value_a.is_some());
        assert!(removed.value_b.is_none());

        let modified =
            DiffEntry::modified("key".into(), PrimitiveType::Kv, "old".into(), "new".into());
        assert!(modified.value_a.is_some());
        assert!(modified.value_b.is_some());
    }

    #[test]
    fn test_primitive_type_display() {
        assert_eq!(format!("{}", PrimitiveType::Kv), "KVStore");
        assert_eq!(format!("{}", PrimitiveType::Event), "EventLog");
    }
}
