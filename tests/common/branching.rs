//! Shared helpers for the B4.x branching coverage suites.
//!
//! Synthesize lifecycle states no production path produces (most notably
//! `Archived`), capture branch-op observer output, and snapshot the
//! lineage surface so tests can assert "no side effects on reject".
//!
//! These live next to the tests because the canonical
//! `BranchControlStore` is `pub(crate)`; the engine exposes two
//! `#[doc(hidden)] pub` accessors (`BranchService::set_lifecycle_for_test`
//! and `BranchService::lineage_edge_count_for_test`) that this module
//! wraps in an ergonomic, test-local API.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use strata_core::branch::BranchLifecycleStatus;
use strata_core::BranchRef;
use strata_engine::database::{BranchOpEvent, BranchOpObserver, ObserverError};
use strata_engine::Database;

// =============================================================================
// Lifecycle synthesis
// =============================================================================

/// Flip the live control record of `name` to `Archived`.
///
/// Panics if the branch has no live record. Callers must create the
/// branch via `db.branches().create(name)` (or fork it) first.
pub(crate) fn archive_branch_for_test(db: &Arc<Database>, name: &str) -> BranchRef {
    db.branches()
        .set_lifecycle_for_test(name, BranchLifecycleStatus::Archived)
        .unwrap_or_else(|e| panic!("archive_branch_for_test({name}): {e:?}"))
}

/// Flip the live control record of `name` to `status`.
///
/// Wraps `BranchService::set_lifecycle_for_test` so tests can synthesize
/// any lifecycle state (`Active`, `Archived`, `Deleted`) without going
/// through the production delete path (which also purges legacy
/// metadata). Returns the affected `BranchRef`.
pub(crate) fn set_lifecycle_for_test(
    db: &Arc<Database>,
    name: &str,
    status: BranchLifecycleStatus,
) -> BranchRef {
    db.branches()
        .set_lifecycle_for_test(name, status)
        .unwrap_or_else(|e| panic!("set_lifecycle_for_test({name}, {status:?}): {e:?}"))
}

// =============================================================================
// Observer capture
// =============================================================================

/// Capture every `BranchOpEvent` the engine fires. Register with
/// `db.branch_op_observers().register(capturing.clone())` before the
/// workload under test.
pub(crate) struct CapturingBranchObserver {
    events: Mutex<Vec<BranchOpEvent>>,
}

impl CapturingBranchObserver {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }

    /// Current number of captured events. Cheap — snapshot only.
    pub(crate) fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Clone of every captured event so far.
    pub(crate) fn snapshot(&self) -> Vec<BranchOpEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl BranchOpObserver for CapturingBranchObserver {
    fn name(&self) -> &'static str {
        "branching_lifecycle::CapturingBranchObserver"
    }

    fn on_branch_op(&self, event: &BranchOpEvent) -> Result<(), ObserverError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

// =============================================================================
// Side-effect checkpoint
// =============================================================================

/// Snapshot of the observable side-effect surface for a single branch:
/// observer event count and lineage edge count.
///
/// Taken before a gate-refused mutation and re-compared after — any drift
/// means the reject path leaked a side effect.
#[derive(Debug, Clone)]
pub(crate) struct BranchSideEffectCheckpoint {
    pub(crate) target_name: String,
    pub(crate) observer_count: usize,
    pub(crate) lineage_edge_count: usize,
}

impl BranchSideEffectCheckpoint {
    pub(crate) fn capture(
        db: &Arc<Database>,
        observer: &CapturingBranchObserver,
        target_name: &str,
    ) -> Self {
        // Surface the error loudly rather than silently pretending the
        // edge count was 0 — a swallowed error here would mask real
        // regressions (unmigrated-follower refusal, corruption) behind
        // a passing side-effect assertion.
        let lineage_edge_count = db
            .branches()
            .lineage_edge_count_for_test(target_name)
            .unwrap_or_else(|e| {
                panic!("lineage_edge_count_for_test({target_name}) failed while checkpointing: {e:?}")
            });
        Self {
            target_name: target_name.to_string(),
            observer_count: observer.count(),
            lineage_edge_count,
        }
    }
}

/// Assert `(observer events, lineage edges)` did not change since
/// `before`. Panics with a diagnostic when the reject path leaked a
/// side effect.
pub(crate) fn assert_no_side_effects_since(
    db: &Arc<Database>,
    observer: &CapturingBranchObserver,
    before: &BranchSideEffectCheckpoint,
    scenario: &str,
) {
    let after = BranchSideEffectCheckpoint::capture(db, observer, &before.target_name);
    assert_eq!(
        after.observer_count, before.observer_count,
        "[{scenario}] BranchOpObserver fired on a lifecycle-refused mutation; expected no side effect (target={})",
        before.target_name
    );
    assert_eq!(
        after.lineage_edge_count, before.lineage_edge_count,
        "[{scenario}] Lineage edge appended on a lifecycle-refused mutation; expected no side effect (target={})",
        before.target_name
    );
}
