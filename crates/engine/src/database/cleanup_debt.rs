//! B6 — per-Database cleanup-debt registry.
//!
//! Per the branching adversarial program at
//! `docs/design/branching/branching-adversarial-verification-program.md`
//! §"Typed operator failures", delete paths that return
//! `Warning`/`PostCommitError` from `complete_delete_post_commit` leave
//! *cleanup debt*: the commit succeeded but some post-commit cleanup
//! step (storage purge, subsystem drop, control-marker write) failed
//! and needs operator attention.
//!
//! Before B6 this only surfaced as a `tracing::warn!` log line, which
//! is invisible to programmatic operators. The registry in this module
//! is the first-class pull surface:
//!
//! 1. `Database::retention_report()` exposes all unacknowledged entries
//!    via `RetentionReport::cleanup_debt`
//! 2. `Database::acknowledge_cleanup_debt(DebtId)` is the only pruning
//!    mechanism
//!
//! **Orphan-visible semantics.** Entries capture the original
//! `BranchRef` at record time and are **not** pruned when the branch is
//! later deleted or recreated. This diverges from the
//! `PrimitiveDegradationRegistry::clear_branch` pattern on purpose: per
//! the retention contract, debt attributed to a tombstoned lifecycle
//! must remain operator-visible after a same-name recreate, because the
//! debt is about work that did not complete against that specific
//! lifecycle instance.
//!
//! **Pull-only.** Per the convergence-and-observability doc §"Required
//! push events", cleanup debt *is* listed as a push-event candidate,
//! but §"Non-goal" admits that pull surfaces with stable branch
//! vocabulary satisfy the operator-routing contract. Push observers are
//! deferred to a follow-up PR with proper D4 amendment.
//!
//! **Delete-only for now.** Materialize-induced debt (recorded only as
//! a `tracing::warn!` at `crates/storage/src/segmented/mod.rs:2042`) is
//! deferred — plumbing it through `storage::MaterializeResult` requires
//! a breaking change to a public non-`#[non_exhaustive]` struct and was
//! ruled out of scope for tranche 4 closeout. See
//! `docs/design/branching/tranche-4-closeout.md` §"Deferred items".

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use strata_core::{BranchRef, StrataError};

/// Monotonic identifier for a cleanup-debt entry.
///
/// Unique per-`Database`. Acknowledgement is by `DebtId`.
pub type DebtId = u64;

/// Discriminates the source of a cleanup-debt entry.
///
/// Kept flat at the kind level: the detailed provenance (storage
/// cleanup vs subsystem drop vs control-marker write) lives in the
/// wrapped `StrataError` on the entry's `detail` field. Splitting this
/// enum into sub-sources would require extending
/// `DeleteBranchCompletion` (currently collapses to untyped
/// `StrataError`), which ripples into rollback paths; that work is
/// deferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CleanupDebtKind {
    /// `complete_delete_post_commit` returned `Warning(...)`: the
    /// delete succeeded but a non-critical cleanup step reported an
    /// advisory failure (e.g. a storage purge raced a concurrent
    /// scan).
    DeleteWarning,
    /// `complete_delete_post_commit` returned `PostCommitError(...)`:
    /// a post-commit cleanup step failed with a harder error (e.g.
    /// subsystem-drop failure, control-marker write failure). The
    /// branch is deleted from an observer point of view but a
    /// follow-up reclaim pass is needed.
    DeletePostCommitError,
}

impl CleanupDebtKind {
    /// Stable string form for logging and wire surfaces.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DeleteWarning => "DeleteWarning",
            Self::DeletePostCommitError => "DeletePostCommitError",
        }
    }
}

impl std::fmt::Display for CleanupDebtKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One unacknowledged cleanup-debt record.
///
/// `branch_ref` is the generation-aware identity *at record time* and
/// is preserved across same-name delete/recreate (orphan-visible).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CleanupDebtEntry {
    /// Monotonic id assigned at `record()` time. Acknowledgement is by
    /// this id.
    pub id: DebtId,
    /// Generation-aware branch identity the debt was recorded against.
    pub branch_ref: BranchRef,
    /// Source of the debt (advisory warning vs hard post-commit
    /// failure).
    pub kind: CleanupDebtKind,
    /// Operator-facing summary message (e.g. "storage cleanup reported
    /// retention debt").
    pub message: String,
    /// Wrapped error carrying typed provenance. Operators drill down
    /// on this when the flat `kind` is not specific enough.
    ///
    /// Wrapped in `Arc` because `StrataError` is not `Clone` (it holds
    /// boxed trait-object sources on some variants) and every
    /// `list()`/`lookup()` call clones the entry.
    pub detail: Arc<StrataError>,
    /// Wall-clock time the debt was first recorded.
    pub recorded_at: SystemTime,
}

/// Per-`Database` registry of unacknowledged cleanup-debt.
///
/// Stored as a `Database` extension via
/// `db.extension::<CleanupDebtRegistry>()`. `pub(crate)` because the
/// only sanctioned external surfaces are
/// `RetentionReport::cleanup_debt` (read) and
/// `Database::acknowledge_cleanup_debt` (write).
#[derive(Default)]
pub(crate) struct CleanupDebtRegistry {
    next_id: AtomicU64,
    entries: DashMap<DebtId, CleanupDebtEntry>,
}

impl CleanupDebtRegistry {
    /// Record a new cleanup-debt entry and return its id.
    ///
    /// Every call inserts a fresh entry with a monotonic id. There is
    /// no deduplication: the caller is expected to invoke this once per
    /// distinct post-commit failure, not on every read-path encounter.
    pub fn record(
        &self,
        branch_ref: BranchRef,
        kind: CleanupDebtKind,
        message: impl Into<String>,
        detail: StrataError,
    ) -> DebtId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let entry = CleanupDebtEntry {
            id,
            branch_ref,
            kind,
            message: message.into(),
            detail: Arc::new(detail),
            recorded_at: SystemTime::now(),
        };
        self.entries.insert(id, entry);
        id
    }

    /// Acknowledge (and remove) a debt entry.
    ///
    /// Returns `true` when the id existed, `false` otherwise. This is
    /// the only pruning mechanism — debt is never cleared by branch
    /// delete or recreate.
    pub fn acknowledge(&self, id: DebtId) -> bool {
        self.entries.remove(&id).is_some()
    }

    /// Snapshot every unacknowledged entry, sorted by id ascending.
    ///
    /// Stable ordering so `RetentionReport` output is deterministic.
    pub fn list(&self) -> Vec<CleanupDebtEntry> {
        let mut v: Vec<CleanupDebtEntry> = self.entries.iter().map(|e| e.value().clone()).collect();
        v.sort_by_key(|e| e.id);
        v
    }

    /// Look up a single entry by id.
    pub fn lookup(&self, id: DebtId) -> Option<CleanupDebtEntry> {
        self.entries.get(&id).map(|e| e.value().clone())
    }

    /// Current number of unacknowledged entries. Testing aid.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no debt is registered. Testing aid.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Record a cleanup-debt entry against the database's registry.
///
/// `pub(crate)` because the only sanctioned recorder is the committed
/// delete path at `branch_service.rs`. This mirrors B5.4's
/// `mark_primitive_degraded` free-function pattern: the recording
/// surface stays on the module, not on `Database`.
pub(crate) fn record_delete_debt(
    db: &crate::database::Database,
    branch_ref: BranchRef,
    kind: CleanupDebtKind,
    message: impl Into<String>,
    detail: StrataError,
) -> Option<DebtId> {
    let registry = db.extension::<CleanupDebtRegistry>().ok()?;
    Some(registry.record(branch_ref, kind, message, detail))
}

/// Acknowledge (and remove) a cleanup-debt entry by id.
///
/// Returns `true` when the id existed, `false` otherwise. This is the
/// only pruning mechanism — debt is never cleared by branch delete or
/// recreate (orphan-visible semantics).
///
/// Mirrors B5.4's `mark_primitive_degraded`/`primitive_degraded_error`
/// free-function pattern so the engine `Database` surface stays
/// narrow.
pub fn acknowledge(db: &crate::database::Database, id: DebtId) -> bool {
    let Ok(registry) = db.extension::<CleanupDebtRegistry>() else {
        return false;
    };
    registry.acknowledge(id)
}

/// Snapshot every unacknowledged entry from the database's registry.
///
/// `pub(crate)` because the sanctioned read surface is
/// `RetentionReport::cleanup_debt` — this helper exists only so the
/// retention-report populator does not need to open-code the
/// extension lookup.
pub(crate) fn snapshot(db: &crate::database::Database) -> Vec<CleanupDebtEntry> {
    db.extension::<CleanupDebtRegistry>()
        .ok()
        .map(|reg| reg.list())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::BranchId;

    fn br(id: u8, gen_: u64) -> BranchRef {
        let mut bytes = [0u8; 16];
        bytes[15] = id;
        BranchRef::new(BranchId::from_bytes(bytes), gen_)
    }

    fn dummy_err(msg: &str) -> StrataError {
        StrataError::internal(msg)
    }

    #[test]
    fn record_assigns_monotonic_ids() {
        let reg = CleanupDebtRegistry::default();
        let a = reg.record(
            br(1, 0),
            CleanupDebtKind::DeleteWarning,
            "a",
            dummy_err("x"),
        );
        let b = reg.record(
            br(1, 0),
            CleanupDebtKind::DeleteWarning,
            "b",
            dummy_err("y"),
        );
        let c = reg.record(
            br(1, 0),
            CleanupDebtKind::DeletePostCommitError,
            "c",
            dummy_err("z"),
        );
        assert_eq!(a, 0);
        assert_eq!(b, 1);
        assert_eq!(c, 2);
        assert_eq!(reg.len(), 3);
    }

    #[test]
    fn list_is_sorted_by_id() {
        let reg = CleanupDebtRegistry::default();
        for i in 0..5u8 {
            reg.record(
                br(i, 0),
                CleanupDebtKind::DeleteWarning,
                format!("entry {i}"),
                dummy_err("e"),
            );
        }
        let listed = reg.list();
        assert_eq!(listed.len(), 5);
        for w in listed.windows(2) {
            assert!(w[0].id < w[1].id, "list must be sorted by id ascending");
        }
    }

    #[test]
    fn acknowledge_removes_by_id() {
        let reg = CleanupDebtRegistry::default();
        let id = reg.record(
            br(1, 0),
            CleanupDebtKind::DeleteWarning,
            "x",
            dummy_err("e"),
        );
        assert!(reg.acknowledge(id));
        assert!(reg.lookup(id).is_none());
        assert_eq!(reg.len(), 0);
        // Second ack is a no-op.
        assert!(!reg.acknowledge(id));
    }

    #[test]
    fn entries_survive_same_name_recreate_semantically() {
        // The registry does not clear by branch name or BranchId.
        // Debt recorded against (name, gen=0) remains visible after a
        // subsequent delete + gen=1 recreate of the same name.
        let reg = CleanupDebtRegistry::default();
        let gen0 = br(1, 0);
        let gen1 = br(1, 1);

        let old_id = reg.record(
            gen0,
            CleanupDebtKind::DeletePostCommitError,
            "old-lifecycle debt",
            dummy_err("old"),
        );

        // Simulate later debt on the recreated lifecycle. Both must
        // remain visible; the registry does not prune by branch id.
        let new_id = reg.record(
            gen1,
            CleanupDebtKind::DeleteWarning,
            "new-lifecycle debt",
            dummy_err("new"),
        );

        let listed = reg.list();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, old_id);
        assert_eq!(listed[0].branch_ref.generation, 0);
        assert_eq!(listed[1].id, new_id);
        assert_eq!(listed[1].branch_ref.generation, 1);
    }

    #[test]
    fn kind_renders_stable_strings() {
        assert_eq!(CleanupDebtKind::DeleteWarning.as_str(), "DeleteWarning");
        assert_eq!(
            CleanupDebtKind::DeletePostCommitError.as_str(),
            "DeletePostCommitError"
        );
        assert_eq!(
            format!("{}", CleanupDebtKind::DeletePostCommitError),
            "DeletePostCommitError"
        );
    }
}
