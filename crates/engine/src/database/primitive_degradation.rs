//! Per-database primitive degradation registry.
//!
//! Named primitive failure paths (for example vector config mismatch or JSON
//! secondary-index load failure) must fail closed with typed errors rather
//! than silently serving stale or wrong branch-visible data. This module is
//! the engine-owned record of those fail-closed events.
//!
//! The registry is stored as a per-`Database` extension via
//! `db.extension::<PrimitiveDegradationRegistry>()`. It is the single
//! source of primitive-degraded state for three callers:
//!
//! 1. primitive read paths consult `lookup()` to fail closed on
//!    subsequent reads (vector `ensure_collection_loaded`, JSON
//!    `load_indexes` / lookup fns)
//! 2. `Database::retention_report()` joins `list()` with live
//!    `BranchControlRecord` lifecycle and surfaces the result as
//!    `RetentionReport::degraded_primitives`
//! 3. `complete_delete_post_commit` calls `clear_branch()` so a
//!    same-name recreate starts with a clean registry
//!    (contract §"Same-name recreate")
//!
//! Cross-branch isolation is structural: the key is
//! `(BranchId, PrimitiveType, String)`, so a degraded entry on one
//! branch cannot affect reads on a sibling branch.
//!
//! Per CLAUDE.md rule 3 ("no process-global semantic state"), the
//! registry lives on `Database`, not a static.

use std::time::SystemTime;

use crate::database::observers::{PrimitiveDegradedEvent, PrimitiveDegradedObserverRegistry};
use crate::{BranchRef, PrimitiveDegradedReason, StrataError};
use dashmap::DashMap;
use strata_core::contract::PrimitiveType;
use strata_core::BranchId;

/// One record of a fail-closed degraded primitive state.
///
/// Attributed to a specific generation-aware `BranchRef` so same-name
/// recreate does not cause old-lifecycle entries to confuse the new
/// lifecycle's reporting (contract §"Same-name recreate").
#[derive(Debug, Clone)]
pub struct PrimitiveDegradationEntry {
    /// Generation-aware branch identity at the time of degradation.
    pub branch_ref: BranchRef,
    /// Which primitive subsystem owns the degraded surface.
    pub primitive: PrimitiveType,
    /// Primitive-level name (collection, index space, etc.).
    pub name: String,
    /// Typed reason for the degradation.
    pub reason: PrimitiveDegradedReason,
    /// Operator-facing free-form detail (e.g. decode error message).
    /// Kept on the registry entry and the push event so operators can
    /// triage without enabling debug logging after the fact.
    pub detail: String,
    /// Wall-clock time the degradation was first marked.
    pub detected_at: SystemTime,
}

impl PrimitiveDegradationEntry {
    /// Build the matching typed `StrataError` for this degradation.
    pub fn to_error(&self) -> StrataError {
        StrataError::primitive_degraded(
            self.branch_ref.into(),
            self.primitive,
            &self.name,
            self.reason,
        )
    }
}

/// Per-`Database` registry of fail-closed primitive degradations.
///
/// Keyed by `(BranchId, PrimitiveType, name)`. The key uses the
/// storage-level `BranchId` (not `BranchRef`) because recovery and
/// lazy-load paths often do not yet have a live `BranchControlRecord`
/// to consult for the current generation — the full `BranchRef` is
/// preserved in the entry value and generation-equality is enforced
/// at `retention_report()` join time.
#[derive(Default)]
pub struct PrimitiveDegradationRegistry {
    entries: DashMap<(BranchId, PrimitiveType, String), PrimitiveDegradationEntry>,
}

impl PrimitiveDegradationRegistry {
    /// Mark a primitive as degraded.
    ///
    /// The first call for a given `(BranchId, primitive, name)` key
    /// "wins": it inserts the entry, stamps `detected_at`, and fires the
    /// observer event (when an observer registry is passed). Subsequent
    /// calls for the same key are no-ops — they do **not** re-fire
    /// observers, so `mark` is safe to call on every read-path encounter
    /// of corrupt data without generating push spam.
    ///
    /// See convergence doc §"Required push events" for the exactly-once
    /// push contract this implements.
    pub fn mark(
        &self,
        branch_ref: BranchRef,
        primitive: PrimitiveType,
        name: impl Into<String>,
        reason: PrimitiveDegradedReason,
        detail: impl Into<String>,
        observers: Option<&PrimitiveDegradedObserverRegistry>,
    ) -> PrimitiveDegradationEntry {
        use dashmap::mapref::entry::Entry;
        let name_str = name.into();
        let detail_str = detail.into();
        let key = (branch_ref.id, primitive, name_str.clone());
        let (entry, newly_inserted) = match self.entries.entry(key) {
            Entry::Occupied(e) => (e.get().clone(), false),
            Entry::Vacant(v) => {
                let fresh = PrimitiveDegradationEntry {
                    branch_ref,
                    primitive,
                    name: name_str,
                    reason,
                    detail: detail_str,
                    detected_at: SystemTime::now(),
                };
                let cloned = fresh.clone();
                v.insert(fresh);
                (cloned, true)
            }
        };

        if newly_inserted {
            if let Some(observers) = observers {
                let event = PrimitiveDegradedEvent {
                    branch_ref: entry.branch_ref,
                    primitive: entry.primitive,
                    name: entry.name.clone(),
                    reason: entry.reason,
                    detail: entry.detail.clone(),
                    detected_at: entry.detected_at,
                };
                observers.notify(&event);
            }
        }

        entry
    }

    /// Look up a degradation record by `(BranchId, primitive, name)`.
    ///
    /// Returns `None` when the primitive is healthy.
    pub fn lookup(
        &self,
        branch_id: BranchId,
        primitive: PrimitiveType,
        name: &str,
    ) -> Option<PrimitiveDegradationEntry> {
        self.entries
            .get(&(branch_id, primitive, name.to_string()))
            .map(|e| e.value().clone())
    }

    /// Remove every entry keyed by `branch_id`.
    ///
    /// Called from `complete_delete_post_commit` so same-name recreate
    /// does not inherit old-lifecycle degradation state (contract
    /// §"Same-name recreate").
    pub fn clear_branch(&self, branch_id: BranchId) {
        self.entries.retain(|(id, _, _), _| *id != branch_id);
    }

    /// Snapshot every current entry.
    ///
    /// Used by `Database::retention_report()` to join primitive
    /// degradation state with live `BranchControlRecord` lifecycle.
    pub fn list(&self) -> Vec<PrimitiveDegradationEntry> {
        self.entries.iter().map(|e| e.value().clone()).collect()
    }

    /// Total number of registered degradations. Testing aid.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no degradations are registered. Testing aid.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Convenience wrapper used by primitive callers that have a
/// `&Database` (or `&Arc<Database>`) and need to mark a primitive
/// degraded without threading the observer registry and registry
/// lookup at every call site.
///
/// Resolves the active [`BranchRef`] from storage via
/// [`Database::active_branch_ref`]; falls back to
/// `BranchRef::new(branch_id, 0)` when the control-store active
/// pointer is absent (legacy / pre-migration state). The gen-0
/// fallback is defensive — per the retention contract, attribution
/// must still be generation-aware at the report join, which enforces
/// `entry.branch_ref.generation == live.branch.generation`.
pub fn mark_primitive_degraded(
    db: &crate::database::Database,
    branch_id: BranchId,
    primitive: PrimitiveType,
    name: impl Into<String>,
    reason: PrimitiveDegradedReason,
    detail: impl Into<String>,
) -> Option<PrimitiveDegradationEntry> {
    let registry = db.extension::<PrimitiveDegradationRegistry>().ok()?;
    let branch_ref = db
        .active_branch_ref(branch_id)
        .unwrap_or_else(|| BranchRef::new(branch_id, 0));
    let observers = db.primitive_degraded_observers();
    Some(registry.mark(branch_ref, primitive, name, reason, detail, Some(observers)))
}

/// Read-path helper that looks up an existing degradation entry and
/// builds the typed [`StrataError::PrimitiveDegraded`] to return to
/// the caller. Used before attempting to load or scan primitive state.
pub fn primitive_degraded_error(
    db: &crate::database::Database,
    branch_id: BranchId,
    primitive: PrimitiveType,
    name: &str,
) -> Option<StrataError> {
    let registry = db.extension::<PrimitiveDegradationRegistry>().ok()?;
    registry
        .lookup(branch_id, primitive, name)
        .map(|e| e.to_error())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn br(id: u8, gen_: u64) -> BranchRef {
        let mut bytes = [0u8; 16];
        bytes[15] = id;
        BranchRef::new(BranchId::from_bytes(bytes), gen_)
    }

    #[test]
    fn mark_and_lookup() {
        let reg = PrimitiveDegradationRegistry::default();
        let b = br(1, 0);
        reg.mark(
            b,
            PrimitiveType::Vector,
            "v1",
            PrimitiveDegradedReason::ConfigDecodeFailure,
            "oops",
            None,
        );
        let got = reg.lookup(b.id, PrimitiveType::Vector, "v1").unwrap();
        assert_eq!(got.name, "v1");
        assert_eq!(got.reason, PrimitiveDegradedReason::ConfigDecodeFailure);
        assert_eq!(got.detail, "oops");
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn mark_is_idempotent_on_detected_at() {
        let reg = PrimitiveDegradationRegistry::default();
        let b = br(1, 0);
        let first = reg.mark(
            b,
            PrimitiveType::Json,
            "users",
            PrimitiveDegradedReason::IndexMetadataCorrupt,
            "bad json",
            None,
        );
        let again = reg.mark(
            b,
            PrimitiveType::Json,
            "users",
            PrimitiveDegradedReason::IndexMetadataCorrupt,
            "different detail ignored",
            None,
        );
        assert_eq!(first.detected_at, again.detected_at);
        assert_eq!(again.detail, "bad json"); // original detail wins
    }

    #[test]
    fn observer_fires_exactly_once_per_degradation_key() {
        use crate::database::observers::{
            ObserverError, PrimitiveDegradedEvent, PrimitiveDegradedObserver,
            PrimitiveDegradedObserverRegistry,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct Counting {
            count: AtomicUsize,
        }
        impl PrimitiveDegradedObserver for Counting {
            fn name(&self) -> &'static str {
                "counting"
            }
            fn on_primitive_degraded(
                &self,
                _event: &PrimitiveDegradedEvent,
            ) -> Result<(), ObserverError> {
                self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let obs_reg = PrimitiveDegradedObserverRegistry::new();
        let counter = Arc::new(Counting {
            count: AtomicUsize::new(0),
        });
        obs_reg.register(counter.clone() as Arc<dyn PrimitiveDegradedObserver>);

        let reg = PrimitiveDegradationRegistry::default();
        let b = br(1, 0);

        // First mark — observer fires once.
        reg.mark(
            b,
            PrimitiveType::Vector,
            "v1",
            PrimitiveDegradedReason::ConfigDecodeFailure,
            "first",
            Some(&obs_reg),
        );
        assert_eq!(counter.count.load(Ordering::SeqCst), 1);

        // Second mark on SAME key — observer must NOT fire again.
        reg.mark(
            b,
            PrimitiveType::Vector,
            "v1",
            PrimitiveDegradedReason::ConfigDecodeFailure,
            "second",
            Some(&obs_reg),
        );
        assert_eq!(
            counter.count.load(Ordering::SeqCst),
            1,
            "observer must not re-fire on repeat mark of same key"
        );

        // Different key fires again.
        reg.mark(
            b,
            PrimitiveType::Vector,
            "v2",
            PrimitiveDegradedReason::ConfigDecodeFailure,
            "third",
            Some(&obs_reg),
        );
        assert_eq!(counter.count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn clear_branch_drops_matching_entries_only() {
        let reg = PrimitiveDegradationRegistry::default();
        let a = br(1, 0);
        let b = br(2, 0);
        reg.mark(
            a,
            PrimitiveType::Vector,
            "v1",
            PrimitiveDegradedReason::ConfigMismatch,
            "",
            None,
        );
        reg.mark(
            b,
            PrimitiveType::Vector,
            "v1",
            PrimitiveDegradedReason::ConfigMismatch,
            "",
            None,
        );
        assert_eq!(reg.len(), 2);
        reg.clear_branch(a.id);
        assert_eq!(reg.len(), 1);
        assert!(reg.lookup(a.id, PrimitiveType::Vector, "v1").is_none());
        assert!(reg.lookup(b.id, PrimitiveType::Vector, "v1").is_some());
    }
}
