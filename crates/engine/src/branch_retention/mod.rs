//! Vocabulary for branch-retention reporting and reasoning.
//!
//! These types make retention code paths explicit and readable without
//! hard-coding prose into every call site. The types are `pub(crate)` because
//! they support engine implementation detail rather than public API.

#![allow(dead_code)]

/// What a retention state element *means* for branch-visible reads
/// and physical reclamation.
///
/// Maps onto the retention contract's §"Barrier model". Doc comments
/// on retention code paths cite this enum to make their barrier role
/// legible without naming a paragraph number.
///
/// See `docs/design/branching/branching-gc/branching-retention-contract.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BarrierKind {
    /// Logical visibility barrier — a tombstone or a TTL-expired head
    /// version. Affects what reads see; does not by itself prove file
    /// reclaimability.
    LogicalVisibility,

    /// Persisted fork-frontier barrier — `fork_version` plus the
    /// inherited-layer manifest entries that bound descendant
    /// visibility. Both shapes branch-visible reads *and* keeps
    /// shared bytes reachable.
    ForkFrontier,

    /// Physical retention barrier — own-segment manifest entries and
    /// inherited-layer manifest segment lists. The direct file-level
    /// reasons a segment remains live.
    PhysicalRetention,

    /// Runtime acceleration barrier — `SegmentRefRegistry` and any
    /// future in-memory per-segment refcount index. Accelerates
    /// candidate identification; never the durable ledger.
    RuntimeAccelerator,

    /// Recovery health gate — blocks reclaim under degraded recovery.
    /// Does not change branch-visible read meaning.
    RecoveryHealthGate,
}

/// What role a `BranchRef` (or, in storage-local code, a branch
/// identity by ID) plays in a reclaim reachability walk.
///
/// Maps onto the retention contract's reclaimability rule and the
/// reclaim protocol's candidate-vs-proven-orphan distinction.
///
/// Storage-side reachability walks use this role vocabulary in documentation
/// and internal reasoning. Production code that
/// matches on these variants lands in B5.2 with the
/// `retention_report()` and reclaim implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReachabilityRole {
    /// Live lifecycle instance — contributes own + inherited
    /// manifest references to the reachability set.
    Live,

    /// Tombstoned, but at least one descendant's inherited-layer
    /// manifest still references segments this branch originally
    /// produced. The lifecycle record is gone; the segments survive
    /// until every descendant releases them.
    DescendantPinned,

    /// Tombstoned and unreferenced by any descendant inherited
    /// layer. Segments may become reclaim candidates pending the
    /// reclaim protocol's manifest proof and quarantine steps.
    Reclaimable,
}

/// Convergence class for a branch-visible derived surface.
///
/// Maps onto §"Convergence classes" in
/// `docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`.
/// Every B5-relevant derived surface is labeled with exactly one
/// class. B5.1 ships this enum so the convergence audit's surface
/// matrix and the staged-publish refresh paths can cite their
/// convergence contract by symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConvergenceClass {
    /// Surface converges as part of primary storage truth itself
    /// (e.g. JSON `_idx/...` rows committed as ordinary KV writes).
    /// No separate refresh contract; ordinary storage commit/replay
    /// semantics carry the convergence.
    StorageCoupled,

    /// Derived surface whose follower visibility is staged behind
    /// the same publish barrier as the underlying storage version
    /// (e.g. search refresh, vector refresh, graph-search refresh).
    /// Hook failure blocks or clamps publication rather than leaking
    /// partial derived state.
    StagedPublish,

    /// Surface may lag in-session on documented paths; reopen
    /// rebuilds or reconciles it to branch-visible truth (e.g.
    /// search and vector on-disk caches). Allowed only when the lag
    /// is documented, the stale result is not silently stronger than
    /// current truth, and reopen healing is real and tested.
    ReopenHealed,

    /// Surface does not participate in correctness decisions
    /// (e.g. `BranchStatusCache` post-B5.3). May lag or be missing
    /// without affecting branch-visible reads or write safety.
    AdvisoryOnly,

    /// Surface must return a typed error rather than serve stale or
    /// invalid data when it cannot be trusted (e.g. vector
    /// config-mismatch path, JSON `_idx` load failure,
    /// `retention_report()` under degraded manifest truth).
    HardFailDegraded,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time tripwire: every variant of every B5.1 vocabulary
    /// enum must be matched here. If a future PR adds a variant
    /// without updating this match, the build fails with a
    /// non-exhaustive-pattern error — forcing the contract reviewer
    /// to confirm the new variant lands in the contract document
    /// and that downstream annotations are aware of it.
    ///
    /// This is the entire test surface for an annotation-only module.
    /// Behavior tests for variant *consumption* (e.g. matching on
    /// `RetentionBlocker` in `retention_report()`) land with B5.2.
    #[test]
    fn vocabulary_variants_are_pinned_to_contract() {
        fn check_barrier(b: BarrierKind) {
            match b {
                BarrierKind::LogicalVisibility
                | BarrierKind::ForkFrontier
                | BarrierKind::PhysicalRetention
                | BarrierKind::RuntimeAccelerator
                | BarrierKind::RecoveryHealthGate => {}
            }
        }
        fn check_role(r: ReachabilityRole) {
            match r {
                ReachabilityRole::Live
                | ReachabilityRole::DescendantPinned
                | ReachabilityRole::Reclaimable => {}
            }
        }
        fn check_class(c: ConvergenceClass) {
            match c {
                ConvergenceClass::StorageCoupled
                | ConvergenceClass::StagedPublish
                | ConvergenceClass::ReopenHealed
                | ConvergenceClass::AdvisoryOnly
                | ConvergenceClass::HardFailDegraded => {}
            }
        }
        // Touch each helper so dead-code lint doesn't elide it.
        check_barrier(BarrierKind::LogicalVisibility);
        check_role(ReachabilityRole::Live);
        check_class(ConvergenceClass::StorageCoupled);
    }
}
