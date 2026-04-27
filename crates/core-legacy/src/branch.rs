//! Branch lineage and lifecycle types.
//!
//! This module defines branch lineage identity and lifecycle status, while the
//! foundational branch-id derivation surface lives in `strata-core`.
//!
//! ## Authority map
//!
//! - Branch control truth = engine-owned control records.
//! - `_branch_dag` = derived lineage projection, not a competing authority.
//! - Storage fork info (inherited layers, fork manifests, refcounts) =
//!   execution truth for `CoW` storage, not branch-control or lineage truth.
//!
//! ## What lives here
//!
//! - `BranchGeneration` — monotonic per-name lifecycle counter (type alias).
//! - `BranchRef` — `(id, generation)` identity carried by lineage-bearing
//!   surfaces (events, hooks, DAG, control records).
//! - `BranchLifecycleStatus` — three-state lifecycle
//!   (`Active`, `Archived`, `Deleted`).
//! - `ForkAnchor` / `BranchControlRecord` — the control-record shape used by
//!   the engine.

use crate::id::CommitVersion;
use crate::types::BranchId;
use serde::{Deserialize, Serialize};
pub use strata_core_foundation::branch::aliases_default_branch_sentinel;

// =============================================================================
// Generation + lineage identity
// =============================================================================

/// Monotonic per-name lifecycle generation counter.
///
/// `BranchId` is deterministic over the name, so two lifecycle instances of
/// the same branch name share the same id. `BranchGeneration` distinguishes
/// them: delete-and-recreate increments the generation so lineage surfaces
/// can tell the old instance from the new one.
///
pub type BranchGeneration = u64;

/// Generation-aware branch identifier.
///
/// `BranchId` alone is the storage/locking identity — same name =
/// same physical KV namespace, regardless of lifecycle instance. `BranchRef`
/// adds the per-name `generation` counter so ancestry, hooks, events, and
/// the branch control overlay can distinguish between lifecycle instances
/// of the same name.
///
/// Used by every surface that carries branch lineage: events,
/// hooks, merge-base queries, DAG keys, control records. `BranchId` alone
/// remains valid for storage isolation and for API inputs that address
/// "whatever generation is currently live for this name."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchRef {
    /// Canonical id, derived via [`BranchId::from_user_name`].
    pub id: BranchId,
    /// Monotonic per-name lifecycle counter.
    pub generation: BranchGeneration,
}

impl BranchRef {
    /// Construct from an id and generation.
    #[inline]
    pub const fn new(id: BranchId, generation: BranchGeneration) -> Self {
        Self { id, generation }
    }
}

// =============================================================================
// Lifecycle status
// =============================================================================

/// Canonical branch lifecycle status.
///
/// This describes branch writability and visibility at the lifecycle level.
///
/// The event-stream enum at `crates/core/src/branch_types.rs::BranchStatus`
/// is distinct; it models event-stream durability lifecycle rather than
/// branch lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BranchLifecycleStatus {
    /// Writable. Reads and writes both proceed.
    Active,
    /// Intended read-only state.
    Archived,
    /// Intended tombstoned state.
    Deleted,
}

impl BranchLifecycleStatus {
    /// `true` iff mutation is allowed on a branch with this status.
    #[inline]
    pub const fn allows_writes(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// `true` iff the branch is visible to list/read surfaces.
    /// `Deleted` is treated as branch-not-found.
    #[inline]
    pub const fn is_visible(&self) -> bool {
        matches!(self, Self::Active | Self::Archived)
    }
}

// =============================================================================
// Control record minimum shape
// =============================================================================

/// Fork anchor: the parent branch and commit version a child was forked from.
///
/// Part of the `BranchControlRecord` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForkAnchor {
    /// The parent branch this one was forked from.
    pub parent: BranchRef,
    /// The parent commit the fork was taken at.
    pub point: CommitVersion,
}

/// Minimum engine-owned branch control record shape.
///
/// This is the record `BranchControlStore` uses as per-branch control truth.
///
/// Fields:
/// - `branch`: generation-aware identity of this lifecycle instance.
/// - `name`: user-facing handle. Distinct from `branch.id`, which is the
///   deterministic UUID derivation of the name.
/// - `lifecycle`: lifecycle status used by write gating and visibility checks.
/// - `fork`: `None` for a root branch (e.g. `"default"`), `Some(_)` for a
///   child forked from a parent at a specific commit version.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchControlRecord {
    /// Generation-aware identity of this branch's lifecycle instance.
    pub branch: BranchRef,
    /// User-facing branch name.
    pub name: String,
    /// Canonical lifecycle status.
    pub lifecycle: BranchLifecycleStatus,
    /// Fork anchor, if this branch was forked from another.
    pub fork: Option<ForkAnchor>,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Hardcoded byte anchors for a subset of branch names. These must match
    /// the engine and executor anchor tables exactly.
    const HARDCODED_ANCHORS: &[(&str, [u8; 16])] = &[
        ("default", [0u8; 16]),
        (
            "main",
            [
                0x1f, 0x64, 0xc0, 0x67, 0xac, 0xf2, 0x50, 0x34, 0xa5, 0xd0, 0x92, 0xd5, 0x76, 0x41,
                0x38, 0xec,
            ],
        ),
        (
            "_system_",
            [
                0x0d, 0x39, 0x06, 0xa0, 0xd8, 0x09, 0x58, 0x17, 0xb9, 0x0b, 0x09, 0xb6, 0x0e, 0x63,
                0xc9, 0x7d,
            ],
        ),
        (
            "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            [
                0xf4, 0x7a, 0xc1, 0x0b, 0x58, 0xcc, 0x43, 0x72, 0xa5, 0x67, 0x0e, 0x02, 0xb2, 0xc3,
                0xd4, 0x79,
            ],
        ),
    ];

    #[test]
    fn from_user_name_default_is_nil_uuid() {
        assert_eq!(*BranchId::from_user_name("default").as_bytes(), [0u8; 16]);
    }

    #[test]
    fn from_user_name_default_sentinel_is_case_sensitive() {
        let nil = [0u8; 16];
        assert_eq!(*BranchId::from_user_name("default").as_bytes(), nil);
        assert_ne!(*BranchId::from_user_name("DEFAULT").as_bytes(), nil);
        assert_ne!(*BranchId::from_user_name("Default").as_bytes(), nil);
    }

    #[test]
    fn aliases_default_branch_sentinel_only_matches_non_literal_nil_aliases() {
        assert!(!aliases_default_branch_sentinel("default"));
        assert!(aliases_default_branch_sentinel(
            "00000000-0000-0000-0000-000000000000"
        ));
        assert!(aliases_default_branch_sentinel(
            "00000000000000000000000000000000"
        ));
        assert!(aliases_default_branch_sentinel(
            "00000000-0000-0000-0000-000000000000"
                .to_uppercase()
                .as_str()
        ));
        assert!(!aliases_default_branch_sentinel(
            "f47ac10b-58cc-4372-a567-0e02b2c3d479"
        ));
        assert!(!aliases_default_branch_sentinel("main"));
    }

    #[test]
    fn from_user_name_matches_hardcoded_anchors() {
        for &(name, expected) in HARDCODED_ANCHORS {
            let actual = *BranchId::from_user_name(name).as_bytes();
            assert_eq!(
                actual, expected,
                "BranchId::from_user_name({name:?}) drifted from its hardcoded \
                 anchor.\nexpected {expected:02x?}\nactual   {actual:02x?}"
            );
        }
    }

    #[test]
    fn from_user_name_uuid_string_passes_through() {
        let name = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        let expected = *Uuid::parse_str(name).unwrap().as_bytes();
        assert_eq!(*BranchId::from_user_name(name).as_bytes(), expected);
    }

    #[test]
    fn from_user_name_tolerates_uuid_case_and_hyphenation() {
        let canonical = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        let upper = "F47AC10B-58CC-4372-A567-0E02B2C3D479";
        let hyphenless = "f47ac10b58cc4372a5670e02b2c3d479";
        let a = *BranchId::from_user_name(canonical).as_bytes();
        let b = *BranchId::from_user_name(upper).as_bytes();
        let c = *BranchId::from_user_name(hyphenless).as_bytes();
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn from_user_name_is_deterministic() {
        let a = *BranchId::from_user_name("feature/stability").as_bytes();
        let b = *BranchId::from_user_name("feature/stability").as_bytes();
        assert_eq!(a, b);
    }

    #[test]
    fn from_user_name_distinct_inputs_produce_distinct_ids() {
        let a = *BranchId::from_user_name("main").as_bytes();
        let b = *BranchId::from_user_name("production").as_bytes();
        let c = *BranchId::from_user_name("feature/abc").as_bytes();
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn from_user_name_empty_string_resolves_via_v5() {
        let expected = [
            0x4e, 0xbd, 0x02, 0x08, 0x83, 0x28, 0x5d, 0x69, 0x8c, 0x44, 0xec, 0x50, 0x93, 0x9c,
            0x09, 0x67,
        ];
        assert_eq!(*BranchId::from_user_name("").as_bytes(), expected);
    }

    #[test]
    fn branch_ref_round_trips_through_serde() {
        let id = BranchId::from_user_name("feature/serde");
        let original = BranchRef::new(id, 7);
        let json = serde_json::to_string(&original).unwrap();
        let restored: BranchRef = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, original);
        assert_eq!(restored.generation, 7);
    }

    #[test]
    fn branch_lifecycle_status_allows_writes_only_when_active() {
        assert!(BranchLifecycleStatus::Active.allows_writes());
        assert!(!BranchLifecycleStatus::Archived.allows_writes());
        assert!(!BranchLifecycleStatus::Deleted.allows_writes());
    }

    #[test]
    fn branch_lifecycle_status_is_visible_hides_deleted() {
        assert!(BranchLifecycleStatus::Active.is_visible());
        assert!(BranchLifecycleStatus::Archived.is_visible());
        assert!(!BranchLifecycleStatus::Deleted.is_visible());
    }

    #[test]
    fn branch_lifecycle_status_round_trips_through_serde() {
        for s in [
            BranchLifecycleStatus::Active,
            BranchLifecycleStatus::Archived,
            BranchLifecycleStatus::Deleted,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let restored: BranchLifecycleStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(restored, s);
        }
    }

    #[test]
    fn fork_anchor_round_trips_through_serde() {
        let anchor = ForkAnchor {
            parent: BranchRef::new(BranchId::from_user_name("main"), 0),
            point: CommitVersion(42),
        };
        let json = serde_json::to_string(&anchor).unwrap();
        let restored: ForkAnchor = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, anchor);
    }

    #[test]
    fn branch_control_record_round_trips_through_serde() {
        let record = BranchControlRecord {
            branch: BranchRef::new(BranchId::from_user_name("feature/x"), 1),
            name: "feature/x".to_string(),
            lifecycle: BranchLifecycleStatus::Active,
            fork: Some(ForkAnchor {
                parent: BranchRef::new(BranchId::from_user_name("main"), 0),
                point: CommitVersion(100),
            }),
        };
        let json = serde_json::to_string(&record).unwrap();
        let restored: BranchControlRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, record);
    }

    #[test]
    fn branch_control_record_root_branch_has_no_fork() {
        let record = BranchControlRecord {
            branch: BranchRef::new(BranchId::from_user_name("default"), 0),
            name: "default".to_string(),
            lifecycle: BranchLifecycleStatus::Active,
            fork: None,
        };
        assert!(record.fork.is_none());
        let json = serde_json::to_string(&record).unwrap();
        let restored: BranchControlRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, record);
    }
}
