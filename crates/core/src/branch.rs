//! Canonical branch-truth types.
//!
//! This module is the single source of truth for branch identity, lineage
//! identity, and lifecycle status. It implements epic B2 of
//! `docs/design/branching/branching-execution-plan.md`, which collapses the
//! previously-duplicated `BRANCH_NAMESPACE` derivations (engine + executor)
//! into one canonical `BranchId::from_user_name` and introduces the
//! generation-aware identity and lifecycle types later epics consume.
//!
//! ## Authority map
//!
//! - Branch control truth = engine-owned control records (`BranchControlRecord`
//!   minimum shape is defined here; the store itself lands in B3/B5).
//! - `_branch_dag` = derived lineage projection, not a competing authority.
//! - Storage fork info (inherited layers, fork manifests, refcounts) =
//!   execution truth for `CoW` storage, not branch-control or lineage truth.
//!
//! ## What lives here
//!
//! - `BRANCH_NAMESPACE` — the deterministic UUID-v5 namespace for
//!   name-to-id derivation. `pub(crate)`; consumers go through
//!   `BranchId::from_user_name`.
//! - `BranchId::from_user_name` — the only code path in the workspace that
//!   performs branch name → UUID derivation.
//! - `BranchGeneration` — monotonic per-name lifecycle counter (type alias).
//! - `BranchRef` — `(id, generation)` identity carried by lineage-bearing
//!   surfaces (events, hooks, DAG, control records). Wiring into those
//!   surfaces is B3 work; B2 only lands the type.
//! - `BranchLifecycleStatus` — canonical three-state lifecycle
//!   (`Active`, `Archived`, `Deleted`). Write-gate enforcement is B4.
//! - `ForkAnchor` / `BranchControlRecord` — the minimum control-record
//!   shape the later engine-owned `BranchControlStore` will use.

use crate::id::CommitVersion;
use crate::types::BranchId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// =============================================================================
// Namespace + canonical name derivation
// =============================================================================

/// Deterministic UUID-v5 namespace used by `BranchId::from_user_name`.
///
/// The byte value is load-bearing: it appears in every existing disk-backed
/// database's branch ids. Changing it renames every branch in every database.
/// Locked by `crates/engine/tests/branch_id_characterization.rs` and the
/// executor-side parity test in `crates/executor/src/bridge.rs`.
pub(crate) const BRANCH_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// `true` if `name` resolves to the reserved nil-UUID branch sentinel without
/// being the literal `"default"` branch name.
///
/// Public, user-facing engine/executor validation rejects these aliases so
/// callers cannot address the nil-UUID sentinel branch by alternate spellings
/// such as the canonical all-zero UUID string.
pub fn aliases_default_branch_sentinel(name: &str) -> bool {
    name != "default" && BranchId::from_user_name(name) == BranchId::from_bytes([0u8; 16])
}

impl BranchId {
    /// Canonical derivation from a user-facing branch name.
    ///
    /// The algorithm:
    /// 1. `"default"` → nil UUID (all-zero bytes). Load-bearing sentinel used
    ///    by storage isolation and global-index keys. Case-sensitive: any
    ///    other casing hits the v5 path.
    /// 2. Input that parses as a UUID → passed through verbatim. This is how
    ///    synthetic/generated branch ids round-trip without being re-hashed.
    ///    Public engine/executor validation rejects non-literal aliases of the
    ///    nil-UUID default-branch sentinel.
    /// 3. Otherwise → deterministic UUID-v5 over (`BRANCH_NAMESPACE`, name).
    ///
    /// This is the only code path in the workspace that performs branch
    /// name-to-UUID derivation. Both engine (`resolve_branch_name`) and
    /// executor (`to_core_branch_id`) delegate here.
    pub fn from_user_name(name: &str) -> Self {
        if name == "default" {
            Self::from_bytes([0u8; 16])
        } else if let Ok(u) = Uuid::parse_str(name) {
            Self::from_bytes(*u.as_bytes())
        } else {
            let uuid = Uuid::new_v5(&BRANCH_NAMESPACE, name.as_bytes());
            Self::from_bytes(*uuid.as_bytes())
        }
    }
}

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
/// B3 wires this into `BranchOpEvent`, `BranchDagHook`, and the branch DAG
/// keys; B2 only lands the type.
pub type BranchGeneration = u64;

/// Generation-aware branch identifier.
///
/// `BranchId` alone is the canonical storage/locking identity — same name =
/// same physical KV namespace, regardless of lifecycle instance. `BranchRef`
/// adds the per-name `generation` counter so ancestry, hooks, events, and
/// the branch control overlay can distinguish between lifecycle instances
/// of the same name.
///
/// Used by every surface that carries branch lineage post-B3: events,
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
/// This becomes the canonical replacement for the parallel `BranchStatus`
/// enums that exist today in engine, executor, and core-branch-types. B2
/// lands the shared type; later epics migrate runtime users and enforce the
/// write gate.
///
/// The four-variant event-stream enum at
/// `crates/core/src/branch_types.rs::BranchStatus` is distinct — it describes
/// event-stream durability lifecycles and is renamed to
/// `EventStreamLifecycleStatus` in a later epic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BranchLifecycleStatus {
    /// Writable. Reads and writes both proceed.
    Active,
    /// Intended read-only state. B4 wires this into the branch write gate.
    Archived,
    /// Intended tombstoned state. B4 hides it from live branch surfaces.
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
/// Part of the `BranchControlRecord` minimum shape. The actual engine-owned
/// control store lands in B3/B5; B2 freezes the record shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForkAnchor {
    /// The parent branch this one was forked from.
    pub parent: BranchRef,
    /// The parent commit the fork was taken at.
    pub point: CommitVersion,
}

/// Minimum engine-owned branch control record shape.
///
/// This is the record the future `BranchControlStore` (B3/B5) holds as the
/// authoritative per-branch control truth. B2 freezes the shape so later
/// epics are execution work, not fresh schema design.
///
/// Fields:
/// - `branch`: generation-aware identity of this lifecycle instance.
/// - `name`: user-facing handle. Distinct from `branch.id`, which is the
///   deterministic UUID derivation of the name.
/// - `lifecycle`: canonical lifecycle status (B4 enforces it at the write gate).
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

    /// Locked baseline for the `BRANCH_NAMESPACE` bytes. Mirrors the B1
    /// characterization test baseline in
    /// `crates/engine/tests/branch_id_characterization.rs` and the
    /// executor-side `B1_BRANCH_NAMESPACE_BYTES` in
    /// `crates/executor/src/bridge.rs`. Any drift here is a BREAKING
    /// compatibility change.
    const LOCKED_NAMESPACE_BYTES: [u8; 16] = [
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ];

    /// Hardcoded byte anchors for a subset of branch names. These MUST match
    /// the engine and executor B1 anchor tables exactly — drift between any
    /// of the three tables is a parity break.
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
    fn branch_namespace_bytes_are_locked() {
        assert_eq!(
            BRANCH_NAMESPACE.as_bytes(),
            &LOCKED_NAMESPACE_BYTES,
            "BRANCH_NAMESPACE drifted from the B1 locked baseline. This is a \
             BREAKING compatibility change — it renames every branch in every \
             existing database."
        );
    }

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
                "BranchId::from_user_name({name:?}) drifted from its B1 hardcoded \
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
        let expected = *Uuid::new_v5(&BRANCH_NAMESPACE, b"").as_bytes();
        let actual = *BranchId::from_user_name("").as_bytes();
        assert_eq!(actual, expected);
        assert_ne!(actual, [0u8; 16]);
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
