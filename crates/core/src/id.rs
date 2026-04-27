//! Foundational identifiers shared across Strata.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Deterministic UUID-v5 namespace used by [`BranchId::from_user_name`].
///
/// The byte value is load-bearing: changing it renames every deterministically
/// derived branch identifier.
const BRANCH_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Unique identifier for a branch namespace.
///
/// `BranchId` is an opaque UUID wrapper used for branch-scoped identity across
/// the stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct BranchId(Uuid);

/// Unique identifier for a transaction.
///
/// Assigned by `TransactionManager` at transaction start. Monotonically
/// increasing within a database instance. Used in WAL records, segment
/// metadata, watermark tracking, and GC coordination.
///
/// Wire format: bare `u64` (via `#[repr(transparent)]` + `#[serde(transparent)]`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct TxnId(pub u64);

/// Monotonic commit version (MVCC sequence number).
///
/// Assigned at commit time. Used for snapshot isolation (`start_version`),
/// version-bounded reads (`max_version`), fork points (`fork_version`),
/// and garbage collection (`min_version`).
///
/// Wire format: bare `u64` (via `#[repr(transparent)]` + `#[serde(transparent)]`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct CommitVersion(pub u64);

impl BranchId {
    /// Create a new random branch identifier using UUID v4.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a branch identifier from raw bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Parse a branch identifier from a UUID string.
    pub fn from_string(s: &str) -> Option<Self> {
        Uuid::parse_str(s).ok().map(Self)
    }

    /// Canonical derivation from a user-facing branch name.
    ///
    /// - `"default"` resolves to the nil UUID sentinel.
    /// - UUID-shaped input passes through unchanged.
    /// - All other input is hashed via UUID-v5 over the locked branch
    ///   namespace.
    pub fn from_user_name(name: &str) -> Self {
        if name == "default" {
            Self::from_bytes([0u8; 16])
        } else if let Ok(uuid) = Uuid::parse_str(name) {
            Self::from_bytes(*uuid.as_bytes())
        } else {
            let uuid = Uuid::new_v5(&BRANCH_NAMESPACE, name.as_bytes());
            Self::from_bytes(*uuid.as_bytes())
        }
    }

    /// Get the raw bytes of this branch identifier.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl TxnId {
    /// The zero transaction ID (used as "no transaction" sentinel).
    pub const ZERO: Self = Self(0);

    /// Create the next sequential transaction ID.
    #[inline]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Return the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl CommitVersion {
    /// The zero version (used as "no version" or "before any commit" sentinel).
    pub const ZERO: Self = Self(0);

    /// The maximum version (used for "latest" reads: `get_versioned(key, CommitVersion::MAX)`).
    pub const MAX: Self = Self(u64::MAX);

    /// Create the next sequential version.
    #[inline]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Return the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnId({})", self.0)
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn:{}", self.0)
    }
}

impl fmt::Debug for CommitVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommitVersion({})", self.0)
    }
}

impl fmt::Display for CommitVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<u64> for TxnId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<TxnId> for u64 {
    #[inline]
    fn from(id: TxnId) -> Self {
        id.0
    }
}

impl From<u64> for CommitVersion {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<CommitVersion> for u64 {
    #[inline]
    fn from(v: CommitVersion) -> Self {
        v.0
    }
}

impl Default for BranchId {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TxnId {
    fn default() -> Self {
        Self::ZERO
    }
}

impl Default for CommitVersion {
    fn default() -> Self {
        Self::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCKED_NAMESPACE_BYTES: [u8; 16] = [
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ];

    const HARDCODED_ANCHORS: &[(&str, [u8; 16])] = &[
        ("default", [0u8; 16]),
        (
            "",
            [
                0x4e, 0xbd, 0x02, 0x08, 0x83, 0x28, 0x5d, 0x69, 0x8c, 0x44, 0xec, 0x50, 0x93, 0x9c,
                0x09, 0x67,
            ],
        ),
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
    fn test_branch_namespace_bytes_are_locked() {
        assert_eq!(BRANCH_NAMESPACE.as_bytes(), &LOCKED_NAMESPACE_BYTES);
    }

    #[test]
    fn test_branch_id_creation_uniqueness() {
        assert_ne!(BranchId::new(), BranchId::new());
    }

    #[test]
    fn test_branch_id_default_is_unique() {
        assert_ne!(BranchId::default(), BranchId::default());
    }

    #[test]
    fn test_branch_id_serialization_roundtrip() {
        let id = BranchId::new();
        let bytes = id.as_bytes();
        let restored = BranchId::from_bytes(*bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn test_branch_id_from_string_roundtrip() {
        let original = BranchId::new();
        let parsed = BranchId::from_string(&format!("{original}")).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn test_branch_id_from_string_invalid_returns_none() {
        assert!(BranchId::from_string("not-a-uuid").is_none());
        assert!(BranchId::from_string("").is_none());
    }

    #[test]
    fn test_branch_id_display_nil_uuid() {
        let nil = BranchId::from_bytes([0u8; 16]);
        assert_eq!(format!("{nil}"), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_from_user_name_default_is_nil_uuid() {
        assert_eq!(*BranchId::from_user_name("default").as_bytes(), [0u8; 16]);
    }

    #[test]
    fn test_from_user_name_default_sentinel_is_case_sensitive() {
        let nil = [0u8; 16];
        assert_eq!(*BranchId::from_user_name("default").as_bytes(), nil);
        assert_ne!(*BranchId::from_user_name("DEFAULT").as_bytes(), nil);
        assert_ne!(*BranchId::from_user_name("Default").as_bytes(), nil);
    }

    #[test]
    fn test_from_user_name_matches_hardcoded_anchors() {
        for (name, expected) in HARDCODED_ANCHORS {
            let actual = *BranchId::from_user_name(name).as_bytes();
            assert_eq!(actual, *expected, "anchor drift for {name:?}");
        }
    }

    #[test]
    fn test_from_user_name_uuid_string_passes_through() {
        let name = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        let expected = *Uuid::parse_str(name).unwrap().as_bytes();
        assert_eq!(*BranchId::from_user_name(name).as_bytes(), expected);
    }

    #[test]
    fn test_from_user_name_tolerates_uuid_case_and_hyphenation() {
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
    fn test_from_user_name_is_deterministic() {
        let a = *BranchId::from_user_name("feature/stability").as_bytes();
        let b = *BranchId::from_user_name("feature/stability").as_bytes();
        assert_eq!(a, b);
    }

    #[test]
    fn test_txn_id_serde_transparent() {
        let id = TxnId(42);
        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, "42");

        let deserialized: TxnId = serde_json::from_str("42").unwrap();
        assert_eq!(deserialized, TxnId(42));
    }

    #[test]
    fn test_commit_version_serde_transparent() {
        let version = CommitVersion(100);
        let serialized = serde_json::to_string(&version).unwrap();
        assert_eq!(serialized, "100");

        let deserialized: CommitVersion = serde_json::from_str("100").unwrap();
        assert_eq!(deserialized, CommitVersion(100));
    }

    #[test]
    fn test_txn_id_constants_and_methods() {
        assert_eq!(TxnId::ZERO.as_u64(), 0);
        assert_eq!(TxnId(5).next(), TxnId(6));
    }

    #[test]
    fn test_commit_version_constants_and_methods() {
        assert_eq!(CommitVersion::ZERO.as_u64(), 0);
        assert_eq!(CommitVersion::MAX.as_u64(), u64::MAX);
        assert_eq!(CommitVersion(10).next(), CommitVersion(11));
    }

    #[test]
    fn test_from_conversions() {
        let txn: TxnId = 42u64.into();
        assert_eq!(txn, TxnId(42));

        let raw: u64 = TxnId(42).into();
        assert_eq!(raw, 42);

        let ver: CommitVersion = 100u64.into();
        assert_eq!(ver, CommitVersion(100));

        let raw: u64 = CommitVersion(100).into();
        assert_eq!(raw, 100);
    }

    #[test]
    fn test_display_and_debug() {
        assert_eq!(format!("{}", TxnId(5)), "txn:5");
        assert_eq!(format!("{:?}", TxnId(5)), "TxnId(5)");
        assert_eq!(format!("{}", CommitVersion(10)), "v10");
        assert_eq!(format!("{:?}", CommitVersion(10)), "CommitVersion(10)");
    }

    #[test]
    fn test_ordering() {
        assert!(TxnId(1) < TxnId(2));
        assert!(CommitVersion(10) > CommitVersion(5));
    }

    #[test]
    fn test_repr_transparent_size() {
        assert_eq!(std::mem::size_of::<BranchId>(), std::mem::size_of::<Uuid>());
        assert_eq!(std::mem::size_of::<TxnId>(), std::mem::size_of::<u64>());
        assert_eq!(
            std::mem::size_of::<CommitVersion>(),
            std::mem::size_of::<u64>()
        );
        assert_eq!(
            std::mem::align_of::<BranchId>(),
            std::mem::align_of::<Uuid>()
        );
        assert_eq!(std::mem::align_of::<TxnId>(), std::mem::align_of::<u64>());
        assert_eq!(
            std::mem::align_of::<CommitVersion>(),
            std::mem::align_of::<u64>()
        );
    }
}
