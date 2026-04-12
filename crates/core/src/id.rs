//! Strongly-typed identifiers for transaction and version tracking.
//!
//! These newtypes prevent accidental parameter swaps between
//! transaction IDs, commit versions, and timestamps — all of which
//! are `u64` at the wire level.

use serde::{Deserialize, Serialize};
use std::fmt;

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

    #[test]
    fn test_txn_id_serde_transparent() {
        // Verify that TxnId serializes as bare u64, not {"TxnId": value}
        let id = TxnId(42);
        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, "42");

        let deserialized: TxnId = serde_json::from_str("42").unwrap();
        assert_eq!(deserialized, TxnId(42));
    }

    #[test]
    fn test_commit_version_serde_transparent() {
        // Verify that CommitVersion serializes as bare u64
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
        // Verify #[repr(transparent)] guarantees same size as u64
        assert_eq!(std::mem::size_of::<TxnId>(), std::mem::size_of::<u64>());
        assert_eq!(
            std::mem::size_of::<CommitVersion>(),
            std::mem::size_of::<u64>()
        );
        assert_eq!(std::mem::align_of::<TxnId>(), std::mem::align_of::<u64>());
        assert_eq!(
            std::mem::align_of::<CommitVersion>(),
            std::mem::align_of::<u64>()
        );
    }
}
