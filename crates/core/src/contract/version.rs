//! Version identifier types
//!
//! This type expresses Invariant 2: Everything is Versioned.
//! Every mutation produces a version. Every read returns version information.
//!
//! ## Version Variants
//!
//! Different primitives use different versioning schemes:
//!
//! - **TxnId**: Transaction-based versioning (KV, Json, Vector, Branch)
//!   Multiple entities modified in the same transaction share this version.
//!
//! - **Sequence**: Position-based versioning (EventLog)
//!   Represents position in an append-only log. Unique within a branch's event log.
//!
//! - **Counter**: Per-entity counter
//!   Increments on each modification. Used for CAS operations.
//!
//! ## Comparison
//!
//! Versions are comparable **within the same variant type**.
//! Cross-variant comparison is undefined (returns `None` for `partial_cmp`).

use serde::{Deserialize, Serialize};

/// Version identifier for an entity
///
/// Versions track mutations and enable optimistic concurrency control.
/// Every write operation returns a Version indicating what was created.
///
/// ## Invariants
///
/// - Versions are monotonically increasing within an entity
/// - Versions within the same variant are totally ordered
/// - Cross-variant comparison is not meaningful
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Version {
    /// Transaction-based version
    ///
    /// Used by: KV, Json, Vector, Branch
    ///
    /// Represents a global transaction ID. Multiple entities modified
    /// in the same transaction share this version.
    Txn(u64),

    /// Sequence-based version
    ///
    /// Used by: EventLog
    ///
    /// Represents position in an append-only log.
    /// Unique within a branch's event log.
    Sequence(u64),

    /// Counter-based version
    ///
    /// Per-entity mutation counter. Increments on each modification.
    /// Used for compare-and-swap (CAS) operations.
    Counter(u64),
}

impl Version {
    // =========================================================================
    // Constructors
    // =========================================================================

    /// Create a TxnId version with value 0
    pub const fn zero_txn() -> Self {
        Version::Txn(0)
    }

    /// Create a Sequence version with value 0
    pub const fn zero_sequence() -> Self {
        Version::Sequence(0)
    }

    /// Create a Counter version with value 0
    pub const fn zero_counter() -> Self {
        Version::Counter(0)
    }

    /// Create a TxnId version
    pub const fn txn(id: u64) -> Self {
        Version::Txn(id)
    }

    /// Create a Sequence version
    pub const fn seq(n: u64) -> Self {
        Version::Sequence(n)
    }

    /// Create a Counter version
    pub const fn counter(n: u64) -> Self {
        Version::Counter(n)
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Get the numeric value
    ///
    /// Useful for storage and display, but NOT for cross-variant comparison.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        match self {
            Version::Txn(v) => *v,
            Version::Sequence(v) => *v,
            Version::Counter(v) => *v,
        }
    }

    /// Check if this is a transaction-based version
    #[inline]
    pub const fn is_txn(&self) -> bool {
        matches!(self, Version::Txn(_))
    }

    /// Check if this is a sequence-based version
    #[inline]
    pub const fn is_sequence(&self) -> bool {
        matches!(self, Version::Sequence(_))
    }

    /// Check if this is a counter-based version
    #[inline]
    pub const fn is_counter(&self) -> bool {
        matches!(self, Version::Counter(_))
    }

    // =========================================================================
    // Operations
    // =========================================================================

    /// Increment the version, returning a new version
    ///
    /// Preserves the variant type.
    ///
    /// # Panics
    ///
    /// Panics if the version value is `u64::MAX` (overflow).
    pub const fn increment(&self) -> Self {
        match self {
            Version::Txn(v) => match v.checked_add(1) {
                Some(n) => Version::Txn(n),
                None => panic!("version overflow: Txn version at u64::MAX"),
            },
            Version::Sequence(v) => match v.checked_add(1) {
                Some(n) => Version::Sequence(n),
                None => panic!("version overflow: Sequence version at u64::MAX"),
            },
            Version::Counter(v) => match v.checked_add(1) {
                Some(n) => Version::Counter(n),
                None => panic!("version overflow: Counter version at u64::MAX"),
            },
        }
    }

    /// Saturating increment (won't overflow)
    pub const fn saturating_increment(&self) -> Self {
        match self {
            Version::Txn(v) => Version::Txn(v.saturating_add(1)),
            Version::Sequence(v) => Version::Sequence(v.saturating_add(1)),
            Version::Counter(v) => Version::Counter(v.saturating_add(1)),
        }
    }

    /// Check if this version is zero
    #[inline]
    pub const fn is_zero(&self) -> bool {
        self.as_u64() == 0
    }
}

impl PartialOrd for Version {
    /// Compare versions within the same variant type.
    ///
    /// Returns `None` for cross-variant comparison (e.g., `Txn` vs `Sequence`),
    /// since comparing across versioning schemes is not meaningful.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Version::Txn(a), Version::Txn(b)) => Some(a.cmp(b)),
            (Version::Sequence(a), Version::Sequence(b)) => Some(a.cmp(b)),
            (Version::Counter(a), Version::Counter(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::Txn(v) => write!(f, "txn:{}", v),
            Version::Sequence(v) => write!(f, "seq:{}", v),
            Version::Counter(v) => write!(f, "cnt:{}", v),
        }
    }
}

impl Default for Version {
    /// Default is TxnId(0)
    fn default() -> Self {
        Version::zero_txn()
    }
}

impl From<u64> for Version {
    /// Create a TxnId version from u64
    fn from(v: u64) -> Self {
        Version::Txn(v)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_constructors() {
        assert_eq!(Version::zero_txn(), Version::Txn(0));
        assert_eq!(Version::zero_sequence(), Version::Sequence(0));
        assert_eq!(Version::zero_counter(), Version::Counter(0));

        assert_eq!(Version::txn(42), Version::Txn(42));
        assert_eq!(Version::seq(100), Version::Sequence(100));
        assert_eq!(Version::counter(5), Version::Counter(5));
    }

    #[test]
    fn test_version_as_u64() {
        assert_eq!(Version::Txn(42).as_u64(), 42);
        assert_eq!(Version::Sequence(100).as_u64(), 100);
        assert_eq!(Version::Counter(5).as_u64(), 5);
    }

    #[test]
    fn test_version_type_checks() {
        let txn = Version::Txn(1);
        let seq = Version::Sequence(1);
        let cnt = Version::Counter(1);

        assert!(txn.is_txn());
        assert!(!txn.is_sequence());
        assert!(!txn.is_counter());

        assert!(!seq.is_txn());
        assert!(seq.is_sequence());
        assert!(!seq.is_counter());

        assert!(!cnt.is_txn());
        assert!(!cnt.is_sequence());
        assert!(cnt.is_counter());
    }

    #[test]
    fn test_version_increment() {
        assert_eq!(Version::Txn(1).increment(), Version::Txn(2));
        assert_eq!(Version::Sequence(10).increment(), Version::Sequence(11));
        assert_eq!(Version::Counter(5).increment(), Version::Counter(6));
    }

    #[test]
    fn test_version_saturating_increment() {
        assert_eq!(
            Version::Txn(u64::MAX).saturating_increment(),
            Version::Txn(u64::MAX)
        );
    }

    #[test]
    fn test_version_is_zero() {
        assert!(Version::Txn(0).is_zero());
        assert!(Version::Sequence(0).is_zero());
        assert!(Version::Counter(0).is_zero());

        assert!(!Version::Txn(1).is_zero());
        assert!(!Version::Sequence(1).is_zero());
        assert!(!Version::Counter(1).is_zero());
    }

    #[test]
    fn test_version_partial_ord_same_type() {
        assert!(Version::Txn(1) < Version::Txn(2));
        assert!(Version::Txn(2) > Version::Txn(1));
        assert!(Version::Txn(1) == Version::Txn(1));

        assert!(Version::Sequence(10) < Version::Sequence(20));
        assert!(Version::Counter(3) == Version::Counter(3));
    }

    #[test]
    fn test_version_partial_ord_different_types() {
        // Cross-variant comparison returns None (not meaningful)
        assert_eq!(Version::Txn(1).partial_cmp(&Version::Sequence(1)), None);
        assert_eq!(Version::Sequence(1).partial_cmp(&Version::Counter(1)), None);
        assert_eq!(Version::Txn(1).partial_cmp(&Version::Counter(1)), None);
    }

    #[test]
    fn test_version_partial_ord_reverse_direction() {
        // Cross-variant comparison returns None in both directions
        assert_eq!(
            Version::Sequence(1).partial_cmp(&Version::Txn(1)),
            None,
            "Cross-variant should return None"
        );
        assert_eq!(
            Version::Counter(1).partial_cmp(&Version::Sequence(1)),
            None,
            "Cross-variant should return None"
        );
        assert_eq!(
            Version::Counter(1).partial_cmp(&Version::Txn(1)),
            None,
            "Cross-variant should return None"
        );
    }

    #[test]
    fn test_version_different_types_never_equal() {
        // Different variant types are never equal, even with same numeric value
        assert_ne!(Version::Txn(42), Version::Sequence(42));
        assert_ne!(Version::Txn(42), Version::Counter(42));
        assert_ne!(Version::Sequence(42), Version::Counter(42));

        // Verify via partial_cmp as well
        use std::cmp::Ordering;
        assert_ne!(
            Version::Txn(42).partial_cmp(&Version::Sequence(42)),
            Some(Ordering::Equal)
        );
        assert_ne!(
            Version::Txn(42).partial_cmp(&Version::Counter(42)),
            Some(Ordering::Equal)
        );
        assert_ne!(
            Version::Sequence(42).partial_cmp(&Version::Counter(42)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_version_boundary_values_different_types() {
        // Cross-variant comparison returns None regardless of values
        assert_eq!(
            Version::Txn(u64::MAX).partial_cmp(&Version::Sequence(0)),
            None,
            "Cross-variant should return None"
        );
        assert_eq!(
            Version::Sequence(u64::MAX).partial_cmp(&Version::Counter(0)),
            None,
            "Cross-variant should return None"
        );
        assert_eq!(
            Version::Counter(0).partial_cmp(&Version::Txn(u64::MAX)),
            None,
            "Cross-variant should return None"
        );
    }

    #[test]
    fn test_version_ordering_symmetry() {
        use std::cmp::Ordering;

        // Same-variant pairs have total ordering
        let same_variant_pairs = [
            (Version::Txn(5), Version::Txn(10)),
            (Version::Sequence(5), Version::Sequence(10)),
            (Version::Counter(5), Version::Counter(10)),
        ];

        for (a, b) in same_variant_pairs {
            let a_cmp_b = a.partial_cmp(&b);
            let b_cmp_a = b.partial_cmp(&a);

            match (a_cmp_b, b_cmp_a) {
                (Some(Ordering::Less), Some(Ordering::Greater)) => {}
                (Some(Ordering::Greater), Some(Ordering::Less)) => {}
                (Some(Ordering::Equal), Some(Ordering::Equal)) => {}
                _ => panic!(
                    "Ordering symmetry violated: {:?} vs {:?} gave {:?} vs {:?}",
                    a, b, a_cmp_b, b_cmp_a
                ),
            }
        }

        // Cross-variant pairs return None
        let cross_variant_pairs = [
            (Version::Txn(5), Version::Sequence(5)),
            (Version::Sequence(5), Version::Counter(5)),
            (Version::Txn(5), Version::Counter(5)),
        ];

        for (a, b) in cross_variant_pairs {
            assert_eq!(
                a.partial_cmp(&b),
                None,
                "Cross-variant {:?} vs {:?} should be None",
                a,
                b
            );
            assert_eq!(
                b.partial_cmp(&a),
                None,
                "Cross-variant {:?} vs {:?} should be None",
                b,
                a
            );
        }
    }

    #[test]
    fn test_version_partial_cmp_same_variant() {
        use std::cmp::Ordering;

        // Same variant: compare by value
        assert_eq!(
            Version::Txn(5).partial_cmp(&Version::Txn(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Version::Txn(10).partial_cmp(&Version::Txn(5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Version::Txn(5).partial_cmp(&Version::Txn(5)),
            Some(Ordering::Equal)
        );

        // Cross-variant: None
        assert_eq!(Version::Txn(5).partial_cmp(&Version::Sequence(5)), None);
        assert_eq!(Version::Sequence(5).partial_cmp(&Version::Counter(5)), None);
    }

    #[test]
    fn test_version_display() {
        assert_eq!(format!("{}", Version::Txn(42)), "txn:42");
        assert_eq!(format!("{}", Version::Sequence(100)), "seq:100");
        assert_eq!(format!("{}", Version::Counter(5)), "cnt:5");
    }

    #[test]
    fn test_version_default() {
        assert_eq!(Version::default(), Version::Txn(0));
    }

    #[test]
    fn test_version_from_u64() {
        let v: Version = 42u64.into();
        assert_eq!(v, Version::Txn(42));
    }

    #[test]
    fn test_version_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Version::Txn(1));
        set.insert(Version::Txn(2));
        set.insert(Version::Txn(1)); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(&Version::Txn(1)));
        assert!(set.contains(&Version::Txn(2)));
    }

    #[test]
    fn test_version_serialization() {
        let versions = vec![
            Version::Txn(42),
            Version::Sequence(100),
            Version::Counter(5),
        ];

        for v in versions {
            let json = serde_json::to_string(&v).unwrap();
            let restored: Version = serde_json::from_str(&json).unwrap();
            assert_eq!(v, restored);
        }
    }

    #[test]
    fn test_version_equality() {
        // Same type, same value
        assert_eq!(Version::Txn(1), Version::Txn(1));

        // Same type, different value
        assert_ne!(Version::Txn(1), Version::Txn(2));

        // Different type, same value
        assert_ne!(Version::Txn(1), Version::Sequence(1));
        assert_ne!(Version::Txn(1), Version::Counter(1));
        assert_ne!(Version::Sequence(1), Version::Counter(1));
    }
}
