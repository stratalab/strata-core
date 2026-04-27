//! Version history container for primitives
//!
//! `VersionedHistory<T>` wraps a non-empty `Vec<Versioned<T>>` ordered
//! newest-first. Users index into it: `h[0]` = latest, `h[1]` = previous,
//! `h.len()` = total versions.

use super::{Timestamp, Version, Versioned};
use std::ops::Index;

/// A non-empty sequence of versioned values, ordered newest-first.
///
/// Returned by `getv()`/`readv()` on primitives. Provides convenient
/// access to the latest value and version metadata, plus indexing into
/// the full history.
///
/// # Example
///
/// ```no_run
/// # use strata_core::{Version, Versioned, VersionedHistory};
/// # use strata_core::value::Value;
/// # let v1 = Versioned::new(Value::Int(2), Version::txn(2));
/// # let v2 = Versioned::new(Value::Int(1), Version::txn(1));
/// # let history = VersionedHistory::new(vec![v1, v2]).unwrap();
/// let latest = &history[0];          // newest version
/// let previous = &history[1];        // one version back
/// println!("total versions: {}", history.len());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedHistory<T> {
    /// Versions ordered newest-first. Always non-empty.
    versions: Vec<Versioned<T>>,
}

impl<T> VersionedHistory<T> {
    /// Create a new `VersionedHistory` from a list of versioned values.
    ///
    /// Returns `None` if the input is empty (key does not exist).
    /// The input must be ordered newest-first.
    pub fn new(versions: Vec<Versioned<T>>) -> Option<Self> {
        if versions.is_empty() {
            None
        } else {
            Some(Self { versions })
        }
    }

    /// Get a reference to the latest value.
    pub fn value(&self) -> &T {
        &self.versions[0].value
    }

    /// Get the number of versions in the history.
    pub fn len(&self) -> usize {
        self.versions.len()
    }

    /// Returns `false` — a `VersionedHistory` is always non-empty.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Get the version identifier of the latest entry.
    pub fn version(&self) -> Version {
        self.versions[0].version
    }

    /// Get the timestamp of the latest entry.
    pub fn timestamp(&self) -> Timestamp {
        self.versions[0].timestamp
    }

    /// Get a slice of all versioned entries (newest-first).
    pub fn versions(&self) -> &[Versioned<T>] {
        &self.versions
    }

    /// Consume and return the inner vector of versioned entries.
    pub fn into_versions(self) -> Vec<Versioned<T>> {
        self.versions
    }
}

impl<T> Index<usize> for VersionedHistory<T> {
    type Output = Versioned<T>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.versions[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_new_returns_none_for_empty() {
        let history: Option<VersionedHistory<Value>> = VersionedHistory::new(vec![]);
        assert!(history.is_none());
    }

    #[test]
    fn test_new_returns_some_for_nonempty() {
        let v = Versioned::new(Value::Int(42), Version::txn(1));
        let history = VersionedHistory::new(vec![v]).unwrap();
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn test_value_returns_latest() {
        let v1 = Versioned::new(Value::Int(2), Version::txn(2));
        let v2 = Versioned::new(Value::Int(1), Version::txn(1));
        let history = VersionedHistory::new(vec![v1, v2]).unwrap();
        assert_eq!(*history.value(), Value::Int(2));
    }

    #[test]
    fn test_version_returns_latest() {
        let v1 = Versioned::new(Value::Int(2), Version::txn(5));
        let v2 = Versioned::new(Value::Int(1), Version::txn(3));
        let history = VersionedHistory::new(vec![v1, v2]).unwrap();
        assert_eq!(history.version(), Version::txn(5));
    }

    #[test]
    fn test_indexing() {
        let v1 = Versioned::new(Value::Int(2), Version::txn(2));
        let v2 = Versioned::new(Value::Int(1), Version::txn(1));
        let history = VersionedHistory::new(vec![v1.clone(), v2.clone()]).unwrap();
        assert_eq!(history[0], v1);
        assert_eq!(history[1], v2);
    }

    #[test]
    fn test_versions_slice() {
        let v1 = Versioned::new(Value::Int(2), Version::txn(2));
        let v2 = Versioned::new(Value::Int(1), Version::txn(1));
        let history = VersionedHistory::new(vec![v1, v2]).unwrap();
        assert_eq!(history.versions().len(), 2);
    }

    #[test]
    fn test_into_versions() {
        let v1 = Versioned::new(Value::Int(2), Version::txn(2));
        let history = VersionedHistory::new(vec![v1]).unwrap();
        let versions = history.into_versions();
        assert_eq!(versions.len(), 1);
    }
}
