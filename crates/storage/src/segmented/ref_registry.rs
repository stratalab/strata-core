//! Reference counting registry for shared segments (COW branching).
//!
//! Tracks how many branches reference each segment file. When a segment's
//! refcount drops to zero (or was never tracked), it is safe to delete.

use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Segment identity — matches `KVSegment::file_id()`.
pub(crate) type SegmentId = u64;

/// Thread-safe reference count registry for shared segments.
///
/// Untracked segments (never `increment`ed) are treated as unreferenced —
/// `decrement` returns `true`, so existing non-COW compaction deletes files
/// normally. In Epic C, shared segments will be `increment`ed; only then
/// does the guard become load-bearing.
pub(crate) struct SegmentRefRegistry {
    refs: DashMap<SegmentId, AtomicUsize>,
}

impl SegmentRefRegistry {
    /// Create an empty registry.
    pub(crate) fn new() -> Self {
        Self {
            refs: DashMap::new(),
        }
    }

    /// Increment the reference count for `id`.
    ///
    /// Creates the entry with count 1 if it does not exist, otherwise adds 1.
    pub(crate) fn increment(&self, id: SegmentId) {
        self.refs
            .entry(id)
            .and_modify(|count| {
                count.fetch_add(1, Ordering::Relaxed);
            })
            .or_insert_with(|| AtomicUsize::new(1));
    }

    /// Decrement the reference count for `id`.
    ///
    /// Returns `true` if the segment is safe to delete:
    /// - count reached zero after decrement, OR
    /// - the segment was never tracked (untracked = owned by one branch).
    ///
    /// Uses `fetch_update` to prevent underflow (wrapping to `usize::MAX`)
    /// and `DashMap::remove_if` to atomically clean up zero-count entries.
    pub(crate) fn decrement(&self, id: SegmentId) -> bool {
        let entry = match self.refs.get(&id) {
            Some(e) => e,
            None => return true, // untracked → safe to delete
        };

        // Atomically subtract 1 only if count > 0, preventing underflow.
        let result = entry.fetch_update(Ordering::AcqRel, Ordering::Acquire, |val| {
            if val > 0 {
                Some(val - 1)
            } else {
                None
            }
        });
        drop(entry); // release read guard before remove_if

        match result {
            Ok(prev) if prev <= 1 => {
                // Count reached zero — clean up atomically
                self.refs
                    .remove_if(&id, |_, count| count.load(Ordering::Relaxed) == 0);
                true
            }
            Ok(_) => false, // still referenced
            Err(_) => {
                // Count was already zero — over-decrement (logic bug in caller).
                // Safe to return true; attempt cleanup of stale entry.
                self.refs
                    .remove_if(&id, |_, count| count.load(Ordering::Relaxed) == 0);
                true
            }
        }
    }

    /// Check if a segment is currently referenced (count > 0).
    pub(crate) fn is_referenced(&self, id: SegmentId) -> bool {
        self.refs
            .get(&id)
            .is_some_and(|count| count.load(Ordering::Relaxed) > 0)
    }

    /// Get the current reference count (test helper).
    #[cfg(test)]
    pub(crate) fn ref_count(&self, id: SegmentId) -> usize {
        self.refs
            .get(&id)
            .map(|count| count.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_registry_increment_decrement() {
        let reg = SegmentRefRegistry::new();
        reg.increment(1);
        assert_eq!(reg.ref_count(1), 1);
        assert!(reg.is_referenced(1));

        let safe = reg.decrement(1);
        assert!(safe);
        assert!(!reg.is_referenced(1));
        assert_eq!(reg.ref_count(1), 0);
    }

    #[test]
    fn ref_registry_decrement_untracked() {
        let reg = SegmentRefRegistry::new();
        // Decrement on a never-incremented ID should return true (safe to delete)
        assert!(reg.decrement(42));
        assert!(!reg.is_referenced(42));
    }

    #[test]
    fn ref_registry_over_decrement_does_not_corrupt() {
        let reg = SegmentRefRegistry::new();
        reg.increment(1);
        assert!(reg.decrement(1)); // 1 → 0, safe to delete

        // Over-decrement: count is already 0, should not wrap to usize::MAX
        assert!(reg.decrement(1)); // already 0, returns true (safe)
        assert_eq!(reg.ref_count(1), 0);
        assert!(!reg.is_referenced(1));

        // Subsequent increment should work correctly (not start from usize::MAX)
        reg.increment(1);
        assert_eq!(reg.ref_count(1), 1);
        assert!(reg.is_referenced(1));
    }

    #[test]
    fn ref_registry_multiple_increments() {
        let reg = SegmentRefRegistry::new();
        reg.increment(1);
        reg.increment(1);
        reg.increment(1);
        assert_eq!(reg.ref_count(1), 3);

        assert!(!reg.decrement(1)); // 3 → 2, still referenced
        assert!(!reg.decrement(1)); // 2 → 1, still referenced
        assert!(reg.decrement(1)); // 1 → 0, safe to delete
        assert!(!reg.is_referenced(1));
    }

    #[test]
    fn ref_registry_multiple_segments() {
        let reg = SegmentRefRegistry::new();
        reg.increment(10);
        reg.increment(20);
        reg.increment(20);

        assert_eq!(reg.ref_count(10), 1);
        assert_eq!(reg.ref_count(20), 2);

        assert!(reg.decrement(10)); // 10 → 0
        assert!(!reg.decrement(20)); // 20: 2 → 1
        assert!(!reg.is_referenced(10));
        assert!(reg.is_referenced(20));
    }

    #[test]
    fn ref_registry_concurrent() {
        use std::sync::Arc;

        let reg = Arc::new(SegmentRefRegistry::new());
        let threads: Vec<_> = (0..10)
            .map(|_| {
                let r = Arc::clone(&reg);
                std::thread::spawn(move || {
                    r.increment(1);
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(reg.ref_count(1), 10);

        // Decrement 9 times — should still be referenced
        let threads: Vec<_> = (0..9)
            .map(|_| {
                let r = Arc::clone(&reg);
                std::thread::spawn(move || {
                    r.decrement(1);
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(reg.ref_count(1), 1);
        assert!(reg.is_referenced(1));

        // Final decrement
        assert!(reg.decrement(1));
        assert!(!reg.is_referenced(1));
    }
}
