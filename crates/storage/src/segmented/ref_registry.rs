//! Reference counting registry for shared segments (COW branching).
//!
//! Tracks how many branches reference each segment file. When a segment's
//! refcount drops to zero (or was never tracked), it is safe to delete.

use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Segment identity — matches `KVSegment::file_id()`.
pub(crate) type SegmentId = u64;

/// Thread-safe reference count registry for shared segments.
///
/// Untracked segments (never `increment`ed) are treated as unreferenced —
/// `decrement` returns `true`, so existing non-COW compaction deletes files
/// normally. In Epic C, shared segments will be `increment`ed; only then
/// does the guard become load-bearing.
///
/// The `deletion_barrier` RwLock prevents a TOCTOU race between
/// `is_referenced()` + file deletion (compaction) and `increment()`
/// (fork_branch). See issue #1682.
///
/// ## B5.1 retention contract
///
/// `BarrierKind::RuntimeAccelerator` per
/// `docs/design/branching/branching-gc/branching-retention-contract.md`
/// §"Authoritative and non-authoritative state". This registry is
/// **not** authoritative for reclaim safety: refcount zero alone is
/// not a deletion proof (Invariant 3, KD1). The reclaim protocol
/// (§"Reclaim protocol") uses this registry only as candidate-
/// selection input and TOCTOU guard; the durable proof remains
/// own-segment + inherited-layer manifest reachability. Disagreement
/// between this registry and manifest evidence must block reclaim
/// (KD10, §"Accelerator disagreement rule").
pub(crate) struct SegmentRefRegistry {
    refs: DashMap<SegmentId, AtomicUsize>,
    /// Fork increments hold a **read** guard (concurrent forks OK).
    /// Segment deletion holds a **write** guard (exclusive with forks).
    deletion_barrier: RwLock<()>,
    /// Test-only hook: called between `drop(entry)` and `remove_if` in
    /// `decrement()`, enabling deterministic reproduction of the race
    /// window where a concurrent increment can slip in (#1719).
    #[cfg(test)]
    pub(crate) post_fetch_update_hook: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl SegmentRefRegistry {
    /// Create an empty registry.
    pub(crate) fn new() -> Self {
        Self {
            refs: DashMap::new(),
            deletion_barrier: RwLock::new(()),
            #[cfg(test)]
            post_fetch_update_hook: std::sync::Mutex::new(None),
        }
    }

    /// Acquire a read guard on the deletion barrier (Lock Level 3, shared).
    ///
    /// Hold this while incrementing refcounts for a batch of segments
    /// (e.g., during `fork_branch`). Prevents concurrent segment file
    /// deletion from observing a transient zero refcount mid-batch.
    pub(crate) fn deletion_read_guard(&self) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.deletion_barrier.read() // Lock Level 3 (shared)
    }

    /// Acquire a write guard on the deletion barrier (Lock Level 3, exclusive).
    ///
    /// Hold this during the check-and-delete sequence in
    /// `delete_segment_if_unreferenced`. Ensures no concurrent fork
    /// can increment a refcount between the `is_referenced()` check
    /// and the `remove_file()` call.
    pub(crate) fn deletion_write_guard(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.deletion_barrier.write() // Lock Level 3 (exclusive)
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

        // Test hook: allows injecting a concurrent increment between
        // drop(entry) and remove_if to reproduce the #1719 race.
        #[cfg(test)]
        {
            let guard = self.post_fetch_update_hook.lock().unwrap();
            if let Some(hook) = guard.as_ref() {
                hook();
            }
        }

        match result {
            Ok(prev) if prev <= 1 => {
                // Count reached zero — clean up only if still zero.
                // A concurrent increment may have bumped the count back above
                // zero between drop(entry) and here (#1719).
                let removed = self
                    .refs
                    .remove_if(&id, |_, count| count.load(Ordering::Acquire) == 0);
                removed.is_some()
            }
            Ok(_) => false, // still referenced
            Err(_) => {
                // Count was already zero — over-decrement (logic bug in caller).
                // Attempt cleanup of stale entry; only report safe-to-delete
                // if the entry was actually removed.
                let removed = self
                    .refs
                    .remove_if(&id, |_, count| count.load(Ordering::Acquire) == 0);
                removed.is_some()
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

    /// #1719: decrement must return false if a concurrent increment
    /// prevents remove_if from actually removing the entry.
    ///
    /// Uses the test hook to deterministically inject an increment between
    /// drop(entry) and remove_if inside decrement(), reproducing the exact
    /// race described in the issue.
    #[test]
    fn test_issue_1719_decrement_returns_false_when_concurrent_increment() {
        use std::sync::Arc;

        let reg = Arc::new(SegmentRefRegistry::new());
        reg.increment(1); // count = 1
        assert_eq!(reg.ref_count(1), 1);

        // Install hook: simulates a concurrent fork_branch increment
        // that slips in between drop(entry) and remove_if.
        let hook_reg = Arc::downgrade(&reg);
        *reg.post_fetch_update_hook.lock().unwrap() = Some(Box::new(move || {
            if let Some(r) = hook_reg.upgrade() {
                r.increment(1); // concurrent fork re-references the segment
            }
        }));

        // Decrement: fetch_update 1→0 (prev=1), drop(entry),
        // hook runs (increment 0→1), remove_if sees count=1 → no removal.
        // BUG: returns true (only checks prev<=1)
        // FIX: returns false (checks remove_if result)
        let safe = reg.decrement(1);

        // Clear hook to break Arc cycle before assertions
        *reg.post_fetch_update_hook.lock().unwrap() = None;

        assert!(
            !safe,
            "decrement must return false when concurrent increment prevents removal"
        );
        assert_eq!(reg.ref_count(1), 1);
        assert!(reg.is_referenced(1));
    }

    /// #1719 concurrent stress test: interleave decrement and increment
    /// on the same segment ID. After all threads complete, verify the
    /// registry is consistent (no corrupted entries, no false positives).
    #[test]
    fn test_issue_1719_decrement_concurrent_increment_stress() {
        use std::sync::Arc;
        use std::sync::Barrier;

        for _ in 0..100 {
            let reg = Arc::new(SegmentRefRegistry::new());
            // Start with refcount 2 (two children sharing segment)
            reg.increment(1);
            reg.increment(1);
            assert_eq!(reg.ref_count(1), 2);

            // Barrier for 3 threads only (main thread does NOT participate)
            let barrier = Arc::new(Barrier::new(3));

            // Thread 1: decrement (2→1)
            let r1 = Arc::clone(&reg);
            let b1 = Arc::clone(&barrier);
            let t1 = std::thread::spawn(move || {
                b1.wait();
                r1.decrement(1)
            });

            // Thread 2: decrement (should go 1→0)
            let r2 = Arc::clone(&reg);
            let b2 = Arc::clone(&barrier);
            let t2 = std::thread::spawn(move || {
                b2.wait();
                r2.decrement(1)
            });

            // Thread 3: increment (concurrent fork re-referencing)
            let r3 = Arc::clone(&reg);
            let b3 = Arc::clone(&barrier);
            let t3 = std::thread::spawn(move || {
                b3.wait();
                r3.increment(1);
            });

            let d1 = t1.join().unwrap();
            let d2 = t2.join().unwrap();
            t3.join().unwrap();

            // After 2 decrements and 1 increment from initial count 2:
            // net count should be 2 - 2 + 1 = 1
            let count = reg.ref_count(1);
            assert_eq!(count, 1, "refcount must be 1 after 2 dec + 1 inc from 2");
            assert!(reg.is_referenced(1));

            // KEY INVARIANT: if the segment is still referenced,
            // at most one decrement may have returned "safe to delete".
            // With the bug, both decrements could return true.
            let safe_count = d1 as usize + d2 as usize;
            assert!(
                safe_count <= 1,
                "at most one decrement should return safe-to-delete when segment is still referenced, got {}",
                safe_count,
            );
        }
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
