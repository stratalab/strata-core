//! Thread-local transaction pool for performance
//!
//! Eliminates allocation overhead by reusing TransactionContext objects.
//! Each thread maintains its own pool of up to MAX_POOL_SIZE contexts.
//!
//! # Key Optimization
//!
//! HashMap/HashSet/Vec `clear()` preserves allocated capacity.
//! By reusing contexts, we avoid:
//! - HashMap bucket allocation
//! - HashSet bucket allocation
//! - Vec buffer allocation
//!
//! After warmup, the hot path has zero allocations.

use std::cell::RefCell;
use strata_concurrency::TransactionContext;
use strata_core::traits::SnapshotView;
use strata_core::types::BranchId;

/// Maximum contexts per thread
///
/// 8 is sufficient for typical concurrent workloads where each thread
/// only needs 1-2 active transactions at a time.
pub const MAX_POOL_SIZE: usize = 8;

thread_local! {
    /// Thread-local pool of reusable contexts
    static TXN_POOL: RefCell<Vec<TransactionContext>> = RefCell::new(Vec::with_capacity(MAX_POOL_SIZE));
}

/// Transaction pool operations
///
/// Provides thread-local pooling of TransactionContext objects.
/// The pool is implicitly created per-thread on first access.
///
/// # Example
///
/// ```text
/// // Acquire a context (from pool or new allocation)
/// let ctx = TransactionPool::acquire(1, branch_id, Some(snapshot));
///
/// // ... use the context ...
///
/// // Return to pool for reuse
/// TransactionPool::release(ctx);
/// ```
pub struct TransactionPool;

impl TransactionPool {
    /// Acquire a transaction context
    ///
    /// Returns pooled context if available, allocates new if pool is empty.
    /// Pooled contexts are reset with new parameters but retain their
    /// internal collection capacity.
    ///
    /// # Arguments
    /// * `txn_id` - Unique transaction identifier
    /// * `branch_id` - BranchId for namespace isolation
    /// * `snapshot` - Optional snapshot for snapshot isolation
    ///
    /// # Returns
    /// * `TransactionContext` - Active transaction ready for operations
    pub fn acquire(
        txn_id: u64,
        branch_id: BranchId,
        snapshot: Option<Box<dyn SnapshotView>>,
    ) -> TransactionContext {
        TXN_POOL.with(|pool| {
            match pool.borrow_mut().pop() {
                Some(mut ctx) => {
                    // Reuse existing allocation - key optimization!
                    ctx.reset(txn_id, branch_id, snapshot);
                    ctx
                }
                None => {
                    // Pool empty - allocate new
                    match snapshot {
                        Some(snap) => TransactionContext::with_snapshot(txn_id, branch_id, snap),
                        None => TransactionContext::new(txn_id, branch_id, 0),
                    }
                }
            }
        })
    }

    /// Return a transaction context to the pool
    ///
    /// Context is returned if pool has room, dropped otherwise.
    /// Contexts that have grown large internal collections are
    /// valuable to keep as they avoid future allocations.
    ///
    /// # Arguments
    /// * `ctx` - Transaction context to return to pool
    pub fn release(ctx: TransactionContext) {
        TXN_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_POOL_SIZE {
                pool.push(ctx);
            }
            // else: drop (pool full)
        });
    }

    /// Get current pool size (for debugging/testing)
    ///
    /// Returns the number of contexts currently in the pool.
    pub fn pool_size() -> usize {
        TXN_POOL.with(|pool| pool.borrow().len())
    }

    /// Clear the pool (for testing)
    ///
    /// Removes all contexts from the thread-local pool.
    #[cfg(test)]
    pub fn clear() {
        TXN_POOL.with(|pool| pool.borrow_mut().clear());
    }

    /// Pre-warm the pool with contexts
    ///
    /// Creates `count` contexts and adds them to the pool.
    /// This eliminates allocation overhead during the hot path.
    ///
    /// # Arguments
    /// * `count` - Number of contexts to pre-allocate (capped at MAX_POOL_SIZE)
    pub fn warmup(count: usize) {
        let count = count.min(MAX_POOL_SIZE);
        TXN_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            let current = pool.len();
            for _ in current..count {
                // Create minimal context for pool
                let ctx = TransactionContext::new(0, BranchId::new(), 0);
                pool.push(ctx);
            }
        });
    }

    /// Get total capacity across all pooled contexts
    ///
    /// Returns sum of (read_set, write_set, delete_set, cas_set) capacities.
    /// Useful for debugging pool effectiveness.
    #[cfg(test)]
    pub fn total_capacity() -> (usize, usize, usize, usize) {
        TXN_POOL.with(|pool| {
            let pool = pool.borrow();
            let mut total = (0, 0, 0, 0);
            for ctx in pool.iter() {
                let (r, w, d, c) = ctx.capacity();
                total.0 += r;
                total.1 += w;
                total.2 += d;
                total.3 += c;
            }
            total
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use strata_concurrency::snapshot::ClonedSnapshotView;
    use strata_core::types::{Namespace, TypeTag};
    use strata_core::value::Value;

    fn create_test_namespace() -> Arc<Namespace> {
        Arc::new(Namespace::new(
            BranchId::new(),
            "default".to_string(),
        ))
    }

    fn create_test_key(ns: &Arc<Namespace>, user_key: &[u8]) -> strata_core::types::Key {
        strata_core::types::Key::new(ns.clone(), TypeTag::KV, user_key.to_vec())
    }

    #[test]
    fn test_acquire_from_empty_pool() {
        TransactionPool::clear();
        assert_eq!(TransactionPool::pool_size(), 0);

        let branch_id = BranchId::new();
        let ctx = TransactionPool::acquire(1, branch_id, None);

        // Pool still empty (we acquired, not released)
        assert_eq!(TransactionPool::pool_size(), 0);
        assert_eq!(ctx.txn_id, 1);
        assert_eq!(ctx.branch_id, branch_id);
    }

    #[test]
    fn test_release_adds_to_pool() {
        TransactionPool::clear();

        let branch_id = BranchId::new();
        let ctx = TransactionPool::acquire(1, branch_id, None);
        TransactionPool::release(ctx);

        assert_eq!(TransactionPool::pool_size(), 1);
    }

    #[test]
    fn test_acquire_reuses_pooled() {
        TransactionPool::clear();
        let ns = create_test_namespace();
        let branch_id = ns.branch_id;

        // Create and release a context with capacity
        let mut ctx = TransactionPool::acquire(1, branch_id, None);

        // Fill with data to grow capacity
        for i in 0..100 {
            let key = create_test_key(&ns, format!("key{}", i).as_bytes());
            ctx.read_set.insert(key.clone(), i as u64);
            ctx.write_set.insert(key, Value::Bytes(vec![i as u8]));
        }

        let original_cap = ctx.capacity();
        assert!(original_cap.0 >= 100, "read_set should have capacity");
        assert!(original_cap.1 >= 100, "write_set should have capacity");

        TransactionPool::release(ctx);
        assert_eq!(TransactionPool::pool_size(), 1);

        // Acquire again - should reuse with preserved capacity
        let new_branch_id = BranchId::new();
        let ctx2 = TransactionPool::acquire(2, new_branch_id, None);

        // Capacity preserved
        assert_eq!(ctx2.capacity(), original_cap);

        // But data cleared
        assert!(ctx2.read_set.is_empty());
        assert!(ctx2.write_set.is_empty());
        assert_eq!(ctx2.txn_id, 2);
        assert_eq!(ctx2.branch_id, new_branch_id);
    }

    #[test]
    fn test_pool_caps_at_max_size() {
        TransactionPool::clear();
        let branch_id = BranchId::new();

        // Acquire multiple contexts first, then release them all
        // This tests that pool caps at MAX_POOL_SIZE
        let mut contexts = Vec::new();
        for i in 0..(MAX_POOL_SIZE + 5) {
            contexts.push(TransactionPool::acquire(i as u64, branch_id, None));
        }

        // Now release them all
        for ctx in contexts {
            TransactionPool::release(ctx);
        }

        // Pool should cap at MAX_POOL_SIZE
        assert_eq!(TransactionPool::pool_size(), MAX_POOL_SIZE);
    }

    #[test]
    fn test_pool_is_thread_local() {
        use std::thread;

        TransactionPool::clear();
        let branch_id = BranchId::new();

        // Add to this thread's pool
        let ctx = TransactionPool::acquire(1, branch_id, None);
        TransactionPool::release(ctx);
        assert_eq!(TransactionPool::pool_size(), 1);

        // Other thread has its own empty pool
        let handle = thread::spawn(|| {
            assert_eq!(TransactionPool::pool_size(), 0);

            // Add to other thread's pool
            let ctx = TransactionPool::acquire(2, BranchId::new(), None);
            TransactionPool::release(ctx);
            assert_eq!(TransactionPool::pool_size(), 1);
        });
        handle.join().unwrap();

        // Our pool unchanged
        assert_eq!(TransactionPool::pool_size(), 1);
    }

    #[test]
    fn test_acquire_with_snapshot() {
        TransactionPool::clear();
        let branch_id = BranchId::new();

        // Create a snapshot
        let snapshot_data = BTreeMap::new();
        let snapshot = Box::new(ClonedSnapshotView::new(500, snapshot_data));

        let ctx = TransactionPool::acquire(1, branch_id, Some(snapshot));

        assert_eq!(ctx.txn_id, 1);
        assert_eq!(ctx.branch_id, branch_id);
        assert_eq!(ctx.start_version, 500);
    }

    #[test]
    fn test_warmup() {
        TransactionPool::clear();
        assert_eq!(TransactionPool::pool_size(), 0);

        TransactionPool::warmup(5);
        assert_eq!(TransactionPool::pool_size(), 5);

        // Warmup respects MAX_POOL_SIZE
        TransactionPool::warmup(100);
        assert_eq!(TransactionPool::pool_size(), MAX_POOL_SIZE);
    }

    #[test]
    fn test_warmup_idempotent() {
        TransactionPool::clear();

        TransactionPool::warmup(4);
        assert_eq!(TransactionPool::pool_size(), 4);

        // Warmup with same count is idempotent (doesn't add more)
        TransactionPool::warmup(4);
        assert_eq!(TransactionPool::pool_size(), 4);

        // Warmup with higher count adds more
        TransactionPool::warmup(6);
        assert_eq!(TransactionPool::pool_size(), 6);
    }

    #[test]
    fn test_acquire_release_cycle() {
        TransactionPool::clear();
        let branch_id = BranchId::new();

        // Simulate typical usage pattern
        for cycle in 0..10 {
            let ctx = TransactionPool::acquire(cycle as u64, branch_id, None);
            assert_eq!(ctx.txn_id, cycle as u64);
            TransactionPool::release(ctx);
        }

        // Pool should have exactly 1 context (last released)
        assert_eq!(TransactionPool::pool_size(), 1);
    }

    #[test]
    fn test_total_capacity() {
        TransactionPool::clear();
        let ns = create_test_namespace();
        let branch_id = ns.branch_id;

        // Create context with capacity and release
        let mut ctx = TransactionPool::acquire(1, branch_id, None);
        for i in 0..50 {
            let key = create_test_key(&ns, format!("key{}", i).as_bytes());
            ctx.read_set.insert(key.clone(), i as u64);
        }
        TransactionPool::release(ctx);

        let (r, _w, _d, _c) = TransactionPool::total_capacity();
        assert!(r >= 50, "should have accumulated read_set capacity");
    }
}
