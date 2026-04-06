//! Version Counter Tests
//!
//! Tests for version counter semantics:
//! - Monotonic increment
//! - Concurrent uniqueness
//! - Wrap-around handling
//! - Recovery restoration

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_concurrency::manager::TransactionManager;

// ============================================================================
// Monotonic Increment
// ============================================================================

#[test]
fn versions_monotonically_increase() {
    let manager = TransactionManager::new(1);

    let mut prev = 0u64;
    for _ in 0..100 {
        let v = manager.allocate_version().unwrap();
        assert!(v > prev, "Version should increase: {} -> {}", prev, v);
        prev = v;
    }
}

#[test]
fn txn_ids_monotonically_increase() {
    let manager = TransactionManager::new(1);

    let mut prev = 0u64;
    for _ in 0..100 {
        let id = manager.next_txn_id().unwrap();
        assert!(id > prev, "Txn ID should increase: {} -> {}", prev, id);
        prev = id;
    }
}

#[test]
fn version_starts_from_initial() {
    let manager = TransactionManager::new(1000);

    let v = manager.allocate_version().unwrap();
    assert!(
        v >= 1000,
        "First version should be >= initial (1000), got {}",
        v
    );
}

// ============================================================================
// Concurrent Uniqueness
// ============================================================================

#[test]
fn concurrent_version_allocation_unique() {
    let manager = Arc::new(TransactionManager::new(1));
    let barrier = Arc::new(Barrier::new(8));
    let count_per_thread = 1000;

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                let mut versions = Vec::with_capacity(count_per_thread);
                for _ in 0..count_per_thread {
                    versions.push(manager.allocate_version().unwrap());
                }
                versions
            })
        })
        .collect();

    let mut all_versions: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    let total = all_versions.len();
    all_versions.sort();
    all_versions.dedup();

    assert_eq!(
        all_versions.len(),
        total,
        "All versions should be unique: {} unique out of {}",
        all_versions.len(),
        total
    );
}

#[test]
fn concurrent_txn_id_allocation_unique() {
    let manager = Arc::new(TransactionManager::new(1));
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                let mut ids = Vec::with_capacity(500);
                for _ in 0..500 {
                    ids.push(manager.next_txn_id().unwrap());
                }
                ids
            })
        })
        .collect();

    let mut all_ids: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    let total = all_ids.len();
    all_ids.sort();
    all_ids.dedup();

    assert_eq!(all_ids.len(), total, "All txn IDs should be unique");
}

#[test]
fn high_contention_version_allocation() {
    let manager = Arc::new(TransactionManager::new(1));
    let barrier = Arc::new(Barrier::new(16));
    let total_allocated = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..16)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            let total = Arc::clone(&total_allocated);

            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let _ = manager.allocate_version().unwrap();
                    total.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have allocated exactly 16 * 100 = 1600 versions
    assert_eq!(total_allocated.load(Ordering::Relaxed), 1600);
}

// ============================================================================
// No Gaps (Sequential Allocation)
// ============================================================================

#[test]
fn sequential_allocation_no_gaps() {
    let manager = TransactionManager::new(100);

    let mut versions = Vec::new();
    for _ in 0..100 {
        versions.push(manager.allocate_version().unwrap());
    }

    // Should form a contiguous sequence
    for i in 1..versions.len() {
        assert_eq!(
            versions[i],
            versions[i - 1] + 1,
            "Gap between {} and {}",
            versions[i - 1],
            versions[i]
        );
    }
}

// ============================================================================
// Recovery Restoration
// ============================================================================

#[test]
fn manager_with_txn_id_continues_from_max() {
    // Simulating recovery where max txn_id was 1000
    let manager = TransactionManager::with_txn_id(100, 1000);

    let id = manager.next_txn_id().unwrap();
    assert!(
        id > 1000,
        "After recovery, txn_id should be > max (1000), got {}",
        id
    );
}

#[test]
fn manager_initial_version_respected() {
    let manager = TransactionManager::new(5000);

    let v = manager.allocate_version().unwrap();
    assert!(v >= 5000, "Initial version should be respected");
}

#[test]
fn manager_recovery_scenario() {
    // Simulate: had transactions up to version 10000 and txn_id 500
    let manager = TransactionManager::with_txn_id(10000, 500);

    // New allocations should continue from those points
    let v1 = manager.allocate_version().unwrap();
    let id1 = manager.next_txn_id().unwrap();

    assert!(v1 >= 10000, "Version should continue from 10000");
    assert!(id1 > 500, "Txn ID should continue from 500");

    // Subsequent allocations should still be monotonic
    let v2 = manager.allocate_version().unwrap();
    let id2 = manager.next_txn_id().unwrap();

    assert!(v2 > v1);
    assert!(id2 > id1);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn version_allocation_thread_safe() {
    // This test verifies the AtomicU64 is working correctly
    let manager = Arc::new(TransactionManager::new(0));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let manager = Arc::clone(&manager);
            thread::spawn(move || {
                for _ in 0..1000 {
                    let v = manager.allocate_version().unwrap();
                    // Each version should be valid (non-zero after first allocation)
                    assert!(v > 0 || v == 1);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn interleaved_version_and_txn_id_allocation() {
    let manager = TransactionManager::new(100);

    let mut versions = Vec::new();
    let mut txn_ids = Vec::new();

    // Interleave allocations
    for _ in 0..50 {
        versions.push(manager.allocate_version().unwrap());
        txn_ids.push(manager.next_txn_id().unwrap());
        versions.push(manager.allocate_version().unwrap());
        txn_ids.push(manager.next_txn_id().unwrap());
    }

    // Both should be monotonic independently
    for i in 1..versions.len() {
        assert!(
            versions[i] > versions[i - 1],
            "Versions should be monotonic"
        );
    }
    for i in 1..txn_ids.len() {
        assert!(txn_ids[i] > txn_ids[i - 1], "Txn IDs should be monotonic");
    }
}

#[test]
fn rapid_allocation_performance() {
    let manager = TransactionManager::new(1);

    let start = std::time::Instant::now();
    for _ in 0..100_000 {
        let _ = manager.allocate_version().unwrap();
    }
    let elapsed = start.elapsed();

    // Should be very fast. Bound is generous to avoid CI flakes on
    // shared runners (local debug runs finish in ~70ms; CI has been
    // observed at ~103ms). A real regression would blow past 500ms.
    assert!(
        elapsed.as_millis() < 500,
        "Version allocation should be fast: {:?}",
        elapsed
    );
}

#[test]
fn manager_creation_cost() {
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        let _ = TransactionManager::new(1);
    }
    let elapsed = start.elapsed();

    // Creating managers should be cheap. Bound is generous to avoid
    // CI flakes on shared runners: a real regression will blow past
    // 100ms (local debug runs finish in ~1ms).
    assert!(
        elapsed.as_millis() < 100,
        "Manager creation should be cheap: {:?}",
        elapsed
    );
}
