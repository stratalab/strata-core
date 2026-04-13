//! Contention profiling counters for diagnosing concurrent read bottlenecks.
//!
//! Three hypotheses tested:
//! - **H1**: DashMap `branches.get()` guard held too long during point lookups
//! - **H2**: Block cache write lock contention under high miss rates
//! - **H3**: NS_CACHE `entry()` acquiring write-capable guard on every read

use std::sync::atomic::{AtomicU64, Ordering};

// H1: DashMap branch guard hold time
pub static BRANCH_GUARD_HOLD_US: AtomicU64 = AtomicU64::new(0);
pub static BRANCH_GUARD_MAX_US: AtomicU64 = AtomicU64::new(0);
pub static BRANCH_GUARD_COUNT: AtomicU64 = AtomicU64::new(0);

// H3: Namespace cache hit/miss
pub static NS_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static NS_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

/// Print contention profile summary to stderr.
pub fn dump_contention_profile() {
    let guard_count = BRANCH_GUARD_COUNT.load(Ordering::Relaxed);
    let guard_total = BRANCH_GUARD_HOLD_US.load(Ordering::Relaxed);
    let guard_max = BRANCH_GUARD_MAX_US.load(Ordering::Relaxed);

    let cache = crate::block_cache::global_cache().stats();
    let cache_total = cache.hits + cache.misses;
    let cache_write_count =
        crate::block_cache::CACHE_WRITE_LOCK_COUNT.load(Ordering::Relaxed);
    let cache_write_wait_ns =
        crate::block_cache::CACHE_WRITE_LOCK_WAIT_NS.load(Ordering::Relaxed);
    let cache_write_hold_ns =
        crate::block_cache::CACHE_WRITE_LOCK_HOLD_NS.load(Ordering::Relaxed);

    let ns_hits = NS_CACHE_HITS.load(Ordering::Relaxed);
    let ns_misses = NS_CACHE_MISSES.load(Ordering::Relaxed);

    eprintln!("\n=== Contention Profile ===");

    eprintln!("\nH1 — DashMap branch guard (get_versioned_direct):");
    if guard_count > 0 {
        eprintln!("  lookups:    {}", guard_count);
        eprintln!("  total hold: {}ms", guard_total / 1000);
        eprintln!("  avg hold:   {}us", guard_total / guard_count);
        eprintln!("  max hold:   {}us", guard_max);
    } else {
        eprintln!("  (no data)");
    }

    eprintln!("\nH2 — Block cache:");
    if cache_total > 0 {
        eprintln!("  hits:       {} ({:.1}%)", cache.hits, cache.hits as f64 / cache_total as f64 * 100.0);
        eprintln!("  misses:     {} ({:.1}%)", cache.misses, cache.misses as f64 / cache_total as f64 * 100.0);
        eprintln!("  entries:    {} ({:.1} MiB / {:.1} MiB)", cache.entries, cache.size_bytes as f64 / 1048576.0, cache.capacity_bytes as f64 / 1048576.0);
    }
    if cache_write_count > 0 {
        eprintln!("  write locks:     {}", cache_write_count);
        eprintln!("  write lock wait: {}ms (avg {}us)", cache_write_wait_ns / 1_000_000, cache_write_wait_ns / cache_write_count / 1000);
        eprintln!("  write lock hold: {}ms (avg {}us)", cache_write_hold_ns / 1_000_000, cache_write_hold_ns / cache_write_count / 1000);
    }

    eprintln!("\nH3 — Namespace cache:");
    let ns_total = ns_hits + ns_misses;
    if ns_total > 0 {
        eprintln!("  hits:   {} ({:.1}%)", ns_hits, ns_hits as f64 / ns_total as f64 * 100.0);
        eprintln!("  misses: {}", ns_misses);
    } else {
        eprintln!("  (no data)");
    }

    eprintln!();
}
