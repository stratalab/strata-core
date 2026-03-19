//! Token-bucket rate limiter for compaction I/O throttling.
//!
//! Tokens represent bytes. The bucket refills continuously at
//! `rate_bytes_per_sec` and is capped at one second of burst (i.e. the
//! bucket never exceeds `rate_bytes_per_sec` tokens). When a caller
//! acquires more bytes than available the bucket goes negative and the
//! caller sleeps for the deficit duration *outside* the lock.

use parking_lot::Mutex;
use std::time::Instant;

/// A token-bucket rate limiter that throttles I/O to a configurable bytes/sec ceiling.
pub struct RateLimiter {
    rate_bytes_per_sec: u64,
    state: Mutex<RateLimiterState>,
}

struct RateLimiterState {
    available_bytes: i64,
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with the given throughput ceiling.
    ///
    /// `rate_bytes_per_sec` must be > 0.
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        assert!(rate_bytes_per_sec > 0, "rate must be > 0");
        Self {
            rate_bytes_per_sec,
            state: Mutex::new(RateLimiterState {
                available_bytes: rate_bytes_per_sec as i64,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Return the configured rate in bytes per second.
    pub fn rate_bytes_per_sec(&self) -> u64 {
        self.rate_bytes_per_sec
    }

    /// Acquire `bytes` tokens. If the bucket is exhausted, the calling
    /// thread sleeps until enough tokens have been refilled.
    ///
    /// The lock is held only for the arithmetic — sleep happens outside
    /// the lock so other threads are not blocked.
    pub fn acquire(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let sleep_duration = {
            let mut state = self.state.lock();
            let now = Instant::now();
            let elapsed = now.duration_since(state.last_refill);
            let refill = (elapsed.as_secs_f64() * self.rate_bytes_per_sec as f64) as i64;
            if refill > 0 {
                state.available_bytes =
                    (state.available_bytes + refill).min(self.rate_bytes_per_sec as i64);
                state.last_refill = now;
            }
            state.available_bytes -= bytes as i64;
            if state.available_bytes < 0 {
                let deficit = (-state.available_bytes) as f64;
                Some(std::time::Duration::from_secs_f64(
                    deficit / self.rate_bytes_per_sec as f64,
                ))
            } else {
                None
            }
        }; // lock dropped

        if let Some(d) = sleep_duration {
            std::thread::sleep(d);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn acquire_within_budget_no_sleep() {
        let limiter = RateLimiter::new(1_000_000); // 1MB/s
        let start = Instant::now();
        limiter.acquire(100); // tiny request
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn acquire_over_budget_sleeps() {
        // 100KB/s — exhaust the budget then acquire more
        let limiter = RateLimiter::new(100_000);
        limiter.acquire(100_000);
        let start = Instant::now();
        limiter.acquire(50_000); // should sleep ~0.5s
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(400),
            "expected >=400ms sleep, got {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(1500),
            "expected <1500ms sleep, got {:?}",
            elapsed
        );
    }

    #[test]
    fn burst_cap_prevents_accumulation() {
        let limiter = RateLimiter::new(100_000); // 100KB/s, burst cap = 100KB

        // Drain the budget completely
        limiter.acquire(100_000);

        // Idle 3 seconds. Without burst cap, 300KB would accumulate.
        // With burst cap, only 100KB (1 second's worth) should be available.
        std::thread::sleep(Duration::from_secs(3));

        // Acquire 100KB — should succeed without sleeping (1s burst available)
        let start = Instant::now();
        limiter.acquire(100_000);
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "first acquire after idle should not sleep"
        );

        // Budget is now ~0. If burst cap was broken and 300KB had accumulated,
        // there would still be ~200KB left and this wouldn't sleep.
        // With correct cap (100KB), budget is ~0 and this should sleep ~500ms.
        let start2 = Instant::now();
        limiter.acquire(50_000);
        assert!(
            start2.elapsed() >= Duration::from_millis(400),
            "second acquire should sleep ~500ms, got {:?}",
            start2.elapsed()
        );
    }

    #[test]
    fn rate_bytes_per_sec_getter() {
        let limiter = RateLimiter::new(42);
        assert_eq!(limiter.rate_bytes_per_sec(), 42);
    }

    #[test]
    fn multiple_acquires_accumulate() {
        // 100KB/s — drain budget in small chunks
        let limiter = RateLimiter::new(100_000);
        for _ in 0..10 {
            limiter.acquire(10_000);
        }
        // Budget should be ~0 now, next acquire should sleep
        let start = Instant::now();
        limiter.acquire(50_000);
        assert!(
            start.elapsed() >= Duration::from_millis(400),
            "expected sleep after budget drained"
        );
    }

    #[test]
    fn acquire_zero_is_noop() {
        let limiter = RateLimiter::new(100);
        // Exhaust budget
        limiter.acquire(100);
        // acquire(0) should return immediately even though budget is exhausted
        let start = Instant::now();
        limiter.acquire(0);
        assert!(start.elapsed() < Duration::from_millis(10));
    }

    #[test]
    fn large_single_acquire_sleeps_proportionally() {
        // Acquire more than the burst cap in one shot — should sleep for the deficit.
        let limiter = RateLimiter::new(100_000); // 100KB/s, burst = 100KB
        let start = Instant::now();
        // Acquire 200KB: budget starts at 100KB, goes to -100KB, sleep = 1s
        limiter.acquire(200_000);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(800),
            "expected ~1s sleep for 200KB at 100KB/s, got {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(2000),
            "sleep too long: {:?}",
            elapsed
        );
    }

    #[test]
    fn concurrent_threads_share_limiter() {
        use std::sync::Arc;
        use std::thread;

        // 100KB/s — two threads each acquire 100KB, so the second must wait
        let limiter = Arc::new(RateLimiter::new(100_000));
        let l1 = Arc::clone(&limiter);
        let l2 = Arc::clone(&limiter);

        let start = Instant::now();
        let t1 = thread::spawn(move || l1.acquire(100_000));
        let t2 = thread::spawn(move || l2.acquire(100_000));
        t1.join().unwrap();
        t2.join().unwrap();
        let elapsed = start.elapsed();

        // One thread gets the full budget, the other must sleep ~1s.
        // Total wall-clock should be at least ~800ms (one thread sleeps).
        assert!(
            elapsed >= Duration::from_millis(700),
            "expected concurrent contention sleep, got {:?}",
            elapsed
        );
    }
}
