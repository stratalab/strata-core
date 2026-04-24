//! Process-level metrics collected from /proc (Linux) for scaling benchmarks.
//!
//! On non-Linux platforms, all values are zero (graceful fallback).

/// Snapshot of process-level metrics at a point in time.
#[derive(Debug, Clone, Default)]
pub struct ProcessMetrics {
    /// User-mode CPU time in milliseconds (from /proc/self/stat field 14: utime)
    pub user_time_ms: u64,
    /// Kernel-mode CPU time in milliseconds (from /proc/self/stat field 15: stime)
    pub system_time_ms: u64,
    /// Voluntary context switches (from /proc/self/status)
    pub voluntary_ctx: u64,
    /// Involuntary context switches (from /proc/self/status)
    pub involuntary_ctx: u64,
}

/// Take a snapshot of current process metrics.
///
/// On Linux, reads `/proc/self/stat` and `/proc/self/status`.
/// On other platforms, returns zeroed metrics.
pub fn snapshot_process_metrics() -> ProcessMetrics {
    #[cfg(target_os = "linux")]
    {
        let (user_time_ms, system_time_ms) = read_proc_stat();
        let (voluntary_ctx, involuntary_ctx) = read_proc_status();
        ProcessMetrics {
            user_time_ms,
            system_time_ms,
            voluntary_ctx,
            involuntary_ctx,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        ProcessMetrics::default()
    }
}

/// Compute the delta between two snapshots.
pub fn delta_process_metrics(before: &ProcessMetrics, after: &ProcessMetrics) -> ProcessMetrics {
    ProcessMetrics {
        user_time_ms: after.user_time_ms.saturating_sub(before.user_time_ms),
        system_time_ms: after.system_time_ms.saturating_sub(before.system_time_ms),
        voluntary_ctx: after.voluntary_ctx.saturating_sub(before.voluntary_ctx),
        involuntary_ctx: after.involuntary_ctx.saturating_sub(before.involuntary_ctx),
    }
}

/// Parse /proc/self/stat for user and system CPU time.
///
/// Fields are space-separated. Field 14 (0-indexed 13) = utime, field 15 (0-indexed 14) = stime.
/// Values are in clock ticks; we convert to milliseconds using sysconf(_SC_CLK_TCK).
#[cfg(target_os = "linux")]
fn read_proc_stat() -> (u64, u64) {
    let Ok(contents) = std::fs::read_to_string("/proc/self/stat") else {
        return (0, 0);
    };

    // The comm field (field 2) is wrapped in parentheses and may contain spaces,
    // so we find the closing ')' and parse fields after it.
    let Some(close_paren) = contents.rfind(')') else {
        return (0, 0);
    };
    let rest = &contents[close_paren + 2..]; // skip ") "
    let fields: Vec<&str> = rest.split_whitespace().collect();

    // After the comm field, field indices shift:
    // field 3 (state) is fields[0], field 14 (utime) is fields[11], field 15 (stime) is fields[12]
    if fields.len() < 13 {
        return (0, 0);
    }

    let utime: u64 = fields[11].parse().unwrap_or(0);
    let stime: u64 = fields[12].parse().unwrap_or(0);

    let ticks_per_sec = ticks_per_second();
    let user_ms = utime * 1000 / ticks_per_sec;
    let sys_ms = stime * 1000 / ticks_per_sec;

    (user_ms, sys_ms)
}

/// Get clock ticks per second via libc sysconf.
#[cfg(target_os = "linux")]
fn ticks_per_second() -> u64 {
    // SAFETY: sysconf(_SC_CLK_TCK) is always safe to call.
    let ticks = unsafe { libc_sysconf_clk_tck() };
    if ticks > 0 {
        ticks as u64
    } else {
        100 // fallback (common default)
    }
}

/// Call sysconf(_SC_CLK_TCK) without depending on libc crate.
///
/// _SC_CLK_TCK = 2 on Linux (glibc and musl).
///
/// # Safety
///
/// Calls the C `sysconf` function with `_SC_CLK_TCK`, which is always safe.
#[cfg(target_os = "linux")]
unsafe fn libc_sysconf_clk_tck() -> i64 {
    const SC_CLK_TCK: i32 = 2;
    unsafe extern "C" {
        fn sysconf(name: i32) -> i64;
    }
    sysconf(SC_CLK_TCK)
}

/// Parse /proc/self/status for voluntary and involuntary context switches.
#[cfg(target_os = "linux")]
fn read_proc_status() -> (u64, u64) {
    let Ok(contents) = std::fs::read_to_string("/proc/self/status") else {
        return (0, 0);
    };

    let mut voluntary = 0u64;
    let mut involuntary = 0u64;

    for line in contents.lines() {
        if let Some(val) = line.strip_prefix("voluntary_ctxt_switches:") {
            voluntary = val.trim().parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("nonvoluntary_ctxt_switches:") {
            involuntary = val.trim().parse().unwrap_or(0);
        }
    }

    (voluntary, involuntary)
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{delta_process_metrics, snapshot_process_metrics, ProcessMetrics};

    #[test]
    fn test_snapshot_returns_something() {
        let m = snapshot_process_metrics();
        // On Linux, user_time_ms should be > 0 for any running process
        #[cfg(target_os = "linux")]
        assert!(
            m.user_time_ms > 0 || m.system_time_ms > 0,
            "Expected nonzero CPU time on Linux"
        );
        // On non-Linux, should be zeros
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(m.user_time_ms, 0);
            assert_eq!(m.system_time_ms, 0);
        }
        let _ = m; // suppress unused warning
    }

    #[test]
    fn test_delta_computation() {
        let before = ProcessMetrics {
            user_time_ms: 100,
            system_time_ms: 20,
            voluntary_ctx: 50,
            involuntary_ctx: 10,
        };
        let after = ProcessMetrics {
            user_time_ms: 250,
            system_time_ms: 35,
            voluntary_ctx: 120,
            involuntary_ctx: 25,
        };
        let d = delta_process_metrics(&before, &after);
        assert_eq!(d.user_time_ms, 150);
        assert_eq!(d.system_time_ms, 15);
        assert_eq!(d.voluntary_ctx, 70);
        assert_eq!(d.involuntary_ctx, 15);
    }

    #[test]
    fn test_delta_saturating() {
        let before = ProcessMetrics {
            user_time_ms: 100,
            ..Default::default()
        };
        let after = ProcessMetrics::default();
        let d = delta_process_metrics(&before, &after);
        assert_eq!(d.user_time_ms, 0); // saturating_sub
    }
}
