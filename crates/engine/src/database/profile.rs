//! Hardware profile detection and config defaults.
//!
//! Runs at every Database open pathway to ensure resource defaults are
//! appropriate for the host. User-supplied values in StrataConfig are never
//! overridden — we only mutate fields that still hold their `Default` value.
//!
//! Classification:
//! - **Embedded** (< 1 GiB RAM): Pi Zero, small IoT devices. Aggressive sizing caps.
//! - **Desktop** (1–16 GiB RAM): laptops, developer workstations. Mostly defaults.
//! - **Server** (> 16 GiB RAM): production deployments. Larger buffers and parallelism.

use crate::database::config::{StorageConfig, StrataConfig};
use std::sync::OnceLock;

/// Detected host hardware. Memoized after first detection.
#[derive(Debug, Clone, Copy)]
pub struct HardwareInfo {
    /// Total system RAM in bytes.
    pub ram_bytes: u64,
    /// Number of CPU cores (from `available_parallelism`).
    pub cores: usize,
}

/// Hardware-derived sizing profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Resource-constrained: Pi Zero, IoT, edge devices (< 1 GiB RAM).
    Embedded,
    /// Typical development machine (1–16 GiB RAM).
    Desktop,
    /// Production server (> 16 GiB RAM).
    Server,
}

impl std::fmt::Display for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Profile::Embedded => write!(f, "embedded"),
            Profile::Desktop => write!(f, "desktop"),
            Profile::Server => write!(f, "server"),
        }
    }
}

impl Profile {
    /// Classify a `HardwareInfo` into a `Profile` based on total RAM.
    pub fn classify(hw: HardwareInfo) -> Profile {
        let gb = hw.ram_bytes / (1024 * 1024 * 1024);
        if gb < 1 {
            Profile::Embedded
        } else if gb <= 16 {
            Profile::Desktop
        } else {
            Profile::Server
        }
    }
}

static CACHED_HW: OnceLock<HardwareInfo> = OnceLock::new();

/// Detect host hardware. Memoized for the process lifetime — hardware doesn't
/// change at runtime, so we avoid repeated `/proc/meminfo` reads in hot paths.
pub fn detect_hardware() -> HardwareInfo {
    *CACHED_HW.get_or_init(|| HardwareInfo {
        ram_bytes: detect_ram_bytes(),
        cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    })
}

#[cfg(target_os = "linux")]
fn detect_ram_bytes() -> u64 {
    if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                if let Some(kb_str) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        return kb * 1024;
                    }
                }
            }
        }
    }
    // Fallback: 4 GiB — better than 0 for default classification as Desktop.
    4 * 1024 * 1024 * 1024
}

#[cfg(target_os = "macos")]
fn detect_ram_bytes() -> u64 {
    // libc::sysctlbyname("hw.memsize", ...) — direct syscall, no shell-out.
    unsafe {
        let mut size: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        let name = b"hw.memsize\0";
        if libc::sysctlbyname(
            name.as_ptr() as *const libc::c_char,
            &mut size as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        ) == 0
        {
            return size;
        }
    }
    4 * 1024 * 1024 * 1024
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_ram_bytes() -> u64 {
    // Windows and other platforms: assume 4 GiB (Desktop default).
    4 * 1024 * 1024 * 1024
}

/// Apply hardware profile defaults to `cfg`, but only for fields that still
/// hold their `StorageConfig::default()` value. User-set fields are preserved.
///
/// Returns the profile that was applied (for logging/test assertions).
pub fn apply_hardware_profile_if_defaults(cfg: &mut StrataConfig) -> Profile {
    let hw = detect_hardware();
    let profile = Profile::classify(hw);
    apply_profile_if_defaults(cfg, profile, hw);
    profile
}

/// Apply a specific profile to `cfg`. Used by the CLI wizard for explicit
/// profile pinning.
pub fn apply_profile_if_defaults(cfg: &mut StrataConfig, profile: Profile, hw: HardwareInfo) {
    let baseline = StorageConfig::default();
    let s = &mut cfg.storage;
    let memory_budget_set = s.memory_budget > 0;
    let default_vector_dtype = StrataConfig::default().default_vector_dtype;

    match profile {
        Profile::Embedded => {
            if !memory_budget_set {
                if s.write_buffer_size == baseline.write_buffer_size {
                    s.write_buffer_size = 16 * 1024 * 1024;
                }
                if s.block_cache_size == baseline.block_cache_size {
                    s.block_cache_size = ((hw.ram_bytes / 8) as usize).min(64 * 1024 * 1024);
                }
            }
            if s.background_threads == baseline.background_threads {
                s.background_threads = 1;
            }
            if s.target_file_size == baseline.target_file_size {
                s.target_file_size = 4 * 1024 * 1024;
            }
            if s.level_base_bytes == baseline.level_base_bytes {
                s.level_base_bytes = 32 * 1024 * 1024;
            }
            if s.compaction_rate_limit == baseline.compaction_rate_limit {
                s.compaction_rate_limit = 5 * 1024 * 1024;
            }
            if cfg.default_vector_dtype == default_vector_dtype {
                cfg.default_vector_dtype = "binary".to_string();
            }
        }
        Profile::Desktop => {
            if s.background_threads == baseline.background_threads {
                s.background_threads = hw.cores.min(4);
            }
            // Other fields: keep stock defaults (matches pre-profile behavior).
        }
        Profile::Server => {
            if !memory_budget_set && s.write_buffer_size == baseline.write_buffer_size {
                s.write_buffer_size = 256 * 1024 * 1024;
            }
            if s.background_threads == baseline.background_threads {
                s.background_threads = hw.cores.min(8);
            }
            if s.target_file_size == baseline.target_file_size {
                s.target_file_size = 128 * 1024 * 1024;
            }
            if s.level_base_bytes == baseline.level_base_bytes {
                s.level_base_bytes = 512 * 1024 * 1024;
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn pi_zero() -> HardwareInfo {
        HardwareInfo {
            ram_bytes: 512 * 1024 * 1024,
            cores: 1,
        }
    }

    fn laptop_8gb() -> HardwareInfo {
        HardwareInfo {
            ram_bytes: 8 * 1024 * 1024 * 1024,
            cores: 8,
        }
    }

    fn server_64gb() -> HardwareInfo {
        HardwareInfo {
            ram_bytes: 64 * 1024 * 1024 * 1024,
            cores: 32,
        }
    }

    #[test]
    fn classify_512mb_is_embedded() {
        assert_eq!(Profile::classify(pi_zero()), Profile::Embedded);
    }

    #[test]
    fn classify_8gb_is_desktop() {
        assert_eq!(Profile::classify(laptop_8gb()), Profile::Desktop);
    }

    #[test]
    fn classify_64gb_is_server() {
        assert_eq!(Profile::classify(server_64gb()), Profile::Server);
    }

    #[test]
    fn classify_boundary_16gb_is_desktop() {
        let hw = HardwareInfo {
            ram_bytes: 16 * 1024 * 1024 * 1024,
            cores: 8,
        };
        assert_eq!(Profile::classify(hw), Profile::Desktop);
    }

    #[test]
    fn classify_boundary_17gb_is_server() {
        let hw = HardwareInfo {
            ram_bytes: 17 * 1024 * 1024 * 1024,
            cores: 8,
        };
        assert_eq!(Profile::classify(hw), Profile::Server);
    }

    #[test]
    fn pi_zero_embedded_is_pi_safe() {
        let mut cfg = StrataConfig::default();
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());

        // Hard constraints: nothing should exceed Pi Zero capacity.
        assert_eq!(cfg.storage.write_buffer_size, 16 * 1024 * 1024);
        assert_eq!(cfg.storage.block_cache_size, 64 * 1024 * 1024); // ram/8 = 64 MB (capped)
        assert_eq!(cfg.storage.background_threads, 1);
        assert_eq!(cfg.storage.target_file_size, 4 * 1024 * 1024);
        assert_eq!(cfg.storage.level_base_bytes, 32 * 1024 * 1024);
        assert_eq!(cfg.storage.compaction_rate_limit, 5 * 1024 * 1024);
        assert_eq!(cfg.default_vector_dtype, "binary");
    }

    #[test]
    fn pi_256mb_block_cache_scales_with_ram() {
        let hw = HardwareInfo {
            ram_bytes: 256 * 1024 * 1024,
            cores: 1,
        };
        let mut cfg = StrataConfig::default();
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, hw);
        // 256 MB / 8 = 32 MB, below the 64 MB cap
        assert_eq!(cfg.storage.block_cache_size, 32 * 1024 * 1024);
    }

    #[test]
    fn user_explicit_block_cache_not_clobbered() {
        let mut cfg = StrataConfig::default();
        cfg.storage.block_cache_size = 128 * 1024 * 1024;
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());
        assert_eq!(cfg.storage.block_cache_size, 128 * 1024 * 1024);
    }

    #[test]
    fn user_explicit_write_buffer_not_clobbered() {
        let mut cfg = StrataConfig::default();
        cfg.storage.write_buffer_size = 200 * 1024 * 1024;
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());
        assert_eq!(cfg.storage.write_buffer_size, 200 * 1024 * 1024);
    }

    #[test]
    fn user_explicit_vector_dtype_not_clobbered() {
        let mut cfg = StrataConfig::default();
        cfg.default_vector_dtype = "int8".to_string();
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());
        assert_eq!(cfg.default_vector_dtype, "int8");
    }

    #[test]
    fn memory_budget_guards_cache_and_buffer() {
        let mut cfg = StrataConfig::default();
        cfg.storage.memory_budget = 32 * 1024 * 1024;
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());
        // With memory_budget set, block_cache_size and write_buffer_size are
        // derived via effective_*() — profile should NOT override them.
        assert_eq!(cfg.storage.block_cache_size, 0);
        assert_eq!(
            cfg.storage.write_buffer_size,
            StorageConfig::default().write_buffer_size
        );
        // But other fields still get the Embedded profile treatment.
        assert_eq!(cfg.storage.background_threads, 1);
        assert_eq!(cfg.storage.target_file_size, 4 * 1024 * 1024);
    }

    #[test]
    fn idempotent_double_apply() {
        let mut cfg = StrataConfig::default();
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());
        let first_snapshot = cfg.clone();
        apply_profile_if_defaults(&mut cfg, Profile::Embedded, pi_zero());
        // After first apply, fields no longer equal defaults → second apply is a no-op.
        assert_eq!(
            cfg.storage.write_buffer_size,
            first_snapshot.storage.write_buffer_size
        );
        assert_eq!(
            cfg.storage.block_cache_size,
            first_snapshot.storage.block_cache_size
        );
        assert_eq!(
            cfg.storage.background_threads,
            first_snapshot.storage.background_threads
        );
        assert_eq!(
            cfg.storage.target_file_size,
            first_snapshot.storage.target_file_size
        );
        assert_eq!(
            cfg.storage.level_base_bytes,
            first_snapshot.storage.level_base_bytes
        );
        assert_eq!(
            cfg.default_vector_dtype,
            first_snapshot.default_vector_dtype
        );
    }

    #[test]
    fn server_large_buffers() {
        let mut cfg = StrataConfig::default();
        apply_profile_if_defaults(&mut cfg, Profile::Server, server_64gb());
        assert_eq!(cfg.storage.write_buffer_size, 256 * 1024 * 1024);
        assert_eq!(cfg.storage.background_threads, 8);
        assert_eq!(cfg.storage.target_file_size, 128 * 1024 * 1024);
        assert_eq!(cfg.storage.level_base_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn desktop_is_mostly_defaults() {
        let mut cfg = StrataConfig::default();
        apply_profile_if_defaults(&mut cfg, Profile::Desktop, laptop_8gb());
        // Desktop only touches background_threads (to min(cores, 4)).
        assert_eq!(cfg.storage.background_threads, 4);
        // Other fields remain at default.
        assert_eq!(
            cfg.storage.write_buffer_size,
            StorageConfig::default().write_buffer_size
        );
        assert_eq!(
            cfg.storage.block_cache_size,
            StorageConfig::default().block_cache_size
        );
    }

    #[test]
    fn detect_hardware_returns_positive() {
        let hw = detect_hardware();
        assert!(hw.ram_bytes > 0);
        assert!(hw.cores > 0);
    }

    #[test]
    fn detect_hardware_is_memoized() {
        let a = detect_hardware();
        let b = detect_hardware();
        assert_eq!(a.ram_bytes, b.ram_bytes);
        assert_eq!(a.cores, b.cores);
    }
}
