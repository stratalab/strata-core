//! Memory pressure tracking for the segmented storage engine.
//!
//! `MemoryPressure` is a lightweight struct that maps a current byte count
//! to a `PressureLevel` (Normal / Warning / Critical). The caller (a future
//! background scheduler) polls `level()` and decides whether to flush or
//! compact. This keeps the storage crate free of threading concerns.

/// Memory pressure level.
///
/// Ordered: `Normal < Warning < Critical`, which allows comparison operators
/// (e.g., `level >= PressureLevel::Warning`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    /// Usage is below the warning threshold.
    Normal,
    /// Usage is between warning and critical thresholds.
    Warning,
    /// Usage is at or above the critical threshold (or over budget).
    Critical,
}

/// Tracks memory budget and threshold configuration.
///
/// Callers supply the current byte count; `level()` returns the corresponding
/// `PressureLevel`. When `budget == 0`, pressure is disabled (always `Normal`).
pub struct MemoryPressure {
    budget: u64,
    warning_threshold: f64,
    critical_threshold: f64,
}

impl MemoryPressure {
    /// Create a pressure tracker with the given budget and thresholds.
    ///
    /// # Thresholds
    /// - `warning_threshold`: fraction of budget at which `Warning` begins (e.g., 0.7)
    /// - `critical_threshold`: fraction of budget at which `Critical` begins (e.g., 0.9)
    pub fn new(budget: u64, warning_threshold: f64, critical_threshold: f64) -> Self {
        debug_assert!(
            warning_threshold <= critical_threshold,
            "warning_threshold ({}) must be <= critical_threshold ({})",
            warning_threshold,
            critical_threshold,
        );
        Self {
            budget,
            warning_threshold,
            critical_threshold,
        }
    }

    /// Create a disabled pressure tracker (always returns `Normal`).
    pub fn disabled() -> Self {
        Self {
            budget: 0,
            warning_threshold: 0.7,
            critical_threshold: 0.9,
        }
    }

    /// Evaluate the current pressure level.
    pub fn level(&self, current_bytes: u64) -> PressureLevel {
        if self.budget == 0 {
            return PressureLevel::Normal;
        }
        let ratio = current_bytes as f64 / self.budget as f64;
        if ratio >= self.critical_threshold {
            PressureLevel::Critical
        } else if ratio >= self.warning_threshold {
            PressureLevel::Warning
        } else {
            PressureLevel::Normal
        }
    }

    /// The configured memory budget in bytes.
    pub fn budget(&self) -> u64 {
        self.budget
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_normal_below_warning() {
        let p = MemoryPressure::new(100, 0.7, 0.9);
        assert_eq!(p.level(69), PressureLevel::Normal);
    }

    #[test]
    fn pressure_warning_at_threshold() {
        let p = MemoryPressure::new(100, 0.7, 0.9);
        assert_eq!(p.level(70), PressureLevel::Warning);
        assert_eq!(p.level(89), PressureLevel::Warning);
    }

    #[test]
    fn pressure_critical_at_threshold() {
        let p = MemoryPressure::new(100, 0.7, 0.9);
        assert_eq!(p.level(90), PressureLevel::Critical);
        assert_eq!(p.level(100), PressureLevel::Critical);
    }

    #[test]
    fn pressure_critical_over_budget() {
        let p = MemoryPressure::new(100, 0.7, 0.9);
        assert_eq!(p.level(110), PressureLevel::Critical);
    }

    #[test]
    fn pressure_disabled_always_normal() {
        let p = MemoryPressure::disabled();
        assert_eq!(p.level(0), PressureLevel::Normal);
        assert_eq!(p.level(u64::MAX), PressureLevel::Normal);
    }

    #[test]
    fn pressure_custom_thresholds() {
        let p = MemoryPressure::new(1000, 0.5, 0.8);
        assert_eq!(p.level(499), PressureLevel::Normal);
        assert_eq!(p.level(500), PressureLevel::Warning);
        assert_eq!(p.level(799), PressureLevel::Warning);
        assert_eq!(p.level(800), PressureLevel::Critical);
    }
}
