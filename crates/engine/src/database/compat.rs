//! Compatibility signature for database instance reuse checks.
//!
//! When the same database path is opened multiple times within a process,
//! we want to return the existing instance if the configuration is compatible.
//! `CompatibilitySignature` captures the key configuration that must match
//! for instance reuse.
//!
//! ## Reuse Rules
//!
//! | Field | Mismatch Behavior |
//! |-------|-------------------|
//! | mode | Reject (can't open as both primary and follower) |
//! | subsystem_names | Reject (hooks/observers would be missing) |
//! | durability_mode | Reject (different fsync behavior) |
//! | codec_id | Reject (wire format incompatibility) |
//! | default_branch | Reject (different branch semantics) |
//! | open_config_fingerprint | Reject (different runtime capabilities) |
//!
//! ## Future Evolution
//!
//! T5 (ProductOpenPlan) will replace `open_config_fingerprint` with a
//! ControlRegistry-derived fingerprint. The field exists now so later
//! product/retrieval config cannot bypass reuse checks.

use std::hash::{Hash, Hasher};

use strata_durability::wal::DurabilityMode;

use super::spec::DatabaseMode;

/// Current codec version for WAL, snapshots, and MANIFEST.
///
/// Bump this when wire format changes.
pub const CURRENT_CODEC_ID: u32 = 1;

/// Compatibility signature for database instance reuse.
///
/// Two signatures must match for the registry to return an existing
/// instance instead of opening a new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilitySignature {
    /// Database operating mode.
    pub mode: DatabaseMode,
    /// Ordered list of subsystem names.
    pub subsystem_names: Vec<String>,
    /// Durability mode.
    pub durability_mode: DurabilityMode,
    /// Codec version identifier.
    pub codec_id: u32,
    /// Default branch name (if any).
    pub default_branch: Option<String>,
    /// Fingerprint of runtime-identity/capability config.
    ///
    /// Currently computed from known `OpenSpec` fields.
    /// T5 replaces with ControlRegistry-derived fingerprint.
    pub open_config_fingerprint: u64,
}

impl CompatibilitySignature {
    /// Create a signature from an `OpenSpec`.
    pub fn from_spec(
        mode: DatabaseMode,
        subsystem_names: Vec<&'static str>,
        durability_mode: DurabilityMode,
        default_branch: Option<String>,
    ) -> Self {
        // Compute fingerprint from known fields
        let fingerprint = compute_fingerprint(&mode, &subsystem_names, &durability_mode);

        Self {
            mode,
            subsystem_names: subsystem_names.into_iter().map(String::from).collect(),
            durability_mode,
            codec_id: CURRENT_CODEC_ID,
            default_branch,
            open_config_fingerprint: fingerprint,
        }
    }

    /// Check if this signature is compatible with another for reuse.
    ///
    /// Returns `Ok(())` if compatible, `Err` with reason if not.
    pub fn check_compatible(&self, other: &Self) -> Result<(), IncompatibleReason> {
        if self.mode != other.mode {
            return Err(IncompatibleReason::ModeMismatch {
                existing: self.mode,
                requested: other.mode,
            });
        }

        if self.subsystem_names != other.subsystem_names {
            return Err(IncompatibleReason::SubsystemMismatch {
                existing: self.subsystem_names.clone(),
                requested: other.subsystem_names.clone(),
            });
        }

        if self.durability_mode != other.durability_mode {
            return Err(IncompatibleReason::DurabilityMismatch {
                existing: self.durability_mode,
                requested: other.durability_mode,
            });
        }

        if self.codec_id != other.codec_id {
            return Err(IncompatibleReason::CodecMismatch {
                existing: self.codec_id,
                requested: other.codec_id,
            });
        }

        if self.default_branch != other.default_branch {
            return Err(IncompatibleReason::DefaultBranchMismatch {
                existing: self.default_branch.clone(),
                requested: other.default_branch.clone(),
            });
        }

        if self.open_config_fingerprint != other.open_config_fingerprint {
            return Err(IncompatibleReason::ConfigFingerprintMismatch);
        }

        Ok(())
    }
}

/// Reason why two signatures are incompatible.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum IncompatibleReason {
    /// Database modes don't match.
    ModeMismatch {
        /// The mode of the existing database instance.
        existing: DatabaseMode,
        /// The mode requested by the new open call.
        requested: DatabaseMode,
    },
    /// Subsystem lists don't match.
    SubsystemMismatch {
        /// The subsystem names of the existing database instance.
        existing: Vec<String>,
        /// The subsystem names requested by the new open call.
        requested: Vec<String>,
    },
    /// Durability modes don't match.
    DurabilityMismatch {
        /// The durability mode of the existing database instance.
        existing: DurabilityMode,
        /// The durability mode requested by the new open call.
        requested: DurabilityMode,
    },
    /// Codec versions don't match.
    CodecMismatch {
        /// The codec version of the existing database instance.
        existing: u32,
        /// The codec version requested by the new open call.
        requested: u32,
    },
    /// Default branch names don't match.
    DefaultBranchMismatch {
        /// The default branch of the existing database instance.
        existing: Option<String>,
        /// The default branch requested by the new open call.
        requested: Option<String>,
    },
    /// Runtime config fingerprints don't match.
    ConfigFingerprintMismatch,
}

impl std::fmt::Display for IncompatibleReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ModeMismatch { existing, requested } => {
                write!(f, "mode mismatch: existing={}, requested={}", existing, requested)
            }
            Self::SubsystemMismatch { existing, requested } => {
                write!(
                    f,
                    "subsystem mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::DurabilityMismatch { existing, requested } => {
                write!(
                    f,
                    "durability mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::CodecMismatch { existing, requested } => {
                write!(f, "codec mismatch: existing={}, requested={}", existing, requested)
            }
            Self::DefaultBranchMismatch { existing, requested } => {
                write!(
                    f,
                    "default branch mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::ConfigFingerprintMismatch => {
                write!(f, "runtime config fingerprint mismatch")
            }
        }
    }
}

impl std::error::Error for IncompatibleReason {}

/// Compute a fingerprint from configuration fields.
///
/// Uses FNV-1a for simplicity. T5 will replace this with a more
/// sophisticated ControlRegistry-derived fingerprint.
fn compute_fingerprint(
    mode: &DatabaseMode,
    subsystem_names: &[&'static str],
    durability_mode: &DurabilityMode,
) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    mode.hash(&mut hasher);
    subsystem_names.hash(&mut hasher);
    std::mem::discriminant(durability_mode).hash(&mut hasher);
    hasher.finish()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_equality() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph", "vector", "search"],
            DurabilityMode::standard_default(),
            Some("main".to_string()),
        );

        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph", "vector", "search"],
            DurabilityMode::standard_default(),
            Some("main".to_string()),
        );

        assert_eq!(sig1, sig2);
        assert!(sig1.check_compatible(&sig2).is_ok());
    }

    #[test]
    fn test_mode_mismatch() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            None,
        );

        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Follower,
            vec!["graph"],
            DurabilityMode::standard_default(),
            None,
        );

        let result = sig1.check_compatible(&sig2);
        assert!(matches!(result, Err(IncompatibleReason::ModeMismatch { .. })));
    }

    #[test]
    fn test_subsystem_mismatch() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph", "vector"],
            DurabilityMode::standard_default(),
            None,
        );

        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            None,
        );

        let result = sig1.check_compatible(&sig2);
        assert!(matches!(result, Err(IncompatibleReason::SubsystemMismatch { .. })));
    }

    #[test]
    fn test_durability_mismatch() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            None,
        );

        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::Always,
            None,
        );

        let result = sig1.check_compatible(&sig2);
        assert!(matches!(result, Err(IncompatibleReason::DurabilityMismatch { .. })));
    }

    #[test]
    fn test_default_branch_mismatch() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            Some("main".to_string()),
        );

        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            Some("develop".to_string()),
        );

        let result = sig1.check_compatible(&sig2);
        assert!(matches!(result, Err(IncompatibleReason::DefaultBranchMismatch { .. })));
    }

    #[test]
    fn test_fingerprint_display() {
        let sig = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph", "vector"],
            DurabilityMode::standard_default(),
            None,
        );
        // Just verify it doesn't panic
        assert!(sig.open_config_fingerprint != 0);
    }
}
