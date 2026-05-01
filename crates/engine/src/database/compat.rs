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

use strata_storage::durability::wal::DurabilityMode;

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
    /// Storage codec name (e.g., "identity", "aes-gcm-256").
    ///
    /// Different from `codec_id` which is the wire format version.
    /// This is the encryption/encoding strategy from config.
    pub codec_name: String,
    /// Default branch name (if any).
    pub default_branch: Option<String>,
    /// Background worker thread count from `StorageConfig::background_threads`.
    ///
    /// Open-time-only (the thread pool is sized once at construction).
    /// Reuse with a different value returns `IncompatibleReuse`.
    pub background_threads: usize,
    /// Whether lossy recovery was permitted at open time.
    ///
    /// Open-time-only (recovery has already happened). Reuse with a different
    /// value returns `IncompatibleReuse` — a strict opener must not silently
    /// accept an instance that lossy-recovered, and vice versa.
    pub allow_lossy_recovery: bool,
    /// Fingerprint of runtime-identity/capability config.
    ///
    /// Currently computed from known `OpenSpec` fields.
    /// T5 replaces with ControlRegistry-derived fingerprint.
    pub open_config_fingerprint: u64,
}

impl CompatibilitySignature {
    /// Create a signature from an `OpenSpec` and resolved config.
    ///
    /// The `codec_name` parameter should be the storage codec from config
    /// (e.g., "identity", "aes-gcm-256"). `background_threads` and
    /// `allow_lossy_recovery` are pulled from `StrataConfig` and
    /// `StorageConfig` respectively — see
    /// `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`
    /// for why they are part of the reuse identity.
    pub fn from_spec(
        mode: DatabaseMode,
        subsystem_names: Vec<&'static str>,
        durability_mode: DurabilityMode,
        codec_name: String,
        default_branch: Option<String>,
        background_threads: usize,
        allow_lossy_recovery: bool,
    ) -> Self {
        let fingerprint = compute_fingerprint(
            &mode,
            &subsystem_names,
            &durability_mode,
            &codec_name,
            background_threads,
            allow_lossy_recovery,
        );

        Self {
            mode,
            subsystem_names: subsystem_names.into_iter().map(String::from).collect(),
            durability_mode,
            codec_id: CURRENT_CODEC_ID,
            codec_name,
            default_branch,
            background_threads,
            allow_lossy_recovery,
            open_config_fingerprint: fingerprint,
        }
    }

    /// Return an updated signature for a runtime durability-mode change.
    pub fn with_durability_mode(&self, durability_mode: DurabilityMode) -> Self {
        let mut signature = self.clone();
        signature.durability_mode = durability_mode;
        signature.open_config_fingerprint = compute_fingerprint(
            &signature.mode,
            &signature.subsystem_names,
            &signature.durability_mode,
            &signature.codec_name,
            signature.background_threads,
            signature.allow_lossy_recovery,
        );
        signature
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

        if self.codec_name != other.codec_name {
            return Err(IncompatibleReason::CodecNameMismatch {
                existing: self.codec_name.clone(),
                requested: other.codec_name.clone(),
            });
        }

        if self.default_branch != other.default_branch {
            return Err(IncompatibleReason::DefaultBranchMismatch {
                existing: self.default_branch.clone(),
                requested: other.default_branch.clone(),
            });
        }

        if self.background_threads != other.background_threads {
            return Err(IncompatibleReason::BackgroundThreadsMismatch {
                existing: self.background_threads,
                requested: other.background_threads,
            });
        }

        if self.allow_lossy_recovery != other.allow_lossy_recovery {
            return Err(IncompatibleReason::LossyRecoveryMismatch {
                existing: self.allow_lossy_recovery,
                requested: other.allow_lossy_recovery,
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
    /// Storage codec names don't match (e.g., "identity" vs "aes-gcm-256").
    CodecNameMismatch {
        /// The codec name of the existing database instance.
        existing: String,
        /// The codec name requested by the new open call.
        requested: String,
    },
    /// Default branch names don't match.
    DefaultBranchMismatch {
        /// The default branch of the existing database instance.
        existing: Option<String>,
        /// The default branch requested by the new open call.
        requested: Option<String>,
    },
    /// `StorageConfig::background_threads` values don't match.
    BackgroundThreadsMismatch {
        /// The background thread count of the existing database instance.
        existing: usize,
        /// The background thread count requested by the new open call.
        requested: usize,
    },
    /// `StrataConfig::allow_lossy_recovery` values don't match.
    LossyRecoveryMismatch {
        /// The lossy-recovery flag of the existing database instance.
        existing: bool,
        /// The lossy-recovery flag requested by the new open call.
        requested: bool,
    },
    /// Runtime config fingerprints don't match.
    ConfigFingerprintMismatch,
}

impl std::fmt::Display for IncompatibleReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ModeMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "mode mismatch: existing={}, requested={}",
                    existing, requested
                )
            }
            Self::SubsystemMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "subsystem mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::DurabilityMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "durability mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::CodecMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "codec mismatch: existing={}, requested={}",
                    existing, requested
                )
            }
            Self::CodecNameMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "storage codec mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::DefaultBranchMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "default branch mismatch: existing={:?}, requested={:?}",
                    existing, requested
                )
            }
            Self::BackgroundThreadsMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "background_threads mismatch: existing={existing}, requested={requested}"
                )
            }
            Self::LossyRecoveryMismatch {
                existing,
                requested,
            } => {
                write!(
                    f,
                    "allow_lossy_recovery mismatch: existing={existing}, requested={requested}"
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
/// sophisticated ControlRegistry-derived fingerprint. The explicit fields
/// above (`background_threads`, `allow_lossy_recovery`, etc.) are also
/// checked individually in `check_compatible`; the fingerprint provides
/// redundant detection plus a single place for future open-time-only
/// knobs to feed in.
fn compute_fingerprint<S: Hash>(
    mode: &DatabaseMode,
    subsystem_names: &[S],
    durability_mode: &DurabilityMode,
    codec_name: &str,
    background_threads: usize,
    allow_lossy_recovery: bool,
) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    mode.hash(&mut hasher);
    subsystem_names.hash(&mut hasher);
    codec_name.hash(&mut hasher);
    std::mem::discriminant(durability_mode).hash(&mut hasher);
    background_threads.hash(&mut hasher);
    allow_lossy_recovery.hash(&mut hasher);
    hasher.finish()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed open-time values used by most tests; exercising mismatches in
    /// these fields is the job of dedicated tests further down.
    const TEST_BG_THREADS: usize = 4;
    const TEST_LOSSY: bool = false;

    fn sig(
        mode: DatabaseMode,
        subsystems: Vec<&'static str>,
        durability: DurabilityMode,
        codec: &str,
        default_branch: Option<&str>,
    ) -> CompatibilitySignature {
        CompatibilitySignature::from_spec(
            mode,
            subsystems,
            durability,
            codec.to_string(),
            default_branch.map(String::from),
            TEST_BG_THREADS,
            TEST_LOSSY,
        )
    }

    #[test]
    fn test_signature_equality() {
        let sig1 = sig(
            DatabaseMode::Primary,
            vec!["graph", "vector", "search"],
            DurabilityMode::standard_default(),
            "identity",
            Some("main"),
        );
        let sig2 = sig(
            DatabaseMode::Primary,
            vec!["graph", "vector", "search"],
            DurabilityMode::standard_default(),
            "identity",
            Some("main"),
        );
        assert_eq!(sig1, sig2);
        assert!(sig1.check_compatible(&sig2).is_ok());
    }

    #[test]
    fn test_mode_mismatch() {
        let sig1 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        let sig2 = sig(
            DatabaseMode::Follower,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::ModeMismatch { .. })
        ));
    }

    #[test]
    fn test_subsystem_mismatch() {
        let sig1 = sig(
            DatabaseMode::Primary,
            vec!["graph", "vector"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        let sig2 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::SubsystemMismatch { .. })
        ));
    }

    #[test]
    fn test_durability_mismatch() {
        let sig1 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        let sig2 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::Always,
            "identity",
            None,
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::DurabilityMismatch { .. })
        ));
    }

    #[test]
    fn test_default_branch_mismatch() {
        let sig1 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            Some("main"),
        );
        let sig2 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            Some("develop"),
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::DefaultBranchMismatch { .. })
        ));
    }

    #[test]
    fn test_codec_name_mismatch() {
        let sig1 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        let sig2 = sig(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "aes-gcm-256",
            None,
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::CodecNameMismatch { .. })
        ));
    }

    #[test]
    fn test_background_threads_mismatch() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity".to_string(),
            None,
            4,
            false,
        );
        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity".to_string(),
            None,
            8,
            false,
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::BackgroundThreadsMismatch {
                existing: 4,
                requested: 8,
            })
        ));
    }

    #[test]
    fn test_allow_lossy_recovery_mismatch() {
        let sig1 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity".to_string(),
            None,
            4,
            false,
        );
        let sig2 = CompatibilitySignature::from_spec(
            DatabaseMode::Primary,
            vec!["graph"],
            DurabilityMode::standard_default(),
            "identity".to_string(),
            None,
            4,
            true,
        );
        assert!(matches!(
            sig1.check_compatible(&sig2),
            Err(IncompatibleReason::LossyRecoveryMismatch {
                existing: false,
                requested: true,
            })
        ));
    }

    #[test]
    fn test_fingerprint_display() {
        let sig = sig(
            DatabaseMode::Primary,
            vec!["graph", "vector"],
            DurabilityMode::standard_default(),
            "identity",
            None,
        );
        assert!(sig.open_config_fingerprint != 0);
    }
}
