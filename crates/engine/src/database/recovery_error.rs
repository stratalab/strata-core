//! Typed recovery-error vocabulary for the engine (Epic D3).
//!
//! `RecoveryError` is the engine-level error emitted by
//! `Database::run_recovery` (see [`super::recovery`]). It replaces the
//! ad-hoc `StrataError::{internal,corruption,storage}(format!(...))`
//! wraps that were previously scattered across `open.rs`.
//!
//! D3's load-bearing requirement is that snapshot and WAL recovery
//! failures survive the coordinator seam in typed form. The engine still
//! converts `RecoveryError` back into `StrataError` at the public open
//! entry points, but `run_recovery()` itself now exposes the structured
//! taxonomy that D4's policy work will build on.

use std::io;
use std::path::PathBuf;

use strata_concurrency::CoordinatorRecoveryError;
use strata_core::id::TxnId;
use strata_core::StrataError;
use strata_durability::wal::WalReaderError;
use strata_durability::{ManifestError, SnapshotReadError};
use strata_storage::{RecoveryHealth, StorageError};
use thiserror::Error;

/// Which open path is emitting a recovery error.
///
/// Public because it appears in public [`RecoveryError`] variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorRole {
    /// Primary (exclusive, read-write) open.
    Primary,
    /// Follower (shared, read-only) open.
    Follower,
}

impl ErrorRole {
    fn is_follower(self) -> bool {
        matches!(self, ErrorRole::Follower)
    }
}

/// Typed engine-level recovery errors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RecoveryError {
    /// A non-legacy [`ManifestError`] observed directly during the engine's
    /// MANIFEST load step.
    #[error("failed to load MANIFEST at {path}: {inner}")]
    ManifestLoad {
        /// Path of the MANIFEST file that failed to parse.
        path: PathBuf,
        /// Which open path observed the failure.
        role: ErrorRole,
        /// The underlying manifest-format error.
        #[source]
        inner: ManifestError,
    },

    /// [`ManifestError::LegacyFormat`] extracted structurally so
    /// `StrataError::LegacyFormat` can be reconstructed exactly.
    #[error("MANIFEST legacy format (version {found_version}): {hint}")]
    ManifestLegacyFormat {
        /// Version number recorded on disk.
        found_version: u32,
        /// Supported-range + remediation text shown to operators.
        hint: String,
    },

    /// The stored codec id in the MANIFEST does not match the configured
    /// codec id.
    #[error("codec mismatch at {db_path}: stored='{stored}', configured='{configured}'")]
    ManifestCodecMismatch {
        /// Codec id recorded in the on-disk MANIFEST.
        stored: String,
        /// Codec id supplied in the runtime configuration.
        configured: String,
        /// Database directory path.
        db_path: PathBuf,
        /// Which open path observed the mismatch.
        role: ErrorRole,
    },

    /// `ManifestManager::create` failed during first-open MANIFEST creation.
    #[error("failed to create MANIFEST: {0}")]
    ManifestCreate(#[source] ManifestError),

    /// `strata_durability::get_codec(id)` returned an error.
    #[error("could not initialize storage codec '{codec_id}': {detail}")]
    CodecInit {
        /// Codec id that could not be resolved.
        codec_id: String,
        /// Display text of the underlying `get_codec` error.
        detail: String,
    },

    /// Hard failure from the coordinator's `plan_recovery()` step.
    ///
    /// This is the coordinator's second MANIFEST read while it resolves
    /// snapshot state. The engine preserves the original `StrataError`
    /// verbatim because the lossy WAL fallback cannot heal a planning
    /// failure.
    #[error(transparent)]
    CoordinatorPlan(StrataError),

    /// MANIFEST referenced a snapshot file that was absent on disk.
    #[error("MANIFEST references snapshot {snapshot_id} but {path} is missing")]
    SnapshotMissing {
        /// Which open path observed the failure.
        role: ErrorRole,
        /// Snapshot id recorded in the MANIFEST.
        snapshot_id: u64,
        /// Snapshot path that was missing.
        path: PathBuf,
    },

    /// Snapshot load failed after the MANIFEST pointed at the file.
    #[error("failed to load snapshot {snapshot_id} at {path}: {inner}")]
    SnapshotRead {
        /// Which open path observed the failure.
        role: ErrorRole,
        /// Snapshot id recorded in the MANIFEST.
        snapshot_id: u64,
        /// Snapshot file path.
        path: PathBuf,
        /// Underlying typed snapshot read error.
        #[source]
        inner: SnapshotReadError,
    },

    /// Legacy WAL segment format — surfaced directly from the typed
    /// coordinator seam.
    #[error("legacy WAL format (version {found_version}): {hint}")]
    WalLegacyFormat {
        /// Version number read from disk.
        found_version: u32,
        /// Remediation hint text.
        hint: String,
    },

    /// Codec decode failure while reading a WAL record.
    #[error("WAL codec decode failure at byte offset {offset}: {detail}")]
    WalCodecDecode {
        /// Byte offset within the segment where decode failed.
        offset: u64,
        /// Decode error detail.
        detail: String,
    },

    /// Mid-segment checksum corruption while reading WAL bytes.
    #[error("WAL checksum mismatch at byte offset {offset}")]
    WalChecksum {
        /// Which open path observed the failure.
        role: ErrorRole,
        /// Byte offset where corruption was detected.
        offset: u64,
        /// Number of valid records before the corrupted region.
        records_before: usize,
    },

    /// Other typed WAL reader failure.
    #[error("WAL read failed: {inner}")]
    WalRead {
        /// Which open path observed the failure.
        role: ErrorRole,
        /// Underlying WAL reader error.
        #[source]
        inner: WalReaderError,
    },

    /// Transaction payload bytes inside a WAL record were invalid.
    #[error("Failed to decode transaction payload for txn {txn_id}: {detail}")]
    PayloadDecode {
        /// Which open path observed the failure.
        role: ErrorRole,
        /// Transaction id of the record whose payload failed to decode.
        txn_id: TxnId,
        /// Decoder error detail.
        detail: String,
    },

    /// Fallback bucket for callback or future coordinator failures that do not
    /// map to one of the typed recovery variants above.
    #[error("WAL recovery failed: {inner}")]
    WalRecoveryFailed {
        /// Which open path observed the failure.
        role: ErrorRole,
        /// Opaque callback/plan error from the coordinator path.
        #[source]
        inner: StrataError,
    },

    /// [`SegmentedStore::recover_segments`] returned a classified
    /// `RecoveryHealth::Degraded` outcome carried as-is.
    #[error("storage recovered in degraded state")]
    StorageDegraded(RecoveryHealth),

    /// [`SegmentedStore::recover_segments`] returned `Err(StorageError)`.
    #[error("Segment recovery failed: {0}")]
    Storage(#[source] StorageError),

    /// I/O error observed inside `run_recovery` outside of the paths above.
    #[error("recovery I/O error: {0}")]
    Io(#[source] io::Error),
}

impl From<RecoveryError> for StrataError {
    #[allow(clippy::too_many_lines)] // sequential variant mapping; splitting scatters message shape.
    fn from(value: RecoveryError) -> Self {
        match value {
            RecoveryError::ManifestLegacyFormat {
                found_version,
                hint,
            }
            | RecoveryError::WalLegacyFormat {
                found_version,
                hint,
            } => StrataError::legacy_format(found_version, hint),
            RecoveryError::WalCodecDecode { detail, .. } => StrataError::codec_decode(detail),
            RecoveryError::CoordinatorPlan(inner) => inner,
            RecoveryError::ManifestCodecMismatch {
                stored,
                configured,
                db_path,
                role,
            } => {
                let message = if role.is_follower() {
                    format!(
                        "codec mismatch: follower target at {} was created with '{}' but config specifies '{}'. \
                         A follower must be configured with the same codec as the primary database.",
                        db_path.display(),
                        stored,
                        configured,
                    )
                } else {
                    format!(
                        "codec mismatch: database at {} was created with '{}' but config specifies '{}'. \
                         A database cannot be reopened with a different codec.",
                        db_path.display(),
                        stored,
                        configured,
                    )
                };
                StrataError::incompatible_reuse(message)
            }
            RecoveryError::ManifestLoad { path, role, inner } => {
                let message = if role.is_follower() {
                    format!(
                        "follower could not load MANIFEST at {}: {inner}",
                        path.display()
                    )
                } else {
                    format!("failed to load MANIFEST: {inner}")
                };
                StrataError::corruption(message)
            }
            RecoveryError::ManifestCreate(inner) => {
                StrataError::internal(format!("failed to create MANIFEST: {inner}"))
            }
            RecoveryError::CodecInit { codec_id, detail } => StrataError::internal(format!(
                "could not initialize storage codec '{codec_id}': {detail}"
            )),
            RecoveryError::SnapshotMissing {
                role,
                snapshot_id,
                path,
            } => StrataError::corruption(strict_recovery_message(
                role,
                &format!(
                    "MANIFEST references snapshot {snapshot_id} but {} is missing",
                    path.display()
                ),
            )),
            RecoveryError::SnapshotRead {
                role,
                snapshot_id,
                path,
                inner,
            } => match inner {
                SnapshotReadError::LegacyFormat {
                    detected_version,
                    supported_range,
                    remediation,
                } => StrataError::legacy_format(
                    detected_version,
                    format!("{supported_range}. {remediation}"),
                ),
                other => StrataError::corruption(strict_recovery_message(
                    role,
                    &format!(
                        "failed to load snapshot {snapshot_id} at {}: {other}",
                        path.display(),
                    ),
                )),
            },
            RecoveryError::WalChecksum {
                role,
                offset,
                records_before,
            } => StrataError::corruption(strict_recovery_message(
                role,
                &format!(
                    "WAL read failed: Corrupted WAL segment at byte offset {offset} ({records_before} valid records before corruption)"
                ),
            )),
            RecoveryError::WalRead { role, inner } => StrataError::corruption(
                strict_recovery_message(role, &format!("WAL read failed: {inner}")),
            ),
            RecoveryError::PayloadDecode {
                role,
                txn_id,
                detail,
            } => StrataError::corruption(strict_recovery_message(
                role,
                &format!("Failed to decode transaction payload for txn {txn_id}: {detail}"),
            )),
            RecoveryError::WalRecoveryFailed { role, inner } => {
                StrataError::corruption(strict_recovery_message(role, &inner.to_string()))
            }
            RecoveryError::StorageDegraded(health) => StrataError::corruption(format!(
                "storage recovered in degraded state: {health:?}"
            )),
            RecoveryError::Storage(inner) => {
                StrataError::corruption(format!("Segment recovery failed: {inner}"))
            }
            RecoveryError::Io(inner) => StrataError::from(inner),
        }
    }
}

impl From<io::Error> for RecoveryError {
    fn from(value: io::Error) -> Self {
        RecoveryError::Io(value)
    }
}

impl From<StorageError> for RecoveryError {
    fn from(value: StorageError) -> Self {
        RecoveryError::Storage(value)
    }
}

fn strict_recovery_message(role: ErrorRole, detail: &str) -> String {
    if role.is_follower() {
        format!(
            "WAL recovery failed in follower mode: {detail}. \
             Set allow_lossy_recovery=true to force open."
        )
    } else {
        format!(
            "WAL recovery failed: {detail}. \
             Set allow_lossy_recovery=true to force open with data loss."
        )
    }
}

/// Classify a `ManifestError` observed during MANIFEST load into the right
/// `RecoveryError` variant, carrying the role + path that the operator-UX
/// messages need.
pub(crate) fn classify_manifest_load_error(
    path: PathBuf,
    role: ErrorRole,
    err: ManifestError,
) -> RecoveryError {
    match err {
        ManifestError::LegacyFormat {
            detected_version,
            supported_range,
            remediation,
        } => RecoveryError::ManifestLegacyFormat {
            found_version: detected_version,
            hint: format!("{supported_range}. {remediation}"),
        },
        inner => RecoveryError::ManifestLoad { path, role, inner },
    }
}

/// Classify a typed coordinator failure into the engine's public
/// `RecoveryError` taxonomy.
pub(crate) fn from_coordinator_error(
    role: ErrorRole,
    err: CoordinatorRecoveryError,
) -> RecoveryError {
    match err {
        CoordinatorRecoveryError::Plan(inner) => RecoveryError::CoordinatorPlan(inner),
        CoordinatorRecoveryError::SnapshotMissing { snapshot_id, path } => {
            RecoveryError::SnapshotMissing {
                role,
                snapshot_id,
                path,
            }
        }
        CoordinatorRecoveryError::SnapshotRead {
            snapshot_id,
            path,
            source,
        } => RecoveryError::SnapshotRead {
            role,
            snapshot_id,
            path,
            inner: source,
        },
        CoordinatorRecoveryError::WalRead(WalReaderError::LegacyFormat {
            found_version,
            hint,
        }) => RecoveryError::WalLegacyFormat {
            found_version,
            hint,
        },
        CoordinatorRecoveryError::WalRead(WalReaderError::CodecDecode { offset, detail }) => {
            RecoveryError::WalCodecDecode { offset, detail }
        }
        CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment {
            offset,
            records_before,
        }) => RecoveryError::WalChecksum {
            role,
            offset: offset as u64,
            records_before,
        },
        CoordinatorRecoveryError::WalRead(inner) => RecoveryError::WalRead { role, inner },
        CoordinatorRecoveryError::PayloadDecode { txn_id, detail } => {
            RecoveryError::PayloadDecode {
                role,
                txn_id,
                detail,
            }
        }
        CoordinatorRecoveryError::Callback(inner) => {
            RecoveryError::WalRecoveryFailed { role, inner }
        }
        _ => RecoveryError::WalRecoveryFailed {
            role,
            inner: StrataError::internal("unknown coordinator recovery error"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_legacy_roundtrips_to_legacy_format() {
        let err = RecoveryError::ManifestLegacyFormat {
            found_version: 1,
            hint: "supported range v2-v2. wipe wal/".into(),
        };
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::LegacyFormat { .. }));
    }

    #[test]
    fn wal_codec_decode_roundtrips_to_codec_decode() {
        let err = RecoveryError::WalCodecDecode {
            offset: 42,
            detail: "auth tag mismatch".into(),
        };
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::CodecDecode { .. }));
    }

    #[test]
    fn codec_mismatch_roundtrips_to_incompatible_reuse() {
        let err = RecoveryError::ManifestCodecMismatch {
            stored: "aes-gcm-256".into(),
            configured: "identity".into(),
            db_path: PathBuf::from("/tmp/db"),
            role: ErrorRole::Primary,
        };
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::IncompatibleReuse { .. }));
    }

    #[test]
    fn follower_codec_mismatch_message_names_follower() {
        let err = RecoveryError::ManifestCodecMismatch {
            stored: "identity".into(),
            configured: "aes-gcm-256".into(),
            db_path: PathBuf::from("/tmp/follower-db"),
            role: ErrorRole::Follower,
        };
        let msg = StrataError::from(err).to_string();
        assert!(msg.contains("codec mismatch"), "message: {msg}");
        assert!(msg.contains("follower"), "message: {msg}");
    }

    #[test]
    fn follower_manifest_load_message_names_path() {
        let err = RecoveryError::ManifestLoad {
            path: PathBuf::from("/tmp/db/MANIFEST"),
            role: ErrorRole::Follower,
            inner: ManifestError::InvalidMagic,
        };
        let msg = StrataError::from(err).to_string();
        assert!(
            msg.contains("follower could not load MANIFEST"),
            "message: {msg}"
        );
        assert!(msg.contains("/tmp/db/MANIFEST"), "message: {msg}");
    }

    #[test]
    fn strict_wal_recovery_failed_message_hints_lossy() {
        let err = RecoveryError::WalRecoveryFailed {
            role: ErrorRole::Primary,
            inner: StrataError::storage("WAL read failed: bad checksum"),
        };
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::Corruption { .. }));
        let msg = strata.to_string();
        assert!(msg.contains("WAL recovery failed"), "message: {msg}");
        assert!(msg.contains("allow_lossy_recovery"), "message: {msg}");
    }

    #[test]
    fn storage_error_roundtrips_to_corruption() {
        let err = RecoveryError::Storage(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "segments dir missing",
        )));
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::Corruption { .. }));
        let msg = strata.to_string();
        assert!(msg.contains("Segment recovery failed"), "message: {msg}");
    }

    #[test]
    fn snapshot_legacy_roundtrips_to_legacy_format() {
        let err = RecoveryError::SnapshotRead {
            role: ErrorRole::Primary,
            snapshot_id: 7,
            path: PathBuf::from("/tmp/db/snapshots/snap-7.chk"),
            inner: SnapshotReadError::LegacyFormat {
                detected_version: 1,
                supported_range: "this build requires snapshot format version 2".into(),
                remediation: "delete the offending snapshot".into(),
            },
        };
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::LegacyFormat { .. }));
    }

    #[test]
    fn coordinator_plan_roundtrips_to_original_variant() {
        let err = from_coordinator_error(
            ErrorRole::Follower,
            CoordinatorRecoveryError::Plan(StrataError::incompatible_reuse(
                "codec mismatch while re-reading MANIFEST",
            )),
        );
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::IncompatibleReuse { .. }));
    }

    #[test]
    fn coordinator_plan_legacy_roundtrips_to_legacy_format() {
        let err = from_coordinator_error(
            ErrorRole::Primary,
            CoordinatorRecoveryError::Plan(StrataError::legacy_format(
                1,
                "this build requires MANIFEST format version 2",
            )),
        );
        let strata = StrataError::from(err);
        assert!(matches!(strata, StrataError::LegacyFormat { .. }));
    }

    #[test]
    fn coordinator_snapshot_read_maps_to_typed_variant() {
        let err = CoordinatorRecoveryError::SnapshotRead {
            snapshot_id: 9,
            path: PathBuf::from("/tmp/db/snapshots/snap-9.chk"),
            source: SnapshotReadError::CrcMismatch {
                stored: 1,
                computed: 2,
            },
        };
        assert!(matches!(
            from_coordinator_error(ErrorRole::Primary, err),
            RecoveryError::SnapshotRead { snapshot_id: 9, .. }
        ));
    }

    #[test]
    fn coordinator_wal_checksum_maps_to_typed_variant() {
        let err = CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment {
            offset: 128,
            records_before: 3,
        });
        assert!(matches!(
            from_coordinator_error(ErrorRole::Follower, err),
            RecoveryError::WalChecksum {
                role: ErrorRole::Follower,
                offset: 128,
                records_before: 3,
            }
        ));
    }

    #[test]
    fn coordinator_callback_falls_back_to_wal_recovery_failed() {
        let err = CoordinatorRecoveryError::Callback(StrataError::storage("apply failed"));
        assert!(matches!(
            from_coordinator_error(ErrorRole::Primary, err),
            RecoveryError::WalRecoveryFailed {
                role: ErrorRole::Primary,
                ..
            }
        ));
    }
}
