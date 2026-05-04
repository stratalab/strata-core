//! Storage-owned recovery bootstrap surface.
//!
//! This module defines the primitive-neutral boundary for storage-owned
//! durability recovery mechanics. The public entry point is
//! `run_storage_recovery`; the smaller helper seams below stay private to keep
//! higher layers from driving the lower recovery runtime piecemeal.

use std::fmt;
use std::path::{Path, PathBuf};

use strata_core::id::CommitVersion;
use tracing::warn;

use crate::durability::codec::{clone_codec, get_codec, StorageCodec};
use crate::durability::layout::DatabaseLayout;
use crate::durability::{
    apply_wal_record_to_memory_storage, CoordinatorRecoveryError, LoadedSnapshot, ManifestError,
    ManifestManager, RecoveryCoordinator, RecoveryStats,
};
use crate::runtime_config::StorageRuntimeConfig;
use crate::{RecoveredState, SegmentedStore, StorageError};

/// Install a loaded snapshot into storage during recovery.
///
/// Storage owns MANIFEST and codec resolution, but it must not decode primitive
/// snapshot sections. The callback therefore receives the
/// resolved install codec and the generic `LoadedSnapshot` container; higher
/// layers keep ownership of primitive decode and install telemetry.
pub trait RecoverySnapshotInstallCallback: Send + Sync {
    /// Install `snapshot` into `storage` using `install_codec`.
    fn install_snapshot(
        &self,
        snapshot: &LoadedSnapshot,
        install_codec: &dyn StorageCodec,
        storage: &SegmentedStore,
    ) -> Result<(), StorageError>;
}

impl<F> RecoverySnapshotInstallCallback for F
where
    F: Fn(&LoadedSnapshot, &dyn StorageCodec, &SegmentedStore) -> Result<(), StorageError>
        + Send
        + Sync,
{
    fn install_snapshot(
        &self,
        snapshot: &LoadedSnapshot,
        install_codec: &dyn StorageCodec,
        storage: &SegmentedStore,
    ) -> Result<(), StorageError> {
        self(snapshot, install_codec, storage)
    }
}

/// Input for storage-owned durability recovery.
#[non_exhaustive]
pub struct StorageRecoveryInput<'a> {
    /// Canonical database disk layout.
    pub layout: DatabaseLayout,
    /// Storage-neutral recovery mode.
    pub mode: StorageRecoveryMode,
    /// Codec id supplied by the caller's runtime configuration.
    pub configured_codec_id: String,
    /// Runtime knobs used to construct and configure recovered storage.
    ///
    /// Recovery has no separate constructor write-buffer argument; it uses
    /// `runtime_config.write_buffer_size` to create the recovered
    /// `SegmentedStore`, then applies the remaining runtime knobs through
    /// `StorageRuntimeConfig::apply_to_store`.
    pub runtime_config: StorageRuntimeConfig,
    /// Whether storage may perform the mechanical lossy WAL replay fallback.
    pub allow_lossy_wal_replay: bool,
    /// Callback used when the recovery coordinator loads a snapshot.
    pub snapshot_install: &'a dyn RecoverySnapshotInstallCallback,
}

impl<'a> StorageRecoveryInput<'a> {
    /// Build storage-owned recovery input without requiring callers to construct
    /// the public struct literally.
    pub fn new(
        layout: DatabaseLayout,
        mode: StorageRecoveryMode,
        configured_codec_id: impl Into<String>,
        runtime_config: StorageRuntimeConfig,
        allow_lossy_wal_replay: bool,
        snapshot_install: &'a dyn RecoverySnapshotInstallCallback,
    ) -> Self {
        Self {
            layout,
            mode,
            configured_codec_id: configured_codec_id.into(),
            runtime_config,
            allow_lossy_wal_replay,
            snapshot_install,
        }
    }
}

impl fmt::Debug for StorageRecoveryInput<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRecoveryInput")
            .field("layout", &self.layout)
            .field("mode", &self.mode)
            .field("configured_codec_id", &self.configured_codec_id)
            .field("runtime_config", &self.runtime_config)
            .field("allow_lossy_wal_replay", &self.allow_lossy_wal_replay)
            .field("snapshot_install", &"<callback>")
            .finish()
    }
}

/// Run storage-owned durability recovery.
///
/// This is the storage-owned wrapper for lower recovery mechanics. It validates
/// and prepares MANIFEST state, resolves codecs, constructs the recovered
/// `SegmentedStore`, drives snapshot/WAL replay through the storage
/// coordinator, applies the mechanical lossy WAL fallback when configured, and
/// finishes segment recovery. Higher layers still own primitive snapshot decode
/// through `StorageRecoveryInput::snapshot_install`, public error mapping,
/// degraded-state policy, and operator-facing lossy reports.
pub fn run_storage_recovery(
    input: StorageRecoveryInput<'_>,
) -> Result<StorageRecoveryOutcome, StorageRecoveryError> {
    let StorageRecoveryInput {
        layout,
        mode,
        configured_codec_id,
        runtime_config,
        allow_lossy_wal_replay,
        snapshot_install,
    } = input;
    let write_buffer_size = runtime_config.write_buffer_size;

    let StorageManifestRecoveryPreparation {
        database_uuid,
        wal_codec,
        snapshot_install_codec,
    } = prepare_storage_manifest_for_recovery(&layout, mode, &configured_codec_id)?;

    let mut storage =
        SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), write_buffer_size);
    let replay = run_storage_coordinator_replay(
        &layout,
        write_buffer_size,
        allow_lossy_wal_replay,
        wal_codec.as_ref(),
        snapshot_install_codec.as_deref(),
        &storage,
        snapshot_install,
    );
    let (wal_replay, lossy_wal_replay) =
        handle_storage_wal_replay_outcome(replay, &layout, write_buffer_size, &mut storage)?;

    complete_storage_recovery_after_replay(
        database_uuid,
        wal_codec,
        storage,
        wal_replay,
        runtime_config,
        lossy_wal_replay,
    )
}

/// Storage-neutral recovery mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageRecoveryMode {
    /// Primary-style storage recovery creates a missing MANIFEST.
    PrimaryCreateManifestIfMissing,
    /// Follower-style storage recovery never creates a missing MANIFEST.
    FollowerNeverCreateManifest,
}

/// Storage-local MANIFEST and codec preparation result for recovery.
///
/// This intentionally carries only raw storage facts. Higher layers still
/// decide public recovery policy and wording after `run_storage_recovery`
/// returns.
struct StorageManifestRecoveryPreparation {
    /// MANIFEST-recorded database UUID, or `[0; 16]` for follower recovery
    /// when no MANIFEST exists.
    database_uuid: [u8; 16],
    /// WAL codec resolved for recovery replay.
    wal_codec: Box<dyn StorageCodec>,
    /// Snapshot install codec when an on-disk MANIFEST exists.
    snapshot_install_codec: Option<Box<dyn StorageCodec>>,
}

impl fmt::Debug for StorageManifestRecoveryPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageManifestRecoveryPreparation")
            .field("database_uuid", &self.database_uuid)
            .field("wal_codec_id", &self.wal_codec.codec_id())
            .field(
                "snapshot_install_codec_id",
                &self.snapshot_install_codec.as_ref().map(|c| c.codec_id()),
            )
            .finish()
    }
}

/// Prepare MANIFEST state and recovery codecs for the storage recovery path.
///
/// The configured codec is validated before any recovery-managed directory or
/// MANIFEST is created. Primary mode creates a missing MANIFEST; follower mode
/// never creates a MANIFEST, but may create the segments directory so storage
/// replay has a stable root.
fn prepare_storage_manifest_for_recovery(
    layout: &DatabaseLayout,
    mode: StorageRecoveryMode,
    configured_codec_id: &str,
) -> Result<StorageManifestRecoveryPreparation, StorageRecoveryError> {
    let configured_codec =
        get_codec(configured_codec_id).map_err(|source| StorageRecoveryError::CodecInit {
            codec_id: configured_codec_id.to_string(),
            detail: source.to_string(),
        })?;

    let manifest_path = layout.manifest_path().to_path_buf();
    if ManifestManager::exists(&manifest_path) {
        let manager = ManifestManager::load(manifest_path.clone()).map_err(|source| {
            StorageRecoveryError::ManifestLoad {
                path: manifest_path,
                source,
            }
        })?;
        let manifest = manager.manifest();
        if manifest.codec_id != configured_codec_id {
            return Err(StorageRecoveryError::ManifestCodecMismatch {
                stored: manifest.codec_id.clone(),
                configured: configured_codec_id.to_string(),
            });
        }

        return Ok(StorageManifestRecoveryPreparation {
            database_uuid: manifest.database_uuid,
            snapshot_install_codec: Some(clone_codec(configured_codec.as_ref())),
            wal_codec: configured_codec,
        });
    }

    match mode {
        StorageRecoveryMode::PrimaryCreateManifestIfMissing => {
            layout
                .create_segments_dir()
                .map_err(|source| StorageRecoveryError::Io { source })?;
            restrict_storage_dir(layout.segments_dir());
            let database_uuid = *uuid::Uuid::new_v4().as_bytes();
            ManifestManager::create(
                layout.manifest_path().to_path_buf(),
                database_uuid,
                configured_codec_id.to_string(),
            )
            .map_err(|source| StorageRecoveryError::ManifestCreate { source })?;

            Ok(StorageManifestRecoveryPreparation {
                database_uuid,
                wal_codec: configured_codec,
                snapshot_install_codec: None,
            })
        }
        StorageRecoveryMode::FollowerNeverCreateManifest => {
            if !layout
                .segments_dir()
                .try_exists()
                .map_err(|source| StorageRecoveryError::Io { source })?
            {
                layout
                    .create_segments_dir()
                    .map_err(|source| StorageRecoveryError::Io { source })?;
                restrict_storage_dir(layout.segments_dir());
            }

            Ok(StorageManifestRecoveryPreparation {
                database_uuid: [0u8; 16],
                wal_codec: configured_codec,
                snapshot_install_codec: None,
            })
        }
    }
}

/// Result of the storage-owned coordinator replay driver.
///
/// This private value is consumed by the storage-owned lossy replay outcome
/// helper before higher layers build public recovery reports.
struct StorageCoordinatorReplay {
    /// Coordinator replay result, mapped into the storage-local recovery error
    /// taxonomy without losing the structured coordinator source.
    recover_result: Result<RecoveryStats, StorageRecoveryError>,
    /// Number of WAL records successfully applied before the replay result was
    /// produced.
    records_applied: u64,
    /// Whether the coordinator replay was configured to scan past WAL
    /// corruption and storage may apply the matching whole-store fallback.
    allow_lossy_wal_replay: bool,
    /// Whether a callback failure came from snapshot installation rather than
    /// WAL record application. Snapshot install failures are not lossy-WAL
    /// replay failures and must not be converted into an empty open.
    snapshot_install_failed: bool,
}

impl fmt::Debug for StorageCoordinatorReplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageCoordinatorReplay")
            .field("recover_result", &self.recover_result)
            .field("records_applied", &self.records_applied)
            .field("allow_lossy_wal_replay", &self.allow_lossy_wal_replay)
            .field("snapshot_install_failed", &self.snapshot_install_failed)
            .finish()
    }
}

/// Drive recovery coordinator snapshot install and WAL replay into storage.
///
/// Storage owns the coordinator and generic WAL record application. Primitive
/// snapshot decode remains in the caller-provided callback, which receives the
/// resolved install codec and the target `SegmentedStore`.
fn run_storage_coordinator_replay(
    layout: &DatabaseLayout,
    write_buffer_size: usize,
    allow_lossy_wal_replay: bool,
    wal_codec: &dyn StorageCodec,
    snapshot_install_codec: Option<&dyn StorageCodec>,
    storage: &SegmentedStore,
    snapshot_install: &dyn RecoverySnapshotInstallCallback,
) -> StorageCoordinatorReplay {
    let recovery = RecoveryCoordinator::new(layout.clone(), write_buffer_size)
        .with_lossy_recovery(allow_lossy_wal_replay)
        .with_codec(clone_codec(wal_codec));

    let mut records_applied = 0_u64;
    let mut snapshot_install_failed = false;
    let recover_result = recovery
        .recover_typed(
            |snapshot| {
                let result = install_recovery_snapshot(
                    &snapshot,
                    snapshot_install_codec,
                    storage,
                    snapshot_install,
                );
                if result.is_err() {
                    snapshot_install_failed = true;
                }
                result
            },
            |record| {
                let result = apply_wal_record_to_memory_storage(storage, record);
                if result.is_ok() {
                    records_applied += 1;
                }
                result
            },
        )
        .map_err(|source| StorageRecoveryError::Coordinator { source });

    StorageCoordinatorReplay {
        recover_result,
        records_applied,
        allow_lossy_wal_replay,
        snapshot_install_failed,
    }
}

/// Complete storage-owned recovery mechanics after coordinator replay.
///
/// Storage folds the in-memory version reached by snapshot install / WAL replay
/// into WAL replay stats, applies storage-native runtime knobs, and runs segment
/// recovery. Versions discovered only from segment files remain in
/// `StorageRecoveryOutcome::segment_recovery.version_floor`; callers must
/// combine that floor with `wal_replay.final_version` when bootstrapping their
/// own version counters.
fn complete_storage_recovery_after_replay(
    database_uuid: [u8; 16],
    wal_codec: Box<dyn StorageCodec>,
    storage: SegmentedStore,
    mut wal_replay: RecoveryStats,
    runtime_config: StorageRuntimeConfig,
    lossy_wal_replay: Option<StorageLossyWalReplayFacts>,
) -> Result<StorageRecoveryOutcome, StorageRecoveryError> {
    wal_replay.final_version = wal_replay.final_version.max(storage.current_version());
    runtime_config.apply_to_store(&storage);
    let segment_recovery = storage
        .recover_segments()
        .map_err(|source| StorageRecoveryError::SegmentRecovery { source })?;

    Ok(StorageRecoveryOutcome::new(
        database_uuid,
        wal_codec,
        storage,
        wal_replay,
        segment_recovery,
        lossy_wal_replay,
    ))
}

/// Classify a coordinator replay result and apply the mechanical lossy WAL
/// replay fallback when configured.
///
/// Storage owns the raw replay mechanics: honoring hard-fail bypass rules,
/// sampling partial progress before any wipe, and replacing partially-applied
/// storage with a fresh layout-rooted store. Higher layers remain responsible
/// for mapping the source error into public taxonomy, wording, and logs.
fn handle_storage_wal_replay_outcome(
    replay: StorageCoordinatorReplay,
    layout: &DatabaseLayout,
    write_buffer_size: usize,
    storage: &mut SegmentedStore,
) -> Result<(RecoveryStats, Option<StorageLossyWalReplayFacts>), StorageRecoveryError> {
    let StorageCoordinatorReplay {
        recover_result,
        records_applied,
        allow_lossy_wal_replay,
        snapshot_install_failed,
    } = replay;
    let source = match recover_result {
        Ok(stats) => return Ok((stats, None)),
        Err(StorageRecoveryError::Coordinator { source }) => source,
        Err(other) => return Err(other),
    };

    if source.should_bypass_lossy() || snapshot_install_failed || !allow_lossy_wal_replay {
        return Err(StorageRecoveryError::Coordinator { source });
    }

    let facts =
        StorageLossyWalReplayFacts::new(records_applied, storage.current_version(), true, source);

    *storage = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), write_buffer_size);

    Ok((RecoveryStats::default(), Some(facts)))
}

fn install_recovery_snapshot(
    snapshot: &LoadedSnapshot,
    install_codec: Option<&dyn StorageCodec>,
    storage: &SegmentedStore,
    snapshot_install: &dyn RecoverySnapshotInstallCallback,
) -> Result<(), StorageError> {
    let Some(codec) = install_codec else {
        return Err(StorageError::corruption(format!(
            "snapshot {} reached recovery install callback without an install codec",
            snapshot.snapshot_id()
        )));
    };

    snapshot_install.install_snapshot(snapshot, codec, storage)
}

#[cfg(unix)]
fn restrict_storage_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)) {
        warn!(
            target: "strata::storage",
            path = %path.display(),
            error = %error,
            "Failed to restrict directory permissions"
        );
    }
}

#[cfg(not(unix))]
fn restrict_storage_dir(_path: &Path) {}

/// Raw storage recovery outcome.
///
/// Higher layers use these facts to construct database runtime state, follower
/// state, public errors, and operator-facing lossy recovery reports.
#[non_exhaustive]
pub struct StorageRecoveryOutcome {
    /// MANIFEST-recorded database UUID, or `[0; 16]` when follower recovery had
    /// no MANIFEST to read.
    pub database_uuid: [u8; 16],
    /// WAL codec resolved by storage recovery.
    pub wal_codec: Box<dyn StorageCodec>,
    /// Recovered storage.
    pub storage: SegmentedStore,
    /// WAL/snapshot replay stats from the recovery coordinator.
    ///
    /// `final_version` includes the in-memory version reached before segment
    /// recovery. Segment-only versions are reported separately through
    /// `segment_recovery.version_floor`.
    pub wal_replay: RecoveryStats,
    /// Segment recovery facts from `SegmentedStore::recover_segments()`.
    pub segment_recovery: RecoveredState,
    /// Mechanical lossy WAL replay facts, when the fallback ran.
    pub lossy_wal_replay: Option<StorageLossyWalReplayFacts>,
}

impl StorageRecoveryOutcome {
    /// Build a raw storage recovery outcome without requiring callers to
    /// construct the public struct literally.
    pub fn new(
        database_uuid: [u8; 16],
        wal_codec: Box<dyn StorageCodec>,
        storage: SegmentedStore,
        wal_replay: RecoveryStats,
        segment_recovery: RecoveredState,
        lossy_wal_replay: Option<StorageLossyWalReplayFacts>,
    ) -> Self {
        Self {
            database_uuid,
            wal_codec,
            storage,
            wal_replay,
            segment_recovery,
            lossy_wal_replay,
        }
    }
}

impl fmt::Debug for StorageRecoveryOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRecoveryOutcome")
            .field("database_uuid", &self.database_uuid)
            .field("wal_codec_id", &self.wal_codec.codec_id())
            .field("storage", &"<SegmentedStore>")
            .field("wal_replay", &self.wal_replay)
            .field("segment_recovery", &self.segment_recovery)
            .field("lossy_wal_replay", &self.lossy_wal_replay)
            .finish()
    }
}

/// Mechanical facts from the lossy WAL replay fallback.
#[derive(Debug)]
#[non_exhaustive]
pub struct StorageLossyWalReplayFacts {
    /// Number of WAL records applied before the failure that triggered the
    /// fallback.
    pub records_applied_before_failure: u64,
    /// Storage version reached before the fallback wiped partial state.
    pub version_reached_before_failure: CommitVersion,
    /// Whether storage discarded partial state during the fallback.
    pub discarded_on_wipe: bool,
    /// Original coordinator failure that caused the fallback.
    pub source: CoordinatorRecoveryError,
}

impl StorageLossyWalReplayFacts {
    /// Build mechanical lossy WAL replay facts without requiring callers to
    /// construct the public struct literally.
    pub fn new(
        records_applied_before_failure: u64,
        version_reached_before_failure: CommitVersion,
        discarded_on_wipe: bool,
        source: CoordinatorRecoveryError,
    ) -> Self {
        Self {
            records_applied_before_failure,
            version_reached_before_failure,
            discarded_on_wipe,
            source,
        }
    }
}

/// Storage-local recovery bootstrap errors.
///
/// These variants intentionally avoid higher-layer public wording. Higher
/// layers map them into their own error taxonomy at the database-open boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageRecoveryError {
    /// The configured or MANIFEST-recorded codec could not be initialized.
    #[error("could not initialize storage codec {codec_id:?}: {detail}")]
    CodecInit {
        /// Codec id that failed to initialize.
        codec_id: String,
        /// Codec initialization detail.
        detail: String,
    },

    /// MANIFEST load failed.
    #[error("failed to load MANIFEST at {path}: {source}")]
    ManifestLoad {
        /// MANIFEST path.
        path: PathBuf,
        /// Underlying MANIFEST error.
        #[source]
        source: ManifestError,
    },

    /// MANIFEST creation failed.
    #[error("failed to create MANIFEST: {source}")]
    ManifestCreate {
        /// Underlying MANIFEST error.
        #[source]
        source: ManifestError,
    },

    /// MANIFEST codec id disagreed with the configured codec id.
    #[error("MANIFEST codec mismatch: stored={stored:?}, configured={configured:?}")]
    ManifestCodecMismatch {
        /// Codec id stored in MANIFEST.
        stored: String,
        /// Codec id supplied by runtime configuration.
        configured: String,
    },

    /// I/O outside MANIFEST-specific paths failed.
    #[error("storage recovery I/O error: {source}")]
    Io {
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Recovery coordinator planning, snapshot load, WAL read, payload decode,
    /// or callback execution failed.
    #[error("storage recovery coordinator failed: {source}")]
    Coordinator {
        /// Structured coordinator failure.
        #[source]
        source: CoordinatorRecoveryError,
    },

    /// Segment recovery failed before it could return classified health.
    #[error("segment recovery failed: {source}")]
    SegmentRecovery {
        /// Underlying storage failure.
        #[source]
        source: StorageError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::codec::IdentityCodec;
    use crate::durability::format::WalRecord;
    use crate::durability::wal::{DurabilityMode, WalConfig, WalReaderError, WalWriter};
    use crate::durability::{
        CoordinatorPlanError, SnapshotHeader, SnapshotWriter, TransactionPayload,
    };
    use crate::manifest::{write_manifest, ManifestEntry};
    use crate::segmented::hex_encode_branch;
    use crate::{Key, Namespace, WriteMode};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use strata_core::id::TxnId;
    use strata_core::{BranchId, Value};

    #[test]
    fn constructs_storage_recovery_input_without_higher_layer_types() {
        let callback = |_snapshot: &LoadedSnapshot,
                        _install_codec: &dyn StorageCodec,
                        _storage: &SegmentedStore|
         -> Result<(), StorageError> { Ok(()) };
        let input = StorageRecoveryInput::new(
            DatabaseLayout::from_root("/tmp/strata-recovery-bootstrap"),
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "identity",
            StorageRuntimeConfig::builder()
                .write_buffer_size(4096)
                .max_branches(128)
                .max_versions_per_key(16)
                .max_immutable_memtables(2)
                .target_file_size(1 << 20)
                .level_base_bytes(4 << 20)
                .data_block_size(4096)
                .bloom_bits_per_key(10)
                .build(),
            true,
            &callback,
        );

        assert_eq!(
            input.mode,
            StorageRecoveryMode::PrimaryCreateManifestIfMissing
        );
        assert_eq!(input.runtime_config.target_file_size, 1 << 20);
        assert!(format!("{input:?}").contains("<callback>"));
    }

    #[test]
    fn manifest_preparation_rejects_invalid_codec_before_side_effects() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path().join("db"));

        let err = prepare_storage_manifest_for_recovery(
            &layout,
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "does-not-exist",
        )
        .expect_err("invalid configured codec should fail");

        assert!(matches!(err, StorageRecoveryError::CodecInit { .. }));
        assert!(
            !layout.manifest_path().exists(),
            "invalid codec must not create MANIFEST"
        );
        assert!(
            !layout.segments_dir().exists(),
            "invalid codec must not create segments dir"
        );
    }

    #[test]
    fn primary_manifest_preparation_creates_manifest_and_segments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());

        let prepared = prepare_storage_manifest_for_recovery(
            &layout,
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "identity",
        )
        .expect("primary missing-MANIFEST preparation should succeed");

        assert_ne!(prepared.database_uuid, [0u8; 16]);
        assert_eq!(prepared.wal_codec.codec_id(), "identity");
        assert!(prepared.snapshot_install_codec.is_none());
        assert!(layout.segments_dir().exists());
        let manifest = ManifestManager::load(layout.manifest_path().to_path_buf()).unwrap();
        assert_eq!(manifest.manifest().database_uuid, prepared.database_uuid);
        assert_eq!(manifest.manifest().codec_id, "identity");
    }

    #[test]
    fn follower_manifest_preparation_never_creates_manifest() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path().join("follower"));

        let prepared = prepare_storage_manifest_for_recovery(
            &layout,
            StorageRecoveryMode::FollowerNeverCreateManifest,
            "identity",
        )
        .expect("follower missing-MANIFEST preparation should succeed");

        assert_eq!(prepared.database_uuid, [0u8; 16]);
        assert_eq!(prepared.wal_codec.codec_id(), "identity");
        assert!(prepared.snapshot_install_codec.is_none());
        assert!(layout.segments_dir().exists());
        assert!(
            !layout.manifest_path().exists(),
            "follower preparation must not create MANIFEST"
        );
    }

    #[test]
    fn existing_manifest_preparation_returns_install_codec_and_mismatch_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x33; 16];
        ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            database_uuid,
            "unknown-stored-codec".to_string(),
        )
        .unwrap();

        let mismatch = prepare_storage_manifest_for_recovery(
            &layout,
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "identity",
        )
        .expect_err("stored/configured codec mismatch should be typed");

        assert!(matches!(
            mismatch,
            StorageRecoveryError::ManifestCodecMismatch { .. }
        ));

        ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            database_uuid,
            "identity".to_string(),
        )
        .unwrap();
        let prepared = prepare_storage_manifest_for_recovery(
            &layout,
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "identity",
        )
        .expect("matching existing MANIFEST should prepare codecs");

        assert_eq!(prepared.database_uuid, database_uuid);
        assert_eq!(prepared.wal_codec.codec_id(), "identity");
        assert_eq!(
            prepared
                .snapshot_install_codec
                .as_ref()
                .expect("existing MANIFEST should provide snapshot install codec")
                .codec_id(),
            "identity"
        );
    }

    #[test]
    fn snapshot_install_callback_receives_codec_and_storage() {
        let callback = |snapshot: &LoadedSnapshot,
                        install_codec: &dyn StorageCodec,
                        storage: &SegmentedStore|
         -> Result<(), StorageError> {
            assert_eq!(snapshot.snapshot_id(), 7);
            assert_eq!(install_codec.codec_id(), "identity");
            assert_eq!(storage.version(), 0);
            Ok(())
        };
        let snapshot = LoadedSnapshot {
            header: SnapshotHeader::new(7, 3, 1, [0x77; 16], "identity".len() as u8),
            codec_id: "identity".to_string(),
            sections: Vec::new(),
            crc: 0,
        };
        let storage = SegmentedStore::new();
        let codec = IdentityCodec;

        callback
            .install_snapshot(&snapshot, &codec, &storage)
            .expect("callback should succeed");
    }

    #[test]
    fn run_storage_recovery_replays_wal_and_returns_raw_outcome() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "storage-wrapper-wal");
        write_wal_txn(
            layout.wal_dir(),
            TxnId(31),
            branch_id,
            vec![(key.clone(), Value::String("from-wrapper".to_string()))],
            Vec::new(),
            41,
        );
        let snapshot_install = |_snapshot: &LoadedSnapshot,
                                _install_codec: &dyn StorageCodec,
                                _storage: &SegmentedStore|
         -> Result<(), StorageError> { Ok(()) };

        let outcome = run_storage_recovery(StorageRecoveryInput::new(
            layout.clone(),
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "identity",
            StorageRuntimeConfig::builder()
                .write_buffer_size(4096)
                .build(),
            false,
            &snapshot_install,
        ))
        .expect("storage recovery wrapper should replay WAL");

        assert!(layout.manifest_path().exists());
        assert!(layout.segments_dir().exists());
        assert_ne!(outcome.database_uuid, [0u8; 16]);
        assert_eq!(outcome.wal_codec.codec_id(), "identity");
        assert_eq!(outcome.wal_replay.txns_replayed, 1);
        assert_eq!(outcome.wal_replay.writes_applied, 1);
        assert_eq!(outcome.wal_replay.final_version, CommitVersion(41));
        assert!(outcome.lossy_wal_replay.is_none());
        let recovered = outcome
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("WAL row should be visible after storage recovery");
        assert_eq!(recovered.value, Value::String("from-wrapper".to_string()));
    }

    #[test]
    fn run_storage_recovery_carries_lossy_facts_after_wipe() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "storage-wrapper-lossy");
        write_wal_txn(
            layout.wal_dir(),
            TxnId(33),
            branch_id,
            vec![(key.clone(), Value::String("discarded".to_string()))],
            Vec::new(),
            43,
        );
        std::fs::write(
            layout.wal_dir().join("wal-000099.seg"),
            b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER",
        )
        .unwrap();
        let snapshot_install = |_snapshot: &LoadedSnapshot,
                                _install_codec: &dyn StorageCodec,
                                _storage: &SegmentedStore|
         -> Result<(), StorageError> { Ok(()) };

        let outcome = run_storage_recovery(StorageRecoveryInput::new(
            layout,
            StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            "identity",
            StorageRuntimeConfig::builder()
                .write_buffer_size(4096)
                .build(),
            true,
            &snapshot_install,
        ))
        .expect("allowed lossy storage recovery should return a raw outcome");

        assert_eq!(outcome.wal_replay, RecoveryStats::default());
        let facts = outcome
            .lossy_wal_replay
            .expect("lossy fallback should preserve mechanical facts");
        assert_eq!(facts.records_applied_before_failure, 1);
        assert_eq!(facts.version_reached_before_failure, CommitVersion(43));
        assert!(facts.discarded_on_wipe);
        assert!(
            outcome
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .is_none(),
            "storage wrapper must return the wiped store"
        );
    }

    #[test]
    fn coordinator_replay_applies_wal_records_and_counts_successes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "storage-replay");
        write_wal_txn(
            layout.wal_dir(),
            TxnId(17),
            branch_id,
            vec![(key.clone(), Value::String("from-wal".to_string()))],
            Vec::new(),
            23,
        );
        let storage = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), 4096);
        let codec = IdentityCodec;
        let snapshot_install = |_snapshot: &LoadedSnapshot,
                                _install_codec: &dyn StorageCodec,
                                _storage: &SegmentedStore|
         -> Result<(), StorageError> { Ok(()) };

        let replay = run_storage_coordinator_replay(
            &layout,
            4096,
            false,
            &codec,
            None,
            &storage,
            &snapshot_install,
        );

        let stats = replay
            .recover_result
            .expect("WAL replay should succeed through storage driver");
        assert_eq!(replay.records_applied, 1);
        assert_eq!(stats.txns_replayed, 1);
        assert_eq!(stats.writes_applied, 1);
        assert_eq!(stats.final_version, CommitVersion(23));
        let recovered = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("WAL row should be applied to storage");
        assert_eq!(recovered.value, Value::String("from-wal".to_string()));
    }

    #[test]
    fn coordinator_replay_counts_records_before_real_wal_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "storage-replay-before-failure");
        write_wal_txn(
            layout.wal_dir(),
            TxnId(19),
            branch_id,
            vec![(key.clone(), Value::String("before-failure".to_string()))],
            Vec::new(),
            29,
        );
        std::fs::write(
            layout.wal_dir().join("wal-000099.seg"),
            b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER",
        )
        .unwrap();
        let storage = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), 4096);
        let codec = IdentityCodec;
        let snapshot_install = |_snapshot: &LoadedSnapshot,
                                _install_codec: &dyn StorageCodec,
                                _storage: &SegmentedStore|
         -> Result<(), StorageError> { Ok(()) };

        let replay = run_storage_coordinator_replay(
            &layout,
            4096,
            false,
            &codec,
            None,
            &storage,
            &snapshot_install,
        );

        assert_eq!(replay.records_applied, 1);
        match replay
            .recover_result
            .expect_err("corrupt later WAL segment should fail replay")
        {
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::WalRead(_),
            } => {}
            other => panic!("expected WAL read coordinator error, got {other:?}"),
        }
        let recovered = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("successful records before the real failure should have been applied");
        assert_eq!(recovered.value, Value::String("before-failure".to_string()));
    }

    #[test]
    fn coordinator_replay_rejects_loaded_snapshot_without_install_codec() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x55; 16];
        SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            database_uuid,
        )
        .unwrap()
        .create_snapshot(7, 5, Vec::new())
        .unwrap();
        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            database_uuid,
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(7, TxnId(5)).unwrap();

        let storage = SegmentedStore::new();
        let codec = IdentityCodec;
        let snapshot_install = |_snapshot: &LoadedSnapshot,
                                _install_codec: &dyn StorageCodec,
                                _storage: &SegmentedStore|
         -> Result<(), StorageError> {
            panic!("storage must reject missing install codec before invoking callback")
        };

        let replay = run_storage_coordinator_replay(
            &layout,
            4096,
            false,
            &codec,
            None,
            &storage,
            &snapshot_install,
        );

        assert_eq!(replay.records_applied, 0);
        match replay
            .recover_result
            .expect_err("loaded snapshot without install codec should hard-fail")
        {
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::Callback(StorageError::Corruption { message }),
            } => {
                assert!(
                    message.contains(
                        "snapshot 7 reached recovery install callback without an install codec"
                    ),
                    "missing-codec error should carry snapshot id, got: {message}"
                );
            }
            other => panic!("expected missing-codec coordinator callback error, got {other:?}"),
        }
    }

    #[test]
    fn lossy_wal_replay_snapshot_callback_failure_preserves_partial_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x56; 16];
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "preserved-by-snapshot-callback-bypass");
        SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            database_uuid,
        )
        .unwrap()
        .create_snapshot(8, 6, Vec::new())
        .unwrap();
        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            database_uuid,
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(8, TxnId(6)).unwrap();

        let mut storage = storage_with_partial_row(&layout, key.clone(), CommitVersion(41));
        let codec = IdentityCodec;
        let snapshot_install = |_snapshot: &LoadedSnapshot,
                                _install_codec: &dyn StorageCodec,
                                _storage: &SegmentedStore|
         -> Result<(), StorageError> {
            panic!("storage must reject missing install codec before invoking callback")
        };

        let replay = run_storage_coordinator_replay(
            &layout,
            4096,
            true,
            &codec,
            None,
            &storage,
            &snapshot_install,
        );

        assert_eq!(replay.records_applied, 0);
        assert!(
            replay.snapshot_install_failed,
            "missing install codec is a snapshot callback failure, not a WAL replay failure"
        );
        let err = handle_storage_wal_replay_outcome(replay, &layout, 4096, &mut storage)
            .expect_err("snapshot callback failures must bypass lossy WAL fallback");
        match err {
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::Callback(StorageError::Corruption { message }),
            } => {
                assert!(
                    message.contains(
                        "snapshot 8 reached recovery install callback without an install codec"
                    ),
                    "missing-codec error should carry snapshot id, got: {message}"
                );
            }
            other => panic!("expected missing-codec coordinator callback error, got {other:?}"),
        }
        let preserved = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("snapshot callback bypass must not wipe partial storage");
        assert_eq!(preserved.value, Value::String("partial".to_string()));
        assert_eq!(storage.current_version(), CommitVersion(41));
    }

    #[test]
    fn lossy_wal_replay_fallback_discards_partial_storage_and_returns_facts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "discarded-by-storage-lossy");
        let mut storage = storage_with_partial_row(&layout, key.clone(), CommitVersion(19));

        let (stats, facts) = handle_storage_wal_replay_outcome(
            replay_error(
                CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment {
                    offset: 128,
                    records_before: 3,
                }),
                true,
                3,
            ),
            &layout,
            4096,
            &mut storage,
        )
        .expect("storage lossy fallback should convert replay failure into empty stats");

        assert_eq!(stats, RecoveryStats::default());
        let facts = facts.expect("allowed lossy fallback should return facts");
        assert_eq!(facts.records_applied_before_failure, 3);
        assert_eq!(facts.version_reached_before_failure, CommitVersion(19));
        assert!(facts.discarded_on_wipe);
        assert!(matches!(
            facts.source,
            CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment { .. })
        ));
        assert!(
            storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .is_none(),
            "lossy fallback must replace partial storage with an empty store"
        );
        assert_eq!(storage.current_version(), CommitVersion::ZERO);
    }

    #[test]
    fn lossy_wal_replay_disabled_preserves_partial_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "preserved-when-lossy-disabled");
        let mut storage = storage_with_partial_row(&layout, key.clone(), CommitVersion(23));

        let err = handle_storage_wal_replay_outcome(
            replay_error(
                CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment {
                    offset: 256,
                    records_before: 4,
                }),
                false,
                4,
            ),
            &layout,
            4096,
            &mut storage,
        )
        .expect_err("disabled lossy fallback must preserve the coordinator error");

        assert!(matches!(
            err,
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment { .. })
            }
        ));
        let preserved = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("disabled lossy fallback must not wipe partial storage");
        assert_eq!(preserved.value, Value::String("partial".to_string()));
        assert_eq!(storage.current_version(), CommitVersion(23));
    }

    #[test]
    fn lossy_wal_replay_bypass_preserves_partial_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "preserved-by-bypass");
        let mut storage = storage_with_partial_row(&layout, key.clone(), CommitVersion(31));

        let err = handle_storage_wal_replay_outcome(
            replay_error(
                CoordinatorRecoveryError::WalRead(WalReaderError::LegacyFormat {
                    found_version: 1,
                    hint: "delete wal/ or migrate".to_string(),
                }),
                true,
                5,
            ),
            &layout,
            4096,
            &mut storage,
        )
        .expect_err("legacy WAL must bypass lossy fallback");

        assert!(matches!(
            err,
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::WalRead(WalReaderError::LegacyFormat { .. })
            }
        ));
        let preserved = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("lossy bypass must not wipe partial storage");
        assert_eq!(preserved.value, Value::String("partial".to_string()));
        assert_eq!(storage.current_version(), CommitVersion(31));
    }

    #[test]
    fn lossy_wal_replay_plan_failure_preserves_partial_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "preserved-by-plan-bypass");
        let mut storage = storage_with_partial_row(&layout, key.clone(), CommitVersion(37));

        let err = handle_storage_wal_replay_outcome(
            replay_error(
                CoordinatorRecoveryError::Plan(CoordinatorPlanError::CodecMismatch {
                    stored: "identity".to_string(),
                    expected: "aes-gcm-256".to_string(),
                }),
                true,
                0,
            ),
            &layout,
            4096,
            &mut storage,
        )
        .expect_err("coordinator planning errors must bypass lossy fallback");

        assert!(matches!(
            err,
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::Plan(CoordinatorPlanError::CodecMismatch { .. })
            }
        ));
        let preserved = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("planning bypass must not wipe partial storage");
        assert_eq!(preserved.value, Value::String("partial".to_string()));
        assert_eq!(storage.current_version(), CommitVersion(37));
    }

    #[test]
    fn lossy_wal_replay_snapshot_failure_preserves_partial_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "preserved-by-snapshot-bypass");
        let mut storage = storage_with_partial_row(&layout, key.clone(), CommitVersion(39));

        let err = handle_storage_wal_replay_outcome(
            replay_error(
                CoordinatorRecoveryError::SnapshotMissing {
                    snapshot_id: 9,
                    path: layout.snapshots_dir().join("snapshot-9.strata"),
                },
                true,
                0,
            ),
            &layout,
            4096,
            &mut storage,
        )
        .expect_err("snapshot failures must bypass lossy fallback");

        assert!(matches!(
            err,
            StorageRecoveryError::Coordinator {
                source: CoordinatorRecoveryError::SnapshotMissing { .. }
            }
        ));
        let preserved = storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("snapshot bypass must not wipe partial storage");
        assert_eq!(preserved.value, Value::String("partial".to_string()));
        assert_eq!(storage.current_version(), CommitVersion(39));
    }

    #[test]
    fn coordinator_replay_invokes_snapshot_callback_with_install_codec() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x66; 16];
        SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            database_uuid,
        )
        .unwrap()
        .create_snapshot(11, 8, Vec::new())
        .unwrap();
        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            database_uuid,
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(11, TxnId(8)).unwrap();

        let storage = SegmentedStore::new();
        let codec = IdentityCodec;
        let callback_called = AtomicBool::new(false);
        let snapshot_install = |snapshot: &LoadedSnapshot,
                                install_codec: &dyn StorageCodec,
                                callback_storage: &SegmentedStore|
         -> Result<(), StorageError> {
            callback_called.store(true, Ordering::SeqCst);
            assert_eq!(snapshot.snapshot_id(), 11);
            assert_eq!(install_codec.codec_id(), "identity");
            assert!(std::ptr::eq(callback_storage, &storage));
            Ok(())
        };

        let replay = run_storage_coordinator_replay(
            &layout,
            4096,
            false,
            &codec,
            Some(&codec),
            &storage,
            &snapshot_install,
        );

        let stats = replay
            .recover_result
            .expect("snapshot callback should succeed through storage driver");
        assert!(stats.from_checkpoint);
        assert_eq!(stats.max_txn_id, TxnId(8));
        assert!(callback_called.load(Ordering::SeqCst));
    }

    #[test]
    fn post_replay_completion_folds_applies_config_and_recovers_segments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "segment-recovery");
        {
            let writer = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), 4096);
            writer
                .put_with_version_mode(
                    key.clone(),
                    Value::String("from-segment".to_string()),
                    CommitVersion(42),
                    None,
                    WriteMode::Append,
                )
                .unwrap();
            writer.rotate_memtable(&branch_id);
            writer.flush_oldest_frozen(&branch_id).unwrap();
        }

        let branch_dir = layout.segments_dir().join(hex_encode_branch(&branch_id));
        let segment_filename = std::fs::read_dir(&branch_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                (path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
                    .then(|| entry.file_name().to_string_lossy().into_owned())
            })
            .next()
            .expect("flush should create one segment file");
        write_manifest(
            &branch_dir,
            &[ManifestEntry {
                filename: segment_filename,
                level: 2,
            }],
            &[],
        )
        .expect("test should pin flushed segment at L2");

        let storage = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), 4096);
        storage.set_version(CommitVersion(73));
        let wal_replay = RecoveryStats {
            final_version: CommitVersion(5),
            ..RecoveryStats::default()
        };
        let runtime_config = StorageRuntimeConfig::builder()
            .write_buffer_size(4096)
            .max_branches(128)
            .max_versions_per_key(16)
            .max_immutable_memtables(2)
            .target_file_size(2 << 20)
            .level_base_bytes(5 << 20)
            .data_block_size(8 << 10)
            .bloom_bits_per_key(12)
            .compaction_rate_limit(1_234_567)
            .build();

        let outcome = complete_storage_recovery_after_replay(
            [0x77; 16],
            Box::new(IdentityCodec),
            storage,
            wal_replay,
            runtime_config,
            None,
        )
        .expect("post-replay storage recovery should succeed");

        assert_eq!(outcome.database_uuid, [0x77; 16]);
        assert_eq!(outcome.wal_codec.codec_id(), "identity");
        assert_eq!(
            outcome.wal_replay.final_version,
            CommitVersion(73),
            "pre-segment storage version should be folded into replay stats"
        );
        assert_eq!(outcome.segment_recovery.segments_loaded, 1);
        assert_eq!(outcome.segment_recovery.version_floor, CommitVersion(42));
        assert_eq!(outcome.storage.max_branches_for_test(), 128);
        assert_eq!(outcome.storage.max_versions_per_key_for_test(), 16);
        assert_eq!(outcome.storage.max_immutable_memtables_for_test(), 2);
        assert_eq!(outcome.storage.target_file_size(), 2 << 20);
        assert_eq!(outcome.storage.level_base_bytes(), 5 << 20);
        assert_eq!(
            outcome.storage.branch_level_target_for_test(&branch_id, 1),
            Some(5 << 20),
            "recover_segments should compute level targets with the configured base"
        );
        assert_eq!(outcome.storage.data_block_size(), 8 << 10);
        assert_eq!(outcome.storage.bloom_bits_per_key(), 12);
        assert_eq!(outcome.storage.compaction_rate_limit_for_test(), 1_234_567);
        assert!(outcome.lossy_wal_replay.is_none());
        let recovered = outcome
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("segment recovery should load the flushed row");
        assert_eq!(recovered.value, Value::String("from-segment".to_string()));
    }

    #[test]
    fn post_replay_completion_carries_lossy_wal_replay_facts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        layout.create_segments_dir().unwrap();
        let storage = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), 4096);
        let lossy = StorageLossyWalReplayFacts::new(
            2,
            CommitVersion(11),
            true,
            CoordinatorRecoveryError::WalRead(WalReaderError::CorruptedSegment {
                offset: 64,
                records_before: 2,
            }),
        );

        let outcome = complete_storage_recovery_after_replay(
            [0x88; 16],
            Box::new(IdentityCodec),
            storage,
            RecoveryStats::default(),
            StorageRuntimeConfig::builder()
                .write_buffer_size(4096)
                .build(),
            Some(lossy),
        )
        .expect("post-replay storage recovery should carry lossy facts");

        let facts = outcome
            .lossy_wal_replay
            .expect("lossy facts should survive post-replay completion");
        assert_eq!(facts.records_applied_before_failure, 2);
        assert_eq!(facts.version_reached_before_failure, CommitVersion(11));
        assert!(facts.discarded_on_wipe);
    }

    #[test]
    fn constructs_storage_recovery_outcome() {
        let outcome = StorageRecoveryOutcome::new(
            [0x44; 16],
            Box::new(IdentityCodec),
            SegmentedStore::new(),
            RecoveryStats::default(),
            RecoveredState::empty(),
            None,
        );

        assert_eq!(outcome.wal_codec.codec_id(), "identity");
        assert_eq!(outcome.segment_recovery.segments_loaded, 0);
        assert!(format!("{outcome:?}").contains("wal_codec_id"));
    }

    #[test]
    fn recovery_error_display_preserves_storage_context() {
        let codec = StorageRecoveryError::CodecInit {
            codec_id: "missing".to_string(),
            detail: "unknown codec".to_string(),
        };
        assert_eq!(
            codec.to_string(),
            "could not initialize storage codec \"missing\": unknown codec"
        );

        let mismatch = StorageRecoveryError::ManifestCodecMismatch {
            stored: "identity".to_string(),
            configured: "aes-gcm-256".to_string(),
        };
        assert_eq!(
            mismatch.to_string(),
            "MANIFEST codec mismatch: stored=\"identity\", configured=\"aes-gcm-256\""
        );

        let coordinator = StorageRecoveryError::Coordinator {
            source: CoordinatorRecoveryError::Callback(StorageError::corruption(
                "snapshot callback failed",
            )),
        };
        assert_eq!(
            coordinator.to_string(),
            "storage recovery coordinator failed: snapshot callback failed"
        );
    }

    fn write_wal_txn(
        wal_dir: &Path,
        txn_id: TxnId,
        branch_id: BranchId,
        puts: Vec<(Key, Value)>,
        deletes: Vec<Key>,
        version: u64,
    ) {
        let mut wal = WalWriter::new(
            wal_dir.to_path_buf(),
            [0u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            Box::new(IdentityCodec),
        )
        .unwrap();

        let payload = TransactionPayload {
            version,
            puts,
            deletes,
            put_ttls: vec![],
        };
        let record = WalRecord::new(txn_id, *branch_id.as_bytes(), 1_000, payload.to_bytes());
        wal.append(&record).unwrap();
        wal.flush().unwrap();
    }

    fn kv_key(branch_id: BranchId, key: &str) -> Key {
        Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), key)
    }

    fn storage_with_partial_row(
        layout: &DatabaseLayout,
        key: Key,
        version: CommitVersion,
    ) -> SegmentedStore {
        let storage = SegmentedStore::with_dir(layout.segments_dir().to_path_buf(), 4096);
        storage
            .put_with_version_mode(
                key,
                Value::String("partial".to_string()),
                version,
                None,
                WriteMode::Append,
            )
            .unwrap();
        storage
    }

    fn replay_error(
        source: CoordinatorRecoveryError,
        allow_lossy_wal_replay: bool,
        records_applied: u64,
    ) -> StorageCoordinatorReplay {
        StorageCoordinatorReplay {
            recover_result: Err(StorageRecoveryError::Coordinator { source }),
            records_applied,
            allow_lossy_wal_replay,
            snapshot_install_failed: false,
        }
    }
}
