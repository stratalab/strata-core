//! Unified recovery orchestration for primary and follower opens.
//!
//! `Database::run_recovery` is the single recovery entry point used by both
//! `open_finish` (primary) and `acquire_follower_db`. It centralizes recovery
//! orchestration, typed error classification, and the shared rules around
//! storage repair, snapshot install, and WAL replay.
//!
//! # Scope
//!
//! This module owns orchestration and typed taxonomy. Recovery policy still
//! branches on `RecoveryHealth::Degraded` after storage recovery returns, and
//! WAL-replay lossy reports remain engine-owned.
//!
//! # First-open ordering
//!
//! Configured-codec validation runs **before** any recovery-managed directory
//! is created or MANIFEST is written. Without that ordering, a fresh database
//! with an invalid codec id would persist a poisoned directory that the next
//! open could not repair.

use std::path::Path;
use std::sync::Arc;

use crate::StrataError;
use strata_core::id::CommitVersion;
use strata_storage::durability::codec::StorageCodec;
use strata_storage::durability::layout::DatabaseLayout;
use strata_storage::durability::wal::WalReaderError;
use strata_storage::durability::{
    run_storage_recovery, CoordinatorPlanError, CoordinatorRecoveryError, LoadedSnapshot,
    ManifestError, RecoveryResult, SnapshotReadError, StorageLossyWalReplayFacts,
    StorageRecoveryError, StorageRecoveryInput, StorageRecoveryMode,
};
use strata_storage::runtime_config::StorageRuntimeConfig;
use strata_storage::{DegradationClass, RecoveryHealth, SegmentedStore, StorageError};
use tracing::{info, warn};

use super::config::{storage_runtime_config_from, StrataConfig};
use super::recovery_error::{
    classify_manifest_load_error, from_coordinator_error, ErrorRole, RecoveryError,
};
use super::refresh::{
    clear_persisted_follower_state, load_persisted_follower_state, validate_blocked_state,
    ContiguousWatermark, PersistedFollowerState,
};
use super::snapshot_install::{install_snapshot, InstallStats};
use super::{Database, LossyErrorKind, LossyRecoveryReport};
use crate::coordinator::TransactionCoordinator;

/// Recovery orchestration mode.
///
/// Cache/ephemeral databases never recover (no disk state), so they do
/// not call `run_recovery`; only primary and follower opens do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryMode {
    /// Primary (exclusive, read-write) open. Creates a MANIFEST on
    /// first open; full snapshot + WAL replay + segment recovery.
    Primary,
    /// Follower (shared, read-only) open. Never creates a MANIFEST;
    /// snapshot install wired only when an on-disk MANIFEST records a
    /// codec id; follower-state restore runs at the end.
    Follower,
}

impl RecoveryMode {
    /// Downcast to the narrower `ErrorRole` used inside operator-UX
    /// error messages. The two enums carry the same information; they
    /// are separate types because `ErrorRole` lives alongside
    /// `RecoveryError` so the error type stays self-contained.
    fn as_error_role(self) -> ErrorRole {
        match self {
            RecoveryMode::Primary => ErrorRole::Primary,
            RecoveryMode::Follower => ErrorRole::Follower,
        }
    }

    fn as_storage_mode(self) -> StorageRecoveryMode {
        match self {
            RecoveryMode::Primary => StorageRecoveryMode::PrimaryCreateManifestIfMissing,
            RecoveryMode::Follower => StorageRecoveryMode::FollowerNeverCreateManifest,
        }
    }
}

/// Engine-owned recovery config bundle.
///
/// `run_recovery` needs both public engine config and storage runtime config.
/// Keeping them in one value makes the invariant explicit: the storage runtime
/// config is always adapted from the same `StrataConfig` passed to recovery.
#[derive(Clone, Copy)]
pub(crate) struct RecoveryRuntimeConfig<'a> {
    strata_config: &'a StrataConfig,
    storage_runtime_config: StorageRuntimeConfig,
}

impl<'a> RecoveryRuntimeConfig<'a> {
    pub(crate) fn from_strata_config(strata_config: &'a StrataConfig) -> Self {
        Self {
            strata_config,
            storage_runtime_config: storage_runtime_config_from(&strata_config.storage),
        }
    }

    pub(crate) fn storage_runtime_config(&self) -> StorageRuntimeConfig {
        self.storage_runtime_config
    }

    fn strata_config(&self) -> &'a StrataConfig {
        self.strata_config
    }
}

/// Output of [`Database::run_recovery`].
///
/// Carries the owned resources the caller needs to finish constructing
/// `Arc<Database>`. All public fields are `pub(crate)` because
/// `RecoveryOutcome` is an implementation detail of `open.rs`; it is
/// not part of the ES4 public surface.
pub(crate) struct RecoveryOutcome {
    /// MANIFEST-recorded database UUID (or `[0u8; 16]` for follower
    /// without a MANIFEST).
    pub(crate) database_uuid: [u8; 16],
    /// WAL codec used by both the writer and the follower-refresh
    /// reader path.
    pub(crate) wal_codec: Box<dyn StorageCodec>,
    /// Recovered, config-applied store. Already wrapped in `Arc` so
    /// the caller can hand it straight to the `Database` struct.
    pub(crate) storage: Arc<SegmentedStore>,
    /// Coordinator bootstrapped from replay stats. Already has
    /// `apply_storage_recovery(&outcome)` applied.
    pub(crate) coordinator: TransactionCoordinator,
    /// Watermark derived from replayed state (fresh) or persisted
    /// follower-state (restored).
    pub(crate) watermark: ContiguousWatermark,
    /// `Some(_)` when the lossy WAL-replay fallback fired; `None`
    /// otherwise.
    pub(crate) lossy_report: Option<LossyRecoveryReport>,
    /// `Some(_)` when a follower's persisted state passed validation
    /// and should be re-installed on the `Database`; always `None` for
    /// primary.
    pub(crate) persisted_follower_state: Option<PersistedFollowerState>,
}

impl Database {
    /// Unified recovery entry point.
    ///
    /// Orchestrates:
    /// 1. Configured-codec validation (before any recovery-managed
    ///    directory or MANIFEST creation).
    /// 2. MANIFEST load or primary-mode create.
    /// 3. WAL codec resolution (stored id on reopen, configured id on
    ///    first-open or follower-without-MANIFEST).
    /// 4. `SegmentedStore` construction at the segments directory using the
    ///    caller-supplied storage runtime config.
    /// 5. Storage-owned durability recovery via an engine primitive snapshot
    ///    install callback.
    /// 6. `TransactionCoordinator::from_recovery_with_limits` +
    ///    `coordinator.apply_storage_recovery`.
    /// 7. Follower-state restore (follower mode only).
    /// 8. Watermark construction.
    #[allow(clippy::too_many_lines)] // orchestrator: splitting further would scatter sequential state.
    pub(crate) fn run_recovery(
        canonical_path: &Path,
        layout: &DatabaseLayout,
        recovery_config: RecoveryRuntimeConfig<'_>,
        mode: RecoveryMode,
    ) -> Result<RecoveryOutcome, RecoveryError> {
        let cfg = recovery_config.strata_config();
        let runtime_config = recovery_config.storage_runtime_config();

        // 1-5. Storage-owned durability recovery. Storage owns MANIFEST/codec
        //      prep, generic WAL replay, the mechanical lossy fallback,
        //      runtime config application, and segment recovery. Engine keeps
        //      primitive snapshot decode and install telemetry in this
        //      callback.
        let snapshot_install = |snapshot: &LoadedSnapshot,
                                install_codec: &dyn StorageCodec,
                                storage: &SegmentedStore|
         -> Result<(), StorageError> {
            install_recovery_snapshot(snapshot, install_codec, storage)?;
            Ok(())
        };
        let storage_outcome = run_storage_recovery(StorageRecoveryInput::new(
            layout.clone(),
            mode.as_storage_mode(),
            cfg.storage.codec.clone(),
            runtime_config,
            cfg.allow_lossy_recovery,
            &snapshot_install,
        ))
        .map_err(|err| map_storage_recovery_error(canonical_path, mode, err))?;
        let database_uuid = storage_outcome.database_uuid;
        let wal_codec = storage_outcome.wal_codec;
        let stats = storage_outcome.wal_replay;
        let seg_outcome = storage_outcome.segment_recovery;
        let lossy_report = storage_outcome
            .lossy_wal_replay
            .map(|facts| storage_lossy_wal_replay_facts_to_report(facts, mode));

        info!(
            target: "strata::db",
            txns_replayed = stats.txns_replayed,
            writes_applied = stats.writes_applied,
            deletes_applied = stats.deletes_applied,
            final_version = stats.final_version.as_u64(),
            from_checkpoint = stats.from_checkpoint,
            mode = match mode {
                RecoveryMode::Primary => "primary",
                RecoveryMode::Follower => "follower",
            },
            "Recovery complete"
        );

        // 6. Coordinator bootstrap + storage recovery policy.
        let result = RecoveryResult {
            storage: storage_outcome.storage,
            stats,
        };
        let coordinator = TransactionCoordinator::from_recovery_with_limits(
            &result,
            cfg.storage.max_write_buffer_entries,
        );
        let storage = Arc::new(result.storage);

        if seg_outcome.segments_loaded > 0 {
            info!(
                target: "strata::db",
                branches = seg_outcome.branches_recovered,
                segments = seg_outcome.segments_loaded,
                "Recovered segments from disk"
            );
        }
        if let RecoveryHealth::Degraded { faults, class } = &seg_outcome.health {
            warn!(
                target: "strata::recovery::health",
                class = ?class,
                fault_count = faults.len(),
                "storage recovered with degraded state"
            );
        }
        // ES4 health-policy branch. Strict mode refuses authoritative
        // loss; the no-manifest legacy fallback is opt-in; rebuildable
        // caches are always accepted. Lossy recovery (`allow_lossy_recovery`)
        // is the blanket escape hatch and permits every class — it leaves
        // `LossyRecoveryReport` untouched because no WAL wipe occurred;
        // operators read the classification via `Database::recovery_health()`.
        if let RecoveryHealth::Degraded { class, .. } = &seg_outcome.health {
            if !degraded_recovery_permitted(
                *class,
                cfg.allow_missing_manifest,
                cfg.allow_lossy_recovery,
            ) {
                return Err(RecoveryError::StorageDegraded(seg_outcome.health.clone()));
            }
        }
        coordinator.apply_storage_recovery(&seg_outcome);

        // 7. Follower state restore.
        let persisted_follower_state = match mode {
            RecoveryMode::Primary => None,
            RecoveryMode::Follower => restore_follower_state(
                canonical_path,
                result.stats.max_txn_id,
                result.stats.final_version,
            ),
        };

        // 8. Watermark.
        let watermark = match persisted_follower_state.as_ref() {
            Some(follower_state) => ContiguousWatermark::from_state(
                follower_state.received_watermark,
                follower_state.applied_watermark,
                Some(follower_state.blocked.clone()),
            ),
            None => ContiguousWatermark::new(result.stats.max_txn_id),
        };

        Ok(RecoveryOutcome {
            database_uuid,
            wal_codec,
            storage,
            coordinator,
            watermark,
            lossy_report,
            persisted_follower_state,
        })
    }
}

/// ES4 strict-mode policy. `true` = refuse to open on this class; `false`
/// = accept. The caller combines this with `allow_lossy_recovery` so
/// that lossy mode is a blanket override regardless of class.
///
/// - `DataLoss` — authoritative storage lost (corrupt segment/manifest,
///   manifest-listed-but-missing segment, dropped inherited layer). Always
///   refused; only `allow_lossy_recovery` permits it.
/// - `PolicyDowngrade` — legacy-compatible fallback engaged (e.g.
///   no-manifest L0 promotion). Refused unless `allow_missing_manifest`
///   is set so operators consent explicitly to reading an unmanifested
///   database.
/// - `Telemetry` — rebuildable-cache failure. Never refused.
/// - Unknown future variants default to refusal (conservative for
///   `#[non_exhaustive]` evolution).
fn policy_refuses(class: DegradationClass, allow_missing_manifest: bool) -> bool {
    match class {
        DegradationClass::PolicyDowngrade => !allow_missing_manifest,
        DegradationClass::Telemetry => false,
        // `DegradationClass::DataLoss` and any future #[non_exhaustive]
        // variant: refuse. Only `allow_lossy_recovery` overrides refusal
        // (applied by the caller).
        _ => true,
    }
}

fn degraded_recovery_permitted(
    class: DegradationClass,
    allow_missing_manifest: bool,
    allow_lossy_recovery: bool,
) -> bool {
    allow_lossy_recovery || !policy_refuses(class, allow_missing_manifest)
}

fn map_storage_recovery_error(
    canonical_path: &Path,
    mode: RecoveryMode,
    err: StorageRecoveryError,
) -> RecoveryError {
    let role = mode.as_error_role();
    match err {
        StorageRecoveryError::CodecInit { codec_id, detail } => {
            RecoveryError::CodecInit { codec_id, detail }
        }
        StorageRecoveryError::ManifestLoad { path, source } => {
            classify_manifest_load_error(path, role, source)
        }
        StorageRecoveryError::ManifestCreate { source } => RecoveryError::ManifestCreate(source),
        StorageRecoveryError::ManifestCodecMismatch { stored, configured } => {
            RecoveryError::ManifestCodecMismatch {
                stored,
                configured,
                db_path: canonical_path.to_path_buf(),
                role,
            }
        }
        StorageRecoveryError::Io { source } => RecoveryError::Io(source),
        StorageRecoveryError::Coordinator { source } => from_coordinator_error(role, source),
        StorageRecoveryError::SegmentRecovery { source } => RecoveryError::Storage(source),
        _ => RecoveryError::WalRecoveryFailed {
            role,
            inner: StrataError::internal(format!("unknown storage recovery error: {err}")),
        },
    }
}

/// Install one loaded recovery snapshot through the engine-owned primitive
/// decode path.
fn install_recovery_snapshot(
    snapshot: &LoadedSnapshot,
    install_codec: &dyn StorageCodec,
    storage: &SegmentedStore,
) -> Result<InstallStats, StorageError> {
    let installed =
        install_snapshot(snapshot, install_codec, storage).map_err(StorageError::from)?;
    log_recovery_snapshot_install(snapshot, &installed);
    Ok(installed)
}

fn log_recovery_snapshot_install(snapshot: &LoadedSnapshot, installed: &InstallStats) {
    info!(
        target: "strata::recovery",
        snapshot_id = snapshot.snapshot_id(),
        watermark = snapshot.watermark_txn(),
        entries = installed.total_installed(),
        kv = installed.kv,
        graph = installed.graph,
        events = installed.events,
        json = installed.json,
        vector_configs = installed.vector_configs,
        vectors = installed.vectors,
        branches = installed.branches,
        sections_skipped = installed.sections_skipped,
        "Installed snapshot into SegmentedStore"
    );
}

fn storage_lossy_wal_replay_facts_to_report(
    facts: StorageLossyWalReplayFacts,
    mode: RecoveryMode,
) -> LossyRecoveryReport {
    let records_applied_before_failure = facts.records_applied_before_failure;
    let version_reached_before_failure = facts.version_reached_before_failure;
    let discarded_on_wipe = facts.discarded_on_wipe;
    let err = coordinator_error_to_lossy_strata_error(facts.source);
    let error_kind = LossyErrorKind::from_strata_error(&err);
    let report = LossyRecoveryReport {
        error: err.to_string(),
        error_kind,
        records_applied_before_failure,
        version_reached_before_failure,
        discarded_on_wipe,
    };
    let follower_flag = matches!(mode, RecoveryMode::Follower);
    warn!(
        target: "strata::recovery::lossy",
        error = %err,
        error_kind = %report.error_kind,
        records_applied_before_failure = report.records_applied_before_failure,
        version_reached_before_failure = report.version_reached_before_failure.as_u64(),
        discarded_on_wipe = report.discarded_on_wipe,
        follower = follower_flag,
        "Lossy recovery fallback — discarding pre-failure state"
    );
    let db_target_message = if follower_flag {
        "Follower recovery failed — starting with empty state"
    } else {
        "Recovery failed — starting with empty state (allow_lossy_recovery=true)"
    };
    warn!(target: "strata::db", error = %err, "{}", db_target_message);

    report
}

fn coordinator_error_to_lossy_strata_error(err: CoordinatorRecoveryError) -> StrataError {
    match err {
        CoordinatorRecoveryError::Plan(CoordinatorPlanError::Manifest(
            ManifestError::LegacyFormat {
                detected_version,
                supported_range,
                remediation,
            },
        )) => StrataError::legacy_format(
            detected_version,
            format!("{supported_range}. {remediation}"),
        ),
        CoordinatorRecoveryError::Plan(CoordinatorPlanError::Manifest(inner)) => {
            StrataError::corruption(format!("failed to load MANIFEST: {inner}"))
        }
        CoordinatorRecoveryError::Plan(CoordinatorPlanError::CodecMismatch { stored, expected }) => {
            StrataError::incompatible_reuse(format!(
                "codec mismatch: database was created with '{stored}' but config specifies '{expected}'. \
                 A database cannot be reopened with a different codec."
            ))
        }
        CoordinatorRecoveryError::SnapshotMissing { snapshot_id, path } => {
            StrataError::corruption(format!(
                "MANIFEST references snapshot {snapshot_id} but {} is missing",
                path.display()
            ))
        }
        CoordinatorRecoveryError::SnapshotRead {
            snapshot_id: _,
            path: _,
            source:
                SnapshotReadError::LegacyFormat {
                    detected_version,
                    supported_range,
                    remediation,
                },
        } => StrataError::legacy_format(
            detected_version,
            format!("{supported_range}. {remediation}"),
        ),
        CoordinatorRecoveryError::SnapshotRead {
            snapshot_id,
            path,
            source,
        } => StrataError::corruption(format!(
            "failed to load snapshot {snapshot_id} at {}: {source}",
            path.display()
        )),
        CoordinatorRecoveryError::WalRead(WalReaderError::CodecDecode { detail, .. }) => {
            StrataError::codec_decode(detail)
        }
        CoordinatorRecoveryError::WalRead(WalReaderError::LegacyFormat {
            found_version,
            hint,
        }) => StrataError::legacy_format(found_version, hint),
        CoordinatorRecoveryError::WalRead(inner) => {
            StrataError::storage(format!("WAL read failed: {inner}"))
        }
        CoordinatorRecoveryError::PayloadDecode { txn_id, detail } => {
            StrataError::storage(format!(
                "Failed to decode transaction payload for txn {txn_id}: {detail}"
            ))
        }
        CoordinatorRecoveryError::Callback(inner) => StrataError::from(inner),
        _ => StrataError::internal("unknown coordinator recovery error"),
    }
}

/// Load and validate the persisted follower blocked-state slot. On
/// validation failure the slot is cleared so a restart does not
/// re-observe the inconsistent state.
///
/// Mirrors the previous follower-state restoration block from `open.rs`.
fn restore_follower_state(
    canonical_path: &Path,
    recovered_max_txn: strata_core::id::TxnId,
    recovered_final_version: CommitVersion,
) -> Option<PersistedFollowerState> {
    let loaded = match load_persisted_follower_state(canonical_path) {
        Ok(state) => state,
        Err(e) => {
            warn!(
                target: "strata::db",
                error = %e,
                "Failed to load persisted follower state"
            );
            return None;
        }
    };
    let state = loaded?;
    match validate_blocked_state(&state, recovered_max_txn, recovered_final_version) {
        Ok(()) => Some(state),
        Err(reason) => {
            warn!(
                target: "strata::db",
                received = state.received_watermark.as_u64(),
                applied = state.applied_watermark.as_u64(),
                visible_version = state.visible_version.as_u64(),
                blocked_txn = state.blocked.blocked.txn_id.as_u64(),
                recovered_txn = recovered_max_txn.as_u64(),
                recovered_version = recovered_final_version.as_u64(),
                reason = %reason,
                "Ignoring inconsistent persisted follower state"
            );
            if let Err(error) = clear_persisted_follower_state(canonical_path) {
                warn!(
                    target: "strata::db",
                    error = %error,
                    "Failed to clear inconsistent persisted follower state"
                );
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::StorageConfig;
    use super::super::refresh::{persist_follower_state, BlockReason, BlockedTxn, BlockedTxnState};
    use super::*;
    use serial_test::serial;
    use strata_core::id::TxnId;
    use strata_core::{BranchId, Value};
    use strata_storage::durability::codec::IdentityCodec;
    use strata_storage::durability::format::{Manifest, WalRecord};
    use strata_storage::durability::wal::{DurabilityMode, WalConfig, WalWriter};
    use strata_storage::durability::{
        primitive_tags, KvSnapshotEntry, LoadedSection, SnapshotHeader, SnapshotSection,
        SnapshotSerializer, SnapshotWriter, TransactionPayload,
    };
    use strata_storage::{Key, Namespace, TypeTag};
    use tempfile::TempDir;

    const TEST_AES_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = self.previous.take() {
                std::env::set_var(self.name, value);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }

    fn run_recovery_for_test(
        canonical_path: &Path,
        layout: &DatabaseLayout,
        cfg: &StrataConfig,
        mode: RecoveryMode,
    ) -> Result<RecoveryOutcome, RecoveryError> {
        Database::run_recovery(
            canonical_path,
            layout,
            RecoveryRuntimeConfig::from_strata_config(cfg),
            mode,
        )
    }

    #[test]
    fn snapshot_plan_failure_bypasses_lossy_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        layout.create_segments_dir().unwrap();
        write_db_manifest_with_snapshot(&layout, [0x11; 16], "identity", 404, TxnId(99));
        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let result = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary);

        match result {
            Err(RecoveryError::SnapshotMissing {
                role: ErrorRole::Primary,
                snapshot_id: 404,
                ..
            }) => {}
            Err(other) => {
                panic!("snapshot plan failure mapped to the wrong error: {other:?}");
            }
            Ok(_) => {
                panic!("snapshot plan failure must hard-fail without lossy wipe");
            }
        }
    }

    #[test]
    #[serial(open_databases)]
    fn primary_first_open_creates_manifest_with_configured_codec() {
        let _key_guard = EnvVarGuard::set("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let cfg = StrataConfig {
            storage: StorageConfig {
                codec: "aes-gcm-256".to_string(),
                ..StorageConfig::default()
            },
            ..StrataConfig::default()
        };

        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary)
            .expect("primary first-open recovery should succeed");

        assert!(
            layout.manifest_path().exists(),
            "primary first-open must create MANIFEST"
        );
        assert!(
            layout.segments_dir().exists(),
            "primary first-open must create the storage segments dir"
        );
        let manifest = read_db_manifest(&layout);
        assert_eq!(
            manifest.codec_id, cfg.storage.codec,
            "first-open MANIFEST must record the configured storage codec"
        );
        assert_eq!(outcome.database_uuid, manifest.database_uuid);
        assert_ne!(
            outcome.database_uuid, [0u8; 16],
            "primary first-open must allocate a real database UUID"
        );
        assert_eq!(outcome.wal_codec.codec_id(), cfg.storage.codec);
        assert!(outcome.lossy_report.is_none());
        assert!(outcome.persisted_follower_state.is_none());
    }

    #[test]
    fn recovery_applies_storage_runtime_config() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let cfg = StrataConfig {
            storage: StorageConfig {
                max_branches: 31,
                max_versions_per_key: 7,
                write_buffer_size: 512 * 1024,
                max_immutable_memtables: 3,
                target_file_size: 3 * 1024 * 1024,
                level_base_bytes: 11 * 1024 * 1024,
                data_block_size: 8 * 1024,
                bloom_bits_per_key: 13,
                compaction_rate_limit: 5 * 1024 * 1024,
                ..StorageConfig::default()
            },
            ..StrataConfig::default()
        };

        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary)
            .expect("recovery should succeed with non-default storage config");

        assert_eq!(
            outcome.storage.write_buffer_size_for_test(),
            cfg.storage.effective_write_buffer_size()
        );
        assert_eq!(
            outcome.storage.max_branches_for_test(),
            cfg.storage.max_branches
        );
        assert_eq!(
            outcome.storage.max_versions_per_key_for_test(),
            cfg.storage.max_versions_per_key
        );
        assert_eq!(
            outcome.storage.max_immutable_memtables_for_test(),
            cfg.storage.effective_max_immutable_memtables()
        );
        assert_eq!(
            outcome.storage.target_file_size(),
            cfg.storage.target_file_size
        );
        assert_eq!(
            outcome.storage.level_base_bytes(),
            cfg.storage.level_base_bytes
        );
        assert_eq!(
            outcome.storage.data_block_size(),
            cfg.storage.data_block_size
        );
        assert_eq!(
            outcome.storage.bloom_bits_per_key(),
            cfg.storage.bloom_bits_per_key
        );
        assert_eq!(
            outcome.storage.compaction_rate_limit_for_test(),
            cfg.storage.compaction_rate_limit
        );
    }

    #[test]
    fn follower_without_manifest_does_not_create_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let cfg = StrataConfig::default();

        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Follower)
            .expect("follower without MANIFEST should fall back to WAL-only recovery");

        assert!(
            !layout.manifest_path().exists(),
            "follower recovery must not create a missing MANIFEST"
        );
        assert!(
            layout.segments_dir().exists(),
            "follower recovery may create the storage segments dir"
        );
        assert_eq!(
            outcome.database_uuid, [0u8; 16],
            "follower without MANIFEST preserves the sentinel UUID"
        );
        assert_eq!(
            outcome.wal_codec.codec_id(),
            cfg.storage.codec,
            "follower without MANIFEST uses the configured WAL codec"
        );
        assert!(outcome.lossy_report.is_none());
        assert!(outcome.persisted_follower_state.is_none());
    }

    #[test]
    fn follower_recovery_clears_invalid_persisted_state_after_replay() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let cfg = StrataConfig::default();
        let stale_state = PersistedFollowerState {
            received_watermark: TxnId(50),
            applied_watermark: TxnId(10),
            visible_version: CommitVersion(10),
            blocked: BlockedTxnState {
                blocked: BlockedTxn {
                    txn_id: TxnId(11),
                    reason: BlockReason::Decode {
                        message: "stale blocked record".to_string(),
                    },
                },
                visibility_version: None,
                skip_allowed: true,
            },
        };
        persist_follower_state(temp_dir.path(), &stale_state)
            .expect("test should persist follower state");
        assert!(load_persisted_follower_state(temp_dir.path())
            .unwrap()
            .is_some());

        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Follower)
            .expect("follower recovery should ignore invalid persisted state");

        assert!(outcome.persisted_follower_state.is_none());
        assert!(load_persisted_follower_state(temp_dir.path())
            .unwrap()
            .is_none());
        assert_eq!(outcome.watermark.applied(), TxnId(0));
    }

    #[test]
    fn snapshot_versions_fold_into_coordinator_before_bootstrap() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x44; 16];
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "snapshot-version-fold");
        let cfg = StrataConfig::default();

        layout.create_segments_dir().unwrap();
        let writer = SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            database_uuid,
        )
        .unwrap();
        writer
            .create_snapshot(
                11,
                9,
                vec![kv_snapshot_section(
                    branch_id,
                    "snapshot-version-fold",
                    Value::String("from-snapshot".into()),
                    73,
                )],
            )
            .unwrap();
        write_db_manifest_with_snapshot(&layout, database_uuid, "identity", 11, TxnId(9));

        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary)
            .expect("snapshot-only recovery should succeed");

        assert_eq!(
            outcome.storage.current_version(),
            CommitVersion(73),
            "snapshot install should advance storage current_version"
        );
        assert_eq!(
            outcome.coordinator.current_version(),
            CommitVersion(73),
            "coordinator bootstrap must see the snapshot-version fold"
        );
        assert_eq!(
            outcome.coordinator.visible_version(),
            CommitVersion(73),
            "visible version should start at the folded snapshot version"
        );
        let recovered = outcome
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("snapshot row should be visible after recovery");
        assert_eq!(recovered.value, Value::String("from-snapshot".into()));
    }

    #[test]
    fn wal_replay_success_bootstraps_storage_and_coordinator() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "wal-success-bootstrap");
        let cfg = StrataConfig::default();
        write_wal_txn(
            layout.wal_dir(),
            TxnId(17),
            branch_id,
            vec![(key.clone(), Value::String("from-wal".to_string()))],
            Vec::new(),
            23,
        );

        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary)
            .expect("WAL-only recovery should succeed");

        assert_eq!(outcome.storage.current_version(), CommitVersion(23));
        assert_eq!(outcome.coordinator.current_version(), CommitVersion(23));
        assert_eq!(outcome.watermark.applied(), TxnId(17));
        assert!(outcome.lossy_report.is_none());
        let recovered = outcome
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("WAL row should be visible after recovery");
        assert_eq!(recovered.value, Value::String("from-wal".to_string()));
    }

    #[test]
    fn wal_legacy_failure_bypasses_lossy_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        std::fs::create_dir_all(layout.wal_dir()).unwrap();
        std::fs::write(
            layout.wal_dir().join("wal-000001.seg"),
            legacy_wal_segment_header(1),
        )
        .unwrap();
        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let result = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary);

        match result {
            Err(RecoveryError::WalLegacyFormat {
                found_version,
                hint,
            }) => {
                assert_eq!(found_version, 1);
                assert!(hint.contains("wal/"));
            }
            Err(other) => panic!("legacy WAL mapped to the wrong error: {other:?}"),
            Ok(_) => panic!("legacy WAL must hard-fail without lossy wipe"),
        }
    }

    #[test]
    fn lossy_replay_discards_partial_storage_and_reports_progress() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let branch_id = BranchId::new();
        let key = kv_key(branch_id, "discarded-before-corruption");
        write_wal_txn(
            layout.wal_dir(),
            TxnId(23),
            branch_id,
            vec![(key.clone(), Value::String("partial".into()))],
            Vec::new(),
            19,
        );
        std::fs::write(
            layout.wal_dir().join("wal-000099.seg"),
            b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER",
        )
        .unwrap();
        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let outcome = run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary)
            .expect("lossy recovery should convert replay failure into an empty recovered store");

        let report = outcome
            .lossy_report
            .expect("lossy fallback must report discarded progress");
        assert_eq!(report.records_applied_before_failure, 1);
        assert_eq!(report.version_reached_before_failure, CommitVersion(19));
        assert!(report.discarded_on_wipe);
        assert!(
            report.error.contains("WAL read failed"),
            "report should render the coordinator failure, got: {}",
            report.error
        );
        assert!(
            outcome
                .storage
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap()
                .is_none(),
            "lossy fallback must replace partial storage with an empty store"
        );
        assert_eq!(outcome.storage.current_version(), CommitVersion::ZERO);
    }

    #[test]
    fn degraded_storage_policy_matrix_is_engine_owned() {
        assert!(
            policy_refuses(DegradationClass::DataLoss, false),
            "strict recovery refuses authoritative data loss"
        );
        assert!(
            policy_refuses(DegradationClass::DataLoss, true),
            "allow_missing_manifest does not permit data loss"
        );
        assert!(
            policy_refuses(DegradationClass::PolicyDowngrade, false),
            "policy downgrade is refused unless explicitly allowed"
        );
        assert!(
            !policy_refuses(DegradationClass::PolicyDowngrade, true),
            "allow_missing_manifest permits the legacy no-MANIFEST fallback"
        );
        assert!(
            !policy_refuses(DegradationClass::Telemetry, false),
            "telemetry degradation is accepted in strict mode"
        );
        assert!(
            !policy_refuses(DegradationClass::Telemetry, true),
            "telemetry degradation is independent of missing-MANIFEST policy"
        );
        assert!(
            degraded_recovery_permitted(DegradationClass::DataLoss, false, true),
            "lossy recovery permits authoritative data-loss degradation"
        );
        assert!(
            degraded_recovery_permitted(DegradationClass::PolicyDowngrade, false, true),
            "lossy recovery overrides the missing-MANIFEST policy gate"
        );
        assert!(
            degraded_recovery_permitted(DegradationClass::PolicyDowngrade, true, false),
            "allow_missing_manifest permits only the policy downgrade class"
        );
        assert!(
            !degraded_recovery_permitted(DegradationClass::DataLoss, true, false),
            "allow_missing_manifest does not permit authoritative data loss"
        );
    }

    #[test]
    fn recovery_snapshot_install_maps_engine_errors_into_storage_callback_errors() {
        let snapshot = loaded_snapshot("other-codec", Vec::new());
        let storage = SegmentedStore::new();
        let codec = IdentityCodec;

        let err = install_recovery_snapshot(&snapshot, &codec, &storage)
            .expect_err("codec mismatch should surface as callback storage error");

        match err {
            StorageError::Corruption { message } => {
                assert!(message.contains("Snapshot codec mismatch during install"));
            }
            other => panic!("expected corruption storage error, got {other:?}"),
        }
    }

    #[test]
    fn recovery_snapshot_install_returns_primitive_stats_after_storage_install() {
        let branch_id = BranchId::new();
        let snapshot = loaded_snapshot(
            "identity",
            vec![kv_section(branch_id, "installed", Value::Int(42), 13)],
        );
        let storage = SegmentedStore::new();
        let codec = IdentityCodec;

        let installed = install_recovery_snapshot(&snapshot, &codec, &storage)
            .expect("snapshot install should succeed");

        assert_eq!(installed.kv, 1);
        assert_eq!(installed.graph, 0);
        assert_eq!(installed.total_installed(), 1);

        let value = storage
            .get_versioned(&kv_key(branch_id, "installed"), CommitVersion::MAX)
            .unwrap()
            .expect("snapshot row should be installed");
        assert_eq!(value.value, Value::Int(42));
    }

    #[test]
    fn recovery_snapshot_install_failure_maps_through_callback_public_error_path() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x55; 16];
        let writer = SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            database_uuid,
        )
        .unwrap();
        writer
            .create_snapshot(
                3,
                9,
                vec![SnapshotSection::new(primitive_tags::KV, vec![1, 0, 0, 0])],
            )
            .unwrap();
        write_db_manifest_with_snapshot(&layout, database_uuid, "identity", 3, TxnId(9));

        let cfg = StrataConfig::default();
        let mapped =
            match run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary) {
                Err(err) => err,
                Ok(_) => panic!("callback error should map to public recovery error"),
            };
        match mapped {
            RecoveryError::WalRecoveryFailed {
                role: ErrorRole::Primary,
                inner: StrataError::Corruption { message },
            } => {
                assert!(message.contains("KV section decode failed"));
            }
            other => panic!("expected primary WalRecoveryFailed corruption, got {other:?}"),
        }
    }

    #[test]
    fn recovery_snapshot_install_failure_bypasses_lossy_fallback_when_enabled() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let database_uuid = [0x56; 16];
        let writer = SnapshotWriter::new(
            layout.snapshots_dir().to_path_buf(),
            Box::new(IdentityCodec),
            database_uuid,
        )
        .unwrap();
        writer
            .create_snapshot(
                4,
                10,
                vec![SnapshotSection::new(primitive_tags::KV, vec![1, 0, 0, 0])],
            )
            .unwrap();
        write_db_manifest_with_snapshot(&layout, database_uuid, "identity", 4, TxnId(10));

        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let mapped =
            match run_recovery_for_test(temp_dir.path(), &layout, &cfg, RecoveryMode::Primary) {
                Err(err) => err,
                Ok(_) => panic!("snapshot install failure must not lossy-open as empty storage"),
            };
        match mapped {
            RecoveryError::WalRecoveryFailed {
                role: ErrorRole::Primary,
                inner: StrataError::Corruption { message },
            } => {
                assert!(message.contains("KV section decode failed"));
            }
            other => panic!("expected primary WalRecoveryFailed corruption, got {other:?}"),
        }
    }

    fn loaded_snapshot(codec_id: &str, sections: Vec<LoadedSection>) -> LoadedSnapshot {
        LoadedSnapshot {
            header: SnapshotHeader::new(7, 7, 1, [0x77; 16], codec_id.len() as u8),
            codec_id: codec_id.to_string(),
            sections,
            crc: 0,
        }
    }

    fn kv_section(branch_id: BranchId, key: &str, value: Value, version: u64) -> LoadedSection {
        let section = kv_snapshot_section(branch_id, key, value, version);
        LoadedSection {
            primitive_type: section.primitive_type,
            data: section.data,
        }
    }

    fn kv_snapshot_section(
        branch_id: BranchId,
        key: &str,
        value: Value,
        version: u64,
    ) -> SnapshotSection {
        let serializer = SnapshotSerializer::canonical_primitive_section();
        let entries = [KvSnapshotEntry {
            branch_id: *branch_id.as_bytes(),
            space: "default".to_string(),
            type_tag: TypeTag::KV.as_byte(),
            user_key: key.as_bytes().to_vec(),
            value: serde_json::to_vec(&value).expect("test value should serialize"),
            version,
            timestamp: 1_000,
            ttl_ms: 0,
            is_tombstone: false,
        }];
        SnapshotSection::new(primitive_tags::KV, serializer.serialize_kv(&entries))
    }

    fn kv_key(branch_id: BranchId, key: &str) -> Key {
        Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), key)
    }

    fn write_db_manifest_with_snapshot(
        layout: &DatabaseLayout,
        database_uuid: [u8; 16],
        codec_id: &str,
        snapshot_id: u64,
        watermark_txn: TxnId,
    ) {
        let mut manifest = Manifest::new(database_uuid, codec_id.to_string());
        manifest.snapshot_id = Some(snapshot_id);
        manifest.snapshot_watermark = Some(watermark_txn.as_u64());
        std::fs::write(layout.manifest_path(), manifest.to_bytes())
            .expect("test should write database MANIFEST");
    }

    fn read_db_manifest(layout: &DatabaseLayout) -> Manifest {
        let bytes = std::fs::read(layout.manifest_path()).expect("test MANIFEST should exist");
        Manifest::from_bytes(&bytes).expect("test MANIFEST should parse")
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

    fn legacy_wal_segment_header(segment_number: u64) -> [u8; 36] {
        let mut header = [0u8; 36];
        header[0..4].copy_from_slice(b"STRA");
        header[4..8].copy_from_slice(&1u32.to_le_bytes());
        header[8..16].copy_from_slice(&segment_number.to_le_bytes());
        header[16..32].copy_from_slice(&[0xAA; 16]);
        let header_crc = crc32fast::hash(&header[0..32]);
        header[32..36].copy_from_slice(&header_crc.to_le_bytes());
        header
    }
}
