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
//! branches on `RecoveryHealth::Degraded` inside the storage step, and the
//! WAL-replay lossy fallback preserves the existing runtime behavior.
//!
//! # First-open ordering
//!
//! Configured-codec validation runs **before** any recovery-managed directory
//! is created or MANIFEST is written. Without that ordering, a fresh database
//! with an invalid codec id would persist a poisoned directory that the next
//! open could not repair.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::StrataError;
use strata_core::id::CommitVersion;
use strata_storage::durability::codec::{clone_codec, StorageCodec};
use strata_storage::durability::layout::DatabaseLayout;
use strata_storage::durability::wal::WalReaderError;
use strata_storage::durability::{
    apply_wal_record_to_memory_storage, CoordinatorPlanError, CoordinatorRecoveryError,
    LoadedSnapshot, ManifestError, ManifestManager, RecoveryCoordinator, RecoveryResult,
    RecoveryStats, SnapshotReadError,
};
use strata_storage::{DegradationClass, RecoveryHealth, SegmentedStore, StorageError};
use tracing::{info, warn};

use super::config::StrataConfig;
use super::open::{apply_storage_config, restrict_dir};
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
}

/// Output of [`Database::run_recovery`].
///
/// Carries the owned resources the caller needs to finish constructing
/// `Arc<Database>`. All public fields are `pub(crate)` because
/// `RecoveryOutcome` is an implementation detail of `open.rs`; it is
/// not part of the D4 public surface.
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
    /// 4. `SegmentedStore` construction at the segments directory.
    /// 5. `RecoveryCoordinator::recover` — snapshot install + WAL
    ///    replay via caller-supplied closures.
    /// 6. WAL-replay lossy fallback.
    /// 7. Snapshot-version fold.
    /// 8. `TransactionCoordinator::from_recovery_with_limits` +
    ///    `apply_storage_config`.
    /// 9. `SegmentedStore::recover_segments` → classified outcome →
    ///    `coordinator.apply_storage_recovery`.
    /// 10. Follower-state restore (follower mode only).
    /// 11. Watermark construction.
    #[allow(clippy::too_many_lines)] // orchestrator: splitting further would scatter sequential state.
    pub(crate) fn run_recovery(
        canonical_path: &Path,
        layout: &DatabaseLayout,
        cfg: &StrataConfig,
        mode: RecoveryMode,
    ) -> Result<RecoveryOutcome, RecoveryError> {
        // 1. Configured-codec validation before any recovery-managed
        //    directory or MANIFEST creation — preserves the pre-D3
        //    first-open safety guard.
        strata_storage::durability::get_codec(&cfg.storage.codec).map_err(|e| {
            RecoveryError::CodecInit {
                codec_id: cfg.storage.codec.clone(),
                detail: e.to_string(),
            }
        })?;

        // 2. MANIFEST load or create.
        let ManifestPreparation {
            database_uuid,
            install_codec_for_snapshot,
        } = prepare_manifest(canonical_path, layout, cfg, mode)?;

        // 3. WAL codec resolution. On reopen we fetch by stored id;
        //    on first-open / follower-without-MANIFEST we fetch by
        //    config id (already validated in step 1).
        let wal_codec_id = install_codec_for_snapshot
            .as_ref()
            .map_or(cfg.storage.codec.as_str(), |c| c.codec_id());
        let wal_codec = strata_storage::durability::get_codec(wal_codec_id).map_err(|e| {
            RecoveryError::CodecInit {
                codec_id: wal_codec_id.to_owned(),
                detail: e.to_string(),
            }
        })?;

        // 4. SegmentedStore at the segments directory.
        let mut storage = SegmentedStore::with_dir(
            layout.segments_dir().to_path_buf(),
            cfg.storage.effective_write_buffer_size(),
        );

        // 5. RecoveryCoordinator::recover via callbacks.
        let records_applied_before_failure = Arc::new(AtomicU64::new(0));
        let recover_result = run_coordinator_recovery(
            layout,
            cfg,
            wal_codec.as_ref(),
            install_codec_for_snapshot.as_deref(),
            &storage,
            &records_applied_before_failure,
        );

        // 6. Lossy fallback on WAL-replay error.
        let (mut stats, lossy_report) = handle_wal_recovery_outcome(
            recover_result,
            cfg,
            layout,
            &mut storage,
            records_applied_before_failure.load(Ordering::SeqCst),
            mode,
        )?;

        // 7. Snapshot-version fold.
        stats.final_version = stats.final_version.max(CommitVersion(storage.version()));

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

        // 8. Coordinator bootstrap + storage config.
        let result = RecoveryResult { storage, stats };
        let coordinator = TransactionCoordinator::from_recovery_with_limits(
            &result,
            cfg.storage.max_write_buffer_entries,
        );
        let storage = Arc::new(result.storage);
        apply_storage_config(&storage, &cfg.storage);

        // 9. Storage recovery (SE2 outcome). D3 plumbs only; D4 adds
        //    the health-policy branch between these two lines.
        let seg_outcome = storage.recover_segments().map_err(RecoveryError::from)?;
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
        // D4 health-policy branch. Strict mode refuses authoritative
        // loss; the no-manifest legacy fallback is opt-in; rebuildable
        // caches are always accepted. Lossy recovery (`allow_lossy_recovery`)
        // is the blanket escape hatch and permits every class — it leaves
        // `LossyRecoveryReport` untouched because no WAL wipe occurred;
        // operators read the classification via `Database::recovery_health()`.
        if let RecoveryHealth::Degraded { class, .. } = &seg_outcome.health {
            let permitted =
                cfg.allow_lossy_recovery || !policy_refuses(*class, cfg.allow_missing_manifest);
            if !permitted {
                return Err(RecoveryError::StorageDegraded(seg_outcome.health.clone()));
            }
        }
        coordinator.apply_storage_recovery(&seg_outcome);

        // 10. Follower state restore.
        let persisted_follower_state = match mode {
            RecoveryMode::Primary => None,
            RecoveryMode::Follower => restore_follower_state(
                canonical_path,
                result.stats.max_txn_id,
                result.stats.final_version,
            ),
        };

        // 11. Watermark.
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

/// D4 strict-mode policy. `true` = refuse to open on this class; `false`
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

/// Output of [`prepare_manifest`].
struct ManifestPreparation {
    database_uuid: [u8; 16],
    /// `Some(clone)` when an on-disk MANIFEST exists and its codec is
    /// available; `None` for a primary first-open (no MANIFEST yet)
    /// and for follower-without-MANIFEST where the coordinator falls
    /// back to WAL-only recovery. Those `None` cases do not have a
    /// manifest-recorded snapshot, so the snapshot callback must not fire.
    /// If it does fire without a codec, recovery hard-fails instead of
    /// silently skipping snapshot install.
    install_codec_for_snapshot: Option<Box<dyn StorageCodec>>,
}

/// MANIFEST load or create. Primary mode creates on absent; follower
/// mode defers (mirrors `open.rs:479-491` pre-D3 behaviour).
///
/// Error mapping preserves the pre-D3 variants: codec mismatch →
/// `IncompatibleReuse`, parse failure → `Corruption`, create failure
/// → `Internal`, codec-init failure → `Internal`.
fn prepare_manifest(
    canonical_path: &Path,
    layout: &DatabaseLayout,
    cfg: &StrataConfig,
    mode: RecoveryMode,
) -> Result<ManifestPreparation, RecoveryError> {
    let manifest_path = layout.manifest_path().to_path_buf();
    let manifest_exists = ManifestManager::exists(&manifest_path);
    let role = mode.as_error_role();

    if manifest_exists {
        let mgr = ManifestManager::load(manifest_path.clone())
            .map_err(|e| classify_manifest_load_error(manifest_path.clone(), role, e))?;
        let manifest = mgr.manifest();
        if manifest.codec_id != cfg.storage.codec {
            return Err(RecoveryError::ManifestCodecMismatch {
                stored: manifest.codec_id.clone(),
                configured: cfg.storage.codec.clone(),
                db_path: canonical_path.to_path_buf(),
                role,
            });
        }
        let codec = strata_storage::durability::get_codec(&manifest.codec_id).map_err(|e| {
            RecoveryError::CodecInit {
                codec_id: manifest.codec_id.clone(),
                detail: e.to_string(),
            }
        })?;
        return Ok(ManifestPreparation {
            database_uuid: manifest.database_uuid,
            install_codec_for_snapshot: Some(codec),
        });
    }

    match mode {
        RecoveryMode::Primary => {
            // First-open: create segments dir + MANIFEST. Codec was
            // already validated in `run_recovery` step 1.
            layout.create_segments_dir().map_err(RecoveryError::Io)?;
            restrict_dir(layout.segments_dir());
            let uuid = *uuid::Uuid::new_v4().as_bytes();
            ManifestManager::create(
                layout.manifest_path().to_path_buf(),
                uuid,
                cfg.storage.codec.clone(),
            )
            .map_err(RecoveryError::ManifestCreate)?;
            Ok(ManifestPreparation {
                database_uuid: uuid,
                install_codec_for_snapshot: None,
            })
        }
        RecoveryMode::Follower => {
            // Follower-without-MANIFEST falls back to WAL-only
            // recovery. We still ensure the segments dir exists so
            // `SegmentedStore::with_dir` has something to read.
            if !layout
                .segments_dir()
                .try_exists()
                .map_err(RecoveryError::Io)?
            {
                layout.create_segments_dir().map_err(RecoveryError::Io)?;
                restrict_dir(layout.segments_dir());
            }
            Ok(ManifestPreparation {
                database_uuid: [0u8; 16],
                install_codec_for_snapshot: None,
            })
        }
    }
}

/// Drive the coordinator's callback-based recovery (`plan_recovery`,
/// snapshot install, WAL replay) and return its typed coordinator error.
///
/// Error classification into `RecoveryError` is deferred to
/// [`handle_wal_recovery_outcome`] so the lossy-fallback arm can run
/// `LossyErrorKind::from_strata_error(&e)` on the original
/// `StrataError` before the engine maps the failure into a typed
/// `RecoveryError`.
fn run_coordinator_recovery(
    layout: &DatabaseLayout,
    cfg: &StrataConfig,
    wal_codec: &dyn StorageCodec,
    install_codec: Option<&dyn StorageCodec>,
    storage: &SegmentedStore,
    records_counter: &Arc<AtomicU64>,
) -> Result<RecoveryStats, CoordinatorRecoveryError> {
    let recovery =
        RecoveryCoordinator::new(layout.clone(), cfg.storage.effective_write_buffer_size())
            .with_lossy_recovery(cfg.allow_lossy_recovery)
            .with_codec(clone_codec(wal_codec));

    let counter = Arc::clone(records_counter);
    recovery.recover_typed(
        |snapshot| {
            install_recovery_snapshot(&snapshot, install_codec, storage)?;
            Ok(())
        },
        |record| {
            let result = apply_wal_record_to_memory_storage(storage, record);
            if result.is_ok() {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            result
        },
    )
}

fn install_recovery_snapshot(
    snapshot: &LoadedSnapshot,
    install_codec: Option<&dyn StorageCodec>,
    storage: &SegmentedStore,
) -> Result<InstallStats, StorageError> {
    let Some(codec) = install_codec else {
        return Err(StorageError::corruption(format!(
            "snapshot {} reached recovery install callback without an install codec",
            snapshot.snapshot_id()
        )));
    };

    let installed = install_snapshot(snapshot, codec, storage).map_err(StorageError::from)?;
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

/// Classify the outcome of `RecoveryCoordinator::recover` and apply
/// the WAL-replay lossy fallback when configured.
///
/// - `Ok(stats)` → `(stats, None)`.
/// - `Err(CoordinatorRecoveryError)` from the coordinator's planning step or
///   carrying a legacy-format source is a hard-fail regardless of
///   `allow_lossy_recovery`. The lossy wipe only recreates in-memory
///   `SegmentedStore` state; it cannot heal a MANIFEST/snapshot plan failure
///   and would re-observe legacy on-disk artifacts on the next open.
/// - `Err(e)` with `cfg.allow_lossy_recovery=false` →
///   typed `RecoveryError` via `from_coordinator_error`.
/// - `Err(e)` with lossy enabled → build `LossyRecoveryReport`,
///   replace `storage` with a fresh `SegmentedStore::with_dir`,
///   return `(RecoveryStats::default(), Some(report))`.
fn handle_wal_recovery_outcome(
    recover_result: Result<RecoveryStats, CoordinatorRecoveryError>,
    cfg: &StrataConfig,
    layout: &DatabaseLayout,
    storage: &mut SegmentedStore,
    records_applied_before_failure: u64,
    mode: RecoveryMode,
) -> Result<(RecoveryStats, Option<LossyRecoveryReport>), RecoveryError> {
    let err = match recover_result {
        Ok(stats) => return Ok((stats, None)),
        Err(e) => e,
    };

    if err.should_bypass_lossy() {
        return Err(from_coordinator_error(mode.as_error_role(), err));
    }

    if !cfg.allow_lossy_recovery {
        return Err(from_coordinator_error(mode.as_error_role(), err));
    }

    let err = coordinator_error_to_lossy_strata_error(err);

    // Sample progress BEFORE the wipe so the report reflects what was
    // discarded.
    let version_reached_before_failure = CommitVersion(storage.version());
    let error_kind = LossyErrorKind::from_strata_error(&err);
    let report = LossyRecoveryReport {
        error: err.to_string(),
        error_kind,
        records_applied_before_failure,
        version_reached_before_failure,
        discarded_on_wipe: true,
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

    // Discard any partial writes accumulated before the failure so
    // lossy-mode semantics match the pre-Epic-5 `RecoveryResult::empty()`
    // fallback: no user data surfaces from a failed recovery pass.
    *storage = SegmentedStore::with_dir(
        layout.segments_dir().to_path_buf(),
        cfg.storage.effective_write_buffer_size(),
    );

    Ok((RecoveryStats::default(), Some(report)))
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
/// Mirrors the pre-D3 block at `open.rs:634-672` verbatim.
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
    use super::*;
    use strata_core::id::TxnId;
    use strata_core::{BranchId, Value};
    use strata_storage::durability::codec::IdentityCodec;
    use strata_storage::durability::{
        primitive_tags, KvSnapshotEntry, LoadedSection, SnapshotHeader, SnapshotSection,
        SnapshotSerializer, SnapshotWriter,
    };
    use strata_storage::{Key, Namespace, TypeTag};
    use tempfile::TempDir;

    #[test]
    fn coordinator_plan_failure_bypasses_lossy_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let layout = DatabaseLayout::from_root(temp_dir.path());
        let mut storage = SegmentedStore::new();
        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };

        let result = handle_wal_recovery_outcome(
            Err(CoordinatorRecoveryError::Plan(
                CoordinatorPlanError::CodecMismatch {
                    stored: "identity".into(),
                    expected: "aes-gcm-256".into(),
                },
            )),
            &cfg,
            &layout,
            &mut storage,
            0,
            RecoveryMode::Primary,
        );

        match result {
            Err(RecoveryError::CoordinatorPlan(inner)) => {
                assert!(matches!(inner, StrataError::IncompatibleReuse { .. }));
            }
            other => panic!("plan failure must hard-fail without lossy wipe, got: {other:?}"),
        }
    }

    #[test]
    fn recovery_snapshot_install_rejects_when_install_codec_absent() {
        let branch_id = BranchId::new();
        let snapshot = loaded_snapshot(
            "identity",
            vec![kv_section(branch_id, "skipped", Value::Int(7), 11)],
        );
        let storage = SegmentedStore::new();

        let err = install_recovery_snapshot(&snapshot, None, &storage)
            .expect_err("loaded snapshot without install codec should violate recovery invariant");

        match err {
            StorageError::Corruption { message } => {
                assert!(message.contains(
                    "snapshot 7 reached recovery install callback without an install codec"
                ));
            }
            other => panic!("expected corruption storage error, got {other:?}"),
        }
        assert!(storage
            .get_versioned(&kv_key(branch_id, "skipped"), CommitVersion::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn recovery_snapshot_install_maps_engine_errors_into_storage_callback_errors() {
        let snapshot = loaded_snapshot("other-codec", Vec::new());
        let storage = SegmentedStore::new();
        let codec = IdentityCodec;

        let err = install_recovery_snapshot(&snapshot, Some(&codec), &storage)
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

        let installed = install_recovery_snapshot(&snapshot, Some(&codec), &storage)
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
        let codec = IdentityCodec;

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
        let mut manifest = ManifestManager::create(
            layout.manifest_path().to_path_buf(),
            database_uuid,
            "identity".to_string(),
        )
        .unwrap();
        manifest.set_snapshot_watermark(3, TxnId(9)).unwrap();

        let cfg = StrataConfig::default();
        let storage = SegmentedStore::new();
        let counter = Arc::new(AtomicU64::new(0));
        let recover_result =
            run_coordinator_recovery(&layout, &cfg, &codec, Some(&codec), &storage, &counter);

        let err = recover_result.expect_err("corrupt snapshot install should fail recovery");
        match &err {
            CoordinatorRecoveryError::Callback(StorageError::Corruption { message }) => {
                assert!(message.contains("KV section decode failed"));
            }
            other => panic!("expected callback corruption from snapshot install, got {other:?}"),
        }

        let mut mapped_storage = storage;
        let mapped = handle_wal_recovery_outcome(
            Err(err),
            &cfg,
            &layout,
            &mut mapped_storage,
            0,
            RecoveryMode::Primary,
        )
        .expect_err("callback error should map to public recovery error");
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
        LoadedSection {
            primitive_type: primitive_tags::KV,
            data: serializer.serialize_kv(&entries),
        }
    }

    fn kv_key(branch_id: BranchId, key: &str) -> Key {
        Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), key)
    }
}
