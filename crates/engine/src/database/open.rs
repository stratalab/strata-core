//! Database opening and initialization.

use super::config::StorageConfig;
use crate::background::BackgroundScheduler;
use crate::coordinator::TransactionCoordinator;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use strata_durability::__internal::WalWriterEngineExt;
use strata_durability::codec::clone_codec;
use strata_durability::layout::DatabaseLayout;
use strata_durability::wal::{DurabilityMode, WalConfig, WalWriter};
use strata_storage::SegmentedStore;
use tracing::{info, warn};

/// Apply all storage configuration settings to a SegmentedStore.
///
/// Centralizes the 7 storage-config setters so every open path
/// (primary, follower, cache) applies the same set of knobs. Visible to
/// `super::recovery` so the unified `run_recovery` entry point can apply
/// the same knobs after it finishes MANIFEST + WAL + segment recovery.
pub(crate) fn apply_storage_config(storage: &SegmentedStore, cfg: &StorageConfig) {
    storage.set_max_branches(cfg.max_branches);
    storage.set_max_versions_per_key(cfg.max_versions_per_key);
    storage.set_max_immutable_memtables(cfg.effective_max_immutable_memtables());
    storage.set_target_file_size(cfg.target_file_size);
    storage.set_level_base_bytes(cfg.level_base_bytes);
    storage.set_data_block_size(cfg.data_block_size);
    storage.set_bloom_bits_per_key(cfg.bloom_bits_per_key);
    if cfg.compaction_rate_limit > 0 {
        storage.set_compaction_rate_limit(cfg.compaction_rate_limit);
    }
}

use strata_core::id::CommitVersion;
use strata_core::{StrataError, StrataResult};

/// Restrict a directory to owner-only access (rwx------).
/// Best-effort: logs a warning on failure but does not block database open.
#[cfg(unix)]
pub(crate) fn restrict_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)) {
        warn!(target: "strata::db", path = %path.display(), error = %e,
            "Failed to restrict directory permissions");
    }
}

#[cfg(not(unix))]
pub(crate) fn restrict_dir(_path: &Path) {}

/// Sanitize config for runtime consistency across all modes.
///
/// Currently clamps `auto_embed` to `false` when the `embed` feature is not
/// compiled. Called from primary, follower, and cache paths to ensure
/// consistent behavior.
#[allow(unused_mut)]
fn sanitize_config(mut cfg: super::config::StrataConfig) -> super::config::StrataConfig {
    #[cfg(not(feature = "embed"))]
    if cfg.auto_embed {
        warn!(
            "auto_embed=true but the 'embed' feature is not compiled; \
             auto-embedding is disabled"
        );
        cfg.auto_embed = false;
    }
    cfg
}

/// Restrict a file to owner-only read/write (rw-------).
/// Best-effort: ignores errors (defense in depth for data files).
#[cfg(unix)]
fn restrict_file(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) {}

use super::config::{self, StrataConfig};
use super::registry::OPEN_DATABASES;
use super::{Database, PersistenceMode, WalWriterHealth};

enum AcquiredDatabase {
    Existing(Arc<Database>),
    New {
        db: Arc<Database>,
        canonical_path: PathBuf,
    },
}

impl Database {
    /// Open database at given path with automatic recovery
    ///
    /// Reads `strata.toml` from the data directory to determine durability mode.
    /// If no config file exists, creates one with defaults (standard durability).
    ///
    /// # Thread Safety
    ///
    /// Opening the same path from multiple threads returns the same `Arc<Database>`.
    /// This ensures all threads share the same database instance, which is safe
    /// because Database uses internal synchronization (DashMap, atomics, etc.).
    ///
    /// ```text
    /// let db1 = Database::open("/data")?;
    /// let db2 = Database::open("/data")?;  // Same Arc as db1
    /// assert!(Arc::ptr_eq(&db1, &db2));
    /// ```
    ///
    /// # Flow
    ///
    /// 1. Create data directory if needed
    /// 2. Read or create `strata.toml`
    /// 3. Parse config to determine durability mode
    /// 4. Check registry for existing instance at this path
    /// 5. If found, return the existing Arc<Database>
    /// 6. Otherwise: open WAL, replay, register, return
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path for the database
    ///
    /// # Returns
    ///
    /// * `Ok(Arc<Database>)` - Ready-to-use database instance (shared if path was already open)
    /// * `Err` - If config is invalid, directory creation, WAL opening, or recovery fails
    ///
    /// # Example
    ///
    /// ```text
    /// use strata_engine::Database;
    ///
    /// let db = Database::open("/path/to/data")?;
    /// ```
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> StrataResult<Arc<Self>> {
        // Engine-only open uses only SearchSubsystem (no graph/vector dependency).
        // For the full subsystem set, use OpenSpec with the executor's
        // default_product_spec() or search_only_primary_spec(path).
        let spec =
            super::spec::OpenSpec::primary(path).with_subsystem(crate::search::SearchSubsystem);
        Self::open_runtime(spec)
    }

    /// Open database at the given path with an explicit configuration.
    ///
    /// This is the programmatic alternative to editing `strata.toml` by hand.
    /// The supplied config is written to `strata.toml` so that subsequent
    /// `Database::open()` calls (e.g. after restart) pick up the same settings.
    ///
    /// # Example
    ///
    /// ```text
    /// use strata_engine::{Database, StrataConfig};
    ///
    /// let config = StrataConfig {
    ///     durability: "always".into(),
    ///     ..Default::default()
    /// };
    /// let db = Database::open_with_config("/path/to/data", config)?;
    /// ```
    pub(crate) fn open_with_config<P: AsRef<Path>>(
        path: P,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        #[cfg(not(feature = "embed"))]
        let cfg = {
            let mut cfg = cfg;
            if cfg.auto_embed {
                warn!(
                    "auto_embed=true but the 'embed' feature is not compiled; \
                     auto-embedding is disabled"
                );
                cfg.auto_embed = false;
            }
            cfg
        };

        // Engine-only open uses only SearchSubsystem (no graph/vector dependency).
        let spec = super::spec::OpenSpec::primary(path)
            .with_config(cfg)
            .with_subsystem(crate::search::SearchSubsystem);
        Self::open_runtime(spec)
    }

    /// Acquire a primary `Database` for `path`, running subsystem recovery
    /// while holding the `OPEN_DATABASES` mutex so concurrent openers for
    /// the same path cannot observe a half-initialized instance.
    ///
    /// Ordering inside the locked region:
    ///
    ///   1. Fast path — if a live `Arc<Database>` already exists for this
    ///      canonical path, return it. The existing instance has already
    ///      completed recovery (enforced by this same lock).
    ///   2. Acquire the `.lock` file for single-process exclusion.
    ///   3. `open_finish` — create the `Database` struct from WAL replay.
    ///   4. `repair_space_metadata_on_open` + the `subsystem.recover(&db)`
    ///      loop. A recovery error propagates out; the registry is never
    ///      populated for a failed open, so a later opener gets a clean
    ///      slate instead of a stale weak ref pointing at a half-baked
    ///      instance.
    ///   5. `db.set_subsystems(subsystems)` — install the same ordered list
    ///      for drop-time freeze. Matches Epic 5's partial-failure
    ///      contract: only install after every `recover()` succeeds.
    ///   6. Mark lifecycle as `initializing` and publish the `Arc` into
    ///      `OPEN_DATABASES` so concurrent openers can wait on it.
    ///   7. Drop the mutex and let the caller run lifecycle hooks.
    ///
    /// A concurrent opener that blocks on step (1)'s mutex will not see
    /// the new `Arc` until step (6), by which point recovery is complete
    /// and subsystems are installed. This closes the race where the
    /// previous version of this function inserted into the registry
    /// before recovery ran (audit follow-up to stratalab/strata-core#2354,
    /// Finding 1).
    fn acquire_primary_db(
        path: &Path,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        subsystems: Vec<Box<dyn crate::recovery::Subsystem>>,
        runtime_signature: Option<super::CompatibilitySignature>,
    ) -> StrataResult<AcquiredDatabase> {
        // Create directory first so we can canonicalize the path
        let data_dir = path.to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;
        restrict_dir(&data_dir);

        // Canonicalize path for consistent registry keys
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        // Lock OPEN_DATABASES for the ENTIRE open-and-recover sequence.
        // See the doc comment above for ordering rationale.
        let mut registry = OPEN_DATABASES.lock();

        if let Some(weak) = registry.get(&canonical_path) {
            if let Some(db) = weak.upgrade() {
                if let Some(requested_signature) = runtime_signature.as_ref() {
                    let existing_signature = db.runtime_signature().ok_or_else(|| {
                        StrataError::incompatible_reuse(
                            "existing database instance was not opened via open_runtime",
                        )
                    })?;
                    if let Err(reason) = existing_signature.check_compatible(requested_signature) {
                        // Hard reuse rejection: all mismatches (including subsystem)
                        // are errors. This prevents silent subsystem dropping when
                        // mixing openers (e.g., Database::open + Strata::open).
                        return Err(StrataError::incompatible_reuse(format!(
                            "cannot reuse existing database instance: {}",
                            reason
                        )));
                    }
                } else {
                    // Mixed-opener detection (audit follow-up to #2354
                    // Finding 2): compare the requested subsystem list to
                    // the list that is actually installed on the existing
                    // instance. If they differ, the second caller's
                    // subsystems are silently dropped — the single-
                    // instance-per-path contract requires us to return
                    // the existing Arc unchanged. Log a warning so the
                    // misuse surfaces at runtime. Order matters: reversed
                    // lists produce different freeze orders.
                    let installed = db.installed_subsystem_names();
                    let requested: Vec<&'static str> =
                        subsystems.iter().map(|s| s.name()).collect();
                    if installed != requested {
                        tracing::warn!(
                            target: "strata::db",
                            path = ?canonical_path,
                            installed = ?installed,
                            requested = ?requested,
                            "Mixed-opener detected: an earlier caller opened this \
                             database with a different subsystem list. Returning \
                             the existing instance with the EARLIER subsystems; \
                             the requested subsystems were silently dropped. Use \
                             the same opener (e.g. `Strata::open` everywhere, or \
                             `OpenSpec` with the same subsystem list) across all \
                             call sites for this path. See audit follow-up to \
                             #2354 Finding 2."
                        );
                    }
                }
                info!(target: "strata::db", path = ?canonical_path, "Returning existing database instance");
                drop(registry);
                return Ok(AcquiredDatabase::Existing(db));
            }
        }

        let lock_path = canonical_path.join(".lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| StrataError::storage(format!("failed to open lock file: {}", e)))?;
        restrict_file(&lock_path);
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|_| {
            StrataError::storage(format!(
                "database at '{}' is already in use by another process",
                canonical_path.display()
            ))
        })?;

        // Apply hardware profile to any fields still at their default value.
        // In-memory only — does NOT persist to strata.toml. See profile.rs.
        let mut cfg = cfg;
        crate::database::profile::apply_hardware_profile_if_defaults(&mut cfg);

        let db = Self::open_finish(
            canonical_path.clone(),
            durability_mode,
            cfg,
            Some(lock_file),
        )?;

        // Repair space metadata BEFORE running subsystem recovery so each
        // subsystem sees the complete set of spaces. Without this, legacy
        // databases (or any bypass write that ever skipped the registration
        // helper) leave orphan data in spaces invisible to enumeration, and
        // subsystems that scan by space would silently miss it.
        Self::repair_space_metadata_on_open(&db);

        // Run recovery via subsystems in registration order. Stop on first
        // error. We hold `registry` for the whole loop, so a concurrent
        // opener for the same path cannot observe the `Arc` mid-recovery.
        //
        // DEADLOCK AVOIDANCE: on a recovery error we must drop `registry`
        // BEFORE propagating the Err. `registry` is declared before `db`,
        // so under normal Rust drop order the `Arc<Self>` would unwind
        // first and `Drop for Database` would reacquire `OPEN_DATABASES`
        // (to remove its own entry) — which self-deadlocks against our
        // still-held `parking_lot::Mutex` guard. Explicit `drop(registry)`
        // releases the mutex so the Arc's Drop can clean up.
        for subsystem in &subsystems {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running subsystem recovery"
            );
            if let Err(e) = subsystem.recover(&db) {
                drop(registry);
                return Err(e);
            }
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Subsystem recovery complete"
            );
        }

        // Install subsystems for freeze-on-drop AFTER every recover()
        // succeeded, so a partial-recovery failure does not leave a
        // populated subsystems vec for `Drop for Database` to freeze.
        db.set_subsystems(subsystems);

        // NOTE: Lifecycle hooks (initialize, bootstrap) are handled by callers.
        // This function does recovery, subsystem installation, runtime-signature
        // capture, and early registry publication so concurrent opens can wait
        // on the in-flight instance. Callers still finish lifecycle and any
        // mode-specific setup (for example default-branch creation).
        if let Some(signature) = runtime_signature {
            db.set_runtime_signature(signature);
        }
        db.set_lifecycle_initializing();
        registry.insert(canonical_path.clone(), Arc::downgrade(&db));
        drop(registry);

        Ok(AcquiredDatabase::New { db, canonical_path })
    }
    /// Repair space metadata at open time by reconciling registered metadata
    /// with the actual data found by `discover_used_spaces`. Skipped on
    /// followers (read-only) — they still get correct enumeration via the
    /// union behaviour in `SpaceIndex::list/exists`.
    fn repair_space_metadata_on_open(db: &Arc<Self>) {
        if db.is_follower() {
            return;
        }
        let space_index = crate::SpaceIndex::new(db.clone());
        for branch_id in db.storage().branch_ids() {
            if let Err(e) = space_index.repair_space_metadata(branch_id) {
                tracing::warn!(
                    target: "strata::space",
                    branch_id = %branch_id,
                    error = %e,
                    "Space metadata repair failed; recovery may miss spaces"
                );
            }
        }
    }

    /// Open a read-only follower of an existing database.
    ///
    /// The follower does not acquire any file lock, so it can open a database
    /// that is already exclusively locked by another process. It replays the
    /// WAL to build in-memory state and can `refresh()` to see new commits.
    ///
    /// All write operations will fail. The primary process retains full
    /// performance (exclusive lock, deferred batching, all extensions).
    ///
    /// This is the recommended way to provide cross-process read access
    /// (similar to RocksDB secondary instances).
    pub(crate) fn open_follower<P: AsRef<Path>>(path: P) -> StrataResult<Arc<Self>> {
        // Engine-only open uses only SearchSubsystem (no graph/vector dependency).
        // For the full subsystem set, use OpenSpec with the executor's
        // default_product_follower_spec() or search_only_follower_spec(path).
        let spec =
            super::spec::OpenSpec::follower(path).with_subsystem(crate::search::SearchSubsystem);
        Self::open_runtime(spec)
    }
    /// Open a follower `Database` at the given canonicalized path.
    ///
    /// Handles hardware-profile application, read-only WAL recovery,
    /// segment recovery, and `Arc<Database>` construction with
    /// `follower: true` and an empty `subsystems` vec. Caller is
    /// responsible for installing subsystems and running recovery.
    fn acquire_follower_db(
        canonical_path: PathBuf,
        mut cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        // Apply hardware profile to any fields still at their default value
        // (in-memory only — followers never persist config).
        crate::database::profile::apply_hardware_profile_if_defaults(&mut cfg);

        // Build canonical layout for this database
        let layout = DatabaseLayout::from_root(&canonical_path);
        let wal_dir = layout.wal_dir().to_path_buf();

        // Epic D3: unified recovery orchestration. `run_recovery` owns
        // the follower-specific MANIFEST-or-config codec resolution,
        // WAL-only-on-missing-MANIFEST branch, coordinator recovery
        // with lossy fallback, segment recovery, and persisted
        // follower-state restore. Pre-D3 this was ~260 inline lines
        // with string-factory error wraps at four sites.
        let outcome = Database::run_recovery(
            &canonical_path,
            &layout,
            &cfg,
            super::recovery::RecoveryMode::Follower,
        )
        .map_err(StrataError::from)?;

        let bg_threads = cfg.storage.background_threads.max(1);

        let db = Arc::new(Self {
            data_dir: canonical_path,
            database_uuid: outcome.database_uuid,
            storage: outcome.storage,
            wal_writer: None, // No WAL writer — read-only
            wal_codec: outcome.wal_codec,

            persistence_mode: PersistenceMode::Disk,
            coordinator: outcome.coordinator,
            durability_mode: parking_lot::RwLock::new(DurabilityMode::Cache), // Irrelevant for follower
            accepting_transactions: Arc::new(AtomicBool::new(true)),
            wal_writer_health: Arc::new(ParkingMutex::new(WalWriterHealth::Healthy)),
            last_lossy_recovery_report: Arc::new(ParkingMutex::new(outcome.lossy_report)),
            extensions: DashMap::new(),
            config: parking_lot::RwLock::new(cfg),
            flush_shutdown: Arc::new(AtomicBool::new(false)),
            flush_handle: ParkingMutex::new(None), // No flush thread
            scheduler: Arc::new(BackgroundScheduler::new(bg_threads, 4096)),
            flush_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            backpressure_counter: AtomicU64::new(0),
            lock_file: parking_lot::Mutex::new(None), // No lock acquired
            wal_dir,
            watermark: outcome.watermark,
            refresh_gate: super::refresh::RefreshGate::new(),
            refresh_publish_barrier: parking_lot::RwLock::new(()),
            follower: true,
            shutdown_started: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
            dag_hook_slot: super::DagHookSlot::new(),
            branch_op_observers: super::BranchOpObserverRegistry::new(),
            commit_observers: super::CommitObserverRegistry::new(),
            abort_observers: super::AbortObserverRegistry::new(),
            replay_observers: super::ReplayObserverRegistry::new(),
            lifecycle_state: std::sync::atomic::AtomicU8::new(
                super::LifecycleState::Uninitialized.as_u8(),
            ),
            lifecycle_state_mu: parking_lot::Mutex::new(()),
            lifecycle_state_cv: parking_lot::Condvar::new(),
            runtime_signature: parking_lot::RwLock::new(None),
            merge_registry: super::MergeHandlerRegistry::new(),
        });

        if let Some(state) = outcome.persisted_follower_state {
            db.storage.set_version(state.visible_version);
            db.coordinator
                .restore_visible_version(state.visible_version);
        }

        Ok(db)
    }
    /// Spawn the background WAL flush thread for Standard durability mode.
    ///
    /// Returns `None` for non-Standard modes (Cache, Always).
    ///
    /// # Arguments
    /// * `durability_mode` - The durability mode (only Standard spawns a thread)
    /// * `wal` - The WAL writer mutex
    /// * `data_dir` - Canonical database path used to scope test fault injection
    /// * `shutdown` - Signal to stop the flush thread
    /// * `accepting_transactions` - Flag to disable when writer halts
    /// * `wal_writer_health` - Health state to update on sync failure
    pub(crate) fn spawn_wal_flush_thread(
        durability_mode: DurabilityMode,
        wal: &Arc<ParkingMutex<WalWriter>>,
        data_dir: &Path,
        shutdown: &Arc<AtomicBool>,
        accepting_transactions: &Arc<AtomicBool>,
        wal_writer_health: &Arc<ParkingMutex<WalWriterHealth>>,
    ) -> StrataResult<Option<std::thread::JoinHandle<()>>> {
        if let DurabilityMode::Standard { interval_ms, .. } = durability_mode {
            let wal = Arc::clone(wal);
            let data_dir = data_dir.to_path_buf();
            let shutdown = Arc::clone(shutdown);
            let accepting = Arc::clone(accepting_transactions);
            let health = Arc::clone(wal_writer_health);
            let interval = std::time::Duration::from_millis(interval_ms);

            #[cfg(test)]
            if crate::database::test_hooks::take_flush_thread_spawn_failure(&data_dir) {
                return Err(StrataError::internal(
                    "injected flush thread spawn failure".to_string(),
                ));
            }

            let handle = std::thread::Builder::new()
                .name("strata-wal-flush".to_string())
                .spawn(move || {
                    while !shutdown.load(Ordering::Relaxed) {
                        std::thread::park_timeout(interval);
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        if matches!(&*health.lock(), WalWriterHealth::Halted { .. }) {
                            continue;
                        }
                        let sync_plan = {
                            let mut w = wal.lock();

                            // In the real path, `begin_background_sync` can fail
                            // before a handle is produced (e.g., `flush_to_os`).
                            //
                            // In the test-injected path, we only simulate a begin
                            // failure when a real begin WOULD have produced a
                            // handle (i.e., unsynced data + interval elapsed) —
                            // otherwise the injection would fire on idle ticks
                            // and race with the test's setup. We abort the
                            // just-opened handle so `sync_in_flight` clears and
                            // bg_error reflects the injected error.
                            #[cfg(test)]
                            let (begin_result, already_recorded) = match w.begin_background_sync() {
                                Ok(Some(handle)) => {
                                    match crate::database::test_hooks::maybe_inject_begin_sync_failure(&data_dir) {
                                        Some(injected) => {
                                            w.abort_background_sync(handle, injected);
                                            (Err(std::io::Error::other("injected begin sync failure")), true)
                                        }
                                        None => (Ok(Some(handle)), false),
                                    }
                                }
                                other => (other, false),
                            };

                            #[cfg(not(test))]
                            let (begin_result, already_recorded) = (w.begin_background_sync(), false);

                            match begin_result {
                                Ok(Some(handle)) => Some((handle, w.snapshot_active_meta())),
                                Ok(None) => None,
                                Err(e) => {
                                    tracing::error!(target: "strata::wal", error = %e, "Background WAL flush failed");
                                    if !already_recorded {
                                        w.record_sync_failure(e);
                                    }
                                    let bg = w.bg_error();
                                    Self::latch_bg_sync_halt(
                                        &health,
                                        &accepting,
                                        bg,
                                        "setup",
                                        Some(data_dir.as_path()),
                                    );
                                    drop(w);
                                    None
                                }
                            }
                        };

                        if let Some((handle, meta_snapshot)) = sync_plan {
                            #[cfg(test)]
                            let sync_result = crate::database::test_hooks::maybe_inject_sync_failure(&data_dir)
                                .map_or_else(|| handle.fd().sync_all(), Err);

                            #[cfg(not(test))]
                            let sync_result = handle.fd().sync_all();

                            let committed = {
                                let mut w = wal.lock();
                                match sync_result {
                                    Ok(()) => {
                                        // In the real path, commit_background_sync
                                        // consumes the handle and does not touch
                                        // bg_error on failure — the halt arm below
                                        // records the failure itself.
                                        //
                                        // In the test-injected path,
                                        // abort_background_sync releases the handle
                                        // and records the injected error, so the
                                        // halt arm must NOT record a second time (that
                                        // would overwrite the injected error message
                                        // with a placeholder).
                                        #[cfg(test)]
                                        let (commit_result, already_recorded) = match crate::database::test_hooks::maybe_inject_commit_sync_failure(&data_dir) {
                                            Some(injected) => {
                                                w.abort_background_sync(handle, injected);
                                                (Err(std::io::Error::other("injected commit failure")), true)
                                            }
                                            None => (w.commit_background_sync(handle), false),
                                        };

                                        #[cfg(not(test))]
                                        let (commit_result, already_recorded) = (w.commit_background_sync(handle), false);

                                        match commit_result {
                                            Ok(()) => true,
                                            Err(e) => {
                                                tracing::error!(target: "strata::wal", error = %e, "Background WAL sync bookkeeping failed");
                                                if !already_recorded {
                                                    w.record_sync_failure(e);
                                                }
                                                let bg = w.bg_error();
                                                Self::latch_bg_sync_halt(
                                                    &health,
                                                    &accepting,
                                                    bg,
                                                    "bookkeeping",
                                                    Some(data_dir.as_path()),
                                                );
                                                drop(w);
                                                false
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(target: "strata::wal", error = %e, "Background WAL sync failed");
                                        w.abort_background_sync(handle, e);
                                        let bg = w.bg_error();
                                        Self::latch_bg_sync_halt(
                                            &health,
                                            &accepting,
                                            bg,
                                            "sync",
                                            Some(data_dir.as_path()),
                                        );
                                        drop(w);
                                        false
                                    }
                                }
                            };

                            if committed {
                                if let Some((meta, wal_dir)) = meta_snapshot {
                                    if let Err(e) = meta.write_to_file(&wal_dir) {
                                        tracing::debug!(target: "strata::wal", error = %e, "Background .meta write failed (non-fatal)");
                                    }
                                }
                            }
                        }
                    }
                    // Final sync: flush any data written since the last periodic sync
                    let mut wal = wal.lock();
                    if let Err(e) = wal.flush() {
                        tracing::error!(target: "strata::wal", error = %e, "Final WAL flush failed during shutdown");
                    }
                })
                .map_err(|e| {
                    StrataError::internal(format!("failed to spawn WAL flush thread: {}", e))
                })?;
            Ok(Some(handle))
        } else {
            Ok(None)
        }
    }

    /// Shared tail of database open: recovery, WAL writer, coordinator, flush thread.
    fn open_finish(
        canonical_path: PathBuf,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        lock_file: Option<std::fs::File>,
    ) -> StrataResult<Arc<Self>> {
        // Build canonical layout for this database
        let layout = DatabaseLayout::from_root(&canonical_path);

        // Epic D3: unified recovery orchestration. `run_recovery` owns
        // codec validation (before any recovery-managed artifact
        // creation), MANIFEST
        // load-or-create, WAL replay with its lossy-fallback branch,
        // coordinator construction, and SE2's segment recovery
        // plus `coordinator.apply_storage_recovery`. Pre-D3 each of
        // those steps lived inline here with string-factory error
        // wraps scattered across 250 lines.
        let outcome = Database::run_recovery(
            &canonical_path,
            &layout,
            &cfg,
            super::recovery::RecoveryMode::Primary,
        )
        .map_err(StrataError::from)?;

        // Create only the non-authoritative support directories after
        // recovery has validated the configured codec. Recreating
        // `segments/` here on reopen would mask authoritative flushed-state
        // loss before storage recovery has a chance to classify it.
        layout
            .create_non_segment_dirs()
            .map_err(StrataError::from)?;
        restrict_dir(layout.wal_dir());
        restrict_dir(layout.snapshots_dir());

        let wal_dir = layout.wal_dir().to_path_buf();

        // Defensive: tighten segments-dir permissions on reopen. On
        // first-open `run_recovery` already restricted it via
        // `prepare_manifest`; on reopen we don't know the historical
        // perms of an existing dir.
        if matches!(layout.segments_dir().try_exists(), Ok(true)) {
            restrict_dir(layout.segments_dir());
        }

        // Clone the WAL codec once for the follower-refresh path kept
        // on `Database.wal_codec`; the original moves into
        // `WalWriter::new` below.
        let wal_codec_for_db = clone_codec(outcome.wal_codec.as_ref());

        // Open segmented WAL writer for appending.
        let wal_writer = WalWriter::new(
            wal_dir.clone(),
            outcome.database_uuid,
            durability_mode,
            WalConfig::default(),
            outcome.wal_codec,
        )?;

        let wal_arc = Arc::new(ParkingMutex::new(wal_writer));
        let flush_shutdown = Arc::new(AtomicBool::new(false));
        // Pre-create Arc-wrapped fields that need to be shared with flush thread.
        let accepting_transactions = Arc::new(AtomicBool::new(true));
        let wal_writer_health = Arc::new(ParkingMutex::new(WalWriterHealth::Healthy));

        // Configure block cache capacity. `run_recovery` already
        // completed segment recovery; post-open reads will hit this
        // cache, so configuring it here is functionally equivalent to
        // the pre-D3 ordering.
        {
            use strata_storage::block_cache;
            let effective_cache = cfg.storage.effective_block_cache_size();
            let cache_bytes = if effective_cache > 0 {
                effective_cache
            } else {
                block_cache::auto_detect_capacity()
            };
            block_cache::set_global_capacity(cache_bytes);
        }

        if cfg.storage.memory_budget > 0 {
            info!(target: "strata::db",
                memory_budget = cfg.storage.memory_budget,
                effective_cache = cfg.storage.effective_block_cache_size(),
                effective_write_buffer = cfg.storage.effective_write_buffer_size(),
                effective_max_immutable = cfg.storage.effective_max_immutable_memtables(),
                "Memory budget active — derived storage parameters"
            );
        }

        let bg_threads = cfg.storage.background_threads.max(1);

        let db = Arc::new(Self {
            data_dir: canonical_path.clone(),
            database_uuid: outcome.database_uuid,
            storage: outcome.storage,
            wal_writer: Some(Arc::clone(&wal_arc)),
            wal_codec: wal_codec_for_db,
            persistence_mode: PersistenceMode::Disk,
            coordinator: outcome.coordinator,
            durability_mode: parking_lot::RwLock::new(durability_mode),
            accepting_transactions: Arc::clone(&accepting_transactions),
            wal_writer_health: Arc::clone(&wal_writer_health),
            last_lossy_recovery_report: Arc::new(ParkingMutex::new(outcome.lossy_report)),
            extensions: DashMap::new(),
            config: parking_lot::RwLock::new(cfg),
            flush_shutdown: Arc::clone(&flush_shutdown),
            flush_handle: ParkingMutex::new(None), // Spawned after construction
            scheduler: Arc::new(BackgroundScheduler::new(bg_threads, 4096)),
            flush_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            backpressure_counter: AtomicU64::new(0),
            lock_file: parking_lot::Mutex::new(lock_file),
            wal_dir,
            watermark: outcome.watermark,
            refresh_gate: super::refresh::RefreshGate::new(),
            refresh_publish_barrier: parking_lot::RwLock::new(()),
            follower: false,
            shutdown_started: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
            dag_hook_slot: super::DagHookSlot::new(),
            branch_op_observers: super::BranchOpObserverRegistry::new(),
            commit_observers: super::CommitObserverRegistry::new(),
            abort_observers: super::AbortObserverRegistry::new(),
            replay_observers: super::ReplayObserverRegistry::new(),
            lifecycle_state: std::sync::atomic::AtomicU8::new(
                super::LifecycleState::Uninitialized.as_u8(),
            ),
            lifecycle_state_mu: parking_lot::Mutex::new(()),
            lifecycle_state_cv: parking_lot::Condvar::new(),
            runtime_signature: parking_lot::RwLock::new(None),
            merge_registry: super::MergeHandlerRegistry::new(),
        });

        // Spawn flush thread now that Database is constructed (shares Arc fields)
        if let Some(handle) = Self::spawn_wal_flush_thread(
            durability_mode,
            &wal_arc,
            &canonical_path,
            &flush_shutdown,
            &accepting_transactions,
            &wal_writer_health,
        )? {
            *db.flush_handle.lock() = Some(handle);
        }

        // Trigger compaction if any levels have accumulated segments from
        // recovery. Without this, compaction only runs after the next write
        // (via schedule_flush_if_needed), leaving L0 files uncompacted on
        // reopen-without-writes.
        db.schedule_background_compaction();

        Ok(db)
    }

    /// Create a cache database with no disk I/O
    ///
    /// This creates a truly in-memory database that:
    /// - Creates no files or directories
    /// - Has no WAL (write-ahead log)
    /// - Cannot recover after crash
    /// - Loses all data when dropped
    /// - Is NOT registered in the global registry (each call creates a new instance)
    ///
    /// Use this for:
    /// - Unit tests that need maximum isolation
    /// - Caching scenarios
    /// - Temporary computations
    ///
    /// # Example
    ///
    /// ```text
    /// use strata_engine::Database;
    /// use strata_core::types::BranchId;
    ///
    /// let db = Database::cache()?;
    /// let branch_id = BranchId::new();
    ///
    /// // All operations work normally
    /// db.transaction(branch_id, |txn| {
    ///     txn.put(key, value)?;
    ///     Ok(())
    /// })?;
    ///
    /// // But data is gone when db is dropped
    /// drop(db);
    /// ```
    ///
    /// # Comparison with disk-backed databases
    ///
    /// | Method | Disk Files | WAL | Recovery |
    /// |--------|------------|-----|----------|
    /// | `cache()` | None | None | No |
    /// | `open(path)` | Yes | Yes (per config) | Yes |
    pub(crate) fn cache() -> StrataResult<Arc<Self>> {
        // Engine-only open uses only SearchSubsystem (no graph/vector dependency).
        // For the full subsystem set, use OpenSpec with the executor's
        // default_product_cache_spec() or search_only_cache_spec().
        let spec = super::spec::OpenSpec::cache().with_subsystem(crate::search::SearchSubsystem);
        Self::open_runtime(spec)
    }

    /// Create a bare ephemeral database without any subsystems.
    ///
    /// This is an internal helper used by `open_runtime_cache()`. External
    /// callers should use `cache()` which adds SearchSubsystem, or
    /// `OpenSpec::cache().with_subsystem(...)` for custom subsystem sets.
    ///
    /// If `spec_config` is provided, it is used as the base config (with hardware
    /// profile applied on top for sizing). If `None`, defaults are used.
    fn create_ephemeral_bare(spec_config: Option<&StrataConfig>) -> StrataResult<Arc<Self>> {
        let mut cfg = sanitize_config(spec_config.cloned().unwrap_or_default());
        // Apply hardware profile so resource-constrained hosts (Pi Zero, etc.)
        // get appropriate sizing. Without this, cache() would inherit the
        // 256 MB DEFAULT_CAPACITY_BYTES from the global block cache singleton,
        // which is fatal on 512 MB devices.
        crate::database::profile::apply_hardware_profile_if_defaults(&mut cfg);

        // Apply effective block cache size to the global singleton so that
        // in-memory reads use the profile-tuned capacity instead of the
        // 256 MB default. On Desktop/Server this is a no-op (effective == 0
        // triggers the existing auto-detect behavior elsewhere).
        let effective_cache = cfg.storage.effective_block_cache_size();
        if effective_cache > 0 {
            strata_storage::block_cache::set_global_capacity(effective_cache);
        }

        // Create fresh storage with config limits
        let storage = SegmentedStore::new();
        apply_storage_config(&storage, &cfg.storage);

        let bg_threads = cfg.storage.background_threads.max(1);

        // Create coordinator starting at version 1 (no recovery needed), with write buffer limit
        let coordinator = TransactionCoordinator::new(CommitVersion(1));
        coordinator.set_max_write_buffer_entries(cfg.storage.max_write_buffer_entries);

        // T3-E12 §D7: cache-mode databases still populate `wal_codec`
        // uniformly — the field is always `Box<dyn StorageCodec>`, not
        // `Option`, so the type signature does not leak "this is a
        // cache database" into the field type. `IdentityCodec` is the
        // typical resolution here and is effectively a no-op at runtime.
        let wal_codec_for_cache =
            strata_durability::get_codec(&cfg.storage.codec).map_err(|e| {
                StrataError::internal(format!(
                    "cache database could not initialize codec '{}': {}",
                    cfg.storage.codec, e
                ))
            })?;

        let db = Arc::new(Self {
            data_dir: PathBuf::new(), // Empty path for ephemeral
            database_uuid: [0u8; 16], // No persistence — UUID not needed
            storage: Arc::new(storage),
            wal_writer: None, // No WAL for ephemeral
            wal_codec: wal_codec_for_cache,

            persistence_mode: PersistenceMode::Ephemeral,
            coordinator,
            durability_mode: parking_lot::RwLock::new(DurabilityMode::Cache), // Irrelevant but set for consistency
            accepting_transactions: Arc::new(AtomicBool::new(true)),
            wal_writer_health: Arc::new(ParkingMutex::new(WalWriterHealth::Healthy)),
            // Ephemeral/cache never performs WAL recovery, so lossy fallback
            // cannot fire here — the slot stays `None` for the life of the
            // instance.
            last_lossy_recovery_report: Arc::new(ParkingMutex::new(None)),
            extensions: DashMap::new(),
            config: parking_lot::RwLock::new(cfg),
            flush_shutdown: Arc::new(AtomicBool::new(false)),
            flush_handle: ParkingMutex::new(None),
            scheduler: Arc::new(BackgroundScheduler::new(bg_threads, 4096)),
            flush_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            backpressure_counter: AtomicU64::new(0),
            lock_file: parking_lot::Mutex::new(None), // No lock for ephemeral databases
            wal_dir: PathBuf::new(),
            watermark: super::refresh::ContiguousWatermark::default(),
            refresh_gate: super::refresh::RefreshGate::new(),
            refresh_publish_barrier: parking_lot::RwLock::new(()),
            follower: false,
            shutdown_started: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
            dag_hook_slot: super::DagHookSlot::new(),
            branch_op_observers: super::BranchOpObserverRegistry::new(),
            commit_observers: super::CommitObserverRegistry::new(),
            abort_observers: super::AbortObserverRegistry::new(),
            replay_observers: super::ReplayObserverRegistry::new(),
            lifecycle_state: std::sync::atomic::AtomicU8::new(
                super::LifecycleState::Uninitialized.as_u8(),
            ),
            lifecycle_state_mu: parking_lot::Mutex::new(()),
            lifecycle_state_cv: parking_lot::Condvar::new(),
            runtime_signature: parking_lot::RwLock::new(None),
            merge_registry: super::MergeHandlerRegistry::new(),
        });

        // Note: Ephemeral databases are NOT registered in the global registry
        // because they have no path and should always be independent instances

        // Enable the inverted index for keyword/BM25 search.
        // Cache databases skip subsystem recovery (nothing to recover),
        // so we enable the index directly.
        let index = db.extension::<crate::search::InvertedIndex>()?;
        index.enable();

        Ok(db)
    }

    // ========================================================================
    // OpenSpec-based Entry Point (T2-E1)
    // ========================================================================

    /// Open a database using an `OpenSpec`.
    ///
    /// This is the single internal constructor dispatcher for all database modes.
    /// It validates the spec, routes to the appropriate open helper, and runs
    /// the full subsystem lifecycle (recover → initialize → bootstrap).
    ///
    /// ## Lifecycle Order
    ///
    /// 1. Validate spec fields
    /// 2. Resolve configuration (from spec or file)
    /// 3. Route to mode-specific open helper (primary, follower, cache)
    /// 4. For each subsystem: `recover()`
    /// 5. For each subsystem: `initialize()` (write-free wiring)
    /// 6. If mode allows bootstrap: for each subsystem `bootstrap()`
    /// 7. Ensure default branch (if specified and mode allows)
    /// 8. Return initialized database
    ///
    /// ## Mode Behavior
    ///
    /// | Mode | Recover | Initialize | Bootstrap | Default Branch |
    /// |------|---------|------------|-----------|----------------|
    /// | Primary | Yes | Yes | Yes | Yes |
    /// | Follower | Yes | Yes | No | No |
    /// | Cache | No* | Yes | Yes | Yes |
    ///
    /// *Cache databases have no persistent state to recover.
    ///
    /// ## Thread Safety
    ///
    /// For Primary and Follower modes, the registry ensures that opening the
    /// same path twice returns the same `Arc<Database>`.
    pub fn open_runtime(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        use super::spec::DatabaseMode;

        // Validate spec
        if !spec.mode.is_ephemeral() && spec.path.as_os_str().is_empty() {
            return Err(StrataError::invalid_input(
                "path is required for Primary and Follower modes",
            ));
        }

        // Route to mode-specific open helper
        match spec.mode {
            DatabaseMode::Primary => Self::open_runtime_primary(spec),
            DatabaseMode::Follower => Self::open_runtime_follower(spec),
            DatabaseMode::Cache => Self::open_runtime_cache(spec),
        }
    }

    /// Open a primary database from an `OpenSpec`.
    fn open_runtime_primary(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        use super::compat::CompatibilitySignature;

        let super::spec::OpenSpec {
            mode: _,
            path,
            config,
            subsystems,
            default_branch,
        } = spec;

        let data_dir = path;
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;
        let config_path = canonical_path.join(config::CONFIG_FILE_NAME);
        let resolved_cfg = {
            let base = if let Some(cfg) = config.as_ref() {
                cfg.clone()
            } else if config_path.exists() {
                config::StrataConfig::from_file(&config_path)?
            } else {
                config::StrataConfig::default()
            };
            sanitize_config(base)
        };

        // Hardware profiling is ephemeral — it rewrites fields left at their
        // baseline defaults for a given host profile (embedded / desktop /
        // server) without persisting the result. `resolved_cfg` above stays
        // un-profiled so the `strata.toml` write below preserves the
        // "profile per host at open time" contract: moving the database
        // directory to a different host class must re-profile, not inherit
        // the first host's values.
        //
        // For signature construction, however, the post-profile values are
        // what the running runtime actually uses, so we extract signature
        // fields from a profiled copy. `acquire_primary_db` applies the
        // profile internally (idempotent for fields already at non-baseline
        // values), so the running database ends up sized with the same
        // post-profile values the signature advertises.
        let profiled_for_signature = {
            let mut c = resolved_cfg.clone();
            crate::database::profile::apply_hardware_profile_if_defaults(&mut c);
            c
        };

        let durability_mode = profiled_for_signature.durability_mode()?;
        let codec_name = profiled_for_signature.storage.codec.clone();
        let background_threads = profiled_for_signature.storage.background_threads;
        let allow_lossy_recovery = profiled_for_signature.allow_lossy_recovery;

        let requested_runtime_cfg = config.as_ref().map(|_| profiled_for_signature.clone());

        let subsystem_names: Vec<&'static str> = subsystems.iter().map(|s| s.name()).collect();
        let requested_signature = CompatibilitySignature::from_spec(
            super::spec::DatabaseMode::Primary,
            subsystem_names.clone(),
            durability_mode,
            codec_name.clone(),
            default_branch.clone(),
            background_threads,
            allow_lossy_recovery,
        );

        match Self::acquire_primary_db(
            &canonical_path,
            durability_mode,
            resolved_cfg.clone(),
            subsystems,
            Some(requested_signature),
        )? {
            AcquiredDatabase::Existing(db) => {
                // On reuse, do NOT overwrite config — the existing DB's config is
                // authoritative. Signature check ensures the caller's request is
                // compatible with the running instance.
                let db = Self::wait_for_opened_db(db)?;
                if let Some(requested_cfg) = requested_runtime_cfg.as_ref() {
                    Self::validate_requested_config_reuse(&db, requested_cfg)?;
                }
                Self::validate_control_artifact_reuse(&db, &config_path)?;
                crate::primitives::branch::validate_reserved_branch_aliases(&db)?;
                Ok(db)
            }
            AcquiredDatabase::New { db, canonical_path } => {
                Self::finish_opened_db(db, &canonical_path, move |db| {
                    crate::primitives::branch::validate_reserved_branch_aliases(db)?;
                    let effective_default_branch =
                        Self::resolve_effective_default_branch(db, default_branch.clone())?;
                    let effective_signature = CompatibilitySignature::from_spec(
                        super::spec::DatabaseMode::Primary,
                        subsystem_names,
                        durability_mode,
                        codec_name,
                        effective_default_branch.clone(),
                        background_threads,
                        allow_lossy_recovery,
                    );
                    db.set_runtime_signature(effective_signature);
                    // Write the *sanitized* resolved_cfg, not the original config.
                    // This ensures persisted config matches runtime state (e.g.,
                    // auto_embed=false when embed feature is not compiled).
                    resolved_cfg.write_to_file(&config_path)?;
                    Self::run_lifecycle_hooks(db, true)?;
                    if let Some(branch_name) = &effective_default_branch {
                        Self::ensure_default_branch(db, branch_name)?;
                    }
                    // B3.1: migrate any legacy BranchMetadata into
                    // BranchControlStore. Runs once per database; second
                    // open is a no-op. Lifecycle hooks above install the
                    // DAG hook first, so migration's uncapped DAG log
                    // fallback for fork-anchor derivation can run.
                    crate::branch_ops::branch_control_store::BranchControlStore::ensure_migrated(
                        db,
                    )?;
                    if let Err(e) =
                        crate::branch_ops::branch_control_store::BranchControlStore::rebuild_dag_projection(db)
                    {
                        warn!(
                            target: "strata::branch::migration",
                            error = %e,
                            "failed to rebuild _branch_dag projection from BranchControlStore during open"
                        );
                    }
                    Ok(())
                })
            }
        }
    }

    /// Open a follower database from an `OpenSpec`.
    ///
    /// Followers are NOT deduplicated via the registry — each caller gets their
    /// own independent instance with its own refresh state. This differs from
    /// primaries, which have singleton semantics (one per path in the process).
    fn open_runtime_follower(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        use super::compat::CompatibilitySignature;

        let super::spec::OpenSpec {
            mode: _,
            path,
            config,
            subsystems,
            default_branch,
        } = spec;

        let data_dir = path;
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;
        let config_path = canonical_path.join(config::CONFIG_FILE_NAME);
        let cfg = {
            let base = if let Some(cfg) = config.as_ref() {
                cfg.clone()
            } else if config_path.exists() {
                config::StrataConfig::from_file(&config_path)?
            } else {
                config::StrataConfig::default()
            };
            sanitize_config(base)
        };

        // Profile into a separate copy for signature extraction so the
        // un-profiled `cfg` is what the caller sees if it ever gets
        // persisted. Hardware profiling is ephemeral — see
        // `open_runtime_primary` for the full rationale.
        let profiled_for_signature = {
            let mut c = cfg.clone();
            crate::database::profile::apply_hardware_profile_if_defaults(&mut c);
            c
        };

        let durability_mode = profiled_for_signature.durability_mode()?;
        let codec_name = profiled_for_signature.storage.codec.clone();
        let background_threads = profiled_for_signature.storage.background_threads;
        let allow_lossy_recovery = profiled_for_signature.allow_lossy_recovery;

        // Validate the configured codec exists before touching any state.
        // Mirrors the primary path so a follower with an unknown codec is
        // rejected consistently — with or without an on-disk MANIFEST.
        strata_durability::get_codec(&cfg.storage.codec).map_err(|e| {
            StrataError::internal(format!(
                "invalid storage codec '{}': {}",
                cfg.storage.codec, e
            ))
        })?;

        // T3-E12 Phase 2 removed the follower codec+WAL rejection;
        // the v3 envelope + codec-threaded reader now handle
        // non-identity codecs on follower replay.

        let subsystem_names: Vec<&'static str> = subsystems.iter().map(|s| s.name()).collect();
        let requested_signature = CompatibilitySignature::from_spec(
            super::spec::DatabaseMode::Follower,
            subsystem_names,
            durability_mode,
            codec_name.clone(),
            default_branch,
            background_threads,
            allow_lossy_recovery,
        );

        // Create follower database — no registry check, followers are independent
        let db = Self::acquire_follower_db(canonical_path.clone(), cfg)?;

        // Followers are read-only — repair_space_metadata_on_open is a no-op
        // but kept for symmetry with primary path.
        Self::repair_space_metadata_on_open(&db);

        // Run subsystem recovery
        for subsystem in &subsystems {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running follower subsystem recovery"
            );
            subsystem.recover(&db)?;
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Follower subsystem recovery complete"
            );
        }

        crate::primitives::branch::validate_reserved_branch_aliases(&db)?;
        let effective_default_branch =
            Self::resolve_effective_default_branch(&db, requested_signature.default_branch)?;
        let effective_signature = CompatibilitySignature::from_spec(
            super::spec::DatabaseMode::Follower,
            subsystems.iter().map(|s| s.name()).collect(),
            durability_mode,
            codec_name,
            effective_default_branch,
            background_threads,
            allow_lossy_recovery,
        );

        db.set_subsystems(subsystems);
        db.set_runtime_signature(effective_signature);
        db.set_lifecycle_initializing();

        // Followers don't use the registry — no insertion needed.
        // Each follower is an independent instance with its own refresh state.

        // Complete lifecycle directly (no registry entry to clean up on failure)
        Self::run_lifecycle_hooks(&db, false)?;
        db.set_lifecycle_complete();
        Ok(db)
    }

    /// Open a cache database from an `OpenSpec`.
    fn open_runtime_cache(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        use super::compat::CompatibilitySignature;

        let super::spec::OpenSpec {
            mode: _,
            path: _,
            config,
            subsystems,
            default_branch,
        } = spec;

        // Create ephemeral database with spec.config if provided.
        // Cache databases are not in the registry, so no reuse check needed.
        let db = Self::create_ephemeral_bare(config.as_ref())?;

        let resolved_cfg = db.config();
        let durability_mode = resolved_cfg.durability_mode()?;
        let codec_name = resolved_cfg.storage.codec.clone();
        let background_threads = resolved_cfg.storage.background_threads;
        let allow_lossy_recovery = resolved_cfg.allow_lossy_recovery;
        let subsystem_names: Vec<&'static str> = subsystems.iter().map(|s| s.name()).collect();
        let requested_signature = CompatibilitySignature::from_spec(
            super::spec::DatabaseMode::Cache,
            subsystem_names,
            durability_mode,
            codec_name,
            default_branch.clone(),
            background_threads,
            allow_lossy_recovery,
        );

        // Run subsystem recovery (no-op for cache, but maintains consistency)
        // and install subsystems for freeze-on-drop
        for subsystem in &subsystems {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running subsystem recovery (cache mode)"
            );
            // Cache databases have no persistent state, so recover() should
            // be fast (initialize to empty). We call it for consistency with
            // primary/follower paths and to let subsystems know they're starting.
            subsystem.recover(&db)?;
        }
        db.set_subsystems(subsystems);
        db.set_runtime_signature(requested_signature);

        // Run lifecycle hooks (initialize and bootstrap)
        Self::run_lifecycle_hooks(&db, true)?;
        crate::primitives::branch::validate_reserved_branch_aliases(&db)?;

        // Ensure default branch if specified
        if let Some(branch_name) = &default_branch {
            Self::ensure_default_branch(&db, branch_name)?;
        }

        // Mark lifecycle complete
        db.set_lifecycle_complete();

        Ok(db)
    }

    /// Run the new lifecycle hooks on subsystems.
    ///
    /// Called after `recover()` has already run (via acquire_*_db helpers).
    /// Runs `initialize()` on all subsystems, then optionally `bootstrap()`.
    fn run_lifecycle_hooks(db: &Arc<Self>, run_bootstrap: bool) -> StrataResult<()> {
        let subsystems = db.subsystems.read();

        // Phase 1: initialize (write-free wiring)
        for subsystem in subsystems.iter() {
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Running subsystem initialize"
            );
            subsystem.initialize(db)?;
            info!(
                target: "strata::recovery",
                subsystem = subsystem.name(),
                "Subsystem initialize complete"
            );
        }

        // Phase 2: bootstrap (idempotent writes) — skipped for followers
        if run_bootstrap {
            for subsystem in subsystems.iter() {
                info!(
                    target: "strata::recovery",
                    subsystem = subsystem.name(),
                    "Running subsystem bootstrap"
                );
                subsystem.bootstrap(db)?;
                info!(
                    target: "strata::recovery",
                    subsystem = subsystem.name(),
                    "Subsystem bootstrap complete"
                );
            }
        }

        Ok(())
    }

    /// Ensure a default branch exists.
    ///
    /// Creates the branch if it doesn't exist. Idempotent.
    fn ensure_default_branch(db: &Arc<Self>, branch_name: &str) -> StrataResult<()> {
        let branches = db.branches();
        if !branches.exists(branch_name)? {
            info!(
                target: "strata::db",
                branch = branch_name,
                "Creating default branch"
            );
            branches.create(branch_name)?;
        }
        crate::primitives::branch::write_default_branch_marker(db, branch_name)?;
        Ok(())
    }

    fn resolve_effective_default_branch(
        db: &Arc<Self>,
        requested: Option<String>,
    ) -> StrataResult<Option<String>> {
        match crate::primitives::branch::read_default_branch_marker(db.as_ref())? {
            Some(stored) => {
                if requested.as_deref().is_some_and(|name| name != stored) {
                    warn!(
                        target: "strata::db",
                        requested = ?requested,
                        stored = %stored,
                        "Ignoring requested default branch; persisted branch metadata is authoritative"
                    );
                }
                Ok(Some(stored))
            }
            None => Ok(requested),
        }
    }

    fn wait_for_opened_db(db: Arc<Self>) -> StrataResult<Arc<Self>> {
        match db.wait_for_lifecycle_state() {
            super::LifecycleState::Initialized => Ok(db),
            super::LifecycleState::Failed => Err(StrataError::internal(
                "existing database instance failed during lifecycle initialization; retry the open",
            )),
            super::LifecycleState::Uninitialized => Err(StrataError::internal(
                "existing database instance was published before lifecycle initialization started",
            )),
            super::LifecycleState::Initializing => {
                unreachable!("wait returned while still initializing")
            }
        }
    }

    fn validate_control_artifact_reuse(db: &Arc<Self>, config_path: &Path) -> StrataResult<()> {
        if db.persistence_mode != PersistenceMode::Disk || db.data_dir.as_os_str().is_empty() {
            return Ok(());
        }

        let live_cfg = db.config.read();
        if !config_path.exists() {
            return Err(StrataError::incompatible_reuse(format!(
                "on-disk strata.toml '{}' is missing while a database instance for this path \
                 is already open",
                config_path.display()
            )));
        }

        let mut on_disk_cfg = config::StrataConfig::from_file(config_path).map_err(|e| {
            StrataError::incompatible_reuse(format!(
                "on-disk strata.toml '{}' is invalid while a database instance for this path \
                 is already open: {}",
                config_path.display(),
                e
            ))
        })?;
        on_disk_cfg = sanitize_config(on_disk_cfg);
        crate::database::profile::apply_hardware_profile_if_defaults(&mut on_disk_cfg);

        if on_disk_cfg != *live_cfg {
            return Err(StrataError::incompatible_reuse(format!(
                "on-disk strata.toml '{}' diverged from the running database configuration; \
                 shut down and reopen the database to apply file edits",
                config_path.display()
            )));
        }

        Ok(())
    }

    fn validate_requested_config_reuse(
        db: &Arc<Self>,
        requested_cfg: &StrataConfig,
    ) -> StrataResult<()> {
        let live_cfg = db.config.read();
        if *requested_cfg != *live_cfg {
            return Err(StrataError::incompatible_reuse(
                "requested explicit configuration diverged from the running database \
                 configuration; shut down and reopen the database to apply programmatic \
                 config changes",
            ));
        }
        Ok(())
    }

    fn finish_opened_db<F>(
        db: Arc<Self>,
        canonical_path: &Path,
        open_fn: F,
    ) -> StrataResult<Arc<Self>>
    where
        F: FnOnce(&Arc<Self>) -> StrataResult<()>,
    {
        match open_fn(&db) {
            Ok(()) => {
                db.set_lifecycle_complete();
                Ok(db)
            }
            Err(error) => {
                db.set_lifecycle_failed();
                Self::remove_registry_entry_if_same(canonical_path, &db);
                Err(error)
            }
        }
    }

    fn remove_registry_entry_if_same(canonical_path: &Path, db: &Arc<Self>) {
        let mut registry = super::OPEN_DATABASES.lock();
        let should_remove = registry.get(canonical_path).is_some_and(|weak| {
            weak.upgrade()
                .is_none_or(|existing| Arc::ptr_eq(&existing, db))
        });
        if should_remove {
            registry.remove(canonical_path);
        }
    }
}
