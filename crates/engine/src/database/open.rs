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
use strata_concurrency::RecoveryCoordinator;
use strata_durability::wal::{DurabilityMode, WalConfig, WalWriter};
use strata_durability::ManifestManager;
use strata_storage::SegmentedStore;
use tracing::{info, warn};

/// Apply all storage configuration settings to a SegmentedStore.
///
/// Centralizes the 7 storage-config setters so every open path
/// (primary, follower, cache) applies the same set of knobs.
fn apply_storage_config(storage: &SegmentedStore, cfg: &StorageConfig) {
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
fn restrict_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)) {
        warn!(target: "strata::db", path = %path.display(), error = %e,
            "Failed to restrict directory permissions");
    }
}

#[cfg(not(unix))]
fn restrict_dir(_path: &Path) {}

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
use super::{Database, PersistenceMode};

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
    pub fn open<P: AsRef<Path>>(path: P) -> StrataResult<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;

        let config_path = data_dir.join(config::CONFIG_FILE_NAME);
        config::StrataConfig::write_default_if_missing(&config_path)?;
        let cfg = config::StrataConfig::from_file(&config_path)?;

        Self::open_with_config(path, cfg)
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
    pub fn open_with_config<P: AsRef<Path>>(path: P, cfg: StrataConfig) -> StrataResult<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;

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

        let mode = cfg.durability_mode()?;

        // Write config to strata.toml so restarts pick it up
        let config_path = data_dir.join(config::CONFIG_FILE_NAME);
        cfg.write_to_file(&config_path)?;

        let db = Self::open_internal(path, mode, cfg)?;
        Ok(db)
    }

    /// Open a primary database with an explicit subsystem list.
    ///
    /// Subsystem-aware mirror of `Database::open`. Reads `strata.toml` from
    /// the data directory (creating a default if missing) and delegates to
    /// [`Database::open_with_config_and_subsystems`]. The same subsystem
    /// list drives recovery on open and freeze-on-drop.
    pub(crate) fn open_with_subsystems<P: AsRef<Path>>(
        path: P,
        subsystems: Vec<Box<dyn crate::recovery::Subsystem>>,
    ) -> StrataResult<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;

        let config_path = data_dir.join(config::CONFIG_FILE_NAME);
        config::StrataConfig::write_default_if_missing(&config_path)?;
        let cfg = config::StrataConfig::from_file(&config_path)?;

        Self::open_with_config_and_subsystems(path, cfg, subsystems)
    }

    /// Open a primary database with an explicit `StrataConfig` and an
    /// explicit subsystem list.
    ///
    /// Subsystem-aware mirror of `Database::open_with_config`. The supplied
    /// config is written to `strata.toml` so that subsequent opens pick up
    /// the same settings, then control is handed to
    /// [`Database::open_internal_with_subsystems`].
    pub(crate) fn open_with_config_and_subsystems<P: AsRef<Path>>(
        path: P,
        cfg: StrataConfig,
        subsystems: Vec<Box<dyn crate::recovery::Subsystem>>,
    ) -> StrataResult<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;

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

        let mode = cfg.durability_mode()?;

        // Write config to strata.toml so restarts pick it up
        let config_path = data_dir.join(config::CONFIG_FILE_NAME);
        cfg.write_to_file(&config_path)?;

        Self::open_internal_with_subsystems(path, mode, cfg, subsystems)
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
    ///   6. Insert `Arc::downgrade(&db)` into `OPEN_DATABASES`.
    ///   7. Drop the mutex (via end-of-scope).
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
    ) -> StrataResult<Arc<Self>> {
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
                let requested: Vec<&'static str> = subsystems.iter().map(|s| s.name()).collect();
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
                         `DatabaseBuilder` with the same list) across all \
                         call sites for this path. See audit follow-up to \
                         #2354 Finding 2."
                    );
                }
                info!(target: "strata::db", path = ?canonical_path, "Returning existing database instance");
                return Ok(db);
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

        // Publish the fully-recovered Arc. Concurrent openers that were
        // waiting on the mutex will now see a Database with recovery
        // complete and subsystems installed.
        registry.insert(canonical_path, Arc::downgrade(&db));

        Ok(db)
    }

    /// Convenience entry point used by `Database::open` / `open_with_config`.
    ///
    /// Delegates to `open_internal_with_subsystems` with a hardcoded
    /// `[SearchSubsystem]` list. The engine crate does not depend on
    /// `strata-vector`, so vector recovery is only available through the
    /// builder with an explicit `VectorSubsystem` registration (see
    /// `DatabaseBuilder`).
    pub(super) fn open_internal<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        Self::open_internal_with_subsystems(
            path,
            durability_mode,
            cfg,
            vec![Box::new(crate::search::SearchSubsystem)],
        )
    }

    /// Open a primary database with an explicit subsystem list.
    ///
    /// The supplied subsystems drive both recovery (called in registration
    /// order) and freeze-on-drop (called in reverse order). This is the
    /// canonical open path — both `DatabaseBuilder` and the `Database::open`
    /// convenience API route through here, so the supplied `subsystems` list
    /// is the sole driver of recovery.
    ///
    /// All the heavy lifting (directory prep, registry dedup, WAL replay,
    /// `repair_space_metadata_on_open`, the `subsystem.recover(&db)` loop,
    /// `set_subsystems`, and the final registry insert) happens inside
    /// `acquire_primary_db` while it holds the `OPEN_DATABASES` mutex, so
    /// concurrent openers for the same path cannot observe a half-
    /// initialized instance.
    pub(crate) fn open_internal_with_subsystems<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        subsystems: Vec<Box<dyn crate::recovery::Subsystem>>,
    ) -> StrataResult<Arc<Self>> {
        Self::acquire_primary_db(path.as_ref(), durability_mode, cfg, subsystems)
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
    pub fn open_follower<P: AsRef<Path>>(path: P) -> StrataResult<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        let config_path = canonical_path.join(config::CONFIG_FILE_NAME);
        let cfg = if config_path.exists() {
            config::StrataConfig::from_file(&config_path)?
        } else {
            config::StrataConfig::default()
        };

        Self::open_follower_internal(canonical_path, cfg)
    }

    /// Open a read-only follower of an existing database with an explicit
    /// subsystem list.
    ///
    /// Subsystem-aware mirror of `Database::open_follower`. Reads
    /// `strata.toml` if present (else defaults) and delegates to
    /// [`Database::open_follower_internal_with_subsystems`]. Followers do
    /// not freeze on drop, but the supplied subsystems still drive recovery.
    pub(crate) fn open_follower_with_subsystems<P: AsRef<Path>>(
        path: P,
        subsystems: Vec<Box<dyn crate::recovery::Subsystem>>,
    ) -> StrataResult<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        let config_path = canonical_path.join(config::CONFIG_FILE_NAME);
        let cfg = if config_path.exists() {
            config::StrataConfig::from_file(&config_path)?
        } else {
            config::StrataConfig::default()
        };

        Self::open_follower_internal_with_subsystems(canonical_path, cfg, subsystems)
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

        let wal_dir = canonical_path.join("wal");

        // Segments directory for reading existing segments (read-only, no create)
        let segments_dir = canonical_path.join("segments");

        // Recovery — purely read-only (no truncation, no file writes)
        let recovery = RecoveryCoordinator::new(wal_dir.clone())
            .with_segments(segments_dir, cfg.storage.effective_write_buffer_size())
            .with_lossy_recovery(cfg.allow_lossy_recovery);
        let result = match recovery.recover() {
            Ok(result) => result,
            Err(e) => {
                if cfg.allow_lossy_recovery {
                    warn!(target: "strata::db",
                        error = %e,
                        "Follower recovery failed — starting with empty state");
                    strata_concurrency::RecoveryResult::empty()
                } else {
                    return Err(StrataError::corruption(format!(
                        "WAL recovery failed in follower mode: {}. \
                         Set allow_lossy_recovery=true to force open.",
                        e
                    )));
                }
            }
        };

        info!(target: "strata::db",
            txns_replayed = result.stats.txns_replayed,
            writes_applied = result.stats.writes_applied,
            "Follower recovery complete");

        let wal_watermark = AtomicU64::new(result.stats.max_txn_id.as_u64());

        let coordinator = TransactionCoordinator::from_recovery_with_limits(
            &result,
            cfg.storage.max_write_buffer_entries,
        );

        let storage = Arc::new(result.storage);
        apply_storage_config(&storage, &cfg.storage);

        let bg_threads = cfg.storage.background_threads.max(1);

        Self::recover_segments_and_bump(&storage, &coordinator, cfg.allow_lossy_recovery)?;

        // Load database UUID from MANIFEST if it exists (read-only, no create)
        let manifest_path = canonical_path.join("MANIFEST");
        let database_uuid = if ManifestManager::exists(&manifest_path) {
            ManifestManager::load(manifest_path)
                .map(|m| m.manifest().database_uuid)
                .unwrap_or([0u8; 16])
        } else {
            [0u8; 16]
        };

        let db = Arc::new(Self {
            data_dir: canonical_path,
            database_uuid,
            storage,
            wal_writer: None, // No WAL writer — read-only

            persistence_mode: PersistenceMode::Disk,
            coordinator,
            durability_mode: DurabilityMode::Cache, // Irrelevant for follower
            accepting_transactions: AtomicBool::new(true),
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
            _lock_file: None, // No lock acquired
            wal_dir,
            wal_watermark,
            follower: true,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
            dag_hook_slot: super::DagHookSlot::new(),
            branch_op_observers: super::BranchOpObserverRegistry::new(),
            commit_observers: super::CommitObserverRegistry::new(),
            replay_observers: super::ReplayObserverRegistry::new(),
            lifecycle_complete: AtomicBool::new(false),
            merge_registry: super::MergeHandlerRegistry::new(),
        });

        Ok(db)
    }

    /// Convenience entry point used by `Database::open_follower`.
    ///
    /// Delegates to `open_follower_internal_with_subsystems` with a hardcoded
    /// `[SearchSubsystem]` list. The engine crate does not depend on
    /// `strata-vector`, so vector recovery for followers is only available
    /// through the builder with an explicit `VectorSubsystem` registration.
    fn open_follower_internal(
        canonical_path: PathBuf,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        Self::open_follower_internal_with_subsystems(
            canonical_path,
            cfg,
            vec![Box::new(crate::search::SearchSubsystem)],
        )
    }

    /// Open a follower database with an explicit subsystem list.
    ///
    /// The supplied subsystems drive recovery (called in registration
    /// order). Followers do not freeze on drop — `Drop for Database`
    /// short-circuits on `is_follower()` — but the supplied subsystems
    /// are still installed via `set_subsystems` for symmetry with the
    /// primary path. The supplied `subsystems` list is the sole driver
    /// of recovery for this open.
    pub(crate) fn open_follower_internal_with_subsystems(
        canonical_path: PathBuf,
        cfg: StrataConfig,
        subsystems: Vec<Box<dyn crate::recovery::Subsystem>>,
    ) -> StrataResult<Arc<Self>> {
        let db = Self::acquire_follower_db(canonical_path, cfg)?;

        // Followers are read-only — `repair_space_metadata_on_open` is a
        // no-op for them but kept on the same code path for symmetry. The
        // union behaviour in `SpaceIndex::list/exists` still gives the
        // follower correct discovery via a data scan.
        Self::repair_space_metadata_on_open(&db);

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

        db.set_subsystems(subsystems);

        Ok(db)
    }

    /// Spawn the background WAL flush thread for Standard durability mode.
    ///
    /// Returns `None` for non-Standard modes (Cache, Always).
    fn spawn_wal_flush_thread(
        durability_mode: DurabilityMode,
        wal: &Arc<ParkingMutex<WalWriter>>,
        shutdown: &Arc<AtomicBool>,
    ) -> StrataResult<Option<std::thread::JoinHandle<()>>> {
        if let DurabilityMode::Standard { interval_ms, .. } = durability_mode {
            let wal = Arc::clone(wal);
            let shutdown = Arc::clone(shutdown);
            let interval = std::time::Duration::from_millis(interval_ms);

            let handle = std::thread::Builder::new()
                .name("strata-wal-flush".to_string())
                .spawn(move || {
                    while !shutdown.load(Ordering::Relaxed) {
                        std::thread::sleep(interval);
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        // Background sync: briefly lock WAL to flush BufWriter
                        // to OS page cache, then fdatasync outside the lock.
                        // The lock hold is microseconds (BufWriter flush only),
                        // not milliseconds (no fdatasync under lock).
                        let (sync_fd, meta_snapshot) = {
                            let mut w = wal.lock();
                            let fd = match w.prepare_background_sync() {
                                Ok(Some(fd)) => Some(fd),
                                Ok(None) => None,
                                Err(e) => {
                                    tracing::error!(target: "strata::wal", error = %e, "Background WAL flush failed");
                                    None
                                }
                            };
                            let meta = w.snapshot_active_meta();
                            (fd, meta)
                        }; // WAL lock released — held only for BufWriter flush
                        if let Some(fd) = sync_fd {
                            if let Err(e) = fd.sync_all() {
                                tracing::error!(target: "strata::wal", error = %e, "Background WAL sync failed");
                            }
                        }
                        // Write .meta sidecar outside the lock (best-effort, 2 fsyncs).
                        if let Some((meta, wal_dir)) = meta_snapshot {
                            if let Err(e) = meta.write_to_file(&wal_dir) {
                                tracing::debug!(target: "strata::wal", error = %e, "Background .meta write failed (non-fatal)");
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

    /// Recover on-disk segments and bump the coordinator's version floor.
    fn recover_segments_and_bump(
        storage: &Arc<SegmentedStore>,
        coordinator: &TransactionCoordinator,
        allow_lossy: bool,
    ) -> StrataResult<()> {
        match storage.recover_segments() {
            Ok(seg_info) => {
                if seg_info.segments_loaded > 0 {
                    info!(target: "strata::db",
                        branches = seg_info.branches_recovered,
                        segments = seg_info.segments_loaded,
                        errors_skipped = seg_info.errors_skipped,
                        "Recovered segments from disk");
                }
                if seg_info.max_commit_id > CommitVersion::ZERO {
                    coordinator.bump_version_floor(seg_info.max_commit_id);
                }
            }
            Err(e) => {
                warn!(target: "strata::db", error = %e, "Segment recovery failed");
                if !allow_lossy {
                    return Err(StrataError::corruption(format!(
                        "Segment recovery failed: {}",
                        e
                    )));
                }
            }
        }
        Ok(())
    }

    /// Shared tail of database open: recovery, WAL writer, coordinator, flush thread.
    fn open_finish(
        canonical_path: PathBuf,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        lock_file: Option<std::fs::File>,
    ) -> StrataResult<Arc<Self>> {
        // Validate the configured codec exists before touching any state.
        // This prevents creating a MANIFEST with an invalid codec_id.
        strata_durability::get_codec(&cfg.storage.codec).map_err(|e| {
            StrataError::internal(format!(
                "invalid storage codec '{}': {}",
                cfg.storage.codec, e
            ))
        })?;

        // WAL recovery does not yet support non-identity codecs — the WalReader
        // parses raw bytes without codec decoding. Encrypted WAL records would be
        // unreadable on restart, causing data loss. Block until WAL codec support
        // is implemented (requires length-prefix envelope + reader codec threading).
        if cfg.storage.codec != "identity" && durability_mode.requires_wal() {
            return Err(StrataError::internal(format!(
                "codec '{}' is not yet supported with WAL-based durability (Standard/Always). \
                 Encryption at rest requires WAL reader codec support (tracked). \
                 Use durability = \"cache\" for encrypted in-memory databases.",
                cfg.storage.codec
            )));
        }

        // Create WAL directory
        let wal_dir = canonical_path.join("wal");
        std::fs::create_dir_all(&wal_dir).map_err(StrataError::from)?;
        restrict_dir(&wal_dir);

        // Create segments directory for on-disk segment storage
        let segments_dir = canonical_path.join("segments");
        std::fs::create_dir_all(&segments_dir).map_err(StrataError::from)?;
        restrict_dir(&segments_dir);

        // Use RecoveryCoordinator for proper transaction-aware recovery
        // This reads all WalRecords from the segmented WAL directory
        let recovery = RecoveryCoordinator::new(wal_dir.clone())
            .with_segments(segments_dir, cfg.storage.effective_write_buffer_size())
            .with_lossy_recovery(cfg.allow_lossy_recovery);
        let result = match recovery.recover() {
            Ok(result) => result,
            Err(e) => {
                if cfg.allow_lossy_recovery {
                    warn!(
                        target: "strata::db",
                        error = %e,
                        "Recovery failed — starting with empty state (allow_lossy_recovery=true)"
                    );
                    strata_concurrency::RecoveryResult::empty()
                } else {
                    return Err(StrataError::corruption(format!(
                        "WAL recovery failed: {}. Set allow_lossy_recovery=true to force open with data loss.",
                        e
                    )));
                }
            }
        };

        info!(
            target: "strata::db",
            txns_replayed = result.stats.txns_replayed,
            writes_applied = result.stats.writes_applied,
            deletes_applied = result.stats.deletes_applied,
            final_version = result.stats.final_version.as_u64(),
            "Recovery complete"
        );

        // Load or create MANIFEST to get the database UUID.
        // On first open: generate a new UUID and persist it with the configured codec.
        // On subsequent opens: load the existing UUID and validate codec matches.
        let manifest_path = canonical_path.join("MANIFEST");
        let database_uuid = if ManifestManager::exists(&manifest_path) {
            let m = ManifestManager::load(manifest_path)
                .map_err(|e| StrataError::internal(format!("failed to load MANIFEST: {}", e)))?;
            let stored_codec = &m.manifest().codec_id;
            if stored_codec != &cfg.storage.codec {
                return Err(StrataError::internal(format!(
                    "codec mismatch: database was created with '{}' but config specifies '{}'. \
                     A database cannot be reopened with a different codec.",
                    stored_codec, cfg.storage.codec
                )));
            }
            m.manifest().database_uuid
        } else {
            let uuid = *uuid::Uuid::new_v4().as_bytes();
            ManifestManager::create(manifest_path, uuid, cfg.storage.codec.clone())
                .map_err(|e| StrataError::internal(format!("failed to create MANIFEST: {}", e)))?;
            uuid
        };

        // Instantiate the configured storage codec (identity or aes-gcm-256)
        let codec = strata_durability::get_codec(&cfg.storage.codec).map_err(|e| {
            StrataError::internal(format!("failed to initialize storage codec: {}", e))
        })?;

        // Open segmented WAL writer for appending
        let wal_writer = WalWriter::new(
            wal_dir.clone(),
            database_uuid,
            durability_mode,
            WalConfig::default(),
            codec,
        )?;

        let wal_watermark = AtomicU64::new(result.stats.max_txn_id.as_u64());

        let wal_arc = Arc::new(ParkingMutex::new(wal_writer));
        let flush_shutdown = Arc::new(AtomicBool::new(false));
        let flush_handle =
            Self::spawn_wal_flush_thread(durability_mode, &wal_arc, &flush_shutdown)?;

        // Create coordinator with write buffer limit from config (before moving result.storage)
        let coordinator = TransactionCoordinator::from_recovery_with_limits(
            &result,
            cfg.storage.max_write_buffer_entries,
        );

        // Configure block cache capacity before any segment reads
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

        // Apply storage resource limits from config
        let storage = Arc::new(result.storage);
        apply_storage_config(&storage, &cfg.storage);

        let bg_threads = cfg.storage.background_threads.max(1);

        Self::recover_segments_and_bump(&storage, &coordinator, cfg.allow_lossy_recovery)?;

        let db = Arc::new(Self {
            data_dir: canonical_path.clone(),
            database_uuid,
            storage,
            wal_writer: Some(wal_arc),
            persistence_mode: PersistenceMode::Disk,
            coordinator,
            durability_mode,
            accepting_transactions: AtomicBool::new(true),
            extensions: DashMap::new(),
            config: parking_lot::RwLock::new(cfg),
            flush_shutdown,
            flush_handle: ParkingMutex::new(flush_handle),
            scheduler: Arc::new(BackgroundScheduler::new(bg_threads, 4096)),
            flush_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            backpressure_counter: AtomicU64::new(0),
            _lock_file: lock_file,
            wal_dir,
            wal_watermark,
            follower: false,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
            dag_hook_slot: super::DagHookSlot::new(),
            branch_op_observers: super::BranchOpObserverRegistry::new(),
            commit_observers: super::CommitObserverRegistry::new(),
            replay_observers: super::ReplayObserverRegistry::new(),
            lifecycle_complete: AtomicBool::new(false),
            merge_registry: super::MergeHandlerRegistry::new(),
        });

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
    pub fn cache() -> StrataResult<Arc<Self>> {
        let mut cfg = StrataConfig::default();
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

        let db = Arc::new(Self {
            data_dir: PathBuf::new(), // Empty path for ephemeral
            database_uuid: [0u8; 16], // No persistence — UUID not needed
            storage: Arc::new(storage),
            wal_writer: None, // No WAL for ephemeral

            persistence_mode: PersistenceMode::Ephemeral,
            coordinator,
            durability_mode: DurabilityMode::Cache, // Irrelevant but set for consistency
            accepting_transactions: AtomicBool::new(true),
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
            _lock_file: None, // No lock for ephemeral databases
            wal_dir: PathBuf::new(),
            wal_watermark: AtomicU64::new(0),
            follower: false,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
            dag_hook_slot: super::DagHookSlot::new(),
            branch_op_observers: super::BranchOpObserverRegistry::new(),
            commit_observers: super::CommitObserverRegistry::new(),
            replay_observers: super::ReplayObserverRegistry::new(),
            lifecycle_complete: AtomicBool::new(false),
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
            DatabaseMode::Primary => {
                Self::open_runtime_primary(spec)
            }
            DatabaseMode::Follower => {
                Self::open_runtime_follower(spec)
            }
            DatabaseMode::Cache => {
                Self::open_runtime_cache(spec)
            }
        }
    }

    /// Open a primary database from an `OpenSpec`.
    fn open_runtime_primary(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        use super::compat::{CompatibilitySignature, CURRENT_CODEC_ID};

        let data_dir = spec.path.clone();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        // Build compatibility signature BEFORE checking registry
        let subsystem_names: Vec<&'static str> = spec.subsystems.iter().map(|s| s.name()).collect();
        let requested_signature = CompatibilitySignature::from_spec(
            super::spec::DatabaseMode::Primary,
            subsystem_names.clone(),
            spec.config.as_ref()
                .map(|c| c.durability_mode().unwrap_or_default())
                .unwrap_or_default(),
            spec.default_branch.clone(),
        );

        // Check for existing instance BEFORE writing config to disk
        {
            let registry = super::OPEN_DATABASES.lock();
            if let Some(weak) = registry.get(&canonical_path) {
                if let Some(existing_db) = weak.upgrade() {
                    // Build signature for existing instance
                    let existing_signature = CompatibilitySignature::from_spec(
                        super::spec::DatabaseMode::Primary,
                        existing_db.installed_subsystem_names(),
                        existing_db.durability_mode,
                        spec.default_branch.clone(), // Compare against requested default_branch
                    );

                    // Reject if incompatible
                    if let Err(reason) = existing_signature.check_compatible(&requested_signature) {
                        return Err(StrataError::invalid_input(format!(
                            "cannot reuse existing database instance: {}",
                            reason
                        )));
                    }

                    // Lifecycle already complete — return existing instance
                    if existing_db.is_lifecycle_complete() {
                        info!(
                            target: "strata::db",
                            path = ?canonical_path,
                            "Returning existing database instance (lifecycle complete)"
                        );
                        return Ok(existing_db);
                    }

                    // Lifecycle not complete on existing instance is a bug
                    // (should not be in registry before lifecycle completes)
                    return Err(StrataError::internal(
                        "existing database instance in registry but lifecycle not complete"
                    ));
                }
            }
        }

        // No existing instance — proceed with creation
        // NOW we can write config to disk
        let config_path = canonical_path.join(config::CONFIG_FILE_NAME);
        let cfg = if let Some(cfg) = spec.config {
            cfg.write_to_file(&config_path)?;
            cfg
        } else {
            config::StrataConfig::write_default_if_missing(&config_path)?;
            config::StrataConfig::from_file(&config_path)?
        };

        let durability_mode = cfg.durability_mode()?;

        // Create new database — acquire_primary_db will insert into registry
        // AFTER recover() completes
        let db = Self::acquire_primary_db(
            &canonical_path,
            durability_mode,
            cfg,
            spec.subsystems,
        )?;

        // Run lifecycle hooks (initialize and bootstrap)
        // If this fails, the DB is in registry in a partial state.
        // We need to remove it from registry on failure.
        if let Err(e) = Self::run_lifecycle_hooks(&db, true) {
            // Remove from registry on lifecycle failure
            let mut registry = super::OPEN_DATABASES.lock();
            registry.remove(&canonical_path);
            return Err(e);
        }

        // Ensure default branch if specified
        if let Some(branch_name) = &spec.default_branch {
            if let Err(e) = Self::ensure_default_branch(&db, branch_name) {
                // Remove from registry on failure
                let mut registry = super::OPEN_DATABASES.lock();
                registry.remove(&canonical_path);
                return Err(e);
            }
        }

        // Mark lifecycle complete AFTER all hooks succeed
        db.set_lifecycle_complete();

        Ok(db)
    }

    /// Open a follower database from an `OpenSpec`.
    fn open_runtime_follower(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        use super::compat::CompatibilitySignature;

        let data_dir = spec.path.clone();
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        // Build compatibility signature
        let subsystem_names: Vec<&'static str> = spec.subsystems.iter().map(|s| s.name()).collect();
        let requested_signature = CompatibilitySignature::from_spec(
            super::spec::DatabaseMode::Follower,
            subsystem_names.clone(),
            spec.config.as_ref()
                .map(|c| c.durability_mode().unwrap_or_default())
                .unwrap_or_default(),
            spec.default_branch.clone(),
        );

        // Check for existing instance
        {
            let registry = super::OPEN_DATABASES.lock();
            if let Some(weak) = registry.get(&canonical_path) {
                if let Some(existing_db) = weak.upgrade() {
                    let existing_signature = CompatibilitySignature::from_spec(
                        super::spec::DatabaseMode::Follower,
                        existing_db.installed_subsystem_names(),
                        existing_db.durability_mode,
                        spec.default_branch.clone(),
                    );

                    if let Err(reason) = existing_signature.check_compatible(&requested_signature) {
                        return Err(StrataError::invalid_input(format!(
                            "cannot reuse existing database instance: {}",
                            reason
                        )));
                    }

                    if existing_db.is_lifecycle_complete() {
                        info!(
                            target: "strata::db",
                            path = ?canonical_path,
                            "Returning existing follower instance (lifecycle complete)"
                        );
                        return Ok(existing_db);
                    }

                    return Err(StrataError::internal(
                        "existing database instance in registry but lifecycle not complete"
                    ));
                }
            }
        }

        // Resolve configuration
        let config_path = canonical_path.join(config::CONFIG_FILE_NAME);
        let cfg = if let Some(cfg) = spec.config {
            cfg
        } else if config_path.exists() {
            config::StrataConfig::from_file(&config_path)?
        } else {
            config::StrataConfig::default()
        };

        // Delegate to existing follower open path with subsystems
        let db = Self::open_follower_internal_with_subsystems(
            canonical_path.clone(),
            cfg,
            spec.subsystems,
        )?;

        // Run lifecycle hooks (initialize only, no bootstrap for followers)
        if let Err(e) = Self::run_lifecycle_hooks(&db, false) {
            let mut registry = super::OPEN_DATABASES.lock();
            registry.remove(&canonical_path);
            return Err(e);
        }

        // Mark lifecycle complete
        db.set_lifecycle_complete();

        Ok(db)
    }

    /// Open a cache database from an `OpenSpec`.
    fn open_runtime_cache(spec: super::spec::OpenSpec) -> StrataResult<Arc<Self>> {
        // Create ephemeral database (config is applied internally by cache())
        // Cache databases are not in the registry, so no reuse check needed.
        let db = Self::cache()?;

        // Run subsystem recovery (no-op for cache, but maintains consistency)
        // and install subsystems for freeze-on-drop
        for subsystem in &spec.subsystems {
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
        db.set_subsystems(spec.subsystems);

        // Run lifecycle hooks (initialize and bootstrap)
        Self::run_lifecycle_hooks(&db, true)?;

        // Ensure default branch if specified
        if let Some(branch_name) = &spec.default_branch {
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
        let branch_index = crate::primitives::branch::BranchIndex::new(db.clone());
        if !branch_index.exists(branch_name)? {
            info!(
                target: "strata::db",
                branch = branch_name,
                "Creating default branch"
            );
            branch_index.create_branch(branch_name)?;
        }
        Ok(())
    }
}
