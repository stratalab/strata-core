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

    pub(super) fn open_internal<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        // Create directory first so we can canonicalize the path
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;
        restrict_dir(&data_dir);

        // Canonicalize path for consistent registry keys
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        // Single-process mode: use registry and exclusive lock
        let mut registry = OPEN_DATABASES.lock();

        if let Some(weak) = registry.get(&canonical_path) {
            if let Some(db) = weak.upgrade() {
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

        let db = Self::open_finish(
            canonical_path.clone(),
            durability_mode,
            cfg,
            Some(lock_file),
        )?;

        registry.insert(canonical_path, Arc::downgrade(&db));
        drop(registry);

        // Register and run recovery for search subsystem.
        // Vector recovery is now handled by strata-vector (caller must register).
        // Also sets subsystems on the Database for freeze-on-drop.
        crate::search::register_search_recovery();
        crate::recovery::recover_all_participants(&db)?;
        let index = db.extension::<crate::search::InvertedIndex>()?;
        if !index.is_enabled() {
            index.enable();
        }
        index.set_positions_enabled(db.config().positions);
        db.set_subsystems(vec![Box::new(crate::search::SearchSubsystem)]);

        Ok(db)
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

    fn open_follower_internal(
        canonical_path: PathBuf,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
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

        let wal_watermark = AtomicU64::new(result.stats.max_txn_id);

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
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: None, // No lock acquired
            wal_dir,
            wal_watermark,
            follower: true,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
        });

        crate::search::register_search_recovery();
        crate::recovery::recover_all_participants(&db)?;
        let index = db.extension::<crate::search::InvertedIndex>()?;
        if !index.is_enabled() {
            index.enable();
        }
        index.set_positions_enabled(db.config().positions);
        db.set_subsystems(vec![Box::new(crate::search::SearchSubsystem)]);

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
                        let mut wal = wal.lock();
                        if let Err(e) = wal.sync_if_overdue() {
                            tracing::error!(target: "strata::wal", error = %e, "Background WAL sync failed");
                        }
                        wal.flush_active_meta();
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
                if seg_info.max_commit_id > 0 {
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
            final_version = result.stats.final_version,
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

        let wal_watermark = AtomicU64::new(result.stats.max_txn_id);

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
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: lock_file,
            wal_dir,
            wal_watermark,
            follower: false,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
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
        let cfg = StrataConfig::default();

        // Create fresh storage with config limits
        let storage = SegmentedStore::new();
        apply_storage_config(&storage, &cfg.storage);

        let bg_threads = cfg.storage.background_threads.max(1);

        // Create coordinator starting at version 1 (no recovery needed), with write buffer limit
        let coordinator = TransactionCoordinator::new(1);
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
            config: parking_lot::RwLock::new(StrataConfig::default()),
            flush_shutdown: Arc::new(AtomicBool::new(false)),
            flush_handle: ParkingMutex::new(None),
            scheduler: Arc::new(BackgroundScheduler::new(bg_threads, 4096)),
            compaction_in_flight: Arc::new(AtomicBool::new(false)),
            compaction_cancelled: Arc::new(AtomicBool::new(false)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: None, // No lock for ephemeral databases
            wal_dir: PathBuf::new(),
            wal_watermark: AtomicU64::new(0),
            follower: false,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
            subsystems: parking_lot::RwLock::new(Vec::new()),
        });

        // Note: Ephemeral databases are NOT registered in the global registry
        // because they have no path and should always be independent instances

        // Enable the inverted index for keyword/BM25 search.
        // Cache databases skip recovery participants (nothing to recover),
        // so we enable the index directly.
        let index = db.extension::<crate::search::InvertedIndex>()?;
        index.enable();

        Ok(db)
    }
}
