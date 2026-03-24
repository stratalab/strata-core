//! Database opening and initialization.

use crate::background::BackgroundScheduler;
use crate::coordinator::TransactionCoordinator;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use strata_concurrency::RecoveryCoordinator;
use strata_durability::codec::IdentityCodec;
use strata_durability::wal::{DurabilityMode, WalConfig, WalWriter};
use strata_storage::SegmentedStore;
use tracing::{info, warn};

use strata_core::{StrataError, StrataResult};

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

        crate::primitives::vector::register_vector_recovery();
        crate::search::register_search_recovery();
        crate::recovery::recover_all_participants(&db)?;
        let index = db.extension::<crate::search::InvertedIndex>()?;
        if !index.is_enabled() {
            index.enable();
        }

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
            .with_segments(segments_dir, cfg.storage.write_buffer_size)
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
        storage.set_max_branches(cfg.storage.max_branches);
        storage.set_max_versions_per_key(cfg.storage.max_versions_per_key);
        storage.set_max_immutable_memtables(cfg.storage.max_immutable_memtables);
        storage.set_target_file_size(cfg.storage.target_file_size);
        storage.set_level_base_bytes(cfg.storage.level_base_bytes);
        storage.set_data_block_size(cfg.storage.data_block_size);
        storage.set_bloom_bits_per_key(cfg.storage.bloom_bits_per_key);

        let bg_threads = cfg.storage.background_threads.max(1);

        // Recover previously flushed segments from disk
        match storage.recover_segments() {
            Ok(seg_info) => {
                if seg_info.segments_loaded > 0 {
                    info!(target: "strata::db",
                        branches = seg_info.branches_recovered,
                        segments = seg_info.segments_loaded,
                        errors_skipped = seg_info.errors_skipped,
                        "Recovered segments from disk");
                }
                // Bump version counter from segment data (#1726)
                if seg_info.max_commit_id > 0 {
                    coordinator.bump_version_floor(seg_info.max_commit_id);
                }
            }
            Err(e) => {
                warn!(target: "strata::db", error = %e, "Segment recovery failed");
                if !cfg.allow_lossy_recovery {
                    return Err(StrataError::corruption(format!(
                        "Segment recovery failed: {}",
                        e
                    )));
                }
            }
        }

        let db = Arc::new(Self {
            data_dir: canonical_path,
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
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: None, // No lock acquired
            wal_dir,
            wal_watermark,
            follower: true,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
        });

        crate::primitives::vector::register_vector_recovery();
        crate::search::register_search_recovery();
        crate::recovery::recover_all_participants(&db)?;
        let index = db.extension::<crate::search::InvertedIndex>()?;
        if !index.is_enabled() {
            index.enable();
        }

        crate::branch_dag::load_status_cache_readonly(&db);

        Ok(db)
    }

    /// Shared tail of database open: recovery, WAL writer, coordinator, flush thread.
    fn open_finish(
        canonical_path: PathBuf,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        lock_file: Option<std::fs::File>,
    ) -> StrataResult<Arc<Self>> {
        // Create WAL directory
        let wal_dir = canonical_path.join("wal");
        std::fs::create_dir_all(&wal_dir).map_err(StrataError::from)?;

        // Create segments directory for on-disk segment storage
        let segments_dir = canonical_path.join("segments");
        std::fs::create_dir_all(&segments_dir).map_err(StrataError::from)?;

        // Use RecoveryCoordinator for proper transaction-aware recovery
        // This reads all WalRecords from the segmented WAL directory
        let recovery = RecoveryCoordinator::new(wal_dir.clone())
            .with_segments(segments_dir, cfg.storage.write_buffer_size)
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

        // Open segmented WAL writer for appending
        let wal_writer = WalWriter::new(
            wal_dir.clone(),
            [0u8; 16], // database UUID placeholder
            durability_mode,
            WalConfig::default(),
            Box::new(IdentityCodec),
        )?;

        let wal_watermark = AtomicU64::new(result.stats.max_txn_id);

        let wal_arc = Arc::new(ParkingMutex::new(wal_writer));
        let flush_shutdown = Arc::new(AtomicBool::new(false));

        // Spawn background WAL flush thread for Standard mode (#969)
        let flush_handle = if let DurabilityMode::Standard { interval_ms, .. } = durability_mode {
            let wal = Arc::clone(&wal_arc);
            let shutdown = Arc::clone(&flush_shutdown);
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
            Some(handle)
        } else {
            None
        };

        // Create coordinator with write buffer limit from config (before moving result.storage)
        let coordinator = TransactionCoordinator::from_recovery_with_limits(
            &result,
            cfg.storage.max_write_buffer_entries,
        );

        // Configure block cache capacity before any segment reads
        {
            use strata_storage::block_cache;
            let cache_bytes = if cfg.storage.block_cache_size > 0 {
                cfg.storage.block_cache_size
            } else {
                block_cache::auto_detect_capacity()
            };
            block_cache::set_global_capacity(cache_bytes);
        }

        // Apply storage resource limits from config
        let storage = Arc::new(result.storage);
        storage.set_max_branches(cfg.storage.max_branches);
        storage.set_max_versions_per_key(cfg.storage.max_versions_per_key);
        storage.set_max_immutable_memtables(cfg.storage.max_immutable_memtables);
        storage.set_target_file_size(cfg.storage.target_file_size);
        storage.set_level_base_bytes(cfg.storage.level_base_bytes);
        storage.set_data_block_size(cfg.storage.data_block_size);
        storage.set_bloom_bits_per_key(cfg.storage.bloom_bits_per_key);
        if cfg.storage.compaction_rate_limit > 0 {
            storage.set_compaction_rate_limit(cfg.storage.compaction_rate_limit);
        }

        let bg_threads = cfg.storage.background_threads.max(1);

        // Recover previously flushed segments from disk
        match storage.recover_segments() {
            Ok(seg_info) => {
                if seg_info.segments_loaded > 0 {
                    info!(target: "strata::db",
                        branches = seg_info.branches_recovered,
                        segments = seg_info.segments_loaded,
                        errors_skipped = seg_info.errors_skipped,
                        "Recovered segments from disk");
                }
                // Bump version counter to at least the max commit_id in
                // recovered segments, preventing version collisions when
                // WAL has been fully compacted (#1726).
                if seg_info.max_commit_id > 0 {
                    coordinator.bump_version_floor(seg_info.max_commit_id);
                }
            }
            Err(e) => {
                warn!(target: "strata::db", error = %e, "Segment recovery failed");
                if !cfg.allow_lossy_recovery {
                    return Err(StrataError::corruption(format!(
                        "Segment recovery failed: {}",
                        e
                    )));
                }
            }
        }

        let db = Arc::new(Self {
            data_dir: canonical_path.clone(),
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
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: lock_file,
            wal_dir,
            wal_watermark,
            follower: false,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
        });

        crate::branch_dag::init_system_branch(&db);

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

        // Create fresh storage with branch limit from default config
        let storage = SegmentedStore::new();
        storage.set_max_branches(cfg.storage.max_branches);
        storage.set_max_versions_per_key(cfg.storage.max_versions_per_key);
        storage.set_max_immutable_memtables(cfg.storage.max_immutable_memtables);

        let bg_threads = cfg.storage.background_threads.max(1);

        // Create coordinator starting at version 1 (no recovery needed), with write buffer limit
        let coordinator = TransactionCoordinator::new(1);
        coordinator.set_max_write_buffer_entries(cfg.storage.max_write_buffer_entries);

        let db = Arc::new(Self {
            data_dir: PathBuf::new(), // Empty path for ephemeral
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
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: None, // No lock for ephemeral databases
            wal_dir: PathBuf::new(),
            wal_watermark: AtomicU64::new(0),
            follower: false,
            shutdown_complete: AtomicBool::new(false),
            opened_at: Instant::now(),
        });

        // Note: Ephemeral databases are NOT registered in the global registry
        // because they have no path and should always be independent instances

        // Enable the inverted index for keyword/BM25 search.
        // Cache databases skip recovery participants (nothing to recover),
        // so we enable the index directly.
        let index = db.extension::<crate::search::InvertedIndex>()?;
        index.enable();

        crate::branch_dag::init_system_branch(&db);

        Ok(db)
    }
}
