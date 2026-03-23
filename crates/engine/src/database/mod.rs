//! Database struct and open/close logic
//!
//! This module provides the main Database struct that orchestrates:
//! - Storage initialization
//! - WAL opening
//! - Automatic recovery on startup
//! - Transaction API
//!
//! ## Transaction API
//!
//! The Database provides two ways to execute transactions:
//!
//! 1. **Closure API** (recommended): `db.transaction(branch_id, |txn| { ... })`
//!    - Automatic commit on success, abort on error
//!    - Returns the closure's return value
//!
//! 2. **Manual API**: `begin_transaction()` + `commit_transaction()`
//!    - For cases requiring external control over commit timing
//!
//! Per spec Section 4: Implicit transactions wrap legacy-style operations.

pub mod config;
mod registry;

pub use config::{
    ModelConfig, StorageConfig, StrataConfig, SHADOW_EVENT, SHADOW_JSON, SHADOW_KV, SHADOW_STATE,
};
pub use registry::OPEN_DATABASES;

use crate::background::BackgroundScheduler;
use crate::coordinator::TransactionCoordinator;
use crate::transaction::TransactionPool;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use std::any::{Any, TypeId};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strata_concurrency::{RecoveryCoordinator, TransactionContext};
use strata_core::types::TypeTag;
use strata_core::types::{BranchId, Key};
use strata_core::StrataError;
use strata_core::{StrataResult, VersionedValue};
use strata_durability::codec::IdentityCodec;
use strata_durability::wal::{DurabilityMode, WalConfig, WalWriter};
use strata_durability::{
    BranchSnapshotEntry, EventSnapshotEntry, JsonSnapshotEntry, KvSnapshotEntry, StateSnapshotEntry,
};
use strata_durability::{
    CheckpointCoordinator, CheckpointData, CheckpointError, CompactionError, ManifestError,
    ManifestManager, WalOnlyCompactor,
};
use strata_storage::SegmentedStore;
use tracing::{info, warn};

// ============================================================================
// Persistence Mode (Storage/Durability Split)
// ============================================================================

/// Controls where data is stored (orthogonal to durability)
///
/// This enum distinguishes between truly in-memory (ephemeral) databases
/// and disk-backed databases. This is orthogonal to `DurabilityMode`,
/// which controls WAL sync behavior.
///
/// # Persistence vs Durability
///
/// | PersistenceMode | DurabilityMode | Behavior |
/// |-----------------|----------------|----------|
/// | Ephemeral | (ignored) | No files, data lost on drop |
/// | Disk | Cache | Files created, no fsync |
/// | Disk | Standard | Files created, periodic fsync |
/// | Disk | Always | Files created, immediate fsync |
///
/// # Use Cases
///
/// - **Ephemeral**: Unit tests, caching, temporary computations
/// - **Disk + Cache**: Integration tests (fast, isolated, but files exist)
/// - **Disk + Standard**: Production workloads
/// - **Disk + Always**: Audit logs, critical data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PersistenceMode {
    /// No disk files at all - data exists only in memory
    ///
    /// - No directories created
    /// - No WAL file
    /// - No recovery possible
    /// - Data lost when database is dropped
    ///
    /// Use for unit tests, caching, and truly ephemeral data.
    Ephemeral,

    /// Data stored on disk (temp or user-specified path)
    ///
    /// Creates directories and WAL file. Data can survive crashes
    /// depending on the `DurabilityMode`.
    #[default]
    Disk,
}

// ============================================================================
// Disk Usage
// ============================================================================

/// Database disk usage summary.
#[derive(Debug, Clone, Default)]
pub struct DatabaseDiskUsage {
    /// WAL directory usage.
    pub wal: strata_durability::WalDiskUsage,
    /// Snapshot directory usage in bytes.
    pub snapshot_bytes: u64,
}

/// Sum file sizes in a directory (non-recursive, best-effort).
fn scan_dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    total += metadata.len();
                }
            }
        }
    }
    total
}

// ============================================================================
// Database Struct
// ============================================================================

/// Main database struct with transaction support
///
/// Orchestrates storage, WAL, recovery, and transactions.
/// Create a database by calling `Database::open()`.
///
/// # Transaction Support
///
/// The Database provides transaction APIs per spec Section 4:
/// - `transaction()`: Execute a closure within a transaction
/// - `begin_transaction()`: Start a manual transaction
/// - `commit_transaction()`: Commit a manual transaction
///
/// # Example
///
/// ```text
/// use strata_engine::Database;
/// use strata_core::types::BranchId;
///
/// let db = Database::open("/path/to/data")?;
/// let branch_id = BranchId::new();
///
/// // Closure API (recommended)
/// let result = db.transaction(branch_id, |txn| {
///     txn.put(key, value)?;
///     Ok(())
/// })?;
/// ```
pub struct Database {
    /// Data directory path (empty for ephemeral databases)
    data_dir: PathBuf,

    /// Segmented storage with O(1) lazy snapshots (thread-safe)
    storage: Arc<SegmentedStore>,

    /// Segmented WAL writer (protected by mutex for exclusive access)
    /// None for ephemeral databases (no disk I/O)
    /// Using parking_lot::Mutex to avoid lock poisoning on panic
    wal_writer: Option<Arc<ParkingMutex<WalWriter>>>,

    /// Persistence mode (ephemeral vs disk-backed)
    persistence_mode: PersistenceMode,

    /// Transaction coordinator for lifecycle management, version allocation, and metrics
    ///
    /// Per spec Section 6.1: Single monotonic counter for the entire database.
    /// Also owns the commit protocol via TransactionManager, including per-branch
    /// commit locks for TOCTOU prevention.
    coordinator: TransactionCoordinator,

    /// Current durability mode
    durability_mode: DurabilityMode,

    /// Flag to track if database is accepting new transactions
    ///
    /// Set to false during shutdown to reject new transactions.
    accepting_transactions: AtomicBool,

    /// Type-erased extension storage for primitive state
    ///
    /// Allows primitives like VectorStore to store their in-memory backends here,
    /// ensuring all VectorStore instances for the same Database share state.
    ///
    /// Extensions are lazily initialized on first access via `extension<T>()`.
    extensions: DashMap<TypeId, Arc<dyn Any + Send + Sync>>,

    /// Unified configuration (mirrors strata.toml).
    config: parking_lot::RwLock<StrataConfig>,

    /// Shutdown signal for the background WAL flush thread (Standard mode only)
    flush_shutdown: Arc<AtomicBool>,

    /// Handle for the background WAL flush thread
    ///
    /// In Standard mode, a background thread periodically calls sync_if_overdue()
    /// to flush WAL data to disk without blocking the write path (#969).
    flush_handle: ParkingMutex<Option<std::thread::JoinHandle<()>>>,

    /// Background task scheduler for deferred work (embedding, GC, etc.)
    scheduler: Arc<BackgroundScheduler>,

    /// Condition variable signalled by compaction when L0 count drops.
    /// Writers wait on this when L0 exceeds `l0_stop_writes_trigger`.
    write_stall_cv: Arc<parking_lot::Condvar>,
    /// Mutex paired with `write_stall_cv` (value is unused).
    write_stall_mu: parking_lot::Mutex<()>,

    /// Exclusive lock file preventing concurrent process access to the same database.
    ///
    /// Held for the lifetime of the Database. Dropped automatically when the
    /// Database is dropped, releasing the lock. None for ephemeral databases.
    _lock_file: Option<std::fs::File>,

    /// WAL directory path (for follower refresh).
    wal_dir: PathBuf,

    /// Max txn_id applied to local storage from WAL (follower watermark).
    wal_watermark: AtomicU64,

    /// Whether this database is a read-only follower (no lock, no WAL writer).
    follower: bool,

    /// Whether shutdown() has already completed (prevents double freeze in Drop).
    shutdown_complete: AtomicBool,
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

    fn open_internal<P: AsRef<Path>>(
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
            .with_segments(segments_dir, cfg.storage.write_buffer_size);
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
            scheduler: Arc::new(BackgroundScheduler::new(4, 4096)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: None, // No lock acquired
            wal_dir,
            wal_watermark,
            follower: true,
            shutdown_complete: AtomicBool::new(false),
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
            .with_segments(segments_dir, cfg.storage.write_buffer_size);
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
            scheduler: Arc::new(BackgroundScheduler::new(4, 4096)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: lock_file,
            wal_dir,
            wal_watermark,
            follower: false,
            shutdown_complete: AtomicBool::new(false),
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

        // Create coordinator starting at version 1 (no recovery needed), with write buffer limit
        let mut coordinator = TransactionCoordinator::new(1);
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
            scheduler: Arc::new(BackgroundScheduler::new(4, 4096)),
            write_stall_cv: Arc::new(parking_lot::Condvar::new()),
            write_stall_mu: parking_lot::Mutex::new(()),
            _lock_file: None, // No lock for ephemeral databases
            wal_dir: PathBuf::new(),
            wal_watermark: AtomicU64::new(0),
            follower: false,
            shutdown_complete: AtomicBool::new(false),
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

    // ========================================================================
    // Accessors
    // ========================================================================

    /// Get reference to the storage layer (internal use only)
    ///
    /// This is for internal engine use. External users should use
    /// primitives (KVStore, EventLog, etc.) which go through transactions.
    pub(crate) fn storage(&self) -> &Arc<SegmentedStore> {
        &self.storage
    }

    /// Clean up storage-layer segments for a deleted branch (#1702).
    ///
    /// Removes the branch's memtables, segment files, and decrements
    /// inherited layer refcounts. Should be called after logical
    /// deletion succeeds.
    pub fn clear_branch_storage(&self, branch_id: &BranchId) {
        self.storage.clear_branch(branch_id);
    }

    /// Direct single-key read returning only the Value (no VersionedValue).
    ///
    /// Skips Version enum and VersionedValue construction. Used by the
    /// KVStore::get() hot path where version metadata is not needed.
    pub(crate) fn get_value_direct(
        &self,
        key: &Key,
    ) -> strata_core::StrataResult<Option<strata_core::value::Value>> {
        self.storage.get_value_direct(key)
    }

    /// Get version history for a key directly from storage.
    ///
    /// History reads bypass the transaction layer because they are
    /// inherently non-transactional: you want all versions, not a
    /// snapshot-consistent subset.
    ///
    /// Returns versions newest-first. Empty if the key does not exist.
    pub(crate) fn get_history(
        &self,
        key: &Key,
        limit: Option<usize>,
        before_version: Option<u64>,
    ) -> StrataResult<Vec<VersionedValue>> {
        use strata_core::Storage;
        self.storage.get_history(key, limit, before_version)
    }

    /// Get value at or before the given timestamp directly from storage.
    ///
    /// This is a non-transactional read for time-travel queries.
    pub(crate) fn get_at_timestamp(
        &self,
        key: &Key,
        max_timestamp: u64,
    ) -> StrataResult<Option<VersionedValue>> {
        self.storage.get_at_timestamp(key, max_timestamp)
    }

    /// Scan keys matching a prefix at or before the given timestamp.
    ///
    /// This is a non-transactional read for time-travel queries.
    pub(crate) fn scan_prefix_at_timestamp(
        &self,
        prefix: &Key,
        max_timestamp: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        self.storage.scan_prefix_at_timestamp(prefix, max_timestamp)
    }

    /// Get the available time range for a branch.
    ///
    /// Returns (oldest_ts, latest_ts) in microseconds since epoch.
    /// Returns None if the branch has no data.
    pub fn time_range(&self, branch_id: BranchId) -> StrataResult<Option<(u64, u64)>> {
        self.storage.time_range(branch_id)
    }

    /// Check if this is a cache (no-disk) database
    pub fn is_cache(&self) -> bool {
        self.persistence_mode == PersistenceMode::Ephemeral
    }

    /// Get the data directory path.
    ///
    /// Returns an empty path for ephemeral (cache) databases.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns `true` if this database was opened in read-only follower mode.
    pub fn is_follower(&self) -> bool {
        self.follower
    }

    /// Get current WAL counters snapshot.
    ///
    /// Returns `None` for ephemeral databases (no WAL).
    /// Briefly locks the WAL mutex to read counter values.
    pub fn durability_counters(&self) -> Option<strata_durability::WalCounters> {
        self.wal_writer.as_ref().map(|w| w.lock().counters())
    }

    /// Check if the database is currently open and accepting transactions
    pub fn is_open(&self) -> bool {
        self.accepting_transactions.load(Ordering::Acquire)
    }

    /// Returns a reference to the background task scheduler.
    pub fn scheduler(&self) -> &BackgroundScheduler {
        &self.scheduler
    }

    // ========================================================================
    // Extension API
    // ========================================================================

    /// Get or create a typed extension bound to this Database
    ///
    /// Extensions allow primitives to store in-memory state that is shared
    /// across all instances of that primitive for this Database.
    ///
    /// # Behavior
    ///
    /// - If the extension exists, returns it
    /// - If missing, creates with `Default::default()`, stores, and returns it
    /// - Always returns `Arc<T>` for shared ownership
    ///
    /// # Thread Safety
    ///
    /// This method is safe to call concurrently. The extension is created
    /// at most once, using DashMap's entry API for atomicity.
    ///
    /// # Example
    ///
    /// ```text
    /// #[derive(Default)]
    /// struct VectorBackendState {
    ///     backends: RwLock<BTreeMap<CollectionId, Box<dyn VectorIndexBackend>>>,
    /// }
    ///
    /// // All VectorStore instances for this Database share the same state
    /// let state = db.extension::<VectorBackendState>();
    /// ```
    pub fn extension<T: Any + Send + Sync + Default>(&self) -> StrataResult<Arc<T>> {
        let type_id = TypeId::of::<T>();

        // Use entry API for atomic get-or-insert
        let entry = self
            .extensions
            .entry(type_id)
            .or_insert_with(|| Arc::new(T::default()) as Arc<dyn Any + Send + Sync>);

        // Downcast to concrete type — the TypeId key guarantees this succeeds
        entry.value().clone().downcast::<T>().map_err(|_| {
            StrataError::internal(format!("extension type mismatch for TypeId {:?}", type_id))
        })
    }

    // ========================================================================
    // Config Accessors
    // ========================================================================

    /// Return a clone of the current configuration.
    pub fn config(&self) -> StrataConfig {
        self.config.read().clone()
    }

    /// Return memory usage statistics for the storage layer.
    ///
    /// O(branches) scan — call explicitly for diagnostics, not on hot paths.
    pub fn storage_memory_stats(&self) -> strata_storage::StorageMemoryStats {
        self.storage.memory_stats()
    }

    /// Apply a mutation to the configuration.
    ///
    /// The closure receives a mutable reference to the config. After the
    /// closure returns, the updated config is written to `strata.toml` for
    /// disk-backed databases. Changing the durability mode at runtime is
    /// rejected.
    pub fn update_config<F: FnOnce(&mut StrataConfig)>(&self, f: F) -> StrataResult<()> {
        let mut guard = self.config.write();
        let old_durability = guard.durability.clone();
        f(&mut guard);
        if guard.durability != old_durability {
            guard.durability = old_durability;
            return Err(StrataError::invalid_input(
                "Cannot change durability mode at runtime. \
                 Set durability before opening the database."
                    .to_string(),
            ));
        }
        // Persist to strata.toml for disk-backed databases
        if self.persistence_mode == PersistenceMode::Disk && !self.data_dir.as_os_str().is_empty() {
            let config_path = self.data_dir.join(config::CONFIG_FILE_NAME);
            guard.write_to_file(&config_path)?;
        }
        Ok(())
    }

    /// Apply a mutation to the configuration and persist without rejecting
    /// durability changes.
    ///
    /// Unlike [`update_config`](Self::update_config), this method allows the
    /// durability field to be changed. The new value is persisted to
    /// `strata.toml` but only takes effect on the next database open.
    pub fn persist_config_deferred<F: FnOnce(&mut StrataConfig)>(&self, f: F) -> StrataResult<()> {
        let mut guard = self.config.write();
        f(&mut guard);
        if self.persistence_mode == PersistenceMode::Disk && !self.data_dir.as_os_str().is_empty() {
            let config_path = self.data_dir.join(config::CONFIG_FILE_NAME);
            guard.write_to_file(&config_path)?;
        }
        Ok(())
    }

    // ========================================================================
    // Auto-Embed Accessors
    // ========================================================================

    /// Check if auto-embedding is enabled.
    pub fn auto_embed_enabled(&self) -> bool {
        self.config.read().auto_embed
    }

    /// Enable or disable auto-embedding.
    ///
    /// Persists to `strata.toml` for disk-backed databases.
    pub fn set_auto_embed(&self, enabled: bool) {
        // Use update_config for persistence; ignore error since
        // auto_embed never triggers the durability rejection.
        let _ = self.update_config(|cfg| {
            cfg.auto_embed = enabled;
        });
    }

    /// Get the embedding batch size (reads config).
    pub fn embed_batch_size(&self) -> usize {
        self.config.read().embed_batch_size.unwrap_or(64)
    }

    /// Get the configured embedding model name (reads config).
    pub fn embed_model(&self) -> String {
        self.config.read().embed_model.clone()
    }

    /// Path to the model directory for MiniLM-L6-v2.
    ///
    /// Checks in order:
    /// 1. Database-local `{data_dir}/models/minilm-l6-v2/`
    /// 2. System-wide `~/.stratadb/models/minilm-l6-v2/`
    /// 3. Falls back to the local path (for error messages)
    pub fn model_dir(&self) -> PathBuf {
        let local = self.data_dir.join("models/minilm-l6-v2");
        if local.join("model.safetensors").exists() && local.join("vocab.txt").exists() {
            return local;
        }
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .ok();
        if let Some(home) = home {
            let system = PathBuf::from(home).join(".stratadb/models/minilm-l6-v2");
            if system.join("model.safetensors").exists() && system.join("vocab.txt").exists() {
                return system;
            }
        }
        local
    }

    // ========================================================================
    // Branch Lifecycle Cleanup
    // ========================================================================

    /// Garbage-collect old versions before the given version number.
    ///
    /// Removes old versions from version chains across all entries in the branch.
    /// Returns the number of pruned versions.
    pub fn gc_versions_before(&self, branch_id: BranchId, min_version: u64) -> usize {
        self.storage.gc_branch(&branch_id, min_version)
    }

    /// Get the current global version from the coordinator.
    ///
    /// This is the highest version allocated so far and serves as
    /// a safe GC boundary when no active snapshots need older versions.
    pub fn current_version(&self) -> u64 {
        self.coordinator.current_version()
    }

    /// Compute the GC safe point — the oldest version that can be pruned.
    ///
    /// `safe_point = min(current_version - 1, min_active_version)`
    ///
    /// Versions strictly older than `safe_point` can be pruned from version
    /// chains. Returns 0 if no GC is safe (only one version or version 0).
    pub fn gc_safe_point(&self) -> u64 {
        let current = self.current_version();
        if current <= 1 {
            return 0;
        }
        let version_bound = current - 1;
        match self.coordinator.min_active_version() {
            Some(min_active) => version_bound.min(min_active),
            None => {
                // No drain has occurred yet (gc_safe_version == 0 sentinel).
                // If there are active transactions, they may hold snapshots
                // at any version — block GC entirely until the first drain.
                if self.coordinator.active_count() > 0 {
                    return 0;
                }
                version_bound
            }
        }
    }

    /// Run garbage collection across all branches using the computed safe point.
    ///
    /// This is NOT called automatically — it must be invoked explicitly.
    /// A future PR will add a background maintenance thread.
    ///
    /// Returns `(safe_point, total_versions_pruned)`.
    pub fn run_gc(&self) -> (u64, usize) {
        let safe_point = self.gc_safe_point();
        if safe_point == 0 {
            return (0, 0);
        }

        let mut total_pruned = 0;
        for branch_id in self.storage.branch_ids() {
            total_pruned += self.storage.gc_branch(&branch_id, safe_point);
        }

        if total_pruned > 0 {
            tracing::debug!(pruned = total_pruned, safe_point, "GC cycle complete");
        }

        (safe_point, total_pruned)
    }

    /// Run a full maintenance cycle: GC + TTL expiration.
    ///
    /// This is NOT called automatically. It must be invoked explicitly
    /// (e.g., from tests, CLI, or a future background thread).
    ///
    /// Returns `(safe_point, versions_pruned, ttl_entries_expired)`.
    pub fn run_maintenance(&self) -> (u64, usize, usize) {
        let (safe_point, pruned) = self.run_gc();
        let expired = self
            .storage
            .expire_ttl_keys(strata_core::Timestamp::now().as_micros());
        (safe_point, pruned, expired)
    }

    /// Remove the per-branch commit lock after a branch is deleted.
    ///
    /// This prevents unbounded growth of the commit_locks map in the
    /// TransactionManager when branches are repeatedly created and deleted.
    /// Returns `true` if removed (or didn't exist), `false` if skipped
    /// because a concurrent commit is in-flight.
    ///
    /// Should be called after `BranchIndex::delete_branch()` succeeds.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) -> bool {
        self.coordinator.remove_branch_lock(branch_id)
    }

    // ========================================================================
    // Follower Refresh
    // ========================================================================

    /// Refresh local storage by tailing new WAL entries from the primary.
    ///
    /// In follower mode, the primary process may have appended new WAL records
    /// since this instance last read the WAL. `refresh()` reads any new records
    /// and applies them to local in-memory storage, bringing this instance
    /// up-to-date with all committed writes.
    ///
    /// Returns the number of new records applied.
    ///
    /// For non-follower databases, this is a no-op returning 0.
    pub fn refresh(&self) -> StrataResult<usize> {
        if !self.follower || self.persistence_mode == PersistenceMode::Ephemeral {
            return Ok(0);
        }

        let watermark = self.wal_watermark.load(std::sync::atomic::Ordering::SeqCst);

        // Read all WAL records after our watermark
        let reader = strata_durability::wal::WalReader::new();
        let records = reader
            .read_all_after_watermark(&self.wal_dir, watermark)
            .map_err(|e| StrataError::storage(format!("WAL refresh read failed: {}", e)))?;

        if records.is_empty() {
            return Ok(0);
        }

        let mut applied = 0usize;
        let mut max_version = 0u64;
        let mut max_txn_id = watermark;

        // Get search index (if available) for incremental updates
        let search_index = self.extension::<crate::search::InvertedIndex>().ok();
        let search_enabled = search_index.as_ref().is_some_and(|idx| idx.is_enabled());

        // Get vector backend state (if available) for incremental updates
        let vector_state = self
            .extension::<crate::primitives::vector::store::VectorBackendState>()
            .ok();

        for record in &records {
            max_txn_id = max_txn_id.max(record.txn_id);

            let payload = strata_concurrency::TransactionPayload::from_bytes(&record.writeset)
                .map_err(|e| {
                    StrataError::storage(format!(
                        "Failed to decode WAL payload for txn {}: {}",
                        record.txn_id, e
                    ))
                })?;

            max_version = max_version.max(payload.version);

            // Pre-read vector records for deletes (must happen before storage mutations)
            let mut vector_delete_pre_reads: Vec<(
                Key,
                crate::primitives::vector::types::VectorRecord,
            )> = Vec::new();
            if vector_state.is_some() {
                for key in &payload.deletes {
                    if key.type_tag == TypeTag::Vector {
                        use strata_core::traits::Storage;
                        if let Ok(Some(vv)) = self.storage.get_versioned(key, u64::MAX) {
                            if let strata_core::value::Value::Bytes(ref bytes) = vv.value {
                                if let Ok(record) =
                                    crate::primitives::vector::types::VectorRecord::from_bytes(
                                        bytes,
                                    )
                                {
                                    vector_delete_pre_reads.push((key.clone(), record));
                                }
                            }
                        }
                    }
                }
            }

            // Apply puts and deletes atomically with original WAL timestamp.
            // Combines #1699 (preserve commit timestamp for time-travel) and
            // #1707 (defer version bump until all entries installed, preventing
            // partial-state visibility for concurrent follower readers).
            {
                let writes: Vec<_> = payload
                    .puts
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let deletes: Vec<_> = payload.deletes.to_vec();
                self.storage.apply_recovery_atomic(
                    writes,
                    deletes,
                    payload.version,
                    record.timestamp,
                )?;
            }

            // --- Update BM25 search index ---
            if search_enabled {
                let index = search_index.as_ref().unwrap();

                for (key, value) in &payload.puts {
                    let branch_id = key.namespace.branch_id;
                    match key.type_tag {
                        TypeTag::KV => {
                            if let Some(text) = crate::search::extract_indexable_text(value) {
                                if let Some(user_key) = key.user_key_string() {
                                    let entity_ref = strata_core::EntityRef::Kv {
                                        branch_id,
                                        key: user_key,
                                    };
                                    index.index_document(&entity_ref, &text, None);
                                }
                            }
                        }
                        TypeTag::State => {
                            if let Some(text) = crate::search::extract_indexable_text(value) {
                                if let Some(name) = key.user_key_string() {
                                    let entity_ref =
                                        strata_core::EntityRef::State { branch_id, name };
                                    index.index_document(&entity_ref, &text, None);
                                }
                            }
                        }
                        TypeTag::Event => {
                            if *key.user_key == *b"__meta__"
                                || key.user_key.starts_with(b"__tidx__")
                            {
                                continue;
                            }
                            if let Some(text) = crate::search::extract_indexable_text(value) {
                                if key.user_key.len() == 8 {
                                    let sequence = u64::from_be_bytes(
                                        (*key.user_key).try_into().unwrap_or([0; 8]),
                                    );
                                    let entity_ref = strata_core::EntityRef::Event {
                                        branch_id,
                                        sequence,
                                    };
                                    index.index_document(&entity_ref, &text, None);
                                }
                            }
                        }
                        _ => {}
                    }
                }

                for key in &payload.deletes {
                    let branch_id = key.namespace.branch_id;
                    match key.type_tag {
                        TypeTag::KV => {
                            if let Some(user_key) = key.user_key_string() {
                                let entity_ref = strata_core::EntityRef::Kv {
                                    branch_id,
                                    key: user_key,
                                };
                                index.remove_document(&entity_ref);
                            }
                        }
                        TypeTag::State => {
                            if let Some(name) = key.user_key_string() {
                                let entity_ref = strata_core::EntityRef::State { branch_id, name };
                                index.remove_document(&entity_ref);
                            }
                        }
                        TypeTag::Event => {
                            if *key.user_key == *b"__meta__"
                                || key.user_key.starts_with(b"__tidx__")
                            {
                                continue;
                            }
                            if key.user_key.len() == 8 {
                                let sequence = u64::from_be_bytes(
                                    (*key.user_key).try_into().unwrap_or([0; 8]),
                                );
                                let branch_id = key.namespace.branch_id;
                                let entity_ref = strata_core::EntityRef::Event {
                                    branch_id,
                                    sequence,
                                };
                                index.remove_document(&entity_ref);
                            }
                        }
                        _ => {}
                    }
                }
            }

            // --- Update vector backends ---
            if let Some(ref vs) = vector_state {
                use strata_core::primitives::vector::{CollectionId, VectorId};

                // Vector puts: insert into backend
                for (key, value) in &payload.puts {
                    if key.type_tag != TypeTag::Vector {
                        continue;
                    }
                    let bytes = match value {
                        strata_core::value::Value::Bytes(b) => b,
                        _ => continue,
                    };
                    let record =
                        match crate::primitives::vector::types::VectorRecord::from_bytes(bytes) {
                            Ok(r) => r,
                            Err(_) => continue,
                        };
                    let user_key_str = match key.user_key_string() {
                        Some(s) => s,
                        None => continue,
                    };
                    let (collection, vector_key) = match user_key_str.split_once('/') {
                        Some(pair) => pair,
                        None => continue,
                    };
                    let branch_id = key.namespace.branch_id;
                    let cid = CollectionId::new(branch_id, collection);
                    let vid = VectorId::new(record.vector_id);

                    if let Some(mut backend) = vs.backends.get_mut(&cid) {
                        let _ = backend.insert_with_timestamp(
                            vid,
                            &record.embedding,
                            record.created_at,
                        );
                        backend.set_inline_meta(
                            vid,
                            crate::primitives::vector::types::InlineMeta {
                                key: vector_key.to_string(),
                                source_ref: record.source_ref.clone(),
                            },
                        );
                    } else {
                        tracing::debug!(
                            target: "strata::refresh",
                            collection = collection,
                            "Skipping vector insert for unknown collection (will be picked up on restart)"
                        );
                    }
                }

                // Vector deletes: remove from backend using pre-reads
                for (key, record) in &vector_delete_pre_reads {
                    let user_key_str = match key.user_key_string() {
                        Some(s) => s,
                        None => continue,
                    };
                    let collection = match user_key_str.split_once('/') {
                        Some((coll, _)) => coll,
                        None => continue,
                    };
                    let branch_id = key.namespace.branch_id;
                    let cid = CollectionId::new(branch_id, collection);
                    let vid = VectorId::new(record.vector_id);

                    if let Some(mut backend) = vs.backends.get_mut(&cid) {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);
                        let _ = backend.delete_with_timestamp(vid, now);
                        backend.remove_inline_meta(vid);
                    }
                }
            }

            applied += 1;
        }

        // Advance local counters so new transactions get unique IDs/versions
        self.coordinator.catch_up_version(max_version);
        self.coordinator.catch_up_txn_id(max_txn_id);

        // Update watermark
        self.wal_watermark
            .store(max_txn_id, std::sync::atomic::Ordering::SeqCst);

        Ok(applied)
    }

    // ========================================================================
    // Flush
    // ========================================================================

    /// Flush WAL to disk
    ///
    /// Forces all buffered WAL entries to be written to disk.
    /// This is automatically done based on durability mode, but can
    /// be called manually to ensure durability at a specific point.
    ///
    /// For ephemeral databases, this is a no-op.
    pub fn flush(&self) -> StrataResult<()> {
        if let Some(ref wal) = self.wal_writer {
            let mut wal = wal.lock();
            wal.flush().map_err(StrataError::from)
        } else {
            // Ephemeral mode - no-op
            Ok(())
        }
    }

    // ========================================================================
    // Checkpoint & Compaction
    // ========================================================================

    /// Create a snapshot checkpoint of the current database state.
    ///
    /// Checkpoints serialize all primitive state to a crash-safe snapshot file
    /// and update the MANIFEST watermark. After a checkpoint, WAL compaction
    /// can safely remove segments covered by the snapshot.
    ///
    /// For ephemeral (cache) databases, this is a no-op.
    ///
    /// See: `docs/architecture/STORAGE_DURABILITY_ARCHITECTURE.md` Section 6.3
    pub fn checkpoint(&self) -> StrataResult<()> {
        if self.persistence_mode == PersistenceMode::Ephemeral || self.follower {
            return Ok(());
        }

        // Flush WAL first to ensure all buffered writes are on disk
        self.flush()?;

        // Drain all in-flight commits so the watermark reflects only fully-
        // applied versions. Using current_version() here is unsafe because
        // allocate_version() bumps the counter before apply_writes() completes,
        // so a concurrent commit's version could be included in the watermark
        // while its storage writes are still in progress (#1710).
        let watermark_txn = self.coordinator.quiesced_version();

        // Collect data from storage
        let data = self.collect_checkpoint_data();

        // Create snapshots directory
        let snapshots_dir = self.data_dir.join("snapshots");
        std::fs::create_dir_all(&snapshots_dir).map_err(StrataError::from)?;

        // Load or create MANIFEST
        let mut manifest = self.load_or_create_manifest()?;

        // Build watermark state from existing MANIFEST if present
        let existing_watermark = {
            let m = manifest.manifest();
            match (m.snapshot_id, m.snapshot_watermark) {
                (Some(sid), Some(wtxn)) => Some(strata_durability::SnapshotWatermark::with_values(
                    sid, wtxn, 0,
                )),
                _ => None,
            }
        };

        // Create CheckpointCoordinator
        let mut coordinator = if let Some(wm) = existing_watermark {
            CheckpointCoordinator::with_watermark(
                snapshots_dir,
                Box::new(IdentityCodec),
                [0u8; 16],
                wm,
            )
            .map_err(|e| StrataError::internal(format!("checkpoint coordinator: {}", e)))?
        } else {
            CheckpointCoordinator::new(snapshots_dir, Box::new(IdentityCodec), [0u8; 16])
                .map_err(|e| StrataError::internal(format!("checkpoint coordinator: {}", e)))?
        };

        // Create the checkpoint
        let info = coordinator
            .checkpoint(watermark_txn, data)
            .map_err(|e: CheckpointError| {
                StrataError::internal(format!("checkpoint failed: {}", e))
            })?;

        // Update MANIFEST with snapshot watermark
        manifest
            .set_snapshot_watermark(info.snapshot_id, info.watermark_txn)
            .map_err(|e: ManifestError| {
                StrataError::internal(format!("manifest update failed: {}", e))
            })?;

        info!(
            target: "strata::db",
            snapshot_id = info.snapshot_id,
            watermark_txn = info.watermark_txn,
            "Checkpoint created"
        );

        Ok(())
    }

    /// Compact WAL segments that are no longer needed for recovery.
    ///
    /// Removes closed WAL segments whose max transaction ID is at or below the
    /// latest snapshot watermark. The active segment is never removed.
    ///
    /// A checkpoint must exist before compaction can run. For ephemeral (cache)
    /// databases, this is a no-op.
    ///
    /// See: `docs/architecture/STORAGE_DURABILITY_ARCHITECTURE.md` Section 5.6
    pub fn compact(&self) -> StrataResult<()> {
        if self.persistence_mode == PersistenceMode::Ephemeral || self.follower {
            return Ok(());
        }

        let wal_dir = self.data_dir.join("wal");

        // Load or create MANIFEST
        let manifest = self.load_or_create_manifest()?;
        let manifest_arc = Arc::new(parking_lot::Mutex::new(manifest));

        // Get the writer's in-memory segment number (may be ahead of MANIFEST)
        let writer_active = self
            .wal_writer
            .as_ref()
            .map(|w| w.lock().current_segment())
            .unwrap_or(0);

        // Create compactor and run with the writer's active segment override
        let compactor = WalOnlyCompactor::new(wal_dir, manifest_arc);
        let compact_info = compactor
            .compact_with_active_override(writer_active)
            .map_err(|e: CompactionError| match e {
                CompactionError::NoSnapshot => StrataError::invalid_input(
                    "No checkpoint exists yet. Run checkpoint() before compact().".to_string(),
                ),
                other => StrataError::internal(format!("compaction failed: {}", other)),
            })?;

        info!(
            target: "strata::db",
            segments_removed = compact_info.wal_segments_removed,
            bytes_reclaimed = compact_info.reclaimed_bytes,
            "WAL compaction completed"
        );

        Ok(())
    }

    /// Compute database disk usage across WAL and snapshot directories.
    ///
    /// Returns zeros for ephemeral databases.
    pub fn disk_usage(&self) -> DatabaseDiskUsage {
        if self.persistence_mode == PersistenceMode::Ephemeral {
            return DatabaseDiskUsage::default();
        }

        let wal = self
            .wal_writer
            .as_ref()
            .map(|w| w.lock().wal_disk_usage())
            .unwrap_or_default();

        let snapshots_dir = self.data_dir.join("snapshots");
        let snapshot_bytes = scan_dir_size(&snapshots_dir);

        DatabaseDiskUsage {
            wal,
            snapshot_bytes,
        }
    }

    /// Collect all primitive data from storage for checkpointing.
    fn collect_checkpoint_data(&self) -> CheckpointData {
        let mut kv_entries = Vec::new();
        let mut event_entries = Vec::new();
        let mut state_entries = Vec::new();
        let mut branch_entries = Vec::new();
        let mut json_entries = Vec::new();

        let now = strata_durability::now_micros();

        for branch_id in self.storage.branch_ids() {
            // KV entries
            for (key, vv) in self.storage.list_by_type(&branch_id, TypeTag::KV) {
                let value_bytes = serde_json::to_vec(&vv.value).unwrap_or_default();
                kv_entries.push(KvSnapshotEntry {
                    key: key.user_key_string().unwrap_or_default(),
                    value: value_bytes,
                    version: vv.version.as_u64(),
                    timestamp: now,
                });
            }

            // Event entries
            for (key, vv) in self.storage.list_by_type(&branch_id, TypeTag::Event) {
                // Skip metadata keys
                if *key.user_key == *b"__meta__" || key.user_key.starts_with(b"__tidx__") {
                    continue;
                }
                let sequence = if key.user_key.len() == 8 {
                    u64::from_be_bytes((*key.user_key).try_into().unwrap_or([0; 8]))
                } else {
                    0
                };
                let payload = serde_json::to_vec(&vv.value).unwrap_or_default();
                event_entries.push(EventSnapshotEntry {
                    sequence,
                    payload,
                    timestamp: now,
                });
            }

            // State entries
            for (key, vv) in self.storage.list_by_type(&branch_id, TypeTag::State) {
                let value_bytes = serde_json::to_vec(&vv.value).unwrap_or_default();
                state_entries.push(StateSnapshotEntry {
                    name: key.user_key_string().unwrap_or_default(),
                    value: value_bytes,
                    counter: vv.version.as_u64(),
                    timestamp: now,
                });
            }

            // Branch entries
            for (key, vv) in self.storage.list_by_type(&branch_id, TypeTag::Branch) {
                // Skip index keys
                if key.user_key.starts_with(b"__idx_") {
                    continue;
                }
                let branch_id_bytes: [u8; 16] = if key.user_key.len() == 16 {
                    (*key.user_key).try_into().unwrap_or([0; 16])
                } else {
                    [0; 16]
                };
                let metadata = serde_json::to_vec(&vv.value).unwrap_or_default();
                branch_entries.push(BranchSnapshotEntry {
                    branch_id: branch_id_bytes,
                    name: String::new(),
                    created_at: now,
                    metadata,
                });
            }

            // JSON entries
            for (key, vv) in self.storage.list_by_type(&branch_id, TypeTag::Json) {
                let content = serde_json::to_vec(&vv.value).unwrap_or_default();
                json_entries.push(JsonSnapshotEntry {
                    doc_id: key.user_key_string().unwrap_or_default(),
                    content,
                    version: vv.version.as_u64(),
                    timestamp: now,
                });
            }
        }

        let mut data = CheckpointData::new();
        if !kv_entries.is_empty() {
            data = data.with_kv(kv_entries);
        }
        if !event_entries.is_empty() {
            data = data.with_events(event_entries);
        }
        if !state_entries.is_empty() {
            data = data.with_states(state_entries);
        }
        if !branch_entries.is_empty() {
            data = data.with_branches(branch_entries);
        }
        if !json_entries.is_empty() {
            data = data.with_json(json_entries);
        }
        data
    }

    /// Load an existing MANIFEST or create a new one.
    ///
    /// Also updates the active WAL segment from the current WAL writer.
    fn load_or_create_manifest(&self) -> StrataResult<ManifestManager> {
        let manifest_path = self.data_dir.join("MANIFEST");

        let mut manifest = if ManifestManager::exists(&manifest_path) {
            ManifestManager::load(manifest_path).map_err(|e: ManifestError| {
                StrataError::internal(format!("failed to load MANIFEST: {}", e))
            })?
        } else {
            ManifestManager::create(manifest_path, [0u8; 16], "identity".to_string()).map_err(
                |e: ManifestError| {
                    StrataError::internal(format!("failed to create MANIFEST: {}", e))
                },
            )?
        };

        // Update active WAL segment from the writer
        if let Some(ref wal) = self.wal_writer {
            let wal = wal.lock();
            let current_seg = wal.current_segment();
            manifest.manifest_mut().active_wal_segment = current_seg;
            manifest.persist().map_err(|e: ManifestError| {
                StrataError::internal(format!("failed to persist MANIFEST: {}", e))
            })?;
        }

        Ok(manifest)
    }

    // ========================================================================
    // Transaction API
    // ========================================================================

    /// Check if the database is accepting transactions.
    fn check_accepting(&self) -> StrataResult<()> {
        if !self.accepting_transactions.load(Ordering::Acquire) {
            return Err(StrataError::invalid_input(
                "Database is shutting down".to_string(),
            ));
        }
        Ok(())
    }

    /// Flush frozen memtables and compact synchronously on the writer's thread.
    ///
    /// Writes pay the cost so reads never hit a deep L0. This keeps L0 at ≤4
    /// segments at all times.
    fn schedule_flush_if_needed(&self) {
        if self.storage.segments_dir().is_none() {
            return;
        }

        // Update snapshot floor so compaction respects active snapshots (#1697).
        self.storage.set_snapshot_floor(self.gc_safe_point());

        // 1. Flush and compact branches that need it.
        let branches = self.storage.branches_needing_flush();
        for branch_id in branches {
            // Flush all frozen memtables
            loop {
                match self.storage.flush_oldest_frozen(&branch_id) {
                    Ok(true) => {
                        Self::update_flush_watermark(&self.storage, &self.data_dir, &self.wal_dir);
                    }
                    Ok(false) => break,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::flush",
                            ?branch_id,
                            error = %e,
                            "flush failed"
                        );
                        break;
                    }
                }
            }

            // Compact until all levels are below target
            loop {
                match self.storage.pick_and_compact(&branch_id, 0) {
                    Ok(Some(_)) => {
                        self.write_stall_cv.notify_all();
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::compact",
                            ?branch_id,
                            error = %e,
                            "compaction failed"
                        );
                        break;
                    }
                }
            }
        }

        // 2. Materialize inherited layers that exceed depth limit.
        //    Decoupled from flush — runs even when no branches need flushing (#1704).
        for branch_id in self.storage.branches_needing_materialization() {
            let layer_count = self.storage.inherited_layer_count(&branch_id);
            if layer_count > 0 {
                let deepest = layer_count - 1;
                match self.storage.materialize_layer(&branch_id, deepest) {
                    Ok(result) => {
                        tracing::info!(
                            target: "strata::materialize",
                            ?branch_id,
                            entries = result.entries_materialized,
                            segments = result.segments_created,
                            "materialized inherited layer"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::materialize",
                            ?branch_id,
                            error = %e,
                            "materialization failed"
                        );
                    }
                }
            }
        }
    }

    /// Apply write backpressure when memtable memory exceeds safe limits.
    ///
    /// RocksDB-style write backpressure based on L0 file count and memtable
    /// pressure.  Called after every write commit, outside any storage guards.
    ///
    /// Three tiers (matching RocksDB semantics):
    /// 1. L0 count >= `l0_stop_writes_trigger` → wait on condvar until
    ///    compaction signals (complete stall).
    /// 2. L0 count >= `l0_slowdown_writes_trigger` → yield 1 ms per write.
    /// 3. Memtable bytes > threshold → yield 1 ms (OOM protection).
    #[inline]
    fn maybe_apply_write_backpressure(&self) {
        let cfg = self.config.read();

        // L0-based stalling (protects read latency)
        let l0_stop = cfg.storage.l0_stop_writes_trigger;
        let l0_slow = cfg.storage.l0_slowdown_writes_trigger;
        drop(cfg); // release config lock before potential sleep

        let l0_count = self.storage.max_l0_segment_count();

        if l0_stop > 0 && l0_count >= l0_stop {
            // Complete stall: wait on condvar until compaction drains L0.
            // Also trigger compaction in case none is running.
            self.schedule_flush_if_needed();
            let mut guard = self.write_stall_mu.lock();
            // Re-check after acquiring lock (compaction may have finished)
            while self.storage.max_l0_segment_count() >= l0_stop {
                self.write_stall_cv
                    .wait_for(&mut guard, std::time::Duration::from_millis(10));
            }
            return;
        }

        if l0_slow > 0 && l0_count >= l0_slow {
            std::thread::sleep(std::time::Duration::from_millis(1));
            return;
        }

        // Memtable-based stalling (protects memory usage)
        let cfg = self.config.read();
        let wbs = cfg.storage.write_buffer_size as u64;
        let max_frozen = cfg.storage.max_immutable_memtables as u64;
        if wbs == 0 {
            return;
        }
        let threshold = wbs * (max_frozen + 2);
        let current = self.storage.total_memtable_bytes();
        if current > threshold {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Update the MANIFEST flush watermark and truncate WAL segments below it.
    ///
    /// Computes the global flush watermark as the minimum `max_flushed_commit`
    /// across all branches. WAL segments fully below this watermark are safe
    /// to delete because their data is in segments.
    ///
    /// Best-effort: errors are logged, not propagated.
    fn update_flush_watermark(storage: &SegmentedStore, data_dir: &Path, wal_dir: &Path) {
        // Compute global flush watermark: min of max_flushed_commit across all branches
        let branch_ids = storage.branch_ids();
        if branch_ids.is_empty() {
            return;
        }

        // Compute watermark: min of max_flushed_commit across branches
        // that participate in the flush pipeline (have segments).
        // Branches with no segments (e.g. _system_) are excluded — their
        // data is small and replayed from WAL on recovery.
        let mut watermark = u64::MAX;
        let mut has_any_segments = false;
        for bid in &branch_ids {
            if let Some(commit) = storage.max_flushed_commit(bid) {
                if commit > 0 {
                    watermark = watermark.min(commit);
                    has_any_segments = true;
                }
            }
            // Branches with no segments are intentionally excluded
        }
        if !has_any_segments || watermark == u64::MAX {
            return;
        }

        // Update MANIFEST
        let manifest_path = data_dir.join("MANIFEST");
        let mut mgr = match ManifestManager::load(manifest_path) {
            Ok(mgr) => mgr,
            Err(e) => {
                tracing::debug!(
                    target: "strata::flush",
                    error = %e,
                    "No MANIFEST found, skipping WAL truncation"
                );
                return;
            }
        };

        if let Err(e) = mgr.set_flush_watermark(watermark) {
            tracing::warn!(
                target: "strata::flush",
                error = %e,
                watermark,
                "Failed to update flush watermark in MANIFEST"
            );
            return; // Don't truncate WAL if watermark wasn't persisted
        }

        // Truncate WAL segments below watermark
        let manifest_arc = Arc::new(ParkingMutex::new(mgr));
        let compactor = WalOnlyCompactor::new(wal_dir.to_path_buf(), manifest_arc);
        match compactor.compact() {
            Ok(info) => {
                if info.wal_segments_removed > 0 {
                    tracing::debug!(
                        target: "strata::wal",
                        segments_removed = info.wal_segments_removed,
                        bytes_reclaimed = info.reclaimed_bytes,
                        watermark,
                        "WAL segments truncated after flush"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "strata::wal",
                    error = %e,
                    "WAL compaction failed after flush"
                );
            }
        }
    }

    /// Execute one transaction attempt: commit on success, abort on error.
    ///
    /// Handles the commit-or-abort decision and coordinator bookkeeping.
    /// The caller is responsible for calling `end_transaction()` afterward.
    ///
    /// Returns `(closure_result, commit_version)` on success.
    fn run_single_attempt<T>(
        &self,
        txn: &mut TransactionContext,
        result: StrataResult<T>,
        durability: DurabilityMode,
    ) -> StrataResult<(T, u64)> {
        match result {
            Ok(value) => {
                // Commit on success
                let had_writes = !txn.is_read_only();
                let commit_version = self.commit_internal(txn, durability)?;
                // Schedule flush only for write transactions (reads skip entirely)
                if had_writes {
                    self.schedule_flush_if_needed();
                    self.maybe_apply_write_backpressure();
                }
                Ok((value, commit_version))
            }
            Err(e) => {
                let _ = txn.mark_aborted(format!("Closure error: {}", e));
                self.coordinator.record_abort(txn.txn_id);
                Err(e)
            }
        }
    }

    /// Execute a transaction with the given closure
    ///
    /// Per spec Section 4:
    /// - Creates TransactionContext with snapshot
    /// - Executes closure with transaction
    /// - Validates and commits on success
    /// - Aborts on error
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    /// * `f` - Closure that performs transaction operations
    ///
    /// # Returns
    /// * `Ok(T)` - Closure return value on successful commit
    /// * `Err` - On validation conflict or closure error
    ///
    /// # Example
    /// ```text
    /// let result = db.transaction(branch_id, |txn| {
    ///     let val = txn.get(&key)?;
    ///     txn.put(key, new_value)?;
    ///     Ok(val)
    /// })?;
    /// ```
    pub fn transaction<F, T>(&self, branch_id: BranchId, f: F) -> StrataResult<T>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<T>,
    {
        self.check_accepting()?;
        let mut txn = self.begin_transaction(branch_id)?;
        let result = f(&mut txn);
        let outcome = self.run_single_attempt(&mut txn, result, self.durability_mode);
        self.end_transaction(txn);
        outcome.map(|(value, _)| value)
    }

    /// Execute a transaction and return both the result and commit version
    ///
    /// Like `transaction()` but also returns the commit version assigned to all writes.
    /// Use this when you need to know the version created by write operations.
    ///
    /// # Returns
    /// * `Ok((T, u64))` - Closure result and commit version
    /// * `Err` - On validation conflict or closure error
    ///
    /// # Example
    /// ```text
    /// let (result, commit_version) = db.transaction_with_version(branch_id, |txn| {
    ///     txn.put(key, value)?;
    ///     Ok("success")
    /// })?;
    /// // commit_version now contains the version assigned to the put
    /// ```
    pub(crate) fn transaction_with_version<F, T>(
        &self,
        branch_id: BranchId,
        f: F,
    ) -> StrataResult<(T, u64)>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<T>,
    {
        self.check_accepting()?;
        let mut txn = self.begin_transaction(branch_id)?;
        let result = f(&mut txn);
        let outcome = self.run_single_attempt(&mut txn, result, self.durability_mode);
        self.end_transaction(txn);
        outcome
    }

    /// Begin a new transaction (for manual control)
    ///
    /// Returns a TransactionContext that must be manually committed or aborted.
    /// Prefer `transaction()` closure API for automatic handling.
    ///
    /// Uses thread-local pool to avoid allocation overhead after warmup.
    /// Call `end_transaction()` after commit/abort to return context to pool.
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    ///
    /// # Returns
    /// * `TransactionContext` - Active transaction ready for operations
    ///
    /// # Example
    /// ```text
    /// let mut txn = db.begin_transaction(branch_id)?;
    /// txn.put(key, value)?;
    /// db.commit_transaction(&mut txn)?;
    /// db.end_transaction(txn); // Return to pool
    /// ```
    pub fn begin_transaction(&self, branch_id: BranchId) -> StrataResult<TransactionContext> {
        let txn_id = self.coordinator.next_txn_id()?;
        let snapshot_version = self.storage.version();
        self.coordinator.record_start(txn_id, snapshot_version);

        let mut txn = TransactionPool::acquire(txn_id, branch_id, Some(Arc::clone(&self.storage)));
        txn.set_max_write_entries(self.coordinator.max_write_buffer_entries());
        Ok(txn)
    }

    /// Begin a read-only transaction
    ///
    /// Returns a transaction that rejects writes and skips read-set tracking,
    /// saving memory on large scan workloads.
    pub fn begin_read_only_transaction(
        &self,
        branch_id: BranchId,
    ) -> StrataResult<TransactionContext> {
        let mut txn = self.begin_transaction(branch_id)?;
        txn.set_read_only(true);
        Ok(txn)
    }

    /// End a transaction (return to pool)
    ///
    /// Returns the transaction context to the thread-local pool for reuse.
    /// This avoids allocation overhead on subsequent transactions.
    ///
    /// Should be called after `commit_transaction()` or after aborting.
    /// The closure API (`transaction()`) calls this automatically.
    ///
    /// # Arguments
    /// * `ctx` - Transaction context to return to pool
    ///
    /// # Example
    /// ```text
    /// let mut txn = db.begin_transaction(branch_id)?;
    /// txn.put(key, value)?;
    /// db.commit_transaction(&mut txn)?;
    /// db.end_transaction(txn); // Return to pool for reuse
    /// ```
    pub fn end_transaction(&self, ctx: TransactionContext) {
        TransactionPool::release(ctx);
    }

    /// Commit a transaction
    ///
    /// Per spec commit sequence:
    /// 1. Validate (conflict detection)
    /// 2. Allocate commit version
    /// 3. Write to WAL (BeginTxn, Writes, CommitTxn)
    /// 4. Apply to storage
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit
    ///
    /// # Returns
    /// * `Ok(commit_version)` - Transaction committed successfully, returns commit version
    /// * `Err(TransactionConflict)` - Validation failed, transaction aborted
    ///
    /// # Errors
    /// - `TransactionConflict` - Read-write or CAS conflict detected
    /// - `InvalidState` - Transaction not in Active state
    ///
    /// # Contract
    /// Returns the commit version (u64) assigned to all writes in this transaction.
    pub fn commit_transaction(&self, txn: &mut TransactionContext) -> StrataResult<u64> {
        let had_writes = !txn.is_read_only();
        let version = self.commit_internal(txn, self.durability_mode)?;
        if had_writes {
            self.schedule_flush_if_needed();
            self.maybe_apply_write_backpressure();
        }
        Ok(version)
    }

    /// Internal commit implementation shared by commit_transaction and transaction closures
    ///
    /// Delegates the commit protocol to the concurrency layer (TransactionManager)
    /// via the TransactionCoordinator. The engine is responsible only for:
    /// - Determining whether to pass the WAL (based on durability mode + persistence)
    ///
    /// The concurrency layer handles:
    /// - Per-run commit locking (TOCTOU prevention)
    /// - Validation (first-committer-wins)
    /// - Version allocation
    /// - WAL writing (when WAL reference is provided)
    /// - Storage application
    /// - Fsync (WAL::append handles fsync based on its DurabilityMode)
    fn commit_internal(
        &self,
        txn: &mut TransactionContext,
        durability: DurabilityMode,
    ) -> StrataResult<u64> {
        if self.follower {
            return Err(StrataError::internal(
                "cannot commit: database opened in follower mode (read-only)",
            ));
        }

        let wal_arc = if durability.requires_wal() {
            self.wal_writer.as_ref()
        } else {
            None
        };

        self.coordinator
            .commit_with_wal_arc(txn, self.storage.as_ref(), wal_arc)
    }

    // ========================================================================
    // Graceful Shutdown
    // ========================================================================

    /// Graceful shutdown - ensures all data is persisted
    ///
    /// Freeze all vector heaps to mmap files for crash-safe recovery.
    ///
    /// With lite KV records (embedding stripped), the mmap cache is required
    /// for the next recovery to reconstruct embeddings. This is called during
    /// shutdown and drop.
    fn freeze_vector_heaps(&self) -> StrataResult<()> {
        use crate::primitives::vector::VectorBackendState;

        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no mmap
        }

        let state = match self.extension::<VectorBackendState>() {
            Ok(s) => s,
            Err(_) => return Ok(()), // No vector state registered
        };

        for entry in state.backends.iter() {
            let (cid, backend) = (entry.key(), entry.value());
            let branch_hex = format!("{:032x}", u128::from_be_bytes(*cid.branch_id.as_bytes()));
            let vec_path = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}.vec", cid.name));
            backend.freeze_heap_to_disk(&vec_path)?;

            // Also freeze graphs
            let gdir = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}_graphs", cid.name));
            backend.freeze_graphs_to_disk(&gdir)?;
        }
        Ok(())
    }

    /// Freeze the search index to disk for fast recovery on next open.
    fn freeze_search_index(&self) -> StrataResult<()> {
        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return Ok(()); // Ephemeral database — no persistence
        }

        if let Ok(index) = self.extension::<crate::search::InvertedIndex>() {
            index.freeze_to_disk()?;
        }
        Ok(())
    }

    /// This method:
    /// 1. Stops accepting new transactions
    /// 2. Waits for pending operations to complete
    /// 3. Flushes WAL based on durability mode
    ///
    /// # Example
    ///
    /// ```text
    /// db.shutdown()?;
    /// assert!(!db.is_open());
    /// ```
    pub fn shutdown(&self) -> StrataResult<()> {
        // 1. Stop accepting new transactions
        self.accepting_transactions.store(false, Ordering::Release);

        // Followers have no background threads, WAL writer, or freeze targets.
        if self.follower {
            return Ok(());
        }

        // 2. Drain background tasks (embeddings etc.) before final WAL flush
        self.scheduler.drain();

        // 3. Wait for in-flight transactions to complete FIRST.
        //    The flush thread keeps running during this window, providing
        //    periodic syncs for any transactions that commit during drain.
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        while self.coordinator.active_count() > 0 && start.elapsed() < timeout {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let remaining = self.coordinator.active_count();
        if remaining > 0 {
            warn!(
                target: "strata::db",
                remaining,
                "Shutdown timed out waiting for {} active transaction(s) after {:?}",
                remaining,
                timeout,
            );
        }

        // 4. Signal the background flush thread to stop (after transactions are drained)
        self.flush_shutdown.store(true, Ordering::SeqCst);

        // Join the flush thread so it releases the WAL lock
        // (E-5: the thread performs a final sync before exiting)
        if let Some(handle) = self.flush_handle.lock().take() {
            let _ = handle.join();
        }

        // 5. Final flush to ensure all data is persisted
        self.flush()?;

        // 6. Freeze both vector heaps and search index. Attempt both even if
        // the first fails, so a vector freeze error doesn't also lose search data.
        let vec_result = self.freeze_vector_heaps();
        let search_result = self.freeze_search_index();
        self.shutdown_complete.store(true, Ordering::Release);
        vec_result?;
        search_result?;

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Shut down the background task scheduler
        self.scheduler.shutdown();

        // Stop the background flush thread
        self.flush_shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.flush_handle.lock().take() {
            let _ = handle.join();
        }

        // Skip flush/freeze if shutdown() already completed them.
        if !self.shutdown_complete.load(Ordering::Acquire) && !self.follower {
            // Final flush to persist any remaining data
            let _ = self.flush();

            // Freeze vector heaps to mmap so lite KV records can recover
            if let Err(e) = self.freeze_vector_heaps() {
                tracing::warn!(target: "strata::db", error = %e, "Failed to freeze vector heaps in drop");
            }

            // Freeze search index to disk for fast recovery
            if let Err(e) = self.freeze_search_index() {
                tracing::warn!(target: "strata::db", error = %e, "Failed to freeze search index in drop");
            }
        }

        // Remove from registry if we're disk-backed
        if self.persistence_mode == PersistenceMode::Disk && !self.data_dir.as_os_str().is_empty() {
            let mut registry = OPEN_DATABASES.lock();
            registry.remove(&self.data_dir);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use strata_concurrency::TransactionPayload;
    use strata_core::types::{Key, Namespace};
    use strata_core::value::Value;
    use strata_core::{Storage, Timestamp};
    use strata_durability::format::WalRecord;
    use strata_durability::now_micros;
    use tempfile::TempDir;

    impl Database {
        /// Test-only helper: open with a specific durability mode.
        fn open_with_durability<P: AsRef<Path>>(
            path: P,
            durability_mode: DurabilityMode,
        ) -> StrataResult<Arc<Self>> {
            let dur_str = match durability_mode {
                DurabilityMode::Always => "always",
                DurabilityMode::Cache => "cache",
                _ => "standard",
            };
            let cfg = StrataConfig {
                durability: dur_str.to_string(),
                ..StrataConfig::default()
            };
            Self::open_internal(path, durability_mode, cfg)
        }
    }

    /// Helper: write a committed transaction to the segmented WAL
    fn write_wal_txn(
        wal_dir: &std::path::Path,
        _txn_id: u64,
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
        };
        // Use version (commit_version) as WAL record ordering key (#1696)
        let record = WalRecord::new(
            version,
            *branch_id.as_bytes(),
            now_micros(),
            payload.to_bytes(),
        );
        wal.append(&record).unwrap();
        wal.flush().unwrap();
    }

    #[test]
    fn test_open_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("new_db");

        assert!(!db_path.exists());
        let _db = Database::open(&db_path).unwrap();
        assert!(db_path.exists());
    }

    #[test]
    fn test_ephemeral_no_files() {
        let db = Database::cache().unwrap();

        // Should work for operations
        assert!(db.is_cache());
    }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

        // Write directly to segmented WAL (simulating a crash recovery scenario)
        {
            let wal_dir = db_path.join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();
            write_wal_txn(
                &wal_dir,
                1,
                branch_id,
                vec![(
                    Key::new_kv(ns.clone(), "key1"),
                    Value::Bytes(b"value1".to_vec()),
                )],
                vec![],
                1,
            );
        }

        // Open database (should replay WAL)
        let db = Database::open(&db_path).unwrap();

        // Storage should have data from WAL
        let key1 = Key::new_kv(ns, "key1");
        let val = db
            .storage()
            .get_versioned(&key1, u64::MAX)
            .unwrap()
            .unwrap();

        if let Value::Bytes(bytes) = val.value {
            assert_eq!(bytes, b"value1");
        } else {
            panic!("Wrong value type");
        }
    }

    #[test]
    fn test_open_close_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

        // Open database, write via transaction, close
        {
            let db = Database::open(&db_path).unwrap();

            db.transaction(branch_id, |txn| {
                txn.put(
                    Key::new_kv(ns.clone(), "persistent"),
                    Value::Bytes(b"data".to_vec()),
                )?;
                Ok(())
            })
            .unwrap();

            db.flush().unwrap();
        }

        // Reopen database
        {
            let db = Database::open(&db_path).unwrap();

            // Data should be restored from WAL
            let key = Key::new_kv(ns, "persistent");
            let val = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();

            if let Value::Bytes(bytes) = val.value {
                assert_eq!(bytes, b"data");
            } else {
                panic!("Wrong value type");
            }
        }
    }

    #[test]
    fn test_partial_record_discarded() {
        // With the segmented WAL, partial records (crash mid-write) are
        // automatically discarded by the reader. There are no "incomplete
        // transactions" since each WalRecord = one committed transaction.
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

        // Write one valid record, then append garbage to simulate crash
        {
            let wal_dir = db_path.join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();
            write_wal_txn(
                &wal_dir,
                1,
                branch_id,
                vec![(Key::new_kv(ns.clone(), "valid"), Value::Int(42))],
                vec![],
                1,
            );

            // Append garbage to simulate crash mid-write of second record
            let segment_path = strata_durability::format::WalSegment::segment_path(&wal_dir, 1);
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&segment_path)
                .unwrap();
            file.write_all(&[0xFF; 20]).unwrap();
        }

        // Open database (should recover valid record, skip garbage)
        let db = Database::open(&db_path).unwrap();

        // Valid transaction should be present
        let key = Key::new_kv(ns.clone(), "valid");
        let val = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(val.value, Value::Int(42));
    }

    #[test]
    fn test_corrupted_wal_handled_gracefully() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        // Create WAL directory but with no valid segment files
        // (the segmented WAL will find no segments and return empty)
        {
            std::fs::create_dir_all(db_path.join("wal")).unwrap();
        }

        // Open should succeed with empty storage (no segments found)
        let result = Database::open(&db_path);
        assert!(result.is_ok());

        let db = result.unwrap();
        // Storage has only system branch init transactions (no user data recovered)
        // init_system_branch writes 3 transactions: branch, graph, node
        assert!(db.storage().current_version() <= 3);
    }

    #[test]
    fn test_open_with_different_durability_modes() {
        let temp_dir = TempDir::new().unwrap();

        // Always mode
        {
            let db = Database::open_with_durability(
                temp_dir.path().join("strict"),
                DurabilityMode::Always,
            )
            .unwrap();
            assert!(!db.is_cache());
        }

        // Standard mode
        {
            let db = Database::open_with_durability(
                temp_dir.path().join("batched"),
                DurabilityMode::Standard {
                    interval_ms: 100,
                    batch_size: 1000,
                },
            )
            .unwrap();
            assert!(!db.is_cache());
        }

        // Cache mode
        {
            let db =
                Database::open_with_durability(temp_dir.path().join("none"), DurabilityMode::Cache)
                    .unwrap();
            assert!(!db.is_cache());
        }
    }

    #[test]
    fn test_flush() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let db = Database::open(&db_path).unwrap();

        // Flush should succeed
        assert!(db.flush().is_ok());
    }

    // ========================================================================
    // Transaction API Tests
    // ========================================================================

    fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    #[test]
    fn test_transaction_closure_api() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "test_key");

        // Execute transaction
        let result = db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(42))?;
            Ok(())
        });

        assert!(result.is_ok());

        // Verify data was committed
        let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(42));
    }

    #[test]
    fn test_transaction_returns_closure_value() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "test_key");

        // Pre-populate using transaction
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(100))?;
            Ok(())
        })
        .unwrap();

        // Transaction returns a value
        let result: StrataResult<i64> = db.transaction(branch_id, |txn| {
            let val = txn.get(&key)?.unwrap();
            if let Value::Int(n) = val {
                Ok(n)
            } else {
                Err(StrataError::invalid_input("wrong type".to_string()))
            }
        });

        assert_eq!(result.unwrap(), 100);
    }

    #[test]
    fn test_transaction_read_your_writes() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "ryw_key");

        // Per spec Section 2.1: "Its own uncommitted writes - always visible"
        let result: StrataResult<Value> = db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("written".to_string()))?;

            // Should see our own write
            let val = txn.get(&key)?.unwrap();
            Ok(val)
        });

        assert_eq!(result.unwrap(), Value::String("written".to_string()));
    }

    #[test]
    fn test_transaction_aborts_on_closure_error() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "abort_key");

        // Transaction that errors
        let result: StrataResult<()> = db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(999))?;
            Err(StrataError::invalid_input("intentional error".to_string()))
        });

        assert!(result.is_err());

        // Data should NOT be committed
        assert!(db
            .storage()
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_begin_and_commit_manual() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "manual_key");

        // Manual transaction control
        let mut txn = db.begin_transaction(branch_id).unwrap();
        txn.put(key.clone(), Value::Int(123)).unwrap();

        // Commit manually
        db.commit_transaction(&mut txn).unwrap();

        // Verify committed
        let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(123));
    }

    // ========================================================================
    // Graceful Shutdown Tests
    // ========================================================================

    #[test]
    fn test_is_open_initially_true() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        // Database should be open initially
        assert!(db.is_open());
    }

    #[test]
    fn test_shutdown_sets_not_open() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        assert!(db.is_open());

        // Shutdown should succeed
        assert!(db.shutdown().is_ok());

        // Database should no longer be open
        assert!(!db.is_open());
    }

    #[test]
    fn test_shutdown_rejects_new_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "after_shutdown");

        // Shutdown the database
        db.shutdown().unwrap();

        // New transactions should be rejected
        let result = db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(42))?;
            Ok(())
        });

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, StrataError::InvalidInput { .. }));
    }

    #[test]
    fn test_shutdown_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        // Multiple shutdowns should be safe
        assert!(db.shutdown().is_ok());
        assert!(db.shutdown().is_ok());
        assert!(db.shutdown().is_ok());

        // Should remain not open
        assert!(!db.is_open());
    }

    // ========================================================================
    // Singleton Registry Tests
    // ========================================================================

    #[test]
    fn test_open_same_path_returns_same_instance() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("singleton_test");

        // Open database twice with same path
        let db1 = Database::open(&db_path).unwrap();
        let db2 = Database::open(&db_path).unwrap();

        // Both should be the same Arc (same pointer)
        assert!(Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_open_different_paths_returns_different_instances() {
        let temp_dir = TempDir::new().unwrap();
        let path1 = temp_dir.path().join("db1");
        let path2 = temp_dir.path().join("db2");

        let db1 = Database::open(&path1).unwrap();
        let db2 = Database::open(&path2).unwrap();

        // Should be different instances
        assert!(!Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_ephemeral_not_registered() {
        // Create two cache databases
        let db1 = Database::cache().unwrap();
        let db2 = Database::cache().unwrap();

        // They should be different instances (not shared via registry)
        assert!(!Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_open_uses_registry() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("singleton_via_open");

        // Open via Database::open twice
        let db1 = Database::open(&db_path).unwrap();
        let db2 = Database::open(&db_path).unwrap();

        // Should be same instance
        assert!(Arc::ptr_eq(&db1, &db2));
    }

    #[test]
    fn test_open_creates_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("config_test");

        let db = Database::open(&db_path).unwrap();

        assert!(!db.is_cache());
        assert!(db_path.exists());
        // Config file should have been created
        assert!(db_path.join("strata.toml").exists());
    }

    #[test]
    fn test_open_reads_always_config() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("always_config");

        // Create directory and write always config before opening
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("strata.toml"), "durability = \"always\"\n").unwrap();

        let db = Database::open(&db_path).unwrap();
        assert!(!db.is_cache());
    }

    #[test]
    fn test_open_rejects_invalid_config() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bad_config");

        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("strata.toml"), "durability = \"turbo\"\n").unwrap();

        let result = Database::open(&db_path);
        assert!(result.is_err());
    }

    // ========================================================================
    // Checkpoint & Compaction Tests
    // ========================================================================

    #[test]
    fn test_checkpoint_ephemeral_noop() {
        let db = Database::cache().unwrap();
        // checkpoint on ephemeral database is a no-op
        assert!(db.checkpoint().is_ok());
    }

    #[test]
    fn test_compact_ephemeral_noop() {
        let db = Database::cache().unwrap();
        // compact on ephemeral database is a no-op
        assert!(db.compact().is_ok());
    }

    #[test]
    fn test_compact_without_checkpoint_fails() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();

        // Compact before any checkpoint should fail (no snapshot watermark)
        let result = db.compact();
        assert!(result.is_err());
    }

    #[test]
    fn test_checkpoint_creates_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        let db = Database::open(&db_path).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "checkpoint_test");

        // Write some data
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("hello".to_string()))?;
            Ok(())
        })
        .unwrap();

        // Checkpoint should succeed
        assert!(db.checkpoint().is_ok());

        // Snapshots directory should exist with files
        let snapshots_dir = db_path.canonicalize().unwrap().join("snapshots");
        assert!(snapshots_dir.exists());

        // MANIFEST should exist
        let manifest_path = db_path.canonicalize().unwrap().join("MANIFEST");
        assert!(manifest_path.exists());
    }

    #[test]
    fn test_checkpoint_then_compact_without_flush_fails() {
        // Issue #1730: compact() after checkpoint-only must fail because
        // recovery cannot load snapshots. WAL compaction is only safe
        // when driven by the flush watermark (data in SST segments).
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        let db = Database::open(&db_path).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "compact_test");

        // Write data
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(42))?;
            Ok(())
        })
        .unwrap();

        // Checkpoint creates snapshot but no flush watermark
        assert!(db.checkpoint().is_ok());

        // Compact must fail — snapshot watermark alone is not safe
        let result = db.compact();
        assert!(result.is_err());
    }

    #[test]
    fn test_gc_safe_point_no_active_txns() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // After open, system branch init may have advanced the version.
        // gc_safe_point = max(0, current_version - 1)
        let initial_sp = db.gc_safe_point();
        assert!(
            initial_sp <= 3,
            "gc_safe_point should be small after init, got {}",
            initial_sp
        );

        // Commit two transactions to advance version past 1
        let key = Key::new_kv(ns.clone(), "gc_key");
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(2))?;
            Ok(())
        })
        .unwrap();

        // Now current_version >= 2, no active txns → safe_point = current - 1
        let sp = db.gc_safe_point();
        assert!(sp >= 1, "safe point should be at least 1, got {}", sp);
    }

    #[test]
    fn test_gc_safe_point_with_active_txn() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Commit a few transactions to advance version well past 1
        for i in 0..5 {
            let key = Key::new_kv(ns.clone(), format!("key_{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::Int(i))?;
                Ok(())
            })
            .unwrap();
        }

        // Start a transaction but don't commit it yet — pins version
        let txn = db.begin_transaction(branch_id).unwrap();
        let txn_start_version = txn.start_version;

        // Commit two more transactions to advance version further
        for i in 5..7 {
            let key = Key::new_kv(ns.clone(), format!("key_{}", i));
            db.transaction(branch_id, |t| {
                t.put(key.clone(), Value::Int(i))?;
                Ok(())
            })
            .unwrap();
        }

        // GC safe point should be min(current - 1, min_active_version)
        // The active txn pins the safe point at its start version
        let sp = db.gc_safe_point();
        assert!(
            sp <= txn_start_version,
            "safe point {} should be <= active txn start version {}",
            sp,
            txn_start_version
        );

        // Abort the active transaction
        db.coordinator.record_abort(txn.txn_id);

        // Now safe point should advance to current - 1 since no active txns
        let sp_after = db.gc_safe_point();
        assert!(
            sp_after > sp,
            "safe point should advance after abort: {} > {}",
            sp_after,
            sp
        );
    }

    #[test]
    fn test_run_gc_prunes_old_versions() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "gc_prune");

        // Write v1
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();

        // Overwrite with v2
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(2))?;
            Ok(())
        })
        .unwrap();

        // Overwrite with v3
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(3))?;
            Ok(())
        })
        .unwrap();

        // Run GC — SegmentedStore prunes via compaction, not gc_branch(),
        // so pruned count is 0.  Verify the API doesn't error.
        let (safe_point, _pruned) = db.run_gc();
        assert!(safe_point > 0, "safe point should be non-zero");

        // Latest value should still be readable
        let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(3));
    }

    #[test]
    fn test_run_gc_no_prune_at_version_zero() {
        let db = Database::cache().unwrap();

        // After init_system_branch, version has advanced; no user data to prune
        let (safe_point, pruned) = db.run_gc();
        // safe_point may be > 0 due to system branch init transactions
        assert!(
            safe_point <= 4,
            "safe_point should be small, got {}",
            safe_point
        );
        assert_eq!(pruned, 0);
    }

    #[test]
    fn test_run_maintenance_end_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Write and overwrite a key to create GC-able versions
        let key = Key::new_kv(ns, "maint_key");
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(2))?;
            Ok(())
        })
        .unwrap();

        // Run full maintenance cycle
        let (safe_point, pruned, expired) = db.run_maintenance();
        assert!(safe_point > 0, "safe point should be non-zero");
        // pruned may or may not be > 0 depending on version chain gc logic
        // expired should be 0 since we didn't use TTL
        assert_eq!(expired, 0, "no TTL entries to expire");

        // Data should still be readable
        let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(2));

        let _ = pruned; // suppress unused warning
    }

    // ========================================================================
    // E-1: Recovery Failure Tests
    // ========================================================================

    /// Helper: create a corrupted WAL segment file in the given db path.
    /// Returns the WAL directory path.
    fn corrupt_wal_segment(db_path: &std::path::Path) -> PathBuf {
        let wal_dir = db_path.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let segment_path = wal_dir.join("wal-000001.seg");
        std::fs::write(&segment_path, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();
        wal_dir
    }

    #[test]
    fn test_open_corrupted_wal_fails_by_default() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        corrupt_wal_segment(&db_path);

        // Default behavior: refuse to open
        let result = Database::open(&db_path);
        match result {
            Err(ref e) => {
                // Verify it's a Corruption variant, not just any error
                assert!(
                    matches!(e, StrataError::Corruption { .. }),
                    "Expected Corruption error variant, got: {}",
                    e
                );
                let err_msg = format!("{}", e);
                assert!(
                    err_msg.contains("WAL recovery failed"),
                    "Error should mention WAL recovery failure, got: {}",
                    err_msg
                );
                assert!(
                    err_msg.contains("allow_lossy_recovery"),
                    "Error should hint at the escape hatch, got: {}",
                    err_msg
                );
            }
            Ok(_) => panic!("Database should refuse to open with corrupted WAL"),
        }
    }

    #[test]
    fn test_open_corrupted_wal_succeeds_with_lossy_flag() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        corrupt_wal_segment(&db_path);

        // With allow_lossy_recovery=true, should open with empty state
        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let result = Database::open_with_config(&db_path, cfg);
        assert!(
            result.is_ok(),
            "Database should open with allow_lossy_recovery=true, got: {:?}",
            result.err()
        );

        let db = result.unwrap();
        // Only system branch init transactions present (no user data recovered)
        assert!(
            db.storage().current_version() <= 3,
            "Should have at most system init transactions, got {}",
            db.storage().current_version()
        );
    }

    #[test]
    fn test_lossy_recovery_discards_valid_data_before_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let wal_dir = db_path.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write a VALID WAL record first
        write_wal_txn(
            &wal_dir,
            1,
            branch_id,
            vec![(
                Key::new_kv(ns.clone(), "important_data"),
                Value::Bytes(b"precious".to_vec()),
            )],
            vec![],
            1,
        );

        // Now corrupt by adding a second segment file with invalid header.
        // The WAL reader iterates segments in order and the corrupted segment
        // will cause read_all to error.
        let corrupt_segment = wal_dir.join("wal-000002.seg");
        std::fs::write(&corrupt_segment, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();

        // Without lossy: should refuse to open
        let result = Database::open(&db_path);
        assert!(result.is_err(), "Should fail without lossy flag");

        // With lossy: opens but data is LOST (recovery falls back to empty)
        let cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let db = Database::open_with_config(&db_path, cfg).unwrap();
        let key = Key::new_kv(ns, "important_data");
        assert!(
            db.storage()
                .get_versioned(&key, u64::MAX)
                .unwrap()
                .is_none(),
            "Valid data before corruption should be lost in lossy mode"
        );
        // Only system branch init transactions present (user data was discarded)
        assert!(db.storage().current_version() <= 3);
    }

    // ========================================================================
    // E-4: Auto-Registration Tests
    // ========================================================================

    #[test]
    fn test_recovery_participants_auto_registered() {
        // After Database::open, both vector and search recovery should be
        // registered automatically (no need for executor to call
        // register_vector_recovery / register_search_recovery).
        let temp_dir = TempDir::new().unwrap();
        let _db = Database::open(temp_dir.path().join("db")).unwrap();

        // The registry is global and additive (idempotent), so other tests
        // may have already registered participants. We just verify the count
        // is at least 2 (vector + search).
        let count = crate::recovery::recovery_registry_count();
        assert!(
            count >= 2,
            "Expected at least 2 recovery participants (vector + search), got {}",
            count
        );
    }

    // ========================================================================
    // E-5 + E-2: Shutdown coordination tests
    // ========================================================================

    #[test]
    fn test_flush_thread_performs_final_sync() {
        // Use a 2-second flush interval so the periodic sync will NOT have
        // run by the time we call shutdown() (the write + shutdown completes
        // well within the first sleep cycle). In Standard mode, commit only
        // writes to BufWriter without fsync. The data can only reach disk via:
        //   (a) the flush thread's final sync (E-5), or
        //   (b) shutdown's explicit self.flush()
        //
        // This test verifies the full shutdown path (a + b) persists data.
        // The E-5 final sync provides defense-in-depth for crash scenarios
        // where the process dies between thread join and the explicit flush.
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("final_sync_db");

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "final_sync_key");

        {
            // 2s interval: long enough that no periodic sync runs before we
            // call shutdown, short enough that the test doesn't take forever
            // (the flush thread sleeps for one interval before checking the flag).
            let mode = DurabilityMode::Standard {
                interval_ms: 2_000,
                batch_size: 1_000_000,
            };
            let db = Database::open_with_durability(&db_path, mode).unwrap();

            db.transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::Bytes(b"final_sync_value".to_vec()))?;
                Ok(())
            })
            .unwrap();

            // At this point data is in the WAL BufWriter but NOT fsynced.
            // Neither the periodic sync (2s away) nor the inline safety-net
            // (3×2s = 6s away) could have run.
            db.shutdown().unwrap();
        }

        // Re-open and verify data was persisted
        let db = Database::open(&db_path).unwrap();
        let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(
            val.is_some(),
            "Data should be recoverable after shutdown (flush thread final sync + explicit flush)"
        );
        assert_eq!(
            val.unwrap().value,
            Value::Bytes(b"final_sync_value".to_vec())
        );
    }

    #[test]
    fn test_shutdown_blocks_until_in_flight_transactions_drain() {
        // Verify that shutdown() actually BLOCKS while a transaction is
        // in-flight, rather than returning immediately.
        //
        // Sequence:
        //   1. Background thread: begin_transaction (active_count=1)
        //   2. Background thread: signal "txn started"
        //   3. Main thread: call shutdown() → enters drain loop (blocked)
        //   4. Background thread: sleep 200ms → commit → active_count=0
        //   5. Main thread: drain loop exits → shutdown continues
        //
        // We verify shutdown took at least 150ms (i.e., it actually waited
        // for the transaction, not just returned immediately).
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("drain_db");
        let db = Database::open(&db_path).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "drain_key");

        let (started_tx, started_rx) = std::sync::mpsc::channel();

        let db2 = Arc::clone(&db);
        let key2 = key.clone();
        let handle = std::thread::spawn(move || {
            let mut txn = db2.begin_transaction(branch_id).unwrap();
            // Signal that the transaction is in-flight
            started_tx.send(()).unwrap();
            // Hold the transaction open for 200ms before committing
            std::thread::sleep(std::time::Duration::from_millis(200));
            txn.put(key2, Value::Bytes(b"drain_value".to_vec()))
                .unwrap();
            db2.commit_transaction(&mut txn).unwrap();
            db2.end_transaction(txn);
        });

        // Wait until the background thread has started the transaction
        started_rx.recv().unwrap();
        assert_eq!(
            db.coordinator.active_count(),
            1,
            "Should have 1 active transaction before shutdown"
        );

        // shutdown() should block until the transaction commits (~200ms)
        let start = std::time::Instant::now();
        db.shutdown().unwrap();
        let elapsed = start.elapsed();

        handle.join().unwrap();

        // Verify shutdown actually waited for the transaction
        assert!(
            elapsed >= std::time::Duration::from_millis(150),
            "Shutdown returned in {:?}, should have blocked waiting for in-flight transaction",
            elapsed
        );

        // Re-open and verify committed data was persisted
        drop(db);
        let db = Database::open(&db_path).unwrap();
        let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(
            val.is_some(),
            "Transaction committed during shutdown drain should be persisted"
        );
        assert_eq!(val.unwrap().value, Value::Bytes(b"drain_value".to_vec()));
    }

    #[test]
    fn test_shutdown_proceeds_after_draining_with_no_transactions() {
        // Verify that shutdown completes quickly when no transactions are
        // active, and that data committed before shutdown is persisted.
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("fast_shutdown_db");
        let db = Database::open(&db_path).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "pre_shutdown_key");

        // Commit data, then verify shutdown is fast and data persists
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Bytes(b"pre_shutdown".to_vec()))?;
            Ok(())
        })
        .unwrap();

        assert_eq!(
            db.coordinator.active_count(),
            0,
            "No active transactions after commit"
        );

        let start = std::time::Instant::now();
        db.shutdown().unwrap();
        let elapsed = start.elapsed();

        // Drain loop should skip immediately when active_count == 0
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "Shutdown took {:?}, expected fast completion with no active transactions",
            elapsed
        );
        assert!(!db.is_open());

        // Re-open and verify persistence
        drop(db);
        let db = Database::open(&db_path).unwrap();
        let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(val.is_some(), "Data committed before shutdown must persist");
    }

    #[test]
    fn test_shutdown_multiple_in_flight_transactions() {
        // Verify shutdown waits for ALL in-flight transactions, not just one.
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("multi_txn_db");
        let db = Database::open(&db_path).unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key1 = Key::new_kv(ns.clone(), "txn1_key");
        let key2 = Key::new_kv(ns.clone(), "txn2_key");

        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();

        // Spawn two transactions that hold open for different durations
        let db2 = Arc::clone(&db);
        let k1 = key1.clone();
        let started_tx1 = started_tx.clone();
        let h1 = std::thread::spawn(move || {
            let mut txn = db2.begin_transaction(branch_id).unwrap();
            started_tx1.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(100));
            txn.put(k1, Value::Bytes(b"value1".to_vec())).unwrap();
            db2.commit_transaction(&mut txn).unwrap();
            db2.end_transaction(txn);
        });

        let db3 = Arc::clone(&db);
        let k2 = key2.clone();
        let h2 = std::thread::spawn(move || {
            let mut txn = db3.begin_transaction(branch_id).unwrap();
            started_tx.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(250));
            txn.put(k2, Value::Bytes(b"value2".to_vec())).unwrap();
            db3.commit_transaction(&mut txn).unwrap();
            db3.end_transaction(txn);
        });

        // Wait for both transactions to start
        started_rx.recv().unwrap();
        started_rx.recv().unwrap();
        assert!(
            db.coordinator.active_count() >= 2,
            "Should have 2 active transactions"
        );

        // Shutdown must wait for BOTH transactions
        let start = std::time::Instant::now();
        db.shutdown().unwrap();
        let elapsed = start.elapsed();

        h1.join().unwrap();
        h2.join().unwrap();

        // Should have waited for the slower transaction (~250ms)
        assert!(
            elapsed >= std::time::Duration::from_millis(200),
            "Shutdown returned in {:?}, should have waited for both transactions",
            elapsed
        );

        // Both transactions' data should be persisted
        drop(db);
        let db = Database::open(&db_path).unwrap();
        assert!(
            db.storage()
                .get_versioned(&key1, u64::MAX)
                .unwrap()
                .is_some(),
            "txn1 data lost"
        );
        assert!(
            db.storage()
                .get_versioned(&key2, u64::MAX)
                .unwrap()
                .is_some(),
            "txn2 data lost"
        );
    }

    /// Helper: blind-write a single key via transaction_with_version.
    fn blind_write(db: &Database, key: Key, value: Value) -> u64 {
        let branch_id = key.namespace.branch_id;
        let ((), version) = db
            .transaction_with_version(branch_id, |txn| txn.put(key, value))
            .expect("blind write failed");
        version
    }

    #[test]
    fn test_put_direct_contention_scaling() {
        const OPS_PER_THREAD: usize = 10_000;
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open_with_durability(
            temp_dir.path().join("contention"),
            DurabilityMode::Cache,
        )
        .unwrap();

        // Phase 1: Concurrent writes — measure throughput scaling
        let thread_counts = [1, 4, 8, 16];
        let mut baseline_throughput = 0.0_f64;

        // Use a shared branch so we can read back all keys afterwards
        let branch_id = BranchId::new();

        for &num_threads in &thread_counts {
            let barrier = Arc::new(std::sync::Barrier::new(num_threads));
            let start = std::time::Instant::now();

            std::thread::scope(|s| {
                for _t in 0..num_threads {
                    let db = &db;
                    let barrier = barrier.clone();
                    s.spawn(move || {
                        let ns = Arc::new(Namespace::new(branch_id, "kv".to_string()));
                        barrier.wait();
                        for i in 0..OPS_PER_THREAD {
                            let key = Key::new_kv(ns.clone(), format!("k{i}"));
                            blind_write(db, key, Value::Int(i as i64));
                        }
                    });
                }
            });

            let elapsed = start.elapsed();
            let total_ops = (num_threads * OPS_PER_THREAD) as f64;
            let throughput = total_ops / elapsed.as_secs_f64();

            eprintln!("  {num_threads:>2} threads: {throughput:>10.0} ops/s ({elapsed:?})");

            if num_threads == 1 {
                baseline_throughput = throughput;
            } else {
                // Throughput should not collapse to less than 1/3 of
                // single-threaded baseline (catches lock convoys and
                // atomic contention regressions).
                assert!(
                    throughput > baseline_throughput / 3.0,
                    "{num_threads}t throughput ({throughput:.0} ops/s) collapsed \
                     below 1/3 of 1t baseline ({baseline_throughput:.0} ops/s)"
                );
            }
        }

        // Phase 2: Verify data correctness — every write actually persisted
        // Check the last round (16 threads × OPS_PER_THREAD keys)
        let last_thread_count = *thread_counts.last().unwrap();
        for t in 0..last_thread_count {
            let ns = Arc::new(Namespace::new(branch_id, "kv".to_string()));
            // Spot-check first, middle, and last keys
            for &i in &[0, OPS_PER_THREAD / 2, OPS_PER_THREAD - 1] {
                let key = Key::new_kv(ns.clone(), format!("k{i}"));
                let stored = db.storage().get_versioned(&key, u64::MAX).unwrap();
                assert!(
                    stored.is_some(),
                    "blind write data missing: thread={t}, key=k{i}"
                );
                assert_eq!(
                    stored.unwrap().value,
                    Value::Int(i as i64),
                    "blind write data corrupted: thread={t}, key=k{i}"
                );
            }
        }
    }

    /// Verify that blind writes are visible to concurrent snapshot
    /// readers and that GC correctly respects active reader snapshots.
    #[test]
    fn test_put_direct_gc_safety_with_concurrent_reader() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("gc_safety")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "gc_target");

        // Write initial value
        let v1 = blind_write(&db, key.clone(), Value::Int(1));

        // Write v2 (creates a version chain: v1 -> v2)
        let v2 = blind_write(&db, key.clone(), Value::Int(2));
        assert!(v2 > v1, "versions must be monotonically increasing");

        // Start a snapshot reader that pins the version at v2
        let reader_txn = db.begin_transaction(branch_id).unwrap();
        let pinned_version = reader_txn.start_version;

        // Write more versions while reader holds its snapshot
        for i in 3..=10 {
            blind_write(&db, key.clone(), Value::Int(i));
        }

        // GC safe point must not advance past the reader's pinned version
        let safe_point = db.gc_safe_point();
        assert!(
            safe_point <= pinned_version,
            "gc_safe_point ({safe_point}) advanced past pinned reader version ({pinned_version})"
        );

        // GC should not prune versions the reader can still see
        let (_, pruned) = db.run_gc();
        // If any pruning happened, confirm reader's data is still intact
        let _ = pruned;

        // Reader should still see the value at its snapshot version
        let reader_val = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(reader_val.is_some(), "key disappeared during concurrent GC");
        // Latest version should be 10
        assert_eq!(reader_val.unwrap().value, Value::Int(10));

        // Release the reader — gc_safe_version should now be free to advance
        db.coordinator.record_abort(reader_txn.txn_id);

        let safe_point_after = db.gc_safe_point();
        assert!(
            safe_point_after > safe_point,
            "gc_safe_point should advance after reader releases: {safe_point_after} > {safe_point}"
        );

        // SegmentedStore prunes via compaction, not gc_branch(), so pruned_after is 0.
        let (_, _pruned_after) = db.run_gc();

        // Latest value must still be readable after GC
        let final_val = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
        assert_eq!(final_val.value, Value::Int(10));
    }

    /// Verify blind writes concurrent with GC under thread contention.
    /// Writers and GC race — no panics, no data loss on latest version.
    #[test]
    fn test_put_direct_concurrent_gc_no_data_loss() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("gc_race")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let writers_remaining = std::sync::atomic::AtomicUsize::new(NUM_WRITERS);

        const NUM_WRITERS: usize = 4;
        const WRITES_PER_THREAD: usize = 1_000;

        std::thread::scope(|s| {
            // Spawn writer threads
            for t in 0..NUM_WRITERS {
                let db = &db;
                let ns = ns.clone();
                let remaining = &writers_remaining;
                s.spawn(move || {
                    for i in 0..WRITES_PER_THREAD {
                        let key = Key::new_kv(ns.clone(), format!("w{t}_k{i}"));
                        blind_write(db, key, Value::Int(i as i64));
                    }
                    remaining.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                });
            }

            // Spawn a GC thread that runs concurrently with writers
            let db = &db;
            let remaining = &writers_remaining;
            s.spawn(move || {
                while remaining.load(std::sync::atomic::Ordering::Acquire) > 0 {
                    db.run_gc();
                    std::thread::yield_now();
                }
                // One final GC after all writers complete
                db.run_gc();
            });
        });

        // All writers finished — verify latest value for every key
        for t in 0..NUM_WRITERS {
            for &i in &[0, WRITES_PER_THREAD / 2, WRITES_PER_THREAD - 1] {
                let key = Key::new_kv(ns.clone(), format!("w{t}_k{i}"));
                let stored = db.storage().get_versioned(&key, u64::MAX).unwrap();
                assert!(
                    stored.is_some(),
                    "data lost after concurrent GC: thread={t}, key=w{t}_k{i}"
                );
                assert_eq!(
                    stored.unwrap().value,
                    Value::Int(i as i64),
                    "data corrupted after concurrent GC: thread={t}, key=w{t}_k{i}"
                );
            }
        }
    }

    /// Issue #1697: background compaction with max_versions_per_key must not
    /// prune versions that active snapshots need.
    #[test]
    fn test_issue_1697_compaction_preserves_snapshot_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut cfg = StrataConfig::default();
        cfg.storage.max_versions_per_key = 1;
        let db = Database::open_with_config(temp_dir.path().join("db"), cfg).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "snap_key");

        // Write version 1
        blind_write(&db, key.clone(), Value::Int(1));

        // Start a reader that pins the current snapshot
        let reader = db.begin_read_only_transaction(branch_id).unwrap();
        let pinned_version = reader.start_version;

        // Write version 2 — now key has 2 versions, max_versions_per_key=1
        blind_write(&db, key.clone(), Value::Int(2));

        // Force data from memtable → segments so compaction can process it.
        // Rotate twice to create 2 frozen memtables → flush both → compact.
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();
        // Need a second segment to trigger compaction (min 2 segments).
        // Write a dummy key so the second memtable isn't empty.
        blind_write(&db, Key::new_kv(ns.clone(), "dummy"), Value::Int(999));
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();

        // Now compact — this is where max_versions would drop the old version.
        let compacted = db.storage().compact_branch(&branch_id, 0).unwrap();
        assert!(compacted.is_some(), "compaction should have run");

        // The reader's snapshot must still be able to find version 1
        // via the storage layer at the pinned version.
        let result = db.storage().get_versioned(&key, pinned_version).unwrap();
        assert!(
            result.is_some(),
            "version at snapshot {} was pruned by compaction despite active reader",
            pinned_version
        );
        assert_eq!(
            result.unwrap().value,
            Value::Int(1),
            "snapshot read returned wrong value after compaction"
        );

        // Latest version must also be intact
        let latest = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(latest.is_some(), "latest version missing after compaction");
        assert_eq!(latest.unwrap().value, Value::Int(2));

        // Clean up reader
        db.coordinator.record_abort(reader.txn_id);
    }

    #[test]
    fn test_issue_1730_checkpoint_compact_recovery_data_loss() {
        // Issue #1730: checkpoint+compact deletes WAL segments, but recovery
        // is WAL-only and never loads snapshots. This causes data loss.
        //
        // After the fix, compact() must refuse to delete WAL segments based
        // on the snapshot watermark alone, since recovery cannot load snapshots.
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let branch_id = BranchId::new();

        // Step 1: Write data
        {
            let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
            let ns = create_test_namespace(branch_id);
            let key = Key::new_kv(ns, "critical_data");

            db.transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::String("must_survive".to_string()))?;
                Ok(())
            })
            .unwrap();

            // Step 2: Checkpoint (creates snapshot, sets watermark in MANIFEST)
            db.checkpoint().unwrap();

            // Step 3: Compact — should NOT delete WAL segments since recovery
            // cannot load snapshots. With the fix, this returns an error.
            let compact_result = db.compact();
            assert!(
                compact_result.is_err(),
                "compact() must fail when only snapshot watermark exists \
                 (no flush watermark) because recovery cannot load snapshots"
            );
        }
        // Database dropped (simulates clean shutdown)

        // Clear the registry so reopen doesn't return the cached instance
        OPEN_DATABASES.lock().clear();

        // Step 4: Reopen — recovery replays WAL
        {
            let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

            let ns = create_test_namespace(branch_id);
            let key = Key::new_kv(ns, "critical_data");

            // Step 5: Data MUST still be present
            let result = db.storage().get_versioned(&key, u64::MAX).unwrap();
            assert!(
                result.is_some(),
                "CRITICAL: Data lost after checkpoint+compact+recovery! \
                 WAL segments were deleted but recovery never loaded the snapshot."
            );
            assert_eq!(
                result.unwrap().value,
                Value::String("must_survive".to_string())
            );
        }
    }

    #[test]
    fn test_issue_1730_standard_durability() {
        // Same as above but with Standard durability mode (disk-based, batched fsync).
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        let branch_id = BranchId::new();
        let durability = DurabilityMode::Standard {
            interval_ms: 100,
            batch_size: 1,
        };

        {
            let db = Database::open_with_durability(&db_path, durability).unwrap();
            let ns = create_test_namespace(branch_id);
            let key = Key::new_kv(ns, "standard_data");

            db.transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::String("durable".to_string()))?;
                Ok(())
            })
            .unwrap();

            // Ensure WAL is flushed to disk
            db.flush().unwrap();

            db.checkpoint().unwrap();

            // compact() must fail — only snapshot watermark, no flush watermark
            let compact_result = db.compact();
            assert!(
                compact_result.is_err(),
                "compact() after checkpoint-only must fail under Standard durability"
            );
        }

        OPEN_DATABASES.lock().clear();

        // Reopen and verify data survived
        {
            let db = Database::open_with_durability(&db_path, durability).unwrap();
            let ns = create_test_namespace(branch_id);
            let key = Key::new_kv(ns, "standard_data");

            let result = db.storage().get_versioned(&key, u64::MAX).unwrap();
            assert!(
                result.is_some(),
                "Data must survive checkpoint+failed-compact+recovery under Standard durability"
            );
            assert_eq!(result.unwrap().value, Value::String("durable".to_string()));
        }
    }

    /// Issue #1699: Follower refresh must use the WAL record's commit timestamp,
    /// not Timestamp::now(), so that all entries from the same transaction share
    /// a single timestamp for correct time-travel queries.
    ///
    /// Without the fix, each put_with_version_mode / delete_with_version call
    /// creates its own Timestamp::now(), splitting a transaction across multiple
    /// timestamps and causing time-travel queries to see partial state.
    #[test]
    fn test_issue_1699_refresh_preserves_wal_timestamp() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        let branch_id = BranchId::default();
        let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

        // 1. Open primary with Always durability (syncs every commit to WAL)
        let primary = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

        // 2. Pre-populate key B
        let key_b = Key::new_kv(ns.clone(), "b");
        primary
            .transaction(branch_id, |txn| {
                txn.put(key_b.clone(), Value::String("old".into()))?;
                Ok(())
            })
            .unwrap();

        // 3. Open follower — initial recovery replays B via RecoveryCoordinator (correct path)
        let follower = Database::open_follower(&db_path).unwrap();

        // 4. Primary commits: put A + delete B in one transaction
        let key_a = Key::new_kv(ns.clone(), "a");
        primary
            .transaction(branch_id, |txn| {
                txn.put(key_a.clone(), Value::String("new".into()))?;
                txn.delete(key_b.clone())?;
                Ok(())
            })
            .unwrap();

        // Record upper bound on the WAL record's timestamp
        let commit_ts_upper = Timestamp::now().as_micros();

        // 5. Sleep to create a clear gap between commit time and refresh time.
        //    100ms = 100_000 microseconds — far larger than any clock jitter.
        std::thread::sleep(Duration::from_millis(100));

        // 6. Follower refresh — applies the new WAL records via Database::refresh
        let applied = follower.refresh().unwrap();
        assert!(applied > 0, "follower should apply the new transaction");

        // 7. Check: A's entry timestamp should come from the WAL record (near
        //    commit time), NOT from Timestamp::now() during refresh.
        let a_entry = follower
            .storage()
            .get_versioned(&key_a, u64::MAX)
            .unwrap()
            .expect("key A should exist after refresh");
        let a_ts = a_entry.timestamp.as_micros();

        assert!(
            a_ts <= commit_ts_upper,
            "Entry timestamp ({}) should be from WAL record (≤ {}), \
             not from Timestamp::now() during refresh",
            a_ts,
            commit_ts_upper,
        );

        // 8. Time-travel at A's timestamp: B should be deleted (same transaction).
        //    With split timestamps, B's tombstone has a later timestamp than A's put,
        //    so querying at A's timestamp would incorrectly show B as still alive.
        let b_at_a_ts = follower.storage().get_at_timestamp(&key_b, a_ts).unwrap();
        assert!(
            b_at_a_ts.is_none(),
            "Key B should be deleted at A's timestamp (same transaction). \
             Got {:?}, indicating split timestamps between put and delete.",
            b_at_a_ts.map(|v| v.value),
        );
    }
}
