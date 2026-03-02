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
mod transactions;

pub use config::{
    ModelConfig, StorageConfig, StrataConfig, SHADOW_EVENT, SHADOW_JSON, SHADOW_KV, SHADOW_STATE,
};
pub use registry::OPEN_DATABASES;
pub use transactions::RetryConfig;

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
use strata_storage::ShardedStore;
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

    /// Sharded storage with O(1) lazy snapshots (thread-safe)
    storage: Arc<ShardedStore>,

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
    scheduler: BackgroundScheduler,

    /// Exclusive lock file preventing concurrent process access to the same database.
    ///
    /// Held for the lifetime of the Database. Dropped automatically when the
    /// Database is dropped, releasing the lock. None for ephemeral databases.
    _lock_file: Option<std::fs::File>,

    /// WAL directory path (for multi-process refresh).
    wal_dir: PathBuf,

    /// Max txn_id applied to local storage from WAL (multi-process watermark).
    wal_watermark: AtomicU64,

    /// Whether this database uses multi-process coordination.
    multi_process: bool,
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

        let db = Self::open_with_mode_and_config(path, mode, cfg)?;
        Ok(db)
    }

    /// Open database with specific durability mode (convenience wrapper).
    ///
    /// Uses a default `StrataConfig` with the given durability mode.
    #[allow(dead_code)]
    pub(crate) fn open_with_mode<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
    ) -> StrataResult<Arc<Self>> {
        let dur_str = match durability_mode {
            DurabilityMode::Always => "always",
            _ => "standard",
        };
        let cfg = StrataConfig {
            durability: dur_str.to_string(),
            ..StrataConfig::default()
        };
        Self::open_with_mode_and_config(path, durability_mode, cfg)
    }

    /// Open database with specific durability mode and full config.
    ///
    /// # Thread Safety
    ///
    /// Uses a global registry to ensure the same path returns the same instance.
    /// If a database at this path is already open, returns the existing Arc.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path for the database
    /// * `durability_mode` - Durability mode for WAL operations
    /// * `cfg` - Full configuration to store in the Database
    ///
    /// # Returns
    ///
    /// * `Ok(Arc<Database>)` - Ready-to-use database instance
    /// * `Err` - If directory creation, WAL opening, or recovery fails
    ///
    /// # Recovery
    ///
    /// Per spec Section 5: Uses RecoveryCoordinator to replay WAL and
    /// initialize TransactionManager with the recovered version.
    pub(crate) fn open_with_mode_and_config<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        Self::open_internal(path, durability_mode, cfg, false)
    }

    /// Open database in multi-process mode with specific durability and config.
    ///
    /// When multi-process mode is enabled, the database uses a shared file lock
    /// instead of an exclusive lock, allowing multiple processes to access the
    /// same database directory concurrently. Commits are coordinated through
    /// the WAL file lock.
    pub fn open_multi_process<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
    ) -> StrataResult<Arc<Self>> {
        Self::open_internal(path, durability_mode, cfg, true)
    }

    fn open_internal<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        multi_process: bool,
    ) -> StrataResult<Arc<Self>> {
        // Create directory first so we can canonicalize the path
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StrataError::from)?;

        // Canonicalize path for consistent registry keys
        let canonical_path = data_dir.canonicalize().map_err(StrataError::from)?;

        if !multi_process {
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
                multi_process,
                Some(lock_file),
            )?;

            registry.insert(canonical_path, Arc::downgrade(&db));
            drop(registry);

            crate::recovery::recover_all_participants(&db)?;
            let index = db.extension::<crate::search::InvertedIndex>()?;
            if !index.is_enabled() {
                index.enable();
            }

            return Ok(db);
        }

        // Multi-process mode: shared lock, skip registry singleton
        let lock_path = canonical_path.join(".lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| StrataError::storage(format!("failed to open lock file: {}", e)))?;
        fs2::FileExt::lock_shared(&lock_file)
            .map_err(|e| StrataError::storage(format!("failed to acquire shared lock: {}", e)))?;

        let db = Self::open_finish(
            canonical_path,
            durability_mode,
            cfg,
            multi_process,
            Some(lock_file),
        )?;

        crate::recovery::recover_all_participants(&db)?;
        let index = db.extension::<crate::search::InvertedIndex>()?;
        if !index.is_enabled() {
            index.enable();
        }

        Ok(db)
    }

    /// Shared tail of database open: recovery, WAL writer, coordinator, flush thread.
    fn open_finish(
        canonical_path: PathBuf,
        durability_mode: DurabilityMode,
        cfg: StrataConfig,
        multi_process: bool,
        lock_file: Option<std::fs::File>,
    ) -> StrataResult<Arc<Self>> {
        // Create WAL directory
        let wal_dir = canonical_path.join("wal");
        std::fs::create_dir_all(&wal_dir).map_err(StrataError::from)?;

        // Use RecoveryCoordinator for proper transaction-aware recovery
        // This reads all WalRecords from the segmented WAL directory
        let recovery = RecoveryCoordinator::new(wal_dir.clone());
        let result = match recovery.recover() {
            Ok(result) => result,
            Err(e) => {
                warn!(
                    target: "strata::db",
                    error = %e,
                    "Recovery failed — starting with empty state. Data from WAL may be lost."
                );
                strata_concurrency::RecoveryResult::empty()
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

        // In multi-process mode, seed the counter file from recovery results.
        // This ensures that if the counter file is missing (first multi-process open)
        // or stale (crash between WAL write and counter update), the first coordinated
        // commit will see correct starting values.
        if multi_process {
            use strata_durability::coordination::CounterFile;
            let counter_file = CounterFile::new(&wal_dir);
            let (cf_version, cf_txn_id) = counter_file
                .read_or_default()
                .map_err(|e| StrataError::storage(format!("Failed to read counter file: {}", e)))?;

            // Only update if recovery found higher values (never go backward)
            let recovered_version = result.stats.final_version;
            let recovered_txn_id = result.stats.max_txn_id;
            if recovered_version > cf_version || recovered_txn_id > cf_txn_id {
                counter_file
                    .write(
                        recovered_version.max(cf_version),
                        recovered_txn_id.max(cf_txn_id),
                    )
                    .map_err(|e| {
                        StrataError::storage(format!("Failed to seed counter file: {}", e))
                    })?;
            }
        }

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

        // Apply storage resource limits from config
        let storage = Arc::new(result.storage);
        storage.set_max_branches(cfg.storage.max_branches);

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
            scheduler: BackgroundScheduler::new(2, 4096),
            _lock_file: lock_file,
            wal_dir,
            wal_watermark,
            multi_process,
        });

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
        let storage = ShardedStore::new();
        storage.set_max_branches(cfg.storage.max_branches);

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
            scheduler: BackgroundScheduler::new(2, 4096),
            _lock_file: None, // No lock for ephemeral databases
            wal_dir: PathBuf::new(),
            wal_watermark: AtomicU64::new(0),
            multi_process: false,
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

    // ========================================================================
    // Accessors
    // ========================================================================

    /// Get reference to the storage layer (internal use only)
    ///
    /// This is for internal engine use. External users should use
    /// primitives (KVStore, EventLog, etc.) which go through transactions.
    pub(crate) fn storage(&self) -> &Arc<ShardedStore> {
        &self.storage
    }

    /// Direct single-key read returning only the Value (no VersionedValue).
    ///
    /// Skips Version enum and VersionedValue construction. Used by the
    /// KVStore::get() hot path where version metadata is not needed.
    pub(crate) fn get_value_direct(&self, key: &Key) -> Option<strata_core::value::Value> {
        self.storage.get_value_direct(key)
    }

    /// Direct single-key blind write bypassing the full transaction machinery.
    ///
    /// Allocates a version, optionally writes to WAL, and applies directly
    /// to storage. No snapshot, no validation, no pool acquire/release.
    ///
    /// Safe for single-key puts where snapshot isolation isn't needed
    /// (blind writes with no read-before-write).
    ///
    /// Falls back to the full transaction path in multi-process mode,
    /// which requires coordinated version allocation via the WAL file lock.
    pub(crate) fn put_direct(
        &self,
        key: Key,
        value: strata_core::value::Value,
    ) -> StrataResult<u64> {
        self.check_accepting()?;

        // Multi-process mode requires coordinated version allocation;
        // fall back to the full transaction path.
        if self.multi_process {
            use strata_core::value::Value;
            let branch_id = key.namespace.branch_id;
            let val: Value = value;
            let ((), version) =
                self.transaction_with_version(branch_id, |txn| txn.put(key, val))?;
            return Ok(version);
        }

        let txn_id = self.coordinator.next_txn_id()?;
        self.coordinator.record_start(txn_id, 0);

        let commit_version = match self.coordinator.allocate_commit_version() {
            Ok(v) => v,
            Err(e) => {
                self.coordinator.record_abort(txn_id);
                return Err(e);
            }
        };

        // WAL write (if needed for durability)
        if self.durability_mode.requires_wal() {
            match self.wal_writer.as_ref() {
                Some(wal_arc) => {
                    use strata_concurrency::TransactionPayload;
                    use strata_durability::format::WalRecord;
                    use strata_durability::now_micros;

                    let timestamp = now_micros();
                    let payload = TransactionPayload {
                        version: commit_version,
                        puts: vec![(key.clone(), value.clone())],
                        deletes: vec![],
                    };
                    let record = WalRecord::new(
                        txn_id,
                        *key.namespace.branch_id.as_bytes(),
                        timestamp,
                        payload.to_bytes(),
                    );

                    // Pre-serialize outside the lock: moves CRC computation
                    // and two Vec allocations out of the critical section.
                    let record_bytes = record.to_bytes();

                    let mut wal = wal_arc.lock();
                    if let Err(e) = wal.append_pre_serialized(&record_bytes, txn_id, timestamp) {
                        self.coordinator.record_abort(txn_id);
                        return Err(StrataError::storage(format!(
                            "WAL write failed in put_direct: {}",
                            e
                        )));
                    }
                }
                None => {
                    self.coordinator.record_abort(txn_id);
                    return Err(StrataError::storage(
                        "WAL required by durability mode but writer is unavailable".to_string(),
                    ));
                }
            }
        }

        // Apply directly to storage
        use strata_core::traits::Storage;
        if let Err(e) = self
            .storage
            .put_with_version(key, value, commit_version, None)
        {
            self.coordinator.record_abort(txn_id);
            return Err(e);
        }

        self.coordinator.record_commit(txn_id);
        Ok(commit_version)
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

    /// Get current WAL counters snapshot.
    ///
    /// Returns `None` for ephemeral databases (no WAL).
    /// Briefly locks the WAL mutex to read counter values.
    pub fn durability_counters(&self) -> Option<strata_durability::WalCounters> {
        self.wal_writer.as_ref().map(|w| w.lock().counters())
    }

    /// Check if the database is currently open and accepting transactions
    pub fn is_open(&self) -> bool {
        self.accepting_transactions.load(Ordering::SeqCst)
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
        self.storage.gc_branch(branch_id, min_version)
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
            None => version_bound,
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
            total_pruned += self.storage.gc_branch(branch_id, safe_point);
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
        let expired = self.storage.expire_ttl_keys(strata_core::Timestamp::now());
        (safe_point, pruned, expired)
    }

    /// Remove the per-branch commit lock after a branch is deleted.
    ///
    /// This prevents unbounded growth of the commit_locks map in the
    /// TransactionManager when branches are repeatedly created and deleted.
    ///
    /// Should be called after `BranchIndex::delete_branch()` succeeds.
    pub fn remove_branch_lock(&self, branch_id: &BranchId) {
        self.coordinator.remove_branch_lock(branch_id);
    }

    // ========================================================================
    // Multi-Process Refresh
    // ========================================================================

    /// Refresh local storage by tailing new WAL entries from other processes.
    ///
    /// In multi-process mode, other processes may have appended new WAL records
    /// since this instance last read the WAL. `refresh()` reads any new records
    /// and applies them to local in-memory storage, bringing this instance
    /// up-to-date with all committed writes.
    ///
    /// Returns the number of new records applied.
    ///
    /// For non-multi-process databases, this is a no-op returning 0.
    pub fn refresh(&self) -> StrataResult<usize> {
        if !self.multi_process || self.persistence_mode == PersistenceMode::Ephemeral {
            return Ok(0);
        }

        let watermark = self.wal_watermark.load(std::sync::atomic::Ordering::SeqCst);

        // Read all WAL records after our watermark
        let reader = strata_durability::wal::WalReader::new(Box::new(
            strata_durability::codec::IdentityCodec,
        ));
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
                        if let Ok(Some(vv)) = Storage::get(self.storage.as_ref(), key) {
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

            // Apply puts
            for (key, value) in &payload.puts {
                use strata_core::traits::Storage;
                Storage::put_with_version(
                    self.storage.as_ref(),
                    key.clone(),
                    value.clone(),
                    payload.version,
                    None,
                )?;
            }

            // Apply deletes
            for key in &payload.deletes {
                use strata_core::traits::Storage;
                Storage::delete_with_version(self.storage.as_ref(), key, payload.version)?;
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

                    let mut backends = vs.backends.write();
                    if let Some(backend) = backends.get_mut(&cid) {
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

                    let mut backends = vs.backends.write();
                    if let Some(backend) = backends.get_mut(&cid) {
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
        if self.persistence_mode == PersistenceMode::Ephemeral {
            return Ok(());
        }

        // Flush WAL first to ensure all buffered writes are on disk
        self.flush()?;

        let watermark_txn = self.coordinator.current_version();

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
        if self.persistence_mode == PersistenceMode::Ephemeral {
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
        if !self.accepting_transactions.load(Ordering::SeqCst) {
            return Err(StrataError::invalid_input(
                "Database is shutting down".to_string(),
            ));
        }
        Ok(())
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
                let commit_version = self.commit_internal(txn, durability)?;
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

    /// Execute a transaction with automatic retry on conflict
    ///
    /// Per spec Section 4.3: Implicit transactions include automatic retry on conflict.
    /// This method provides explicit retry control for transactions that may conflict.
    ///
    /// The closure is called repeatedly until either:
    /// - The transaction commits successfully
    /// - A non-conflict error occurs (not retried)
    /// - Maximum retries are exceeded
    ///
    /// # Arguments
    /// * `branch_id` - BranchId for namespace isolation
    /// * `config` - Retry configuration (max retries, delays)
    /// * `f` - Closure that performs transaction operations (must be `Fn`, not `FnOnce`)
    ///
    /// # Returns
    /// * `Ok(T)` - Closure return value on successful commit
    /// * `Err` - On non-conflict error or max retries exceeded
    ///
    /// # Example
    /// ```text
    /// let config = RetryConfig::default();
    /// let result = db.transaction_with_retry(branch_id, config, |txn| {
    ///     let val = txn.get(&key)?;
    ///     txn.put(key.clone(), Value::Int(val.value + 1))?;
    ///     Ok(())
    /// })?;
    /// ```
    pub(crate) fn transaction_with_retry<F, T>(
        &self,
        branch_id: BranchId,
        config: RetryConfig,
        f: F,
    ) -> StrataResult<T>
    where
        F: Fn(&mut TransactionContext) -> StrataResult<T>,
    {
        self.check_accepting()?;

        let mut last_error = None;

        for attempt in 0..=config.max_retries {
            let mut txn = self.begin_transaction(branch_id)?;
            let result = f(&mut txn);
            let outcome = self.run_single_attempt(&mut txn, result, self.durability_mode);
            self.end_transaction(txn);

            match outcome {
                Ok((value, _)) => return Ok(value),
                Err(e) if e.is_conflict() && attempt < config.max_retries => {
                    last_error = Some(e);
                    std::thread::sleep(config.calculate_delay(attempt));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        // Unreachable: the loop always returns — either Ok on success,
        // Err on non-conflict or final attempt. This is a defensive fallback.
        Err(last_error.unwrap_or_else(|| {
            StrataError::internal("retry loop exited without returning a result".to_string())
        }))
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
        let snapshot = self.storage.create_snapshot();
        let snapshot_version = snapshot.version();
        self.coordinator.record_start(txn_id, snapshot_version);

        let mut txn = TransactionPool::acquire(txn_id, branch_id, Some(Box::new(snapshot)));
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
        self.commit_internal(txn, self.durability_mode)
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
        if self.multi_process {
            return self.commit_coordinated(txn, durability);
        }

        let needs_wal =
            durability.requires_wal() && (!txn.is_read_only() || !txn.json_writes().is_empty());

        let mut wal_guard = if needs_wal {
            self.wal_writer.as_ref().map(|w| w.lock())
        } else {
            None
        };
        let wal_ref = wal_guard.as_deref_mut();

        self.coordinator.commit(txn, self.storage.as_ref(), wal_ref)
    }

    /// Coordinated commit for multi-process mode.
    ///
    /// Critical section (under WAL file lock):
    /// 1. Refresh — tail new WAL entries into local storage
    /// 2. Read counters — get (max_version, max_txn_id)
    /// 3. Allocate — new_version = max_version + 1, new_txn_id = max_txn_id + 1
    /// 4. Validate against refreshed storage (OCC re-validation)
    /// 5. WAL append + flush
    /// 6. Update counters
    /// 7. Drop WAL file lock
    /// 8. Apply to local storage + update watermark
    fn commit_coordinated(
        &self,
        txn: &mut TransactionContext,
        durability: DurabilityMode,
    ) -> StrataResult<u64> {
        use strata_durability::coordination::{CounterFile, WalFileLock};

        // Read-only transactions skip coordination entirely
        if txn.is_read_only() && txn.json_writes().is_empty() {
            return self.coordinator.commit(txn, self.storage.as_ref(), None);
        }

        // Acquire WAL file lock with stale-lock recovery (blocks until available)
        let _wal_lock = WalFileLock::acquire_with_stale_check(&self.wal_dir)
            .map_err(|e| StrataError::storage(format!("Failed to acquire WAL file lock: {}", e)))?;

        // Step 1: Refresh from WAL (apply other processes' writes)
        self.refresh()?;

        // Step 2: Read counters — take max of counter file and local state.
        //
        // The counter file may be stale if:
        // - This is the first multi-process open (counter file doesn't exist yet)
        // - A previous process crashed after WAL write but before counter update
        // - The database was converted from single-process to multi-process mode
        //
        // The local coordinator already reflects WAL reality (from recovery + refresh),
        // so we take the max to ensure we never allocate a duplicate version/txn_id.
        let counter_file = CounterFile::new(&self.wal_dir);
        let (cf_version, cf_txn_id) = counter_file
            .read_or_default()
            .map_err(|e| StrataError::storage(format!("Failed to read counter file: {}", e)))?;

        let local_version = self.coordinator.current_version();
        let local_max_txn_id = self.wal_watermark.load(std::sync::atomic::Ordering::SeqCst);

        let max_version = cf_version.max(local_version);
        let max_txn_id = cf_txn_id.max(local_max_txn_id);

        // Step 3: Allocate new version and txn_id
        let new_version = max_version + 1;
        let new_txn_id = max_txn_id + 1;

        // Override the transaction's txn_id with the globally-unique one
        txn.txn_id = new_txn_id;

        // Step 4: Validate + WAL append using commit_with_version
        let needs_wal =
            durability.requires_wal() && (!txn.is_read_only() || !txn.json_writes().is_empty());

        let mut wal_guard = if needs_wal {
            self.wal_writer.as_ref().map(|w| w.lock())
        } else {
            None
        };

        // If we have a WAL writer, ensure it sees the latest segment
        if let Some(ref mut wal) = wal_guard {
            wal.reopen_if_needed()
                .map_err(|e| StrataError::storage(format!("WAL reopen failed: {}", e)))?;
        }

        let wal_ref = wal_guard.as_deref_mut();

        // Use append_and_flush via commit_with_version to ensure immediate visibility
        let commit_version = self.coordinator.commit_with_version(
            txn,
            self.storage.as_ref(),
            wal_ref,
            new_version,
        )?;

        // Flush WAL for cross-process visibility
        if let Some(ref mut wal) = wal_guard {
            wal.flush().map_err(StrataError::from)?;
        }

        // Step 5: Update counter file
        counter_file
            .write(new_version, new_txn_id)
            .map_err(|e| StrataError::storage(format!("Failed to update counter file: {}", e)))?;

        // Step 6: Update local watermark
        self.wal_watermark
            .store(new_txn_id, std::sync::atomic::Ordering::SeqCst);

        // WAL file lock is dropped here (end of scope)
        Ok(commit_version)
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
    fn freeze_vector_heaps(&self) {
        use crate::primitives::vector::VectorBackendState;

        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return; // Ephemeral database — no mmap
        }

        let state = match self.extension::<VectorBackendState>() {
            Ok(s) => s,
            Err(_) => return, // No vector state registered
        };

        let backends = state.backends.read();
        for (cid, backend) in backends.iter() {
            let branch_hex = format!("{:032x}", u128::from_be_bytes(*cid.branch_id.as_bytes()));
            let vec_path = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}.vec", cid.name));
            if let Err(e) = backend.freeze_heap_to_disk(&vec_path) {
                tracing::warn!(
                    target: "strata::vector",
                    collection = %cid.name,
                    error = %e,
                    "Failed to freeze vector heap at shutdown"
                );
            }

            // Also freeze graphs
            let gdir = data_dir
                .join("vectors")
                .join(&branch_hex)
                .join(format!("{}_graphs", cid.name));
            if let Err(e) = backend.freeze_graphs_to_disk(&gdir) {
                tracing::warn!(
                    target: "strata::vector",
                    collection = %cid.name,
                    error = %e,
                    "Failed to freeze vector graphs at shutdown"
                );
            }
        }
    }

    /// Freeze the search index to disk for fast recovery on next open.
    fn freeze_search_index(&self) {
        let data_dir = self.data_dir();
        if data_dir.as_os_str().is_empty() {
            return; // Ephemeral database — no persistence
        }

        if let Ok(index) = self.extension::<crate::search::InvertedIndex>() {
            if let Err(e) = index.freeze_to_disk() {
                tracing::warn!(
                    target: "strata::search",
                    error = %e,
                    "Failed to freeze search index at shutdown"
                );
            }
        }
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
        // Stop accepting new transactions
        self.accepting_transactions.store(false, Ordering::SeqCst);

        // Drain background tasks (embeddings etc.) before final WAL flush
        self.scheduler.drain();

        // Signal the background flush thread to stop
        self.flush_shutdown.store(true, Ordering::SeqCst);

        // Join the flush thread so it releases the WAL lock
        if let Some(handle) = self.flush_handle.lock().take() {
            let _ = handle.join();
        }

        // Wait for in-flight transactions to complete
        // This ensures all transactions that started before shutdown
        // have a chance to commit before we flush the WAL.
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        while self.coordinator.active_count() > 0 && start.elapsed() < timeout {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Final flush to ensure all data is persisted
        self.flush()?;

        // Freeze vector heaps to mmap so lite KV records can recover
        self.freeze_vector_heaps();

        // Freeze search index to disk for fast recovery
        self.freeze_search_index();

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

        // Final flush to persist any remaining data
        let _ = self.flush();

        // Freeze vector heaps to mmap so lite KV records can recover
        self.freeze_vector_heaps();

        // Freeze search index to disk for fast recovery
        self.freeze_search_index();

        // Remove from registry if we're disk-backed (skip in multi-process mode:
        // multi-process instances are not registered in the singleton registry).
        if self.persistence_mode == PersistenceMode::Disk
            && !self.data_dir.as_os_str().is_empty()
            && !self.multi_process
        {
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
    use strata_concurrency::TransactionPayload;
    use strata_core::types::{Key, Namespace};
    use strata_core::value::Value;
    use strata_core::Storage;
    use strata_durability::format::WalRecord;
    use strata_durability::now_micros;
    use tempfile::TempDir;

    /// Helper: write a committed transaction to the segmented WAL
    fn write_wal_txn(
        wal_dir: &std::path::Path,
        txn_id: u64,
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
        let record = WalRecord::new(
            txn_id,
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
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

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
        let val = db.storage().get(&key1).unwrap().unwrap();

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
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

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
            let val = db.storage().get(&key).unwrap().unwrap();

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
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

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
        let val = db.storage().get(&key).unwrap().unwrap();
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
        // Storage should be empty since no valid segments exist
        assert_eq!(db.storage().current_version(), 0);
    }

    #[test]
    fn test_open_with_different_durability_modes() {
        let temp_dir = TempDir::new().unwrap();

        // Always mode
        {
            let db =
                Database::open_with_mode(temp_dir.path().join("strict"), DurabilityMode::Always)
                    .unwrap();
            assert!(!db.is_cache());
        }

        // Standard mode
        {
            let db = Database::open_with_mode(
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
            let db = Database::open_with_mode(temp_dir.path().join("none"), DurabilityMode::Cache)
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
        Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ))
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
        let stored = db.storage().get(&key).unwrap().unwrap();
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
        assert!(db.storage().get(&key).unwrap().is_none());
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
        let stored = db.storage().get(&key).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(123));
    }

    // ========================================================================
    // Retry Tests
    // ========================================================================

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay_ms, 10);
        assert_eq!(config.max_delay_ms, 100);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_retries(5)
            .with_base_delay_ms(20)
            .with_max_delay_ms(200);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.base_delay_ms, 20);
        assert_eq!(config.max_delay_ms, 200);
    }

    #[test]
    fn test_retry_config_no_retry() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_retry_config_delay_calculation() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay_ms: 10,
            max_delay_ms: 100,
        };

        // Exponential backoff: 10, 20, 40, 80, 100 (capped)
        assert_eq!(config.calculate_delay(0).as_millis(), 10);
        assert_eq!(config.calculate_delay(1).as_millis(), 20);
        assert_eq!(config.calculate_delay(2).as_millis(), 40);
        assert_eq!(config.calculate_delay(3).as_millis(), 80);
        assert_eq!(config.calculate_delay(4).as_millis(), 100); // Capped at max
        assert_eq!(config.calculate_delay(5).as_millis(), 100); // Still capped
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
    fn test_checkpoint_then_compact() {
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

        // Checkpoint first
        assert!(db.checkpoint().is_ok());

        // Now compact should succeed
        assert!(db.compact().is_ok());
    }

    #[test]
    fn test_gc_safe_point_no_active_txns() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("db")).unwrap();
        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);

        // Fresh DB: version starts at 0, gc_safe_point should be 0 (current <= 1)
        assert_eq!(db.gc_safe_point(), 0);

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
            let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
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
            let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
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

        // Run GC — should prune old versions
        let (safe_point, pruned) = db.run_gc();
        assert!(safe_point > 0, "safe point should be non-zero");
        assert!(pruned > 0, "should have pruned at least 1 old version");

        // Latest value should still be readable
        let stored = db.storage().get(&key).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(3));
    }

    #[test]
    fn test_run_gc_no_prune_at_version_zero() {
        let db = Database::cache().unwrap();

        // Version is 1, gc_safe_point returns 0
        let (safe_point, pruned) = db.run_gc();
        assert_eq!(safe_point, 0);
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
        let stored = db.storage().get(&key).unwrap().unwrap();
        assert_eq!(stored.value, Value::Int(2));

        let _ = pruned; // suppress unused warning
    }
}
