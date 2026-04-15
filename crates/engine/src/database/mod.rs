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

pub mod branch_mutation;
pub mod branch_service;
pub mod compat;
pub mod config;
pub mod dag_hook;
pub mod merge_registry;
pub mod observers;
pub mod profile;
mod registry;
pub mod spec;

#[cfg(test)]
mod test_hooks;

pub use branch_mutation::{BranchMutation, FailurePoint};
pub use branch_service::{BranchService, ForkOptions, MergeOptions};
pub use compat::{CompatibilitySignature, IncompatibleReason, CURRENT_CODEC_ID};
pub use dag_hook::{
    AncestryEntry, BranchDagError, BranchDagErrorKind, BranchDagHook, DagEvent, DagEventKind,
    DagHookSlot, MergeBaseResult,
};
pub use merge_registry::{
    GraphMergePlanFn, MergeHandlerRegistry, VectorMergeCallbacks, VectorMergePostCommitFn,
    VectorMergePrecheckFn,
};
pub use observers::{
    AbortInfo, AbortObserver, AbortObserverRegistry, BranchOpEvent, BranchOpKind, BranchOpObserver,
    BranchOpObserverRegistry, CommitInfo, CommitObserver, CommitObserverRegistry, ObserverError,
    ObserverErrorKind, ReplayInfo, ReplayObserver, ReplayObserverRegistry,
};
pub use spec::{
    search_only_cache_spec, search_only_follower_spec, search_only_primary_spec, DatabaseMode,
    OpenSpec,
};

pub use config::{ModelConfig, StorageConfig, StrataConfig, SHADOW_EVENT, SHADOW_JSON, SHADOW_KV};
pub use profile::{
    apply_hardware_profile_if_defaults, apply_profile_if_defaults, detect_hardware, HardwareInfo,
    Profile,
};
pub use registry::OPEN_DATABASES;

use crate::background::BackgroundScheduler;
use crate::coordinator::TransactionCoordinator;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use std::any::{Any, TypeId};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;
use std::time::Instant;
use strata_core::id::CommitVersion;
use strata_core::types::{BranchId, Key};
use strata_core::{StrataError, StrataResult, VersionedValue};
use strata_durability::__internal::{BackgroundSyncError, WalWriterEngineExt};
use strata_durability::wal::{DurabilityMode, WalWriter};
use strata_storage::{SegmentedStore, StorageIterator};

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
// Health Check Types
// ============================================================================

/// Overall health report from `Database::health()`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HealthReport {
    /// Worst-case status across all subsystems.
    pub status: SubsystemStatus,
    /// Seconds since the database was opened.
    pub uptime_secs: u64,
    /// Per-subsystem health details.
    pub subsystems: Vec<SubsystemHealth>,
}

/// Health status of a single subsystem.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SubsystemHealth {
    /// Subsystem name (e.g. "storage", "wal", "disk").
    pub name: String,
    /// Current status.
    pub status: SubsystemStatus,
    /// Human-readable detail.
    pub message: Option<String>,
}

/// Three-level health status.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum SubsystemStatus {
    /// Everything is working normally.
    Healthy,
    /// Working but with warnings (e.g. low disk, large queue).
    Degraded,
    /// Subsystem is not functional.
    Unhealthy,
}

impl std::fmt::Display for SubsystemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubsystemStatus::Healthy => write!(f, "healthy"),
            SubsystemStatus::Degraded => write!(f, "degraded"),
            SubsystemStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// WAL writer health status.
///
/// The WAL writer can halt on background sync (fsync) failure to prevent
/// data loss. This enum reports the current health state and, when halted,
/// provides diagnostic information for recovery.
///
/// # Recovery
///
/// When halted, the writer refuses new commits until the underlying issue
/// is resolved and [`Database::resume_wal_writer`] is called. A successful
/// resume requires that a sync operation succeed, proving the underlying
/// storage is healthy again.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WalWriterHealth {
    /// Writer is accepting transactions normally.
    Healthy,
    /// Writer has halted due to a background sync failure.
    ///
    /// No new commits will be accepted until [`Database::resume_wal_writer`]
    /// is called and succeeds.
    Halted {
        /// Human-readable reason for the halt.
        reason: String,
        /// Timestamp of the first observed failure in the current streak.
        first_observed_at: std::time::SystemTime,
        /// Number of consecutive failed sync attempts.
        failed_sync_count: u64,
    },
}

impl Default for WalWriterHealth {
    fn default() -> Self {
        WalWriterHealth::Healthy
    }
}

impl std::fmt::Display for WalWriterHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalWriterHealth::Healthy => write!(f, "healthy"),
            WalWriterHealth::Halted {
                reason,
                failed_sync_count,
                ..
            } => write!(f, "halted: {} ({} failed syncs)", reason, failed_sync_count),
        }
    }
}

// ============================================================================
// Disk Usage
// ============================================================================

/// Database disk usage summary.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseDiskUsage {
    /// WAL directory usage.
    pub wal: strata_durability::WalDiskUsage,
    /// Snapshot directory usage in bytes.
    pub snapshot_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleState {
    Uninitialized,
    Initializing,
    Initialized,
    Failed,
}

impl LifecycleState {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Uninitialized => 0,
            Self::Initializing => 1,
            Self::Initialized => 2,
            Self::Failed => 3,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Uninitialized,
            1 => Self::Initializing,
            2 => Self::Initialized,
            3 => Self::Failed,
            _ => {
                debug_assert!(false, "invalid lifecycle state value: {}", value);
                Self::Failed
            }
        }
    }
}

// ============================================================================
// Unified Metrics
// ============================================================================

/// Unified database metrics snapshot from `Database::metrics()`.
///
/// Aggregates all subsystem metrics. The health check (`Database::health()`)
/// consumes this internally rather than poking each subsystem directly.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SystemMetrics {
    /// Seconds since the database was opened.
    pub uptime_secs: u64,
    /// Transaction metrics.
    pub transactions: crate::coordinator::TransactionMetrics,
    /// WAL counters (None for ephemeral databases).
    pub wal_counters: Option<strata_durability::WalCounters>,
    /// WAL disk usage (None for ephemeral databases).
    pub wal_disk_usage: Option<strata_durability::WalDiskUsage>,
    /// Background scheduler metrics.
    pub scheduler: crate::background::SchedulerStats,
    /// Storage memory usage summary.
    pub storage: StorageMetricsSummary,
    /// Block cache performance metrics.
    pub cache: CacheMetrics,
    /// Database disk usage (WAL + snapshots).
    pub disk_usage: DatabaseDiskUsage,
    /// Available disk space in bytes (None for ephemeral databases).
    pub available_disk_bytes: Option<u64>,
}

/// Summary of storage memory usage (excludes per-branch detail).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StorageMetricsSummary {
    /// Total number of branches.
    pub total_branches: usize,
    /// Total entries across all branches.
    pub total_entries: usize,
    /// Estimated total memory usage in bytes.
    pub estimated_bytes: usize,
}

/// Block cache performance metrics.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CacheMetrics {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Current number of cached blocks.
    pub entries: usize,
    /// Current total size of cached data in bytes.
    pub size_bytes: usize,
    /// Maximum capacity in bytes.
    pub capacity_bytes: usize,
    /// Hit ratio (0.0 to 1.0).
    pub hit_ratio: f64,
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

    /// Unique identifier for this database instance, persisted in MANIFEST.
    /// Used by WAL and checkpoint to detect cross-database file contamination.
    /// Ephemeral databases use all-zeros (no persistence).
    database_uuid: [u8; 16],

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

    /// Current durability mode.
    durability_mode: parking_lot::RwLock<DurabilityMode>,

    /// Flag to track if database is accepting new transactions
    ///
    /// Set to false during shutdown to reject new transactions.
    /// Arc-wrapped to share with the flush thread (for halt behavior).
    accepting_transactions: Arc<AtomicBool>,

    /// WAL writer health state.
    ///
    /// Set to `Halted` when background sync fails; prevents new commits until
    /// `resume_wal_writer()` succeeds. Per-database, not process-global.
    /// Arc-wrapped to share with the flush thread (for halt behavior).
    wal_writer_health: Arc<ParkingMutex<WalWriterHealth>>,

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
    /// In Standard mode, a background thread runs the shared three-phase
    /// background sync loop from `open.rs`.
    flush_handle: ParkingMutex<Option<std::thread::JoinHandle<()>>>,

    /// Background task scheduler for deferred work (embedding, GC, etc.)
    scheduler: Arc<BackgroundScheduler>,

    /// Flag preventing duplicate background flush submissions.
    /// Set to `true` when a flush task is in flight; cleared when it completes.
    /// The in-flight task drains all frozen memtables, so redundant submissions
    /// during fast ingest are pure waste.
    flush_in_flight: Arc<AtomicBool>,

    /// Flag preventing duplicate background compaction submissions.
    /// Set to `true` when a compaction task is in flight; cleared when it completes.
    compaction_in_flight: Arc<AtomicBool>,

    /// Cancellation flag for background compaction. Set during shutdown/drop to
    /// stop the compaction loop promptly instead of waiting for it to exhaust
    /// all branches and levels.
    compaction_cancelled: Arc<AtomicBool>,

    /// Condition variable signalled by compaction when L0 count drops.
    /// Writers wait on this when L0 exceeds `l0_stop_writes_trigger`.
    write_stall_cv: Arc<parking_lot::Condvar>,
    /// Mutex paired with `write_stall_cv` (value is unused).
    write_stall_mu: parking_lot::Mutex<()>,

    /// Counter for amortizing backpressure checks. Only every Nth write runs the
    /// full check (L0 count, memtable bytes, segment metadata), since these values
    /// change only on flush/compaction, not per write.
    backpressure_counter: AtomicU64,

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

    /// Whether shutdown() has started (prevents halt-resume from reopening it).
    shutdown_started: AtomicBool,

    /// Whether shutdown() has already completed (prevents double freeze in Drop).
    shutdown_complete: AtomicBool,

    /// Instant when this database instance was created (for uptime tracking).
    opened_at: Instant,

    /// Registered subsystems for recovery and shutdown hooks.
    ///
    /// Populated by `OpenSpec::with_subsystem()` via `Database::open_runtime()`.
    /// Frozen in reverse order during shutdown/drop.
    subsystems: parking_lot::RwLock<Vec<Box<dyn crate::recovery::Subsystem>>>,

    /// Per-database DAG hook slot.
    ///
    /// Installed by `GraphSubsystem::initialize()`. Used by `BranchService`
    /// for branch DAG operations (merge-base, log, ancestors, record_event).
    /// Replaces the process-global `BRANCH_DAG_HOOKS` OnceCell.
    dag_hook_slot: DagHookSlot,

    /// Per-database branch operation observer registry.
    ///
    /// Observers are notified after branch operations complete (create, delete,
    /// fork, merge, etc.). Best-effort: failures are logged, not propagated.
    branch_op_observers: BranchOpObserverRegistry,

    /// Per-database commit observer registry.
    ///
    /// Observers are notified after each successful WAL-backed commit.
    /// Best-effort: failures are logged, not propagated.
    commit_observers: CommitObserverRegistry,

    /// Per-database abort observer registry.
    ///
    /// Observers are notified after a transaction aborts or fails to commit.
    /// Best-effort: failures are logged, not propagated.
    abort_observers: AbortObserverRegistry,

    /// Per-database replay observer registry.
    ///
    /// Observers are notified after each fully-applied follower replay record.
    /// Best-effort: failures are logged, not propagated.
    replay_observers: ReplayObserverRegistry,

    /// Lifecycle state for `open_runtime` and registry reuse.
    ///
    /// Tracks whether this instance is uninitialized, currently initializing,
    /// fully initialized, or failed during lifecycle.
    lifecycle_state: AtomicU8,

    /// Wait primitive for lifecycle transitions.
    lifecycle_state_mu: parking_lot::Mutex<()>,
    lifecycle_state_cv: parking_lot::Condvar,

    /// Runtime compatibility signature, when opened via `open_runtime`.
    runtime_signature: parking_lot::RwLock<Option<CompatibilitySignature>>,

    /// Per-database merge handler registry.
    ///
    /// Stores vector and graph merge callbacks. Replaces the process-global
    /// OnceCell patterns in primitive_merge.rs.
    merge_registry: MergeHandlerRegistry,
}

// Split impl blocks
mod compaction;
mod lifecycle;
mod open;
pub mod refresh;
mod transaction;

#[cfg(test)]
mod tests;

impl Database {
    // ========================================================================
    // Accessors
    // ========================================================================

    pub(crate) fn current_durability_mode(&self) -> DurabilityMode {
        *self.durability_mode.read()
    }

    pub(crate) fn start_flush_thread(
        &self,
        mode: DurabilityMode,
        wal: &Arc<ParkingMutex<WalWriter>>,
    ) -> StrataResult<()> {
        debug_assert!(matches!(mode, DurabilityMode::Standard { .. }));
        self.flush_shutdown.store(false, Ordering::SeqCst);
        if let Some(handle) = Self::spawn_wal_flush_thread(
            mode,
            wal,
            &self.flush_shutdown,
            &self.accepting_transactions,
            &self.wal_writer_health,
        )? {
            *self.flush_handle.lock() = Some(handle);
        }
        Ok(())
    }

    pub(crate) fn stop_flush_thread(&self) {
        self.flush_shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.flush_handle.lock().take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }

    fn update_runtime_signature_durability(&self, mode: DurabilityMode) {
        if let Some(signature) = self.runtime_signature() {
            self.set_runtime_signature(signature.with_durability_mode(mode));
        }
    }

    /// Get reference to the storage layer (internal use only)
    ///
    /// This is for internal engine use. External users should use
    /// primitives (KVStore, EventLog, etc.) which go through transactions.
    pub fn storage(&self) -> &Arc<SegmentedStore> {
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

    /// Mark a branch as being deleted (#1916).
    ///
    /// Blocks future commits on this branch. The caller must also acquire
    /// `branch_commit_lock()` to drain in-flight commits.
    pub fn mark_branch_deleting(&self, branch_id: &BranchId) {
        self.coordinator.mark_branch_deleting(branch_id);
    }

    /// Check if a branch is currently marked as deleting (#2108).
    pub fn is_branch_deleting(&self, branch_id: &BranchId) -> bool {
        self.coordinator.is_branch_deleting(branch_id)
    }

    /// Remove the deleting mark for a branch (#1916).
    pub fn unmark_branch_deleting(&self, branch_id: &BranchId) {
        self.coordinator.unmark_branch_deleting(branch_id);
    }

    /// Acquire the commit quiesce lock (#2105).
    ///
    /// Blocks until all in-flight commits complete, then prevents new
    /// commits from starting. Hold the guard across operations that
    /// require a stable storage version (e.g., fork_branch).
    pub fn quiesce_commits(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.coordinator.quiesce_commits()
    }

    /// Get the commit lock Arc for a branch (#1916).
    ///
    /// Locking this serializes with in-flight commits on the branch.
    pub fn branch_commit_lock(
        &self,
        branch_id: &BranchId,
    ) -> std::sync::Arc<parking_lot::Mutex<()>> {
        self.coordinator.branch_commit_lock(branch_id)
    }

    /// Set subsystems for this database (called by `open_runtime()`).
    pub(crate) fn set_subsystems(&self, subsystems: Vec<Box<dyn crate::recovery::Subsystem>>) {
        *self.subsystems.write() = subsystems;
    }

    /// Return the names of the subsystems currently installed on this
    /// `Database`, in registration order.
    ///
    /// Used by `acquire_primary_db`'s fast-path mixed-opener detection
    /// to surface cases where a caller opened the same path with a
    /// different subsystem set (see audit follow-up to
    /// stratalab/strata-core#2354, Finding 2). Also useful as a general
    /// diagnostic accessor.
    ///
    /// Returns an empty vec for cache databases or any `Database`
    /// whose subsystems have not yet been installed (e.g. a partially-
    /// opened instance inside `acquire_primary_db` after creation but
    /// before the recovery loop completes).
    pub fn installed_subsystem_names(&self) -> Vec<&'static str> {
        self.subsystems.read().iter().map(|s| s.name()).collect()
    }

    /// Return a read guard to the subsystems list.
    ///
    /// Used by `acquire_primary_db` to iterate subsystems for lifecycle
    /// hooks (initialize, bootstrap) after recovery completes.
    pub(crate) fn installed_subsystems(
        &self,
    ) -> parking_lot::RwLockReadGuard<'_, Vec<Box<dyn crate::recovery::Subsystem>>> {
        self.subsystems.read()
    }

    // =========================================================================
    // DAG Hook
    // =========================================================================

    /// Get the per-database DAG hook slot.
    ///
    /// Used by `BranchService` to access the installed DAG hook for
    /// merge-base, log, ancestors, and record_event operations.
    pub fn dag_hook(&self) -> &DagHookSlot {
        &self.dag_hook_slot
    }

    /// Install a DAG hook for this database.
    ///
    /// Called by `GraphSubsystem::initialize()` to install the graph crate's
    /// implementation. Returns an error if a hook is already installed.
    ///
    /// ## Usage
    ///
    /// ```text
    /// impl Subsystem for GraphSubsystem {
    ///     fn initialize(&self, db: &Arc<Database>) -> StrataResult<()> {
    ///         let hook = Arc::new(GraphDagHook::new(db.clone()));
    ///         db.install_dag_hook(hook).map_err(|e| ...)?;
    ///         Ok(())
    ///     }
    /// }
    /// ```
    pub fn install_dag_hook(
        &self,
        hook: Arc<dyn BranchDagHook>,
    ) -> Result<(), dag_hook::BranchDagError> {
        self.dag_hook_slot.install(hook)
    }

    // =========================================================================
    // Branch Operation Observers
    // =========================================================================

    /// Get the per-database branch operation observer registry.
    ///
    /// Used by `BranchService` to notify observers after branch operations.
    pub fn branch_op_observers(&self) -> &BranchOpObserverRegistry {
        &self.branch_op_observers
    }

    // =========================================================================
    // Commit/Abort/Replay Observers
    // =========================================================================

    /// Get the per-database commit observer registry.
    ///
    /// Observers are notified after each successful WAL-backed commit.
    pub fn commit_observers(&self) -> &CommitObserverRegistry {
        &self.commit_observers
    }

    /// Get the per-database abort observer registry.
    ///
    /// Observers are notified after transaction abort/failure cleanup points.
    pub fn abort_observers(&self) -> &AbortObserverRegistry {
        &self.abort_observers
    }

    /// Get the per-database replay observer registry.
    ///
    /// Observers are notified after each fully-applied follower replay record.
    pub fn replay_observers(&self) -> &ReplayObserverRegistry {
        &self.replay_observers
    }

    // =========================================================================
    // Lifecycle State
    // =========================================================================

    /// Check if lifecycle hooks have completed.
    ///
    /// Returns true if initialize() and bootstrap() have run successfully.
    /// Used by open_runtime to avoid re-running lifecycle on reuse.
    pub fn is_lifecycle_complete(&self) -> bool {
        self.lifecycle_state() == LifecycleState::Initialized
    }

    pub(crate) fn lifecycle_state(&self) -> LifecycleState {
        LifecycleState::from_u8(
            self.lifecycle_state
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    pub(crate) fn set_lifecycle_initializing(&self) {
        self.lifecycle_state.store(
            LifecycleState::Initializing.as_u8(),
            std::sync::atomic::Ordering::Release,
        );
        self.lifecycle_state_cv.notify_all();
    }

    /// Mark lifecycle as complete.
    ///
    /// Called after initialize() and bootstrap() succeed.
    pub(crate) fn set_lifecycle_complete(&self) {
        self.lifecycle_state.store(
            LifecycleState::Initialized.as_u8(),
            std::sync::atomic::Ordering::Release,
        );
        self.lifecycle_state_cv.notify_all();
    }

    pub(crate) fn set_lifecycle_failed(&self) {
        self.lifecycle_state.store(
            LifecycleState::Failed.as_u8(),
            std::sync::atomic::Ordering::Release,
        );
        self.lifecycle_state_cv.notify_all();
    }

    pub(crate) fn wait_for_lifecycle_state(&self) -> LifecycleState {
        let mut guard = self.lifecycle_state_mu.lock();
        loop {
            let state = self.lifecycle_state();
            if state != LifecycleState::Initializing {
                return state;
            }
            self.lifecycle_state_cv.wait(&mut guard);
        }
    }

    pub(crate) fn set_runtime_signature(&self, signature: CompatibilitySignature) {
        *self.runtime_signature.write() = Some(signature);
    }

    pub(crate) fn runtime_signature(&self) -> Option<CompatibilitySignature> {
        self.runtime_signature.read().clone()
    }

    pub(crate) fn configured_default_branch(&self) -> Option<String> {
        self.runtime_signature()
            .and_then(|signature| signature.default_branch)
    }

    /// Return the runtime-configured default branch, if any.
    ///
    /// This is populated by `Database::open_runtime()` from the compatibility
    /// signature and may differ from the legacy executor fallback of
    /// literal `"default"`.
    pub fn default_branch_name(&self) -> Option<String> {
        self.configured_default_branch().or_else(|| {
            crate::primitives::branch::read_default_branch_marker(self)
                .map_err(|error| {
                    tracing::warn!(
                        target: "strata::db",
                        error = %error,
                        "Failed to read persisted default-branch marker"
                    );
                    error
                })
                .ok()
                .flatten()
        })
    }

    // =========================================================================
    // Merge Handler Registry
    // =========================================================================

    /// Get the per-database merge handler registry.
    ///
    /// Used by subsystems to register merge callbacks and by `merge_branches`
    /// to look them up. Replaces the process-global OnceCell patterns.
    pub fn merge_registry(&self) -> &MergeHandlerRegistry {
        &self.merge_registry
    }

    // =========================================================================
    // Branch Service
    // =========================================================================

    /// Get the branch service facade for this database.
    ///
    /// This is the canonical entry point for all branch operations:
    /// create, delete, fork, merge, revert, cherry-pick, tag, etc.
    ///
    /// ## Example
    ///
    /// ```text
    /// let branches = db.branches();
    /// branches.create("feature")?;
    /// branches.fork("main", "experiment")?;
    /// branches.merge("experiment", "main")?;
    /// ```
    pub fn branches(self: &Arc<Self>) -> BranchService {
        BranchService::new(self.clone())
    }

    /// Ensure the reserved `_system_` branch exists for subsystem bootstrap.
    ///
    /// This is a narrow lifecycle helper for workspace-internal subsystem
    /// initialization. User-visible branch management should go through
    /// `db.branches()`.
    #[doc(hidden)]
    pub fn ensure_system_branch_exists(self: &Arc<Self>) -> StrataResult<()> {
        let index = crate::primitives::branch::BranchIndex::new(self.clone());
        if index.exists(crate::SYSTEM_BRANCH)? {
            return Ok(());
        }
        index.create_branch(crate::SYSTEM_BRANCH).map(|_| ())
    }

    /// Run freeze hooks on all registered subsystems.
    ///
    /// Called during shutdown and drop. Attempts all hooks even if one fails,
    /// so a vector freeze error doesn't also lose search data.
    pub(crate) fn run_freeze_hooks(&self) -> StrataResult<()> {
        let subsystems = self.subsystems.read();
        let mut first_error: Option<strata_core::StrataError> = None;
        for subsystem in subsystems.iter().rev() {
            if let Err(e) = subsystem.freeze(self) {
                tracing::warn!(
                    target: "strata::db",
                    subsystem = subsystem.name(),
                    error = %e,
                    "Subsystem freeze failed"
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Direct single-key read returning only the Value (no VersionedValue).
    ///
    /// Skips Version enum and VersionedValue construction. Used by the
    /// KVStore::get() hot path where version metadata is not needed.
    pub fn get_value_direct(
        &self,
        key: &Key,
    ) -> strata_core::StrataResult<Option<strata_core::value::Value>> {
        self.storage.get_value_direct(key)
    }

    /// Direct single-key read returning full VersionedValue metadata.
    ///
    /// Bypasses the transaction layer (no coordinator mutex, no read-set
    /// tracking). Provides per-key read consistency; for multi-key snapshot
    /// isolation use `Database::transaction()`.
    pub fn get_versioned_direct(
        &self,
        key: &Key,
    ) -> strata_core::StrataResult<Option<strata_core::VersionedValue>> {
        self.storage.get_versioned_direct(key)
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
        use strata_core::id::CommitVersion;
        use strata_core::Storage;
        self.storage
            .get_history(key, limit, before_version.map(CommitVersion))
    }

    /// Get value at or before the given timestamp directly from storage.
    ///
    /// This is a non-transactional read for time-travel queries.
    /// Get value at or before the given timestamp directly from storage.
    pub fn get_at_timestamp(
        &self,
        key: &Key,
        max_timestamp: u64,
    ) -> StrataResult<Option<VersionedValue>> {
        self.storage.get_at_timestamp(key, max_timestamp)
    }

    /// Count entries matching a prefix directly from storage.
    ///
    /// Bypasses the transaction layer (read-only, no conflict tracking needed).
    /// Uses the same MVCC pipeline as scan_prefix but counts instead of
    /// collecting, avoiding the O(N) Vec allocation.
    pub(crate) fn count_prefix(
        &self,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<u64> {
        self.storage.count_prefix(prefix, max_version)
    }

    /// Scan entries in a range directly from storage.
    ///
    /// Bypasses the transaction layer (read-only, no conflict tracking needed).
    /// Uses the lazy merge pipeline with seek pushdown and optional limit.
    #[allow(dead_code)]
    pub(crate) fn scan_range(
        &self,
        prefix: &Key,
        start_key: &Key,
        max_version: CommitVersion,
        limit: Option<usize>,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        self.storage
            .scan_range(prefix, start_key, max_version, limit)
    }

    /// Create a persistent [`StorageIterator`] for cursor-based pagination.
    ///
    /// Captures a branch snapshot and returns an iterator supporting
    /// `seek()` + `next()` cycles.
    pub(crate) fn storage_iterator(
        &self,
        branch_id: &BranchId,
        prefix: Key,
        snapshot_version: CommitVersion,
    ) -> Option<StorageIterator> {
        self.storage
            .new_storage_iterator(branch_id, prefix, snapshot_version)
    }

    /// Scan keys matching a prefix at or before the given timestamp.
    ///
    /// This is a non-transactional read for time-travel queries.
    /// Scan keys matching a prefix at or before the given timestamp.
    pub fn scan_prefix_at_timestamp(
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

    /// Returns the unique database identifier persisted in MANIFEST.
    pub fn database_uuid(&self) -> [u8; 16] {
        self.database_uuid
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

    pub(crate) fn writer_halted_error(&self) -> Option<StrataError> {
        match &*self.wal_writer_health.lock() {
            WalWriterHealth::Healthy => None,
            WalWriterHealth::Halted {
                reason,
                first_observed_at,
                ..
            } => Some(StrataError::WriterHalted {
                reason: reason.clone(),
                first_observed_at: *first_observed_at,
            }),
        }
    }

    pub(crate) fn ensure_writer_healthy(&self) -> StrataResult<()> {
        if let Some(err) = self.writer_halted_error() {
            return Err(err);
        }
        Ok(())
    }

    pub(crate) fn halted_health_from_bg_error(bg_error: BackgroundSyncError) -> WalWriterHealth {
        WalWriterHealth::Halted {
            reason: bg_error.message().to_string(),
            first_observed_at: bg_error.first_observed_at(),
            failed_sync_count: bg_error.failed_sync_count(),
        }
    }

    // ========================================================================
    // WAL Writer Health
    // ========================================================================

    /// Returns the current WAL writer health status.
    ///
    /// The WAL writer halts on background sync (fsync) failure to prevent
    /// data loss. When halted, no new commits will be accepted until the
    /// underlying issue is resolved and [`resume_wal_writer`](Self::resume_wal_writer)
    /// is called.
    ///
    /// For ephemeral databases (no WAL), this always returns `Healthy`.
    ///
    /// # Example
    /// ```text
    /// match db.wal_writer_health() {
    ///     WalWriterHealth::Healthy => println!("Writer healthy"),
    ///     WalWriterHealth::Halted { reason, failed_sync_count, .. } => {
    ///         eprintln!("Writer halted: {} ({} failed syncs)", reason, failed_sync_count);
    ///     }
    /// }
    /// ```
    pub fn wal_writer_health(&self) -> WalWriterHealth {
        self.wal_writer_health.lock().clone()
    }

    /// Attempt to resume a halted WAL writer.
    ///
    /// When the WAL writer halts due to background sync failure, this method
    /// attempts to resume normal operation. A successful resume requires that
    /// a sync operation succeed, proving the underlying storage is healthy.
    ///
    /// # Arguments
    ///
    /// * `confirm_reason` - A brief description of what action was taken to
    ///   resolve the underlying issue. This is logged for audit purposes.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Writer resumed successfully and is accepting commits
    /// * `Err(WriterHalted)` - Sync still failing; writer remains halted
    /// * `Err(InvalidInput)` - Shutdown has started or the database is closed
    /// * `Err(Internal)` - No WAL writer (ephemeral database)
    ///
    /// # Example
    /// ```text
    /// // After fixing disk space issue:
    /// db.resume_wal_writer("freed 10GB disk space")?;
    /// ```
    pub fn resume_wal_writer(&self, confirm_reason: &str) -> StrataResult<()> {
        let wal = self.wal_writer.as_ref().ok_or_else(|| {
            StrataError::internal("cannot resume WAL writer on ephemeral database".to_string())
        })?;

        if self.shutdown_started.load(Ordering::Acquire)
            || self.shutdown_complete.load(Ordering::Acquire)
        {
            return Err(StrataError::invalid_input(
                "cannot resume WAL writer after shutdown has started".to_string(),
            ));
        }

        let was_halted = matches!(self.wal_writer_health(), WalWriterHealth::Halted { .. });
        if !was_halted {
            if self.accepting_transactions.load(Ordering::Acquire) {
                return Ok(());
            }
            return Err(StrataError::invalid_input(
                "database is not accepting transactions".to_string(),
            ));
        }

        // Attempt a sync to prove storage is healthy
        {
            let mut writer = wal.lock();
            #[cfg(test)]
            let flush_result = crate::database::test_hooks::maybe_inject_sync_failure()
                .map_or_else(|| writer.flush(), Err);

            #[cfg(not(test))]
            let flush_result = writer.flush();

            if let Err(e) = flush_result {
                writer.record_sync_failure(e);
                let bg_error = writer.bg_error().expect(
                    "record_sync_failure must preserve a background error for halted writer",
                );
                let reason = bg_error.message().to_string();
                let first_observed_at = bg_error.first_observed_at();
                let mut health = self.wal_writer_health.lock();
                *health = Self::halted_health_from_bg_error(bg_error);
                return Err(StrataError::WriterHalted {
                    reason,
                    first_observed_at,
                });
            }

            // Clear any recorded background error in the writer
            writer.clear_bg_error();
        }

        // Sync succeeded — restore healthy state
        let mut health = self.wal_writer_health.lock();
        *health = WalWriterHealth::Healthy;
        drop(health);

        // Re-enable transactions
        self.accepting_transactions.store(true, Ordering::Release);

        if was_halted {
            tracing::info!(
                target: "strata::wal",
                confirm_reason = confirm_reason,
                "WAL writer resumed"
            );
        }

        Ok(())
    }

    /// Returns a reference to the background task scheduler.
    pub fn scheduler(&self) -> &BackgroundScheduler {
        &self.scheduler
    }

    /// Seconds since the database was opened.
    pub fn uptime_secs(&self) -> u64 {
        self.opened_at.elapsed().as_secs()
    }

    /// Approximate total number of entries (keys) across all branches.
    ///
    /// Includes both in-memory (memtable) entries and on-disk segment entries.
    /// This is an approximation — concurrent writes may cause slight drift.
    pub fn approximate_total_keys(&self) -> u64 {
        self.storage.memory_stats().total_entries as u64
    }

    /// Collect a unified snapshot of all database metrics.
    ///
    /// This is the single aggregation point for all subsystem metrics.
    /// The health check (`health()`) calls this internally.
    pub fn metrics(&self) -> SystemMetrics {
        let transactions = self.coordinator.metrics();

        let wal_counters = self.durability_counters();
        let wal_disk_usage = self.wal_writer.as_ref().map(|w| w.lock().wal_disk_usage());

        let scheduler = self.scheduler.stats();

        let mem_stats = self.storage.memory_stats();
        let storage = StorageMetricsSummary {
            total_branches: mem_stats.total_branches,
            total_entries: mem_stats.total_entries,
            estimated_bytes: mem_stats.estimated_bytes,
        };

        let bc = strata_storage::block_cache::global_cache().stats();
        let total_accesses = bc.hits + bc.misses;
        let cache = CacheMetrics {
            hits: bc.hits,
            misses: bc.misses,
            entries: bc.entries,
            size_bytes: bc.size_bytes,
            capacity_bytes: bc.capacity_bytes,
            hit_ratio: if total_accesses > 0 {
                bc.hits as f64 / total_accesses as f64
            } else {
                0.0
            },
        };

        let disk_usage = self.disk_usage();

        let available_disk_bytes = if self.data_dir.as_os_str().is_empty() {
            None
        } else {
            fs2::available_space(&self.data_dir).ok()
        };

        SystemMetrics {
            uptime_secs: self.uptime_secs(),
            transactions,
            wal_counters,
            wal_disk_usage,
            scheduler,
            storage,
            cache,
            disk_usage,
            available_disk_bytes,
        }
    }

    /// Run health checks against all subsystems and return a report.
    ///
    /// Collects metrics via `self.metrics()` and interprets them into
    /// healthy/degraded/unhealthy status levels. The flush thread liveness
    /// check is the only direct subsystem access (not a metric).
    pub fn health(&self) -> HealthReport {
        let m = self.metrics();
        let mut subsystems = Vec::new();

        // 1. Storage
        subsystems.push(SubsystemHealth {
            name: "storage".into(),
            status: SubsystemStatus::Healthy,
            message: Some(format!(
                "{} branches, {} entries",
                m.storage.total_branches, m.storage.total_entries
            )),
        });

        // 2. WAL
        {
            let (status, message) = match &m.wal_counters {
                Some(counters) => (
                    SubsystemStatus::Healthy,
                    Some(format!(
                        "{} appends, {} syncs, {} bytes written",
                        counters.wal_appends, counters.sync_calls, counters.bytes_written
                    )),
                ),
                None => {
                    if self.persistence_mode == PersistenceMode::Ephemeral {
                        (SubsystemStatus::Healthy, Some("ephemeral (no WAL)".into()))
                    } else if self.follower {
                        (
                            SubsystemStatus::Healthy,
                            Some("follower (read-only, no WAL writer)".into()),
                        )
                    } else {
                        (
                            SubsystemStatus::Unhealthy,
                            Some("WAL writer is missing".into()),
                        )
                    }
                }
            };
            subsystems.push(SubsystemHealth {
                name: "wal".into(),
                status,
                message,
            });
        }

        // 3. Flush thread — liveness check, not derivable from metrics
        {
            let guard = self.flush_handle.lock();
            let (status, message) = if let Some(handle) = guard.as_ref() {
                if handle.is_finished() {
                    (
                        SubsystemStatus::Unhealthy,
                        Some("flush thread has exited unexpectedly".into()),
                    )
                } else {
                    (SubsystemStatus::Healthy, Some("running".into()))
                }
            } else {
                match self.current_durability_mode() {
                    DurabilityMode::Standard { .. } => (
                        SubsystemStatus::Degraded,
                        Some("flush thread not running (standard mode)".into()),
                    ),
                    _ => (
                        SubsystemStatus::Healthy,
                        Some("not applicable (cache/always mode)".into()),
                    ),
                }
            };
            subsystems.push(SubsystemHealth {
                name: "flush_thread".into(),
                status,
                message,
            });
        }

        // 4. Disk
        {
            let is_ephemeral = self.data_dir.as_os_str().is_empty();
            let (status, message) = match m.available_disk_bytes {
                None if is_ephemeral => {
                    (SubsystemStatus::Healthy, Some("ephemeral (no disk)".into()))
                }
                None => (
                    SubsystemStatus::Degraded,
                    Some("could not check disk space".into()),
                ),
                Some(avail) => {
                    let avail_mb = avail / (1024 * 1024);
                    if avail_mb < 100 {
                        (
                            SubsystemStatus::Unhealthy,
                            Some(format!("{} MB available (critically low)", avail_mb)),
                        )
                    } else if avail_mb < 1024 {
                        (
                            SubsystemStatus::Degraded,
                            Some(format!("{} MB available (low)", avail_mb)),
                        )
                    } else {
                        (
                            SubsystemStatus::Healthy,
                            Some(format!("{} MB available", avail_mb)),
                        )
                    }
                }
            };
            subsystems.push(SubsystemHealth {
                name: "disk".into(),
                status,
                message,
            });
        }

        // 5. Coordinator
        {
            let status = if !self.is_open() {
                SubsystemStatus::Unhealthy
            } else {
                SubsystemStatus::Healthy
            };
            subsystems.push(SubsystemHealth {
                name: "coordinator".into(),
                status,
                message: Some(format!(
                    "{} active, {} committed, {} aborted",
                    m.transactions.active_count,
                    m.transactions.total_committed,
                    m.transactions.total_aborted
                )),
            });
        }

        // 6. Scheduler
        {
            let status = if m.scheduler.queue_depth > 1000 {
                SubsystemStatus::Degraded
            } else {
                SubsystemStatus::Healthy
            };
            subsystems.push(SubsystemHealth {
                name: "scheduler".into(),
                status,
                message: Some(format!(
                    "{} queued, {} active, {} completed",
                    m.scheduler.queue_depth, m.scheduler.active_tasks, m.scheduler.tasks_completed
                )),
            });
        }

        let overall = subsystems
            .iter()
            .map(|s| &s.status)
            .max()
            .cloned()
            .unwrap_or(SubsystemStatus::Healthy);

        HealthReport {
            status: overall,
            uptime_secs: m.uptime_secs,
            subsystems,
        }
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
    /// disk-backed databases, and storage/coordinator/cache parameters are
    /// applied to the live database immediately.
    pub fn update_config<F: FnOnce(&mut StrataConfig)>(&self, f: F) -> StrataResult<()> {
        let mut guard = self.config.write();
        f(&mut guard);
        // Persist to strata.toml for disk-backed databases
        if self.persistence_mode == PersistenceMode::Disk && !self.data_dir.as_os_str().is_empty() {
            let config_path = self.data_dir.join(config::CONFIG_FILE_NAME);
            guard.write_to_file(&config_path)?;
        }
        // Apply storage/coordinator/cache parameters to the live database
        self.apply_storage_config_inner(&guard);
        Ok(())
    }

    /// Push storage-layer configuration to the live database.
    ///
    /// Called after every `update_config()` to make storage, coordinator,
    /// and block cache parameters take effect immediately.
    fn apply_storage_config_inner(&self, cfg: &StrataConfig) {
        self.storage.set_max_branches(cfg.storage.max_branches);
        self.storage
            .set_max_versions_per_key(cfg.storage.max_versions_per_key);
        self.storage
            .set_max_immutable_memtables(cfg.storage.effective_max_immutable_memtables());
        self.storage
            .set_write_buffer_size(cfg.storage.effective_write_buffer_size());
        self.storage
            .set_target_file_size(cfg.storage.target_file_size);
        self.storage
            .set_level_base_bytes(cfg.storage.level_base_bytes);
        self.storage
            .set_data_block_size(cfg.storage.data_block_size);
        self.storage
            .set_bloom_bits_per_key(cfg.storage.bloom_bits_per_key);
        self.storage
            .set_compaction_rate_limit(cfg.storage.compaction_rate_limit);

        self.coordinator
            .set_max_write_buffer_entries(cfg.storage.max_write_buffer_entries);

        // Block cache
        use strata_storage::block_cache;
        let effective_cache = cfg.storage.effective_block_cache_size();
        let cache_bytes = if effective_cache > 0 {
            effective_cache
        } else {
            block_cache::auto_detect_capacity()
        };
        block_cache::set_global_capacity(cache_bytes);
    }

    /// Switch the durability mode at runtime (Standard ↔ Always only).
    ///
    /// Updates the WAL writer's fsync policy and restarts the shared
    /// background flush thread when the Standard-mode configuration changes.
    pub fn set_durability_mode(&self, mode: DurabilityMode) -> StrataResult<()> {
        let wal = self.wal_writer.as_ref().ok_or_else(|| {
            StrataError::invalid_input(
                "Cannot change durability mode on an ephemeral (cache) database".to_string(),
            )
        })?;

        let old_mode = wal.lock().durability_mode();
        if old_mode == mode {
            *self.durability_mode.write() = mode;
            self.update_runtime_signature_durability(mode);
            return Ok(());
        }

        if matches!(mode, DurabilityMode::Cache) || matches!(old_mode, DurabilityMode::Cache) {
            return Err(StrataError::invalid_input(
                "Cannot switch to or from Cache mode at runtime".to_string(),
            ));
        }

        let had_standard_thread = matches!(old_mode, DurabilityMode::Standard { .. });
        if had_standard_thread {
            self.stop_flush_thread();
        }

        if let Err(e) = wal.lock().set_durability_mode(mode) {
            if had_standard_thread {
                self.start_flush_thread(old_mode, wal)?;
            }
            return Err(StrataError::invalid_input(e.to_string()));
        }

        if matches!(mode, DurabilityMode::Standard { .. }) {
            if let Err(e) = self.start_flush_thread(mode, wal) {
                let rollback_err = wal.lock().set_durability_mode(old_mode);
                if rollback_err.is_ok() && had_standard_thread {
                    self.start_flush_thread(old_mode, wal)?;
                }
                if let Err(rollback_err) = rollback_err {
                    return Err(StrataError::internal(format!(
                        "failed to start flush thread after durability switch: {}; rollback failed: {}",
                        e, rollback_err
                    )));
                }
                return Err(e);
            }
        }

        *self.durability_mode.write() = mode;
        self.update_runtime_signature_durability(mode);

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
}

impl Drop for Database {
    fn drop(&mut self) {
        // Signal background compaction to stop promptly
        self.compaction_cancelled.store(true, Ordering::Release);
        // Shut down the background task scheduler
        self.scheduler.shutdown();

        // Stop the background flush thread
        self.stop_flush_thread();

        // Skip flush/freeze if shutdown() already completed them.
        if !self.shutdown_complete.load(Ordering::Acquire) && !self.follower {
            // Final flush to persist any remaining data
            if let Err(e) = self.flush() {
                tracing::error!(target: "strata::db", error = %e,
                    "Final flush on drop failed — data may not be durable");
            }

            // Freeze all registered subsystems
            if let Err(e) = self.run_freeze_hooks() {
                tracing::warn!(target: "strata::db", error = %e, "Subsystem freeze failed in drop");
            }
        }

        // Remove from registry if we're disk-backed. Best-effort via
        // `try_lock`: if the mutex is contended we skip the remove and
        // leave a stale `Weak<Database>` entry behind. That is safe
        // because the next call to `acquire_primary_db` for this path
        // will find the entry, fail to upgrade the weak ref (strong
        // count is zero now), fall through to creating a fresh
        // `Database`, and overwrite the stale entry on insert.
        //
        // The contention case this handles is the recovery-failure /
        // recovery-panic path inside `acquire_primary_db`: that
        // function holds the `OPEN_DATABASES` guard across subsystem
        // recovery, and if recovery errors or panics, the local
        // `Arc<Self>` unwinds through Rust's drop order *before* the
        // guard does. A blocking `lock()` call here would self-deadlock
        // against the still-held guard because `parking_lot::Mutex` is
        // non-reentrant.
        if self.persistence_mode == PersistenceMode::Disk && !self.data_dir.as_os_str().is_empty() {
            if let Some(mut registry) = OPEN_DATABASES.try_lock() {
                registry.remove(&self.data_dir);
            }
        }
    }
}
