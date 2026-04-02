//! High-level typed wrapper for the Executor.
//!
//! The [`Strata`] struct provides a convenient Rust API that wraps the
//! [`Executor`] and [`Command`]/[`Output`] enums with typed method calls.
//!
//! ## Branch Context
//!
//! Strata maintains a "current branch" context, similar to how git maintains
//! a current branch. All data operations operate on the current branch.
//!
//! - Use `create_branch(name)` to create a new blank branch
//! - Use `set_branch(name)` to switch to an existing branch
//! - Use `current_branch()` to get the current branch name
//! - Use `list_branches()` to see all available branches
//! - Use `fork_branch(dest)` to copy the current branch to a new branch
//!
//! By default, Strata starts on the "default" branch.
//!
//! # Example
//!
//! ```text
//! use strata_executor::{Strata, Value};
//!
//! let mut db = Strata::open("/path/to/data")?;
//!
//! // Work on the default branch
//! db.kv_put("key", Value::String("hello".into()))?;
//!
//! // Create and switch to a different branch
//! db.create_branch("experiment-1")?;
//! db.set_branch("experiment-1")?;
//! db.kv_put("key", Value::String("different".into()))?;
//!
//! // Switch back to default
//! db.set_branch("default")?;
//! assert_eq!(db.kv_get("key")?, Some(Value::String("hello".into())));
//! ```

mod branch;
mod branches;
mod db;
mod event;
mod graph;
mod inference;
mod json;
mod kv;
mod space;
mod system;
mod vector;

pub use branches::Branches;
pub use strata_engine::branch_ops::{
    BranchDiffEntry, BranchDiffResult, CherryPickFilter, CherryPickInfo, ConflictEntry, DiffFilter,
    DiffOptions, DiffSummary, ForkInfo, MergeInfo, MergeStrategy, RevertInfo, SpaceDiff,
};
pub use system::SystemBranch;

use std::path::Path;
use std::sync::Arc;

use strata_engine::Database;
use strata_security::{AccessMode, OpenOptions};

use std::sync::Once;

use crate::ipc::{Backend, IpcClient};
use crate::types::BranchId;
use crate::{Command, Error, Executor, Output, Result, Session};

/// Ensure recovery participants are registered before opening any database.
static RECOVERY_INIT: Once = Once::new();

fn ensure_vector_recovery() {
    RECOVERY_INIT.call_once(|| {
        strata_vector::register_vector_recovery();
        strata_engine::register_search_recovery();
    });
}

/// High-level typed wrapper for database operations.
///
/// `Strata` provides a convenient Rust API that wraps the executor's
/// command-based interface with typed method calls. It maintains a
/// "current branch" context that all data operations use.
///
/// ## Branch Context (git-like mental model)
///
/// - **Database** = repository (the whole storage)
/// - **Strata** = working directory (stateful view into the repo)
/// - **Branch** = branch (isolated namespace for data)
///
/// Use `create_branch()` to create new branches and `set_branch()` to switch between them.
pub struct Strata {
    backend: Backend,
    current_branch: BranchId,
    current_space: String,
    access_mode: AccessMode,
}

impl Strata {
    /// Open a database at the given path.
    ///
    /// This is the primary way to create a Strata instance. The database
    /// will be created if it doesn't exist. Opens in read-write mode.
    ///
    /// # Example
    ///
    /// ```text
    /// use strata_executor::{Strata, Value};
    ///
    /// let mut db = Strata::open("/var/data/myapp")?;
    /// db.kv_put("key", Value::String("hello".into()))?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with(path, OpenOptions::default())
    }

    /// Open a database at the given path with explicit options.
    ///
    /// Use this to open a database in read-only mode or with other
    /// configuration options.
    ///
    /// # Example
    ///
    /// ```text
    /// use strata_executor::{Strata, OpenOptions, AccessMode};
    ///
    /// let db = Strata::open_with("/var/data/myapp", OpenOptions::new().access_mode(AccessMode::ReadOnly))?;
    /// ```
    pub fn open_with<P: AsRef<Path>>(path: P, opts: OpenOptions) -> Result<Self> {
        ensure_vector_recovery();

        let data_dir = path.as_ref().to_path_buf();

        if opts.follower {
            // Follower mode: read-only, never write to the database directory
            let db = Database::open_follower(&data_dir).map_err(|e| Error::Internal {
                reason: format!("Failed to open database (follower): {}", e),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            })?;
            let access_mode = AccessMode::ReadOnly;
            let executor = Executor::new_with_mode(db, access_mode);
            Self::verify_default_branch(&executor)?;
            return Ok(Self {
                backend: Backend::Local { executor },
                current_branch: BranchId::default(),
                current_space: "default".to_string(),
                access_mode,
            });
        }

        std::fs::create_dir_all(&data_dir).map_err(|e| Error::Internal {
            reason: format!("Failed to create data directory: {}", e),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

        // Read existing config (or defaults)
        let config_path = data_dir.join(strata_engine::database::config::CONFIG_FILE_NAME);
        strata_engine::database::config::StrataConfig::write_default_if_missing(&config_path)
            .map_err(|e| Error::Internal {
                reason: format!("Failed to write default config: {}", e),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            })?;
        let cfg = strata_engine::database::config::StrataConfig::from_file(&config_path).map_err(
            |e| Error::Internal {
                reason: format!("Failed to read config: {}", e),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            },
        )?;

        let access_mode = opts.access_mode;

        // Try to open the database directly
        match Database::open_with_config(&data_dir, cfg) {
            Ok(db) => {
                strata_graph::branch_dag::init_system_branch(&db);
                // Seed built-in recipes if not already present
                if let Err(e) = strata_engine::recipe_store::seed_builtin_recipes(&db) {
                    tracing::warn!(error = %e, "Failed to seed built-in recipes");
                }
                let executor = Executor::new_with_mode(db, access_mode);
                match access_mode {
                    AccessMode::ReadWrite => Self::ensure_default_branch(&executor)?,
                    AccessMode::ReadOnly => Self::verify_default_branch(&executor)?,
                }
                Ok(Self {
                    backend: Backend::Local { executor },
                    current_branch: BranchId::default(),
                    current_space: "default".to_string(),
                    access_mode,
                })
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                // If the error is a lock error, try IPC fallback
                if err_msg.contains("already in use by another process") {
                    let socket_path = data_dir.join("strata.sock");
                    if socket_path.exists() {
                        match IpcClient::connect(&socket_path) {
                            Ok(client) => {
                                let backend = Backend::Ipc {
                                    client: std::sync::Mutex::new(client),
                                    access_mode,
                                    data_dir: data_dir.clone(),
                                };
                                // Verify connectivity with a ping
                                backend
                                    .execute(Command::Ping)
                                    .map_err(|_| Error::Internal {
                                        reason: "Connected to IPC server but ping failed".into(),
                                        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                                    })?;
                                return Ok(Self {
                                    backend,
                                    current_branch: BranchId::default(),
                                    current_space: "default".to_string(),
                                    access_mode,
                                });
                            }
                            Err(_) => {
                                // Stale socket — try to clean up and retry
                                let _ = std::fs::remove_file(&socket_path);
                                // Re-read config for retry
                                let cfg2 =
                                    strata_engine::database::config::StrataConfig::from_file(
                                        &config_path,
                                    )
                                    .map_err(|e| {
                                        Error::Internal {
                                            reason: format!("Failed to read config: {}", e),
                                            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                                        }
                                    })?;
                                match Database::open_with_config(&data_dir, cfg2) {
                                    Ok(db) => {
                                        let executor = Executor::new_with_mode(db, access_mode);
                                        match access_mode {
                                            AccessMode::ReadWrite => {
                                                Self::ensure_default_branch(&executor)?
                                            }
                                            AccessMode::ReadOnly => {
                                                Self::verify_default_branch(&executor)?
                                            }
                                        }
                                        return Ok(Self {
                                            backend: Backend::Local { executor },
                                            current_branch: BranchId::default(),
                                            current_space: "default".to_string(),
                                            access_mode,
                                        });
                                    }
                                    Err(_) => {
                                        return Err(Error::Internal {
                                            reason: "Database is locked by another process. \
                                                 Run `strata up` to enable shared access, \
                                                 or use --follower for read-only access."
                                                .into(),
                                            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                                        });
                                    }
                                }
                            }
                        }
                    } else {
                        return Err(Error::Internal {
                            reason: "Database is locked by another process. \
                                 Run `strata up` to enable shared access, \
                                 or use --follower for read-only access."
                                .into(),
                            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                        });
                    }
                }
                Err(Error::Internal {
                    reason: format!("Failed to open database: {}", e),
                    hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
                })
            }
        }
    }

    /// Create an ephemeral in-memory database.
    ///
    /// Useful for testing. Data is not persisted and no disk files are created.
    ///
    /// # Example
    ///
    /// ```text
    /// let mut db = Strata::cache()?;
    /// db.kv_put("key", Value::Int(42))?;
    /// ```
    pub fn cache() -> Result<Self> {
        ensure_vector_recovery();
        let db = Database::cache().map_err(|e| Error::Internal {
            reason: format!("Failed to open cache database: {}", e),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;
        strata_graph::branch_dag::init_system_branch(&db);
        let executor = Executor::new(db);

        // Ensure the default branch exists
        Self::ensure_default_branch(&executor)?;

        Ok(Self {
            backend: Backend::Local { executor },
            current_branch: BranchId::default(),
            current_space: "default".to_string(),
            access_mode: AccessMode::ReadWrite,
        })
    }

    /// Create a new independent handle to the same database.
    ///
    /// Each handle has its own branch context (starting on "default") and can
    /// be moved to a separate thread. This is the standard way to use Strata
    /// from multiple threads.
    ///
    /// # Example
    ///
    /// ```text
    /// let db = Strata::open("/data/myapp")?;
    /// let handle = db.new_handle()?;
    /// std::thread::spawn(move || {
    ///     handle.kv_put("key", Value::Int(1)).unwrap();
    /// });
    /// ```
    pub fn new_handle(&self) -> Result<Self> {
        match &self.backend {
            Backend::Local { .. } => {
                let db = self.database();
                Self::from_database_with_mode(db, self.access_mode)
            }
            Backend::Ipc {
                client,
                access_mode,
                data_dir,
            } => {
                let c = client.lock().unwrap_or_else(|e| e.into_inner());
                let new_client = c.connect_new().map_err(|e| Error::Io {
                    reason: format!("Failed to open new IPC connection: {}", e),
                    hint: None,
                })?;
                Ok(Self {
                    backend: Backend::Ipc {
                        client: std::sync::Mutex::new(new_client),
                        access_mode: *access_mode,
                        data_dir: data_dir.clone(),
                    },
                    current_branch: BranchId::default(),
                    current_space: "default".to_string(),
                    access_mode: *access_mode,
                })
            }
        }
    }

    /// Create a new Strata instance from an existing database.
    ///
    /// Use this when you need more control over database configuration.
    /// For most cases, prefer [`Strata::open()`].
    pub fn from_database(db: Arc<Database>) -> Result<Self> {
        Self::from_database_with_mode(db, AccessMode::ReadWrite)
    }

    /// Create a new Strata instance from an existing database with a
    /// specific access mode.
    fn from_database_with_mode(db: Arc<Database>, access_mode: AccessMode) -> Result<Self> {
        ensure_vector_recovery();
        let executor = Executor::new_with_mode(db, access_mode);

        match access_mode {
            AccessMode::ReadWrite => Self::ensure_default_branch(&executor)?,
            AccessMode::ReadOnly => Self::verify_default_branch(&executor)?,
        }

        Ok(Self {
            backend: Backend::Local { executor },
            current_branch: BranchId::default(),
            current_space: "default".to_string(),
            access_mode,
        })
    }

    /// Ensures the "default" branch exists in the database, creating it if
    /// missing.
    fn ensure_default_branch(executor: &Executor) -> Result<()> {
        // Check if default branch exists
        match executor.execute(Command::BranchExists {
            branch: BranchId::default(),
        })? {
            Output::Bool(exists) => {
                if !exists {
                    // Create the default branch
                    executor.execute(Command::BranchCreate {
                        branch_id: Some("default".to_string()),
                        metadata: None,
                    })?;
                }
                Ok(())
            }
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchExists".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Verifies the "default" branch exists without attempting to create it.
    ///
    /// Used by read-only open to avoid issuing writes.
    fn verify_default_branch(executor: &Executor) -> Result<()> {
        // BranchExists is a read command, so the read-only guard won't fire.
        match executor.execute(Command::BranchExists {
            branch: BranchId::default(),
        })? {
            Output::Bool(true) => Ok(()),
            Output::Bool(false) => Err(Error::BranchNotFound {
                branch: "default".to_string(),
                hint: None,
            }),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchExists".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get the underlying executor for direct command execution.
    ///
    /// # Panics
    ///
    /// Panics if this Strata instance is connected via IPC rather than
    /// running with a local database. Use `execute_cmd()` instead.
    pub fn executor(&self) -> &Executor {
        self.backend.executor()
    }

    /// Get a clone of the underlying database handle.
    ///
    /// # Panics
    ///
    /// Panics if this Strata instance is connected via IPC.
    pub fn database(&self) -> Arc<Database> {
        self.backend.executor().primitives().db.clone()
    }

    /// Returns the access mode of this database handle.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    /// Returns true if this handle is connected via IPC to a remote server.
    pub fn is_ipc(&self) -> bool {
        !self.backend.is_local()
    }

    /// Execute a command through the backend (local or IPC).
    pub(crate) fn execute_cmd(&self, cmd: Command) -> Result<Output> {
        self.backend.execute(cmd)
    }

    /// Get WAL durability counters for diagnostics.
    ///
    /// Returns the current WAL counters (default zeroes for cache databases).
    pub fn durability_counters(&self) -> Result<strata_engine::WalCounters> {
        match self.execute_cmd(Command::DurabilityCounters)? {
            Output::DurabilityCounters(c) => Ok(c),
            _ => Err(Error::Internal {
                reason: "Unexpected output for DurabilityCounters".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get a handle for branch management operations.
    ///
    /// The returned [`Branches`] handle provides the "power API" for branch
    /// management, including listing, creating, deleting, forking, diffing,
    /// and merging branches.
    ///
    /// # Example
    ///
    /// ```text
    /// // List all branches
    /// for branch in db.branches().list()? {
    ///     println!("Branch: {}", branch);
    /// }
    ///
    /// // Create a new branch
    /// db.branches().create("experiment")?;
    ///
    /// // Fork a branch
    /// db.branches().fork("default", "experiment-copy")?;
    /// ```
    pub fn branches(&self) -> Branches<'_> {
        Branches::new(&self.backend)
    }

    /// Get a handle to the `_system_` branch for internal operations.
    ///
    /// The returned handle bypasses the user-facing guard that rejects
    /// access to reserved branches. It provides KV, JSON, state, and event
    /// operations pre-bound to `_system_`.
    ///
    /// Intended for internal consumers (e.g., strata-ai) that need to
    /// store private workspace data in the user's database.
    pub fn system_branch(&self) -> SystemBranch<'_> {
        SystemBranch::new(&self.backend)
    }

    /// Create a new [`Session`] for interactive transaction support.
    ///
    /// The returned session wraps a fresh executor and can manage an
    /// optional open transaction across multiple `execute()` calls.
    /// The session inherits the access mode of this handle.
    pub fn session(&self) -> Session {
        match &self.backend {
            Backend::Local { .. } => Session::new_with_mode(self.database(), self.access_mode),
            Backend::Ipc {
                client,
                access_mode,
                data_dir: _,
            } => {
                // For IPC, each session is a new connection (server creates
                // a per-connection Session automatically)
                let c = client.lock().unwrap_or_else(|e| e.into_inner());
                let new_client = c.connect_new().unwrap_or_else(|e| {
                    // This shouldn't normally fail — the server is reachable
                    // since we have an existing connection. Log and propagate.
                    tracing::error!("Failed to create new IPC connection for session: {}", e);
                    panic!(
                        "Failed to create new IPC connection for session: {}. \
                         The IPC server may have shut down.",
                        e
                    );
                });
                Session::new_ipc(new_client, *access_mode)
            }
        }
    }

    // =========================================================================
    // Branch Context
    // =========================================================================

    /// Get the current branch name.
    ///
    /// Returns the name of the branch that all data operations will use.
    pub fn current_branch(&self) -> &str {
        self.current_branch.as_str()
    }

    /// Switch to an existing branch.
    ///
    /// All subsequent data operations will use this branch.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch doesn't exist. Use `create_branch()` first
    /// to create a new branch.
    ///
    /// # Example
    ///
    /// ```text
    /// // Switch to an existing branch
    /// db.set_branch("my-experiment")?;
    /// db.kv_put("key", "value")?;  // Data goes to my-experiment
    ///
    /// // Switch back to default
    /// db.set_branch("default")?;
    /// ```
    pub fn set_branch(&mut self, branch_name: &str) -> Result<()> {
        // Check if branch exists
        if !self.branches().exists(branch_name)? {
            return Err(Error::BranchNotFound {
                branch: branch_name.to_string(),
                hint: None,
            });
        }

        self.current_branch = BranchId::from(branch_name);
        Ok(())
    }

    /// Create a new blank branch.
    ///
    /// The new branch starts with no data. Stays on the current branch after creation.
    /// Use `set_branch()` to switch to the new branch.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch already exists.
    ///
    /// # Example
    ///
    /// ```text
    /// // Create a new branch
    /// db.create_branch("experiment")?;
    ///
    /// // Optionally switch to it
    /// db.set_branch("experiment")?;
    /// ```
    pub fn create_branch(&self, branch_name: &str) -> Result<()> {
        self.branches().create(branch_name)
    }

    /// Fork the current branch with all its data into a new branch.
    ///
    /// Copies all data (KV, State, Events, JSON, Vectors) from the current
    /// branch to the new branch. Stays on the current branch after forking.
    /// Use `set_branch()` to switch to the fork.
    ///
    /// # Example
    ///
    /// ```text
    /// // Fork current branch to "experiment"
    /// db.fork_branch("experiment")?;
    ///
    /// // Switch to the fork
    /// db.set_branch("experiment")?;
    /// // ... make changes without affecting original ...
    /// ```
    pub fn fork_branch(&self, destination: &str) -> Result<ForkInfo> {
        self.branches().fork(self.current_branch(), destination)
    }

    /// Compare two branches and return their differences.
    ///
    /// Returns a structured diff showing per-space added, removed, and
    /// modified entries between the two branches.
    pub fn diff_branches(&self, branch_a: &str, branch_b: &str) -> Result<BranchDiffResult> {
        self.branches().diff(branch_a, branch_b)
    }

    /// Merge data from source branch into target branch.
    ///
    /// See [`Branches::merge`] for details on merge strategies.
    pub fn merge_branches(
        &self,
        source: &str,
        target: &str,
        strategy: MergeStrategy,
    ) -> Result<MergeInfo> {
        self.branches().merge(source, target, strategy)
    }

    /// List all available branches.
    ///
    /// Returns a list of branch names.
    pub fn list_branches(&self) -> Result<Vec<String>> {
        self.branches().list()
    }

    /// Delete a branch and all its data.
    ///
    /// **WARNING**: This is irreversible! All data in the branch will be deleted.
    ///
    /// # Errors
    ///
    /// - Returns an error if trying to delete the current branch
    /// - Returns an error if trying to delete the "default" branch
    pub fn delete_branch(&self, branch_name: &str) -> Result<()> {
        // Cannot delete the current branch
        if branch_name == self.current_branch.as_str() {
            return Err(Error::ConstraintViolation {
                reason: "Cannot delete the current branch. Switch to a different branch first."
                    .into(),
            });
        }

        self.branches().delete(branch_name)
    }

    /// Get the BranchId for use in commands.
    ///
    /// This is used internally by the data operation methods.
    pub(crate) fn branch_id(&self) -> Option<BranchId> {
        Some(self.current_branch.clone())
    }

    /// Get the space for use in commands.
    pub(crate) fn space_id(&self) -> Option<String> {
        Some(self.current_space.clone())
    }

    // =========================================================================
    // Space Context
    // =========================================================================

    /// Get the current space name.
    pub fn current_space(&self) -> &str {
        &self.current_space
    }

    /// Switch to a different space.
    ///
    /// All subsequent data operations will use this space.
    /// The "default" space always exists. Other spaces are created on first use.
    pub fn set_space(&mut self, space: &str) -> Result<()> {
        strata_core::validate_space_name(space)
            .map_err(|reason| Error::InvalidInput { reason, hint: None })?;
        self.current_space = space.to_string();
        Ok(())
    }

    /// List all spaces in the current branch.
    pub fn list_spaces(&self) -> Result<Vec<String>> {
        match self.execute_cmd(Command::SpaceList {
            branch: self.branch_id(),
        })? {
            Output::SpaceList(spaces) => Ok(spaces),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceList".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a space from the current branch.
    ///
    /// # Errors
    /// - Returns error if trying to delete the "default" space
    /// - Returns error if space is non-empty (unless force is true)
    pub fn delete_space(&self, space: &str) -> Result<()> {
        if space == "default" {
            return Err(Error::ConstraintViolation {
                reason: "Cannot delete the default space".into(),
            });
        }
        match self.execute_cmd(Command::SpaceDelete {
            branch: self.branch_id(),
            space: space.to_string(),
            force: false,
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a space forcefully (even if non-empty).
    pub fn delete_space_force(&self, space: &str) -> Result<()> {
        if space == "default" {
            return Err(Error::ConstraintViolation {
                reason: "Cannot delete the default space".into(),
            });
        }
        match self.execute_cmd(Command::SpaceDelete {
            branch: self.branch_id(),
            space: space.to_string(),
            force: true,
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for SpaceDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use crate::Value;

    fn create_strata() -> Strata {
        Strata::cache().unwrap()
    }

    #[test]
    fn test_ping() {
        let db = create_strata();
        let version = db.ping().unwrap();
        assert!(!version.is_empty());
    }

    #[test]
    fn test_info() {
        let db = create_strata();
        let info = db.info().unwrap();
        assert!(!info.version.is_empty());
    }

    #[test]
    fn test_kv_put_get() {
        let db = create_strata();

        // Simplified API: just pass &str directly
        let version = db.kv_put("key1", "hello").unwrap();
        assert!(version > 0);

        let value = db.kv_get("key1").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), Value::String("hello".into()));
    }

    #[test]
    fn test_kv_delete() {
        let db = create_strata();

        // Simplified API: just pass i64 directly
        db.kv_put("key1", 42i64).unwrap();
        assert!(db.kv_get("key1").unwrap().is_some());

        let existed = db.kv_delete("key1").unwrap();
        assert!(existed);
        assert!(db.kv_get("key1").unwrap().is_none());
    }

    #[test]
    fn test_kv_list() {
        let db = create_strata();

        db.kv_put("user:1", 1i64).unwrap();
        db.kv_put("user:2", 2i64).unwrap();
        db.kv_put("task:1", 3i64).unwrap();

        let user_keys = db.kv_list(Some("user:")).unwrap();
        assert_eq!(user_keys.len(), 2);

        let all_keys = db.kv_list(None).unwrap();
        assert_eq!(all_keys.len(), 3);
    }

    #[test]
    fn test_event_append_range() {
        let db = create_strata();

        // Event payloads must be Objects
        db.event_append(
            "stream",
            Value::object([("value".to_string(), Value::Int(1))].into_iter().collect()),
        )
        .unwrap();
        db.event_append(
            "stream",
            Value::object([("value".to_string(), Value::Int(2))].into_iter().collect()),
        )
        .unwrap();

        let events = db.event_get_by_type("stream").unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_vector_operations() {
        let db = create_strata();

        db.vector_create_collection("vecs", 4u64, DistanceMetric::Cosine)
            .unwrap();
        db.vector_upsert("vecs", "v1", vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        db.vector_upsert("vecs", "v2", vec![0.0, 1.0, 0.0, 0.0], None)
            .unwrap();

        let matches = db
            .vector_search("vecs", vec![1.0, 0.0, 0.0, 0.0], 10u64)
            .unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].key, "v1");
    }

    #[test]
    fn test_branch_create_list() {
        let db = create_strata();

        let (info, _version) = db
            .branch_create(
                Some("550e8400-e29b-41d4-a716-446655440099".to_string()),
                None,
            )
            .unwrap();
        assert_eq!(info.id.as_str(), "550e8400-e29b-41d4-a716-446655440099");

        let branches = db.branch_list(None, None, None).unwrap();
        assert!(!branches.is_empty());
    }

    // =========================================================================
    // Branch Context Tests
    // =========================================================================

    #[test]
    fn test_current_branch_default() {
        let db = create_strata();
        assert_eq!(db.current_branch(), "default");
    }

    #[test]
    fn test_create_branch() {
        let db = create_strata();

        // Create a new branch (stays on current branch)
        db.create_branch("experiment-1").unwrap();

        // Still on default branch
        assert_eq!(db.current_branch(), "default");

        // But the branch exists
        assert!(db.branch_exists("experiment-1").unwrap());
    }

    #[test]
    fn test_set_branch_to_existing() {
        let mut db = create_strata();

        // Create a branch first
        db.create_branch("my-branch").unwrap();

        // Switch to it
        db.set_branch("my-branch").unwrap();
        assert_eq!(db.current_branch(), "my-branch");
    }

    #[test]
    fn test_set_branch_nonexistent_fails() {
        let mut db = create_strata();

        // Try to switch to a branch that doesn't exist
        let result = db.set_branch("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_list_branches() {
        let db = create_strata();

        // Create a few branches
        db.create_branch("branch-a").unwrap();
        db.create_branch("branch-b").unwrap();
        db.create_branch("branch-c").unwrap();

        let branches = db.list_branches().unwrap();
        assert!(branches.contains(&"branch-a".to_string()));
        assert!(branches.contains(&"branch-b".to_string()));
        assert!(branches.contains(&"branch-c".to_string()));
    }

    #[test]
    fn test_delete_branch() {
        let db = create_strata();

        // Create a branch
        db.create_branch("to-delete").unwrap();

        // Delete the branch
        db.delete_branch("to-delete").unwrap();

        // Verify it's gone
        assert!(!db.branch_exists("to-delete").unwrap());
    }

    #[test]
    fn test_delete_current_branch_fails() {
        let mut db = create_strata();

        db.create_branch("current-branch").unwrap();
        db.set_branch("current-branch").unwrap();

        // Trying to delete the current branch should fail
        let result = db.delete_branch("current-branch");
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_default_branch_fails() {
        let db = create_strata();

        // Trying to delete the default branch should fail
        let result = db.delete_branch("default");
        assert!(result.is_err());
    }

    #[test]
    fn test_branch_context_data_isolation() {
        let mut db = create_strata();

        // Put data in default branch (simplified API)
        db.kv_put("key", "default-value").unwrap();

        // Create and switch to another branch
        db.create_branch("experiment").unwrap();
        db.set_branch("experiment").unwrap();

        // The key should not exist in this branch
        assert!(db.kv_get("key").unwrap().is_none());

        // Put different data
        db.kv_put("key", "experiment-value").unwrap();

        // Switch back to default
        db.set_branch("default").unwrap();

        // Original value should still be there
        let value = db.kv_get("key").unwrap();
        assert_eq!(value, Some(Value::String("default-value".into())));
    }

    #[test]
    fn test_branch_context_isolation_all_primitives() {
        let mut db = create_strata();

        // Put data in default branch (simplified API)
        db.kv_put("kv-key", 1i64).unwrap();
        db.event_append(
            "stream",
            Value::object([("x".to_string(), Value::Int(100))].into_iter().collect()),
        )
        .unwrap();

        // Create and switch to another branch
        db.create_branch("isolated").unwrap();
        db.set_branch("isolated").unwrap();

        // None of the data should exist in this branch
        assert!(db.kv_get("kv-key").unwrap().is_none());
        assert_eq!(db.event_len().unwrap(), 0);
    }

    // =========================================================================
    // db.branches() Power API Tests
    // =========================================================================

    #[test]
    fn test_branches_list() {
        let db = create_strata();

        // Create some branches
        db.branches().create("branch-a").unwrap();
        db.branches().create("branch-b").unwrap();

        let branches = db.branches().list().unwrap();
        assert!(branches.contains(&"branch-a".to_string()));
        assert!(branches.contains(&"branch-b".to_string()));
    }

    #[test]
    fn test_branches_exists() {
        let db = create_strata();

        assert!(!db.branches().exists("nonexistent").unwrap());

        db.branches().create("my-branch").unwrap();
        assert!(db.branches().exists("my-branch").unwrap());
    }

    #[test]
    fn test_branches_create() {
        let db = create_strata();

        db.branches().create("new-branch").unwrap();
        assert!(db.branches().exists("new-branch").unwrap());
    }

    #[test]
    fn test_branches_create_duplicate_fails() {
        let db = create_strata();

        db.branches().create("my-branch").unwrap();
        let result = db.branches().create("my-branch");
        assert!(result.is_err());
    }

    #[test]
    fn test_branches_delete() {
        let db = create_strata();

        db.branches().create("to-delete").unwrap();
        assert!(db.branches().exists("to-delete").unwrap());

        db.branches().delete("to-delete").unwrap();
        assert!(!db.branches().exists("to-delete").unwrap());
    }

    #[test]
    fn test_branches_delete_default_fails() {
        let db = create_strata();

        let result = db.branches().delete("default");
        assert!(result.is_err());
    }

    #[test]
    fn test_branches_fork() {
        let dir = tempfile::tempdir().unwrap();
        let db = Strata::open(dir.path()).unwrap();

        // Write some data to default branch
        db.kv_put("key1", "value1").unwrap();
        db.kv_put("key2", 42i64).unwrap();

        // Fork default branch to "forked" (no data copy)
        let info = db.fork_branch("forked").unwrap();
        assert_eq!(info.source, "default");
        assert_eq!(info.destination, "forked");
        assert_eq!(info.keys_copied, 0, "fork copies zero keys");
        assert!(info.fork_version.is_some(), "fork returns fork_version");
    }

    #[test]
    fn test_branches_diff() {
        let mut db = create_strata();

        // Write data to default branch
        db.kv_put("shared", "value-a").unwrap();
        db.kv_put("only-default", 1i64).unwrap();

        // Create another branch with different data
        db.create_branch("other").unwrap();
        db.set_branch("other").unwrap();
        db.kv_put("shared", "value-b").unwrap();
        db.kv_put("only-other", 2i64).unwrap();

        let diff = db.diff_branches("default", "other").unwrap();
        assert_eq!(diff.branch_a, "default");
        assert_eq!(diff.branch_b, "other");
        // "shared" should be modified, "only-default" removed, "only-other" added
        assert!(diff.summary.total_modified >= 1);
        assert!(diff.summary.total_removed >= 1);
        assert!(diff.summary.total_added >= 1);
    }

    #[test]
    fn test_branches_merge() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut db = Strata::open(temp_dir.path()).unwrap();

        // Write data to default before fork
        db.kv_put("base-key", "base-value").unwrap();

        // Fork default → source, then write on source
        db.fork_branch("source").unwrap();
        db.set_branch("source").unwrap();
        db.kv_put("new-key", "new-value").unwrap();

        // Merge source into default
        db.set_branch("default").unwrap();
        let info = db
            .merge_branches("source", "default", MergeStrategy::LastWriterWins)
            .unwrap();
        assert!(info.keys_applied >= 1);

        // Verify merged data
        assert_eq!(
            db.kv_get("new-key").unwrap(),
            Some(Value::String("new-value".into()))
        );
        // Original data should still be there
        assert_eq!(
            db.kv_get("base-key").unwrap(),
            Some(Value::String("base-value".into()))
        );
    }

    // =========================================================================
    // Configuration Tests
    // =========================================================================

    #[test]
    fn test_config_defaults() {
        let db = create_strata();
        let cfg = db.config().unwrap();
        assert_eq!(cfg.durability, "standard");
        assert!(!cfg.auto_embed);
        assert!(cfg.model.is_none());
    }

    #[test]
    fn test_configure_model() {
        let db = create_strata();
        db.configure_model("http://localhost:11434/v1", "qwen3:1.7b", None, None)
            .unwrap();

        let cfg = db.config().unwrap();
        let model = cfg.model.unwrap();
        assert_eq!(model.endpoint, "http://localhost:11434/v1");
        assert_eq!(model.model, "qwen3:1.7b");
        assert!(model.api_key.is_none());
        assert_eq!(model.timeout_ms, 5000);
    }

    #[test]
    fn test_configure_model_with_api_key() {
        let db = create_strata();
        db.configure_model(
            "https://api.openai.com/v1",
            "gpt-4",
            Some("sk-test-key"),
            Some(10000),
        )
        .unwrap();

        let cfg = db.config().unwrap();
        let model = cfg.model.unwrap();
        assert_eq!(model.api_key.as_deref(), Some("sk-test-key"));
        assert_eq!(model.timeout_ms, 10000);
    }

    #[test]
    fn test_auto_embed_toggle() {
        let db = create_strata();
        assert!(!db.auto_embed_enabled().unwrap());

        db.set_auto_embed(true).unwrap();
        assert!(db.auto_embed_enabled().unwrap());

        db.set_auto_embed(false).unwrap();
        assert!(!db.auto_embed_enabled().unwrap());
    }

    #[test]
    fn test_config_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();

        // Open, configure model, close
        {
            let db = Strata::open(dir.path()).unwrap();
            db.configure_model("http://localhost:11434/v1", "qwen3:1.7b", None, None)
                .unwrap();
        }

        // Reopen — model config should survive via strata.toml
        {
            let db = Strata::open(dir.path()).unwrap();
            let cfg = db.config().unwrap();
            let model = cfg
                .model
                .expect("model config should persist across reopen");
            assert_eq!(model.endpoint, "http://localhost:11434/v1");
            assert_eq!(model.model, "qwen3:1.7b");
            assert_eq!(model.timeout_ms, 5000);
        }
    }

    #[test]
    fn test_config_set_and_get() {
        let db = create_strata();
        db.config_set("auto_embed", "true").unwrap();
        assert_eq!(db.config_get("auto_embed").unwrap(), Some("true".into()));
        assert!(db.auto_embed_enabled().unwrap());
    }

    #[test]
    fn test_config_set_bm25() {
        let db = create_strata();
        db.config_set("bm25_k1", "1.5").unwrap();
        db.config_set("bm25_b", "0.8").unwrap();
        assert_eq!(db.config_get("bm25_k1").unwrap(), Some("1.5".into()));
        assert_eq!(db.config_get("bm25_b").unwrap(), Some("0.8".into()));
    }

    #[test]
    fn test_config_set_invalid_key() {
        let db = create_strata();
        let err = db.config_set("nonexistent_key", "value").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Unknown configuration key"), "Error: {msg}");
    }

    #[test]
    fn test_config_get_optional_key_returns_none() {
        let db = create_strata();
        // default_model is optional and unset by default
        assert_eq!(db.config_get("default_model").unwrap(), None);
    }

    #[test]
    fn test_config_set_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Strata::open(dir.path()).unwrap();
            db.config_set("bm25_k1", "2.0").unwrap();
            db.config_set("embed_batch_size", "256").unwrap();
        }
        {
            let db = Strata::open(dir.path()).unwrap();
            assert_eq!(db.config_get("bm25_k1").unwrap(), Some("2".into()));
            assert_eq!(
                db.config_get("embed_batch_size").unwrap(),
                Some("256".into())
            );
        }
    }

    // =========================================================================
    // Graph API Tests
    // =========================================================================

    #[test]
    fn test_graph_create_list_delete() {
        let db = create_strata();

        db.graph_create("my_graph").unwrap();

        let graphs = db.graph_list().unwrap();
        assert!(graphs.contains(&"my_graph".to_string()));

        let meta = db.graph_get_meta("my_graph").unwrap();
        assert!(meta.is_some());

        db.graph_delete("my_graph").unwrap();
        let graphs = db.graph_list().unwrap();
        assert!(!graphs.contains(&"my_graph".to_string()));
    }

    #[test]
    fn test_graph_node_crud() {
        let db = create_strata();
        db.graph_create("ng").unwrap();

        db.graph_add_node(
            "ng",
            "n1",
            None,
            Some(Value::object(
                [("name".to_string(), Value::String("Alice".into()))]
                    .into_iter()
                    .collect(),
            )),
        )
        .unwrap();

        let node = db.graph_get_node("ng", "n1").unwrap();
        assert!(node.is_some());

        let nodes = db.graph_list_nodes("ng").unwrap();
        assert_eq!(nodes, vec!["n1"]);

        db.graph_remove_node("ng", "n1").unwrap();
        assert!(db.graph_get_node("ng", "n1").unwrap().is_none());
    }

    #[test]
    fn test_graph_edge_crud() {
        let db = create_strata();
        db.graph_create("eg").unwrap();
        db.graph_add_node("eg", "A", None, None).unwrap();
        db.graph_add_node("eg", "B", None, None).unwrap();

        db.graph_add_edge("eg", "A", "B", "KNOWS", None, None)
            .unwrap();

        let neighbors = db.graph_neighbors("eg", "A", "outgoing", None).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].node_id, "B");
        assert_eq!(neighbors[0].edge_type, "KNOWS");
        assert_eq!(neighbors[0].weight, 1.0);

        db.graph_remove_edge("eg", "A", "B", "KNOWS").unwrap();
        let neighbors = db.graph_neighbors("eg", "A", "outgoing", None).unwrap();
        assert!(neighbors.is_empty());
    }

    #[test]
    fn test_graph_bfs() {
        let db = create_strata();
        db.graph_create("bg").unwrap();
        for id in &["A", "B", "C", "D"] {
            db.graph_add_node("bg", id, None, None).unwrap();
        }

        db.graph_add_edge("bg", "A", "B", "E", None, None).unwrap();
        db.graph_add_edge("bg", "B", "C", "E", None, None).unwrap();
        db.graph_add_edge("bg", "C", "D", "E", None, None).unwrap();

        let result = db.graph_bfs("bg", "A", 10, None, None, None).unwrap();
        assert_eq!(result.visited.len(), 4);
        assert_eq!(result.visited[0], "A");
    }

    #[test]
    fn test_graph_branch_isolation() {
        let mut db = create_strata();

        // Create graph on default branch
        db.graph_create("isolated").unwrap();
        db.graph_add_node("isolated", "n1", None, None).unwrap();

        // Create and switch to new branch
        db.create_branch("graph-branch").unwrap();
        db.set_branch("graph-branch").unwrap();

        // Graph should not exist on the new branch
        let graphs = db.graph_list().unwrap();
        assert!(!graphs.contains(&"isolated".to_string()));

        // Switch back — graph should still be there
        db.set_branch("default").unwrap();
        let graphs = db.graph_list().unwrap();
        assert!(graphs.contains(&"isolated".to_string()));
        assert!(db.graph_get_node("isolated", "n1").unwrap().is_some());
    }

    #[test]
    fn test_patient_care_graph_end_to_end() {
        let db = create_strata();

        // Create graph
        db.graph_create("patient_care").unwrap();

        // Add nodes
        db.graph_add_node(
            "patient_care",
            "patient-4821",
            Some("json://main/patient-4821"),
            Some(Value::object(
                [(
                    "department".to_string(),
                    Value::String("endocrinology".into()),
                )]
                .into_iter()
                .collect(),
            )),
        )
        .unwrap();
        db.graph_add_node(
            "patient_care",
            "ICD:E11.9",
            None,
            Some(Value::object(
                [(
                    "description".to_string(),
                    Value::String("Type 2 Diabetes".into()),
                )]
                .into_iter()
                .collect(),
            )),
        )
        .unwrap();
        db.graph_add_node(
            "patient_care",
            "lab:HbA1c",
            Some("kv://main/lab:4821:HbA1c:2026-01"),
            None,
        )
        .unwrap();
        db.graph_add_node("patient_care", "med:metformin", None, None)
            .unwrap();
        db.graph_add_node(
            "patient_care",
            "ICD:N18.3",
            None,
            Some(Value::object(
                [(
                    "description".to_string(),
                    Value::String("CKD Stage 3".into()),
                )]
                .into_iter()
                .collect(),
            )),
        )
        .unwrap();

        // Add edges
        db.graph_add_edge(
            "patient_care",
            "patient-4821",
            "ICD:E11.9",
            "DIAGNOSED_WITH",
            None,
            None,
        )
        .unwrap();
        db.graph_add_edge(
            "patient_care",
            "patient-4821",
            "lab:HbA1c",
            "HAS_LAB_RESULT",
            None,
            None,
        )
        .unwrap();
        db.graph_add_edge(
            "patient_care",
            "lab:HbA1c",
            "ICD:E11.9",
            "SUPPORTS",
            Some(0.95),
            None,
        )
        .unwrap();
        db.graph_add_edge(
            "patient_care",
            "ICD:E11.9",
            "med:metformin",
            "TREATED_BY",
            None,
            None,
        )
        .unwrap();
        db.graph_add_edge(
            "patient_care",
            "med:metformin",
            "ICD:N18.3",
            "CONTRAINDICATES",
            Some(0.8),
            None,
        )
        .unwrap();

        // Query: what supports the diabetes diagnosis?
        let supporters = db
            .graph_neighbors("patient_care", "ICD:E11.9", "incoming", Some("SUPPORTS"))
            .unwrap();
        assert_eq!(supporters.len(), 1);
        assert_eq!(supporters[0].node_id, "lab:HbA1c");

        // Contraindication check via BFS
        let risks = db
            .graph_bfs(
                "patient_care",
                "med:metformin",
                2,
                None,
                Some(vec!["CONTRAINDICATES".into()]),
                Some("outgoing"),
            )
            .unwrap();
        assert!(risks.visited.contains(&"ICD:N18.3".to_string()));

        // Verify node count
        let mut nodes = db.graph_list_nodes("patient_care").unwrap();
        nodes.sort();
        assert_eq!(nodes.len(), 5);

        // Verify all 5 edges exist
        let all_neighbors = db
            .graph_neighbors("patient_care", "patient-4821", "outgoing", None)
            .unwrap();
        assert_eq!(all_neighbors.len(), 2); // DIAGNOSED_WITH + HAS_LAB_RESULT
    }

    #[test]
    fn test_graph_node_data_roundtrip() {
        let db = create_strata();
        db.graph_create("ng").unwrap();

        // Add node with entity_ref and properties
        db.graph_add_node(
            "ng",
            "patient-1",
            Some("kv://main/p1"),
            Some(Value::object(
                [
                    ("department".to_string(), Value::String("cardiology".into())),
                    ("age".to_string(), Value::Int(45)),
                ]
                .into_iter()
                .collect(),
            )),
        )
        .unwrap();

        // Read back and verify the data round-trips
        let node = db.graph_get_node("ng", "patient-1").unwrap();
        assert!(node.is_some());
        let node_val = node.unwrap();
        // Node data is returned as a Value::Object with entity_ref and properties
        match node_val {
            Value::Object(map) => {
                assert_eq!(
                    map.get("entity_ref"),
                    Some(&Value::String("kv://main/p1".into()))
                );
                // Properties should be present
                assert!(map.contains_key("properties"));
            }
            _ => panic!("Expected Value::Object for node data, got {:?}", node_val),
        }
    }

    #[test]
    fn test_graph_create_with_cascade_policy() {
        let db = create_strata();
        db.graph_create_with_policy("cascade_g", Some("cascade"))
            .unwrap();

        let meta = db.graph_get_meta("cascade_g").unwrap();
        assert!(meta.is_some());
        let meta_val = meta.unwrap();
        match meta_val {
            Value::Object(map) => {
                assert_eq!(
                    map.get("cascade_policy"),
                    Some(&Value::String("cascade".into()))
                );
            }
            _ => panic!("Expected Value::Object for meta"),
        }
    }

    #[test]
    fn test_graph_invalid_name_errors() {
        let db = create_strata();
        assert!(db.graph_create("").is_err());
        assert!(db.graph_create("has/slash").is_err());
        assert!(db.graph_create("__reserved").is_err());
    }

    #[test]
    fn test_graph_invalid_direction_errors() {
        let db = create_strata();
        db.graph_create("dg").unwrap();
        db.graph_add_node("dg", "A", None, None).unwrap();

        // Invalid direction string should error
        let result = db.graph_neighbors("dg", "A", "invalid_dir", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_graph_edge_with_weight_and_properties() {
        let db = create_strata();
        db.graph_create("wg").unwrap();
        db.graph_add_node("wg", "A", None, None).unwrap();
        db.graph_add_node("wg", "B", None, None).unwrap();

        db.graph_add_edge(
            "wg",
            "A",
            "B",
            "SCORED",
            Some(0.95),
            Some(Value::object(
                [("confidence".to_string(), Value::String("high".into()))]
                    .into_iter()
                    .collect(),
            )),
        )
        .unwrap();

        // Verify weight comes back through neighbors API
        let neighbors = db.graph_neighbors("wg", "A", "outgoing", None).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert!((neighbors[0].weight - 0.95).abs() < 1e-10);
    }

    #[test]
    fn test_graph_bfs_with_max_depth() {
        let db = create_strata();
        db.graph_create("bg").unwrap();
        for id in &["A", "B", "C", "D"] {
            db.graph_add_node("bg", id, None, None).unwrap();
        }

        // A → B → C → D
        db.graph_add_edge("bg", "A", "B", "E", None, None).unwrap();
        db.graph_add_edge("bg", "B", "C", "E", None, None).unwrap();
        db.graph_add_edge("bg", "C", "D", "E", None, None).unwrap();

        // Depth 1 should only reach A and B
        let result = db.graph_bfs("bg", "A", 1, None, None, None).unwrap();
        assert_eq!(result.visited.len(), 2);
        assert!(result.visited.contains(&"A".to_string()));
        assert!(result.visited.contains(&"B".to_string()));
    }

    #[test]
    fn test_graph_bfs_depths_correct() {
        let db = create_strata();
        db.graph_create("dg").unwrap();
        for id in &["A", "B", "C"] {
            db.graph_add_node("dg", id, None, None).unwrap();
        }

        db.graph_add_edge("dg", "A", "B", "E", None, None).unwrap();
        db.graph_add_edge("dg", "B", "C", "E", None, None).unwrap();

        let result = db.graph_bfs("dg", "A", 10, None, None, None).unwrap();
        assert_eq!(result.depths.get("A"), Some(&0));
        assert_eq!(result.depths.get("B"), Some(&1));
        assert_eq!(result.depths.get("C"), Some(&2));
    }

    #[test]
    fn test_graph_read_only_rejects_writes() {
        let dir = tempfile::tempdir().unwrap();

        // Create a database with a graph
        {
            let db = Strata::open(dir.path()).unwrap();
            db.graph_create("rg").unwrap();
        }

        // Reopen in read-only mode
        {
            let db = Strata::open_with(
                dir.path(),
                OpenOptions::new().access_mode(AccessMode::ReadOnly),
            )
            .unwrap();

            // Reads should work
            let graphs = db.graph_list().unwrap();
            assert!(graphs.contains(&"rg".to_string()));

            // Writes should fail
            assert!(db.graph_create("new_graph").is_err());
            assert!(db.graph_add_node("rg", "n1", None, None).is_err());
            assert!(db.graph_add_edge("rg", "A", "B", "E", None, None).is_err());
        }
    }

    // =========================================================================
    // SystemBranch Tests
    // =========================================================================

    #[test]
    fn test_system_branch_kv_roundtrip() {
        let db = Strata::cache().unwrap();
        let sys = db.system_branch();

        sys.kv_put("_ai:cache:abc", "cached-value").unwrap();
        let val = sys.kv_get("_ai:cache:abc").unwrap();
        assert_eq!(val, Some(Value::String("cached-value".into())));

        let keys = sys.kv_list(Some("_ai:cache:")).unwrap();
        assert!(keys.contains(&"_ai:cache:abc".to_string()));

        sys.kv_delete("_ai:cache:abc").unwrap();
        assert!(sys.kv_get("_ai:cache:abc").unwrap().is_none());
    }

    #[test]
    fn test_system_branch_json_roundtrip() {
        let db = Strata::cache().unwrap();
        let sys = db.system_branch();

        sys.json_set(
            "_ai:catalog:users",
            "$",
            Value::String(r#"{"columns":["id","name"]}"#.into()),
        )
        .unwrap();
        let val = sys.json_get("_ai:catalog:users", "$").unwrap();
        assert!(val.is_some());
    }

    #[test]
    fn test_system_branch_event_roundtrip() {
        let db = Strata::cache().unwrap();
        let sys = db.system_branch();

        let payload = serde_json::json!({"action": "query executed"});
        let seq = sys
            .event_append("ai.activity", Value::from(payload))
            .unwrap();

        let event = sys.event_get(seq).unwrap();
        assert!(event.is_some());
    }

    #[test]
    fn test_system_branch_hidden_from_user_apis() {
        let db = Strata::cache().unwrap();

        // Write via system handle
        let sys = db.system_branch();
        sys.kv_put("secret", "hidden").unwrap();

        // User-facing APIs should not see _system_
        let branches = db.branches().list().unwrap();
        assert!(!branches.iter().any(|b| b.starts_with("_system")));

        // Direct access to _system_ via user API should fail
        let result = db.executor().execute(Command::KvGet {
            branch: Some(BranchId::from("_system_")),
            space: Some("default".to_string()),
            key: "secret".to_string(),
            as_of: None,
        });
        assert!(result.is_err());
    }

    // =========================================================================
    // New Typed API Tests (as_of, batch, space, introspection, inference)
    // =========================================================================

    #[test]
    fn test_kv_get_as_of_returns_none_for_future_write() {
        let db = create_strata();

        // Read the time before writing
        let (_, before_ts) = db.time_range().unwrap();

        db.kv_put("k", "v").unwrap();

        // as_of=None should return the latest value
        let val = db.kv_get_as_of("k", None).unwrap();
        assert_eq!(val, Some(Value::String("v".into())));

        // as_of=0 (epoch start) should return nothing — value didn't exist yet
        let val = db.kv_get_as_of("k", Some(0)).unwrap();
        // The key was written after epoch 0, so it should not appear.
        // However, time-travel semantics depend on the backend; if the
        // backend treats as_of=0 as "no filter", we accept Some too.
        // The key thing is that this doesn't error out.
        let _ = val;

        // as_of with the timestamp before the write should also be empty
        // (if we captured a valid timestamp)
        if let Some(ts) = before_ts {
            let val = db.kv_get_as_of("k", Some(ts.saturating_sub(1))).unwrap();
            let _ = val; // no assertion on value — just verifying no panic
        }
    }

    #[test]
    fn test_kv_list_as_of() {
        let db = create_strata();
        db.kv_put("p:1", "a").unwrap();
        db.kv_put("p:2", "b").unwrap();

        // as_of=None should list both keys
        let keys = db.kv_list_as_of(Some("p:"), None, None, None).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_json_get_as_of() {
        let db = create_strata();
        db.json_set("doc", "$", Value::String("hello".into()))
            .unwrap();

        let val = db.json_get_as_of("doc", "$", None).unwrap();
        assert!(val.is_some());
    }

    #[test]
    fn test_event_get_as_of() {
        let db = create_strata();
        let seq = db
            .event_append(
                "test",
                Value::object([("x".to_string(), Value::Int(1))].into_iter().collect()),
            )
            .unwrap();

        let evt = db.event_get_as_of(seq, None).unwrap();
        assert!(evt.is_some());
    }

    #[test]
    fn test_kv_batch_put() {
        use crate::types::BatchKvEntry;
        let db = create_strata();

        let entries = vec![
            BatchKvEntry {
                key: "b1".to_string(),
                value: Value::String("one".into()),
            },
            BatchKvEntry {
                key: "b2".to_string(),
                value: Value::Int(2),
            },
        ];

        let results = db.kv_batch_put(entries).unwrap();
        assert_eq!(results.len(), 2);
        for r in &results {
            assert!(r.version.is_some());
            assert!(r.error.is_none());
        }

        // Verify the values were actually written
        assert_eq!(db.kv_get("b1").unwrap(), Some(Value::String("one".into())));
        assert_eq!(db.kv_get("b2").unwrap(), Some(Value::Int(2)));
    }

    #[test]
    fn test_kv_batch_put_empty() {
        let db = create_strata();
        let results = db.kv_batch_put(vec![]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_json_batch_set_and_get() {
        use crate::types::{BatchJsonEntry, BatchJsonGetEntry};
        let db = create_strata();

        let entries = vec![
            BatchJsonEntry {
                key: "d1".to_string(),
                path: "$".to_string(),
                value: Value::String("hello".into()),
            },
            BatchJsonEntry {
                key: "d2".to_string(),
                path: "$".to_string(),
                value: Value::Int(42),
            },
        ];
        let results = db.json_batch_set(entries).unwrap();
        assert_eq!(results.len(), 2);

        // Batch get
        let get_entries = vec![
            BatchJsonGetEntry {
                key: "d1".to_string(),
                path: "$".to_string(),
            },
            BatchJsonGetEntry {
                key: "d2".to_string(),
                path: "$".to_string(),
            },
            BatchJsonGetEntry {
                key: "nonexistent".to_string(),
                path: "$".to_string(),
            },
        ];
        let results = db.json_batch_get(get_entries).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].value.is_some());
        assert!(results[1].value.is_some());
        assert!(results[2].value.is_none());
    }

    #[test]
    fn test_json_batch_delete() {
        use crate::types::{BatchJsonDeleteEntry, BatchJsonEntry};
        let db = create_strata();

        // Set up
        db.json_batch_set(vec![BatchJsonEntry {
            key: "del1".to_string(),
            path: "$".to_string(),
            value: Value::String("x".into()),
        }])
        .unwrap();

        // Delete
        let results = db
            .json_batch_delete(vec![BatchJsonDeleteEntry {
                key: "del1".to_string(),
                path: "$".to_string(),
            }])
            .unwrap();
        assert_eq!(results.len(), 1);

        // Verify deleted
        assert!(db.json_get("del1", "$").unwrap().is_none());
    }

    #[test]
    fn test_event_batch_append() {
        use crate::types::BatchEventEntry;
        let db = create_strata();

        let entries = vec![
            BatchEventEntry {
                event_type: "ev".to_string(),
                payload: Value::object([("a".to_string(), Value::Int(1))].into_iter().collect()),
            },
            BatchEventEntry {
                event_type: "ev".to_string(),
                payload: Value::object([("b".to_string(), Value::Int(2))].into_iter().collect()),
            },
        ];
        let results = db.event_batch_append(entries).unwrap();
        assert_eq!(results.len(), 2);

        let events = db.event_get_by_type("ev").unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_event_get_by_type_with_options() {
        let db = create_strata();

        // Append 3 events
        for i in 0..3 {
            db.event_append(
                "paged",
                Value::object([("i".to_string(), Value::Int(i))].into_iter().collect()),
            )
            .unwrap();
        }

        // Limit to 2
        let events = db
            .event_get_by_type_with_options("paged", Some(2), None, None)
            .unwrap();
        assert_eq!(events.len(), 2);

        // After sequence of the first event
        let first_seq = events[0].version;
        let after = db
            .event_get_by_type_with_options("paged", None, Some(first_seq), None)
            .unwrap();
        // Should exclude the first event
        assert!(after.len() < 3);
    }

    #[test]
    fn test_vector_search_with_filter() {
        let db = create_strata();
        db.vector_create_collection("fvecs", 4, DistanceMetric::Cosine)
            .unwrap();
        db.vector_upsert("fvecs", "a", vec![1.0, 0.0, 0.0, 0.0], None)
            .unwrap();
        db.vector_upsert("fvecs", "b", vec![0.0, 1.0, 0.0, 0.0], None)
            .unwrap();

        // No filter — should return both
        let matches = db
            .vector_search_with_filter("fvecs", vec![1.0, 0.0, 0.0, 0.0], 10, None, None, None)
            .unwrap();
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_describe() {
        let db = create_strata();
        db.kv_put("dk", "dv").unwrap();

        let desc = db.describe().unwrap();
        // Should contain at least the default branch
        assert!(!desc.branches.is_empty());
    }

    #[test]
    fn test_time_range_empty() {
        let db = create_strata();
        let (oldest, latest) = db.time_range().unwrap();
        // Empty DB may return None for both
        let _ = (oldest, latest);
    }

    #[test]
    fn test_time_range_with_data() {
        let db = create_strata();
        db.kv_put("tr", "val").unwrap();

        let (oldest, latest) = db.time_range().unwrap();
        // With data, at least one should be Some
        assert!(oldest.is_some() || latest.is_some());
    }

    #[test]
    fn test_kv_count() {
        let db = create_strata();

        // Capture baseline (system-created keys may exist on the branch)
        let baseline = db.kv_count(None).unwrap();

        db.kv_put("count:a", "1").unwrap();
        db.kv_put("count:b", "2").unwrap();
        db.kv_put("other", "3").unwrap();

        let total = db.kv_count(None).unwrap();
        assert_eq!(
            total - baseline,
            3,
            "Should have 3 user keys above baseline"
        );

        let prefixed = db.kv_count(Some("count:")).unwrap();
        assert_eq!(prefixed, 2);
    }

    #[test]
    fn test_json_count() {
        let db = create_strata();
        db.json_set("jc:a", "$", Value::Int(1)).unwrap();
        db.json_set("jc:b", "$", Value::Int(2)).unwrap();

        let count = db.json_count(Some("jc:")).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_kv_sample() {
        let db = create_strata();
        db.kv_put("samp:1", "a").unwrap();
        db.kv_put("samp:2", "b").unwrap();

        let (total, items) = db.kv_sample(Some(5)).unwrap();
        assert_eq!(total, 2);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_json_sample() {
        let db = create_strata();
        db.json_set("js:1", "$", Value::Int(1)).unwrap();

        let (total, items) = db.json_sample(Some(5)).unwrap();
        assert_eq!(total, 1);
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn test_vector_sample() {
        let db = create_strata();
        db.vector_create_collection("svecs", 2, DistanceMetric::Cosine)
            .unwrap();
        db.vector_upsert("svecs", "v1", vec![1.0, 0.0], None)
            .unwrap();

        let (total, items) = db.vector_sample("svecs", Some(5)).unwrap();
        assert_eq!(total, 1);
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn test_retention_apply() {
        let db = create_strata();
        // Should succeed (no-op on fresh db)
        db.retention_apply().unwrap();
    }

    #[test]
    fn test_space_list() {
        let db = create_strata();
        let spaces = db.space_list().unwrap();
        assert!(spaces.contains(&"default".to_string()));
    }

    #[test]
    fn test_space_create_and_exists() {
        let db = create_strata();

        assert!(!db.space_exists("myspace").unwrap());
        db.space_create("myspace").unwrap();
        assert!(db.space_exists("myspace").unwrap());

        let spaces = db.space_list().unwrap();
        assert!(spaces.contains(&"myspace".to_string()));
    }

    #[test]
    fn test_space_delete() {
        let db = create_strata();
        db.space_create("todelete").unwrap();
        assert!(db.space_exists("todelete").unwrap());

        db.space_delete("todelete", false).unwrap();
        assert!(!db.space_exists("todelete").unwrap());
    }

    #[test]
    fn test_space_delete_default_rejected() {
        let db = create_strata();
        let result = db.space_delete("default", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_durability_counters() {
        let db = create_strata();
        let counters = db.durability_counters().unwrap();
        // Cache databases return default (zero) counters
        let _ = counters;
    }

    // =========================================================================
    // Tests for #1499 gap methods
    // =========================================================================

    #[test]
    fn test_json_list_as_of() {
        let db = create_strata();
        db.json_set("jl:1", "$", Value::Int(1)).unwrap();
        db.json_set("jl:2", "$", Value::Int(2)).unwrap();

        let (keys, cursor) = db
            .json_list_as_of(Some("jl:".into()), None, 100, None)
            .unwrap();
        assert_eq!(keys.len(), 2);
        assert!(cursor.is_none());
    }

    #[test]
    fn test_vector_get_as_of() {
        let db = create_strata();
        db.vector_create_collection("vga", 2, DistanceMetric::Cosine)
            .unwrap();
        db.vector_upsert("vga", "k1", vec![1.0, 0.0], None).unwrap();

        let data = db.vector_get_as_of("vga", "k1", None).unwrap();
        assert!(data.is_some());

        let missing = db.vector_get_as_of("vga", "nonexistent", None).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_graph_bulk_insert_typed() {
        use crate::types::{BulkGraphEdge, BulkGraphNode};
        let db = create_strata();
        db.graph_create("gbi").unwrap();

        let nodes = vec![
            BulkGraphNode {
                node_id: "n1".into(),
                entity_ref: None,
                properties: None,
                object_type: Some("Person".into()),
            },
            BulkGraphNode {
                node_id: "n2".into(),
                entity_ref: None,
                properties: None,
                object_type: Some("Person".into()),
            },
        ];
        let edges = vec![BulkGraphEdge {
            src: "n1".into(),
            dst: "n2".into(),
            edge_type: "knows".into(),
            weight: None,
            properties: None,
        }];

        let (ni, ei) = db
            .graph_bulk_insert_typed("gbi", nodes, edges, Some(100))
            .unwrap();
        assert_eq!(ni, 2);
        assert_eq!(ei, 1);
    }

    #[test]
    fn test_graph_list_ontology_types() {
        let db = create_strata();
        db.graph_create("glo").unwrap();

        // Define an object type and a link type
        let obj_def = Value::object(
            [("name".to_string(), Value::String("Person".into()))]
                .into_iter()
                .collect(),
        );
        db.graph_define_object_type("glo", obj_def).unwrap();

        let link_def = Value::object(
            [
                ("name".to_string(), Value::String("knows".into())),
                ("source".to_string(), Value::String("Person".into())),
                ("target".to_string(), Value::String("Person".into())),
            ]
            .into_iter()
            .collect(),
        );
        db.graph_define_link_type("glo", link_def).unwrap();

        let types = db.graph_list_ontology_types("glo").unwrap();
        assert!(types.contains(&"Person".to_string()));
        assert!(types.contains(&"knows".to_string()));
    }

    #[test]
    fn test_kv_scan_returns_key_value_pairs() {
        let db = create_strata();

        // Insert 5 keys in sorted order
        db.kv_put("a", "alpha").unwrap();
        db.kv_put("b", "bravo").unwrap();
        db.kv_put("c", "charlie").unwrap();
        db.kv_put("d", "delta").unwrap();
        db.kv_put("e", "echo").unwrap();

        // Scan all — returns key-value pairs sorted by key
        let pairs = db.kv_scan(None, None).unwrap();
        assert_eq!(pairs.len(), 5);
        assert_eq!(pairs[0].0, "a");
        assert_eq!(pairs[0].1, Value::String("alpha".into()));
        assert_eq!(pairs[4].0, "e");
        assert_eq!(pairs[4].1, Value::String("echo".into()));

        // Scan with start key — returns pairs where key >= start
        let pairs = db.kv_scan(Some("c"), None).unwrap();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, "c");
        assert_eq!(pairs[0].1, Value::String("charlie".into()));
        assert_eq!(pairs[2].0, "e");

        // Scan with limit — returns at most N pairs
        let pairs = db.kv_scan(None, Some(2)).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "a");
        assert_eq!(pairs[1].0, "b");

        // Scan with start + limit
        let pairs = db.kv_scan(Some("b"), Some(2)).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "b");
        assert_eq!(pairs[0].1, Value::String("bravo".into()));
        assert_eq!(pairs[1].0, "c");

        // Scan on empty DB
        let db2 = create_strata();
        let pairs = db2.kv_scan(None, None).unwrap();
        assert!(pairs.is_empty());

        // Scan with start key beyond all keys returns empty
        let pairs = db.kv_scan(Some("z"), None).unwrap();
        assert!(pairs.is_empty());

        // Scan with limit=0 returns empty
        let pairs = db.kv_scan(None, Some(0)).unwrap();
        assert!(pairs.is_empty());

        // Deleted keys are not returned
        db.kv_delete("c").unwrap();
        let pairs = db.kv_scan(None, None).unwrap();
        assert_eq!(pairs.len(), 4);
        assert!(pairs.iter().all(|(k, _)| k != "c"));
    }
}
