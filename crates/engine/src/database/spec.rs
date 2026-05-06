//! Runtime specification for database opening.
//!
//! `OpenSpec` describes how to open a database instance: mode (primary, follower,
//! cache), path, configuration, subsystems, and default branch. This is the
//! engine's single entry point for all database construction.
//!
//! ## Design
//!
//! - **Engine-owned:** `OpenSpec` belongs to the engine crate. Product open
//!   policy is owned by the engine's product-open API.
//! - **Mode constructors:** `primary()`, `follower()`, `cache()` make intent clear.
//! - **Builder pattern:** `with_*` methods support explicit low-level runtime
//!   opening for engine internals and tests.
//!
//! ## Example
//!
//! ```text
//! use strata_engine::database::{OpenSpec, DatabaseMode};
//!
//! let spec = OpenSpec::primary("/data")
//!     .with_config(config)
//!     .with_subsystem(SearchSubsystem);
//!
//! let db = Database::open_runtime(spec)?;
//! ```
//!
//! Product callers should use `open_product_database()` or
//! `open_product_cache()`. Those functions compose the engine-owned graph,
//! vector, and search subsystems internally.

use std::path::{Path, PathBuf};

use crate::database::config::StrataConfig;
use crate::recovery::Subsystem;

/// Database operating mode.
///
/// Determines write capability, bootstrap behavior, and lifecycle hooks.
///
/// | Mode | Writes | Bootstrap | Use Case |
/// |------|--------|-----------|----------|
/// | Primary | Yes | Yes | Main database instance |
/// | Follower | No | No | Read replica, hot standby |
/// | Cache | Yes | Yes | Ephemeral, in-memory only |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DatabaseMode {
    /// Primary read-write instance.
    ///
    /// - Accepts transactions
    /// - Runs subsystem bootstrap (creates default branch, system state)
    /// - Writes WAL and checkpoints
    Primary,

    /// Read-only follower of a primary.
    ///
    /// - Rejects write transactions
    /// - Skips subsystem bootstrap (reads state from shared storage)
    /// - Reads WAL but does not write
    /// - Calls `Subsystem::recover` but not `Subsystem::bootstrap`
    Follower,

    /// Ephemeral in-memory instance.
    ///
    /// - Accepts transactions
    /// - Runs subsystem bootstrap
    /// - No WAL, no persistence (data lost on drop)
    /// - Useful for tests, caching, temporary computations
    Cache,
}

impl DatabaseMode {
    /// Returns `true` if this mode accepts write transactions.
    #[inline]
    pub fn is_writable(&self) -> bool {
        matches!(self, Self::Primary | Self::Cache)
    }

    /// Returns `true` if this mode should run subsystem bootstrap.
    ///
    /// Bootstrap creates initial state (default branch, system branch, etc.).
    /// Followers skip bootstrap because they read state from the primary.
    #[inline]
    pub fn should_bootstrap(&self) -> bool {
        matches!(self, Self::Primary | Self::Cache)
    }

    /// Returns `true` if this mode is ephemeral (no disk persistence).
    #[inline]
    pub fn is_ephemeral(&self) -> bool {
        matches!(self, Self::Cache)
    }
}

impl std::fmt::Display for DatabaseMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Follower => write!(f, "follower"),
            Self::Cache => write!(f, "cache"),
        }
    }
}

/// Specification for opening a database instance.
///
/// Constructed via mode-specific factory methods (`primary`, `follower`, `cache`)
/// and configured via builder methods (`with_config`, `with_subsystem`, etc.).
///
/// ## Lifecycle
///
/// When passed to `Database::open_runtime`:
///
/// 1. Validate spec fields
/// 2. Recover core engine state
/// 3. Build `Arc<Database>`
/// 4. For each subsystem: `recover()` then `initialize()`
/// 5. If mode allows bootstrap: for each subsystem `bootstrap()`
/// 6. Ensure default branch (if specified and mode allows)
/// 7. Return initialized database
pub struct OpenSpec {
    /// Operating mode (primary, follower, cache).
    pub mode: DatabaseMode,

    /// Path to the data directory.
    ///
    /// - Primary/Follower: directory containing WAL, checkpoints, config
    /// - Cache: ignored (ephemeral, no disk access)
    pub path: PathBuf,

    /// Database configuration.
    ///
    /// If `None`, configuration is read from `strata.toml` in the data directory
    /// (or defaults are used if the file doesn't exist).
    pub config: Option<StrataConfig>,

    /// Subsystems to initialize.
    ///
    /// Order matters: subsystems are recovered/initialized in order and
    /// frozen in reverse order on shutdown.
    pub subsystems: Vec<Box<dyn Subsystem>>,

    /// Default branch name to create/ensure on open.
    ///
    /// If `Some`, the branch is created during bootstrap if it doesn't exist.
    /// If `None`, no default branch is ensured (caller is responsible).
    pub default_branch: Option<String>,
}

impl OpenSpec {
    // =========================================================================
    // Mode constructors
    // =========================================================================

    /// Create a spec for a primary database at the given path.
    ///
    /// Primary mode: read-write, runs bootstrap, persists to disk.
    pub fn primary<P: AsRef<Path>>(path: P) -> Self {
        Self {
            mode: DatabaseMode::Primary,
            path: path.as_ref().to_path_buf(),
            config: None,
            subsystems: Vec::new(),
            default_branch: None,
        }
    }

    /// Create a spec for a read-only follower at the given path.
    ///
    /// Follower mode: read-only, skips bootstrap, reads from shared storage.
    pub fn follower<P: AsRef<Path>>(path: P) -> Self {
        Self {
            mode: DatabaseMode::Follower,
            path: path.as_ref().to_path_buf(),
            config: None,
            subsystems: Vec::new(),
            default_branch: None,
        }
    }

    /// Create a spec for an ephemeral in-memory cache.
    ///
    /// Cache mode: read-write, runs bootstrap, no persistence.
    pub fn cache() -> Self {
        Self {
            mode: DatabaseMode::Cache,
            path: PathBuf::new(),
            config: None,
            subsystems: Vec::new(),
            default_branch: None,
        }
    }

    // =========================================================================
    // Builder methods
    // =========================================================================

    /// Set the database configuration.
    ///
    /// If not set, configuration is read from `strata.toml` in the data directory.
    pub fn with_config(mut self, config: StrataConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Add a subsystem to the initialization list.
    ///
    /// Subsystems are recovered/initialized in the order they are added.
    ///
    /// This is a low-level runtime hook for engine internals and tests. Product
    /// callers should use `open_product_database()` or `open_product_cache()`
    /// so engine owns runtime composition.
    #[doc(hidden)]
    pub fn with_subsystem(mut self, subsystem: impl Subsystem) -> Self {
        self.subsystems.push(Box::new(subsystem));
        self
    }

    /// Add multiple subsystems to the initialization list.
    ///
    /// Convenience method for adding pre-boxed subsystems. This is a low-level
    /// runtime hook for engine internals and tests, not product composition.
    #[doc(hidden)]
    pub fn with_subsystems(mut self, subsystems: Vec<Box<dyn Subsystem>>) -> Self {
        self.subsystems.extend(subsystems);
        self
    }

    /// Set the default branch name.
    ///
    /// The branch is created during bootstrap if it doesn't exist.
    pub fn with_default_branch(mut self, name: impl Into<String>) -> Self {
        self.default_branch = Some(name.into());
        self
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Returns the ordered list of subsystem names.
    ///
    /// Used for `CompatibilitySignature` computation.
    pub fn subsystem_names(&self) -> Vec<&'static str> {
        self.subsystems.iter().map(|s| s.name()).collect()
    }
}

impl std::fmt::Debug for OpenSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenSpec")
            .field("mode", &self.mode)
            .field("path", &self.path)
            .field("config", &self.config.is_some())
            .field("subsystems", &self.subsystem_names())
            .field("default_branch", &self.default_branch)
            .finish()
    }
}

// =============================================================================
// Named internal runtime profiles
// =============================================================================

/// Create an `OpenSpec` for a minimal cache database with only `SearchSubsystem`.
///
/// This is the engine-internal profile for tests and utilities that don't need
/// the product open policy. Product code should use the engine product-open
/// API, which owns default branch and built-in recipe bootstrap behavior.
pub fn search_only_cache_spec() -> OpenSpec {
    OpenSpec::cache().with_subsystem(crate::search::SearchSubsystem)
}

/// Create an `OpenSpec` for a minimal primary database with only `SearchSubsystem`.
///
/// This is a low-level runtime profile for tests and utilities. Product code
/// should use the engine product-open API, and application callers should use
/// the executor-facing `Strata::open()` API.
pub fn search_only_primary_spec<P: AsRef<std::path::Path>>(path: P) -> OpenSpec {
    OpenSpec::primary(path).with_subsystem(crate::search::SearchSubsystem)
}

/// Create an `OpenSpec` for a minimal follower database with only `SearchSubsystem`.
///
/// This is a low-level runtime profile for tests and utilities. Product code
/// should use the engine product-open API with follower options.
pub fn search_only_follower_spec<P: AsRef<std::path::Path>>(path: P) -> OpenSpec {
    OpenSpec::follower(path).with_subsystem(crate::search::SearchSubsystem)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSubsystem;
    impl Subsystem for TestSubsystem {
        fn name(&self) -> &'static str {
            "test"
        }
        fn recover(&self, _db: &std::sync::Arc<crate::Database>) -> crate::StrataResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_primary_spec() {
        let spec = OpenSpec::primary("/data");
        assert_eq!(spec.mode, DatabaseMode::Primary);
        assert_eq!(spec.path, PathBuf::from("/data"));
        assert!(spec.mode.is_writable());
        assert!(spec.mode.should_bootstrap());
        assert!(!spec.mode.is_ephemeral());
    }

    #[test]
    fn test_follower_spec() {
        let spec = OpenSpec::follower("/data");
        assert_eq!(spec.mode, DatabaseMode::Follower);
        assert!(!spec.mode.is_writable());
        assert!(!spec.mode.should_bootstrap());
        assert!(!spec.mode.is_ephemeral());
    }

    #[test]
    fn test_cache_spec() {
        let spec = OpenSpec::cache();
        assert_eq!(spec.mode, DatabaseMode::Cache);
        assert!(spec.mode.is_writable());
        assert!(spec.mode.should_bootstrap());
        assert!(spec.mode.is_ephemeral());
    }

    #[test]
    fn test_builder_methods() {
        let config = StrataConfig::default();
        let spec = OpenSpec::primary("/data")
            .with_config(config)
            .with_subsystem(TestSubsystem)
            .with_default_branch("main");

        assert!(spec.config.is_some());
        assert_eq!(spec.subsystem_names(), vec!["test"]);
        assert_eq!(spec.default_branch, Some("main".to_string()));
    }

    #[test]
    fn test_mode_display() {
        assert_eq!(DatabaseMode::Primary.to_string(), "primary");
        assert_eq!(DatabaseMode::Follower.to_string(), "follower");
        assert_eq!(DatabaseMode::Cache.to_string(), "cache");
    }
}
