//! Engine-owned product open policy, outcome, and lock fallback classification.
//!
//! Engine owns product runtime composition, product default-branch policy,
//! built-in recipe seeding, open outcome, and IPC fallback classification;
//! executor remains responsible for IPC transport and session construction.

use std::error::Error as StdError;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::recovery::Subsystem;
use crate::StrataError;

use super::{AccessMode, Database, OpenOptions, OpenSpec};

const DEFAULT_PRODUCT_BRANCH: &str = "default";

/// User-facing message returned when a primary database is locked but no IPC
/// socket is available for fallback.
const LOCKED_WITHOUT_IPC_SOCKET_MESSAGE: &str = "Database is locked by another process. Run `strata up` to enable shared access, or use --follower for read-only access.";

/// Result alias for engine-owned product open operations.
pub type ProductOpenResult<T> = Result<T, ProductOpenError>;

/// Engine-owned result of opening a product database.
///
/// IPC transport objects deliberately do not appear here. A caller that
/// receives [`ProductOpenOutcome::Ipc`] is responsible for constructing its own
/// IPC client against `socket_path`.
#[non_exhaustive]
pub enum ProductOpenOutcome {
    /// Product open resolved to a local engine database.
    #[non_exhaustive]
    Local {
        /// Local engine database handle.
        db: Arc<Database>,
        /// Access mode selected for the returned product handle.
        access_mode: AccessMode,
    },

    /// Product open should fall back to an executor-owned IPC client.
    #[non_exhaustive]
    Ipc {
        /// Requested database directory.
        data_dir: PathBuf,
        /// IPC socket path discovered by engine open classification.
        socket_path: PathBuf,
        /// Access mode selected for the returned product handle.
        access_mode: AccessMode,
    },
}

impl ProductOpenOutcome {
    /// Return the access mode selected for the opened product handle.
    pub fn access_mode(&self) -> AccessMode {
        match self {
            Self::Local { access_mode, .. } | Self::Ipc { access_mode, .. } => *access_mode,
        }
    }
}

impl fmt::Debug for ProductOpenOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local { db, access_mode } => f
                .debug_struct("Local")
                .field("data_dir", &db.data_dir())
                .field("access_mode", access_mode)
                .finish(),
            Self::Ipc {
                data_dir,
                socket_path,
                access_mode,
            } => f
                .debug_struct("Ipc")
                .field("data_dir", data_dir)
                .field("socket_path", socket_path)
                .field("access_mode", access_mode)
                .finish(),
        }
    }
}

/// Engine-owned product open error.
#[derive(Debug)]
#[non_exhaustive]
pub enum ProductOpenError {
    /// Opening a disk-backed product database failed for a reason unrelated to
    /// lock-to-IPC classification.
    #[non_exhaustive]
    Open {
        /// Requested database directory.
        data_dir: PathBuf,
        /// Underlying engine error.
        source: StrataError,
    },

    /// Opening an ephemeral cache product database failed.
    #[non_exhaustive]
    CacheOpen {
        /// Underlying engine error.
        source: StrataError,
    },

    /// A primary database was locked and no IPC socket existed for fallback.
    #[non_exhaustive]
    LockedWithoutIpcSocket {
        /// Requested database directory.
        data_dir: PathBuf,
        /// Socket path that was checked and found missing.
        socket_path: PathBuf,
    },
}

impl fmt::Display for ProductOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { source, .. } => write!(f, "Failed to open database: {source}"),
            Self::CacheOpen { source } => write!(f, "Failed to open cache database: {source}"),
            Self::LockedWithoutIpcSocket { .. } => f.write_str(LOCKED_WITHOUT_IPC_SOCKET_MESSAGE),
        }
    }
}

impl StdError for ProductOpenError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Open { source, .. } | Self::CacheOpen { source } => Some(source),
            Self::LockedWithoutIpcSocket { .. } => None,
        }
    }
}

/// Open a disk-backed product database and classify primary lock failures.
///
pub fn open_product_database<P: AsRef<Path>>(
    path: P,
    options: OpenOptions,
) -> ProductOpenResult<ProductOpenOutcome> {
    let data_dir = path.as_ref().to_path_buf();

    if options.follower {
        let spec = product_follower_spec(&data_dir);
        let db = Database::open_runtime(spec).map_err(|source| ProductOpenError::Open {
            data_dir: data_dir.clone(),
            source,
        })?;
        return Ok(ProductOpenOutcome::Local {
            db,
            access_mode: AccessMode::ReadOnly,
        });
    }

    let access_mode = options.access_mode;
    let default_branch = product_default_branch(&options);
    let spec = product_primary_spec(&data_dir, default_branch);
    match Database::open_runtime(spec) {
        Ok(db) => {
            seed_builtin_recipes_warning_only(&db);
            Ok(ProductOpenOutcome::Local { db, access_mode })
        }
        Err(source) if is_primary_lock_in_use(&source) => {
            let socket_path = data_dir.join("strata.sock");
            if socket_path.exists() {
                Ok(ProductOpenOutcome::Ipc {
                    data_dir,
                    socket_path,
                    access_mode,
                })
            } else {
                Err(ProductOpenError::LockedWithoutIpcSocket {
                    data_dir,
                    socket_path,
                })
            }
        }
        Err(source) => Err(ProductOpenError::Open { data_dir, source }),
    }
}

/// Open an ephemeral product cache database.
///
pub fn open_product_cache() -> ProductOpenResult<ProductOpenOutcome> {
    let spec = product_cache_spec();
    let db =
        Database::open_runtime(spec).map_err(|source| ProductOpenError::CacheOpen { source })?;
    seed_builtin_recipes_warning_only(&db);
    Ok(ProductOpenOutcome::Local {
        db,
        access_mode: AccessMode::ReadWrite,
    })
}

fn product_primary_spec(path: &Path, default_branch: &str) -> OpenSpec {
    OpenSpec::primary(path)
        .with_subsystems(product_runtime_subsystems())
        .with_default_branch(default_branch)
}

fn product_follower_spec(path: &Path) -> OpenSpec {
    OpenSpec::follower(path).with_subsystems(product_runtime_subsystems())
}

fn product_cache_spec() -> OpenSpec {
    OpenSpec::cache()
        .with_subsystems(product_runtime_subsystems())
        .with_default_branch(DEFAULT_PRODUCT_BRANCH)
}

fn product_default_branch(options: &OpenOptions) -> &str {
    options
        .default_branch
        .as_deref()
        .unwrap_or(DEFAULT_PRODUCT_BRANCH)
}

fn product_runtime_subsystems() -> Vec<Box<dyn Subsystem>> {
    vec![
        Box::new(crate::GraphSubsystem),
        Box::new(crate::VectorSubsystem),
        Box::new(crate::SearchSubsystem),
    ]
}

fn seed_builtin_recipes_warning_only(db: &Arc<Database>) {
    if let Err(error) = crate::recipe_store::seed_builtin_recipes(db) {
        tracing::warn!(error = %error, "Failed to seed built-in recipes");
    }
}

fn is_primary_lock_in_use(error: &StrataError) -> bool {
    error
        .to_string()
        .contains("already in use by another process")
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::BranchId;
    use tempfile::tempdir;

    fn hold_database_lock(path: &Path) -> std::fs::File {
        std::fs::create_dir_all(path).expect("database directory should be creatable");
        let lock_path = path.join(".lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .expect("lock file should open");
        fs2::FileExt::try_lock_exclusive(&lock_file).expect("test should acquire database lock");
        lock_file
    }

    fn assert_default_branch_bootstrapped(db: &Arc<Database>) {
        assert_eq!(
            db.default_branch_name().as_deref(),
            Some(DEFAULT_PRODUCT_BRANCH)
        );
        assert!(
            db.branches()
                .exists(DEFAULT_PRODUCT_BRANCH)
                .expect("default branch existence check should succeed"),
            "product open should create the default branch"
        );
    }

    fn assert_builtin_recipes_seeded(db: &Arc<Database>) {
        let recipe = crate::recipe_store::get_recipe(db, BranchId::new(), "keyword")
            .expect("built-in recipe lookup should succeed");
        assert!(
            recipe.is_some(),
            "product open should seed built-in recipes"
        );
    }

    fn assert_builtin_recipes_not_seeded(db: &Arc<Database>) {
        let recipe = crate::recipe_store::get_recipe(db, BranchId::new(), "keyword")
            .expect("built-in recipe lookup should succeed");
        assert!(
            recipe.is_none(),
            "injected seed failure should leave built-in recipes unseeded"
        );
    }

    #[test]
    fn disk_product_open_returns_local_database_with_requested_access_mode() {
        let dir = tempdir().expect("tempdir should succeed");

        let outcome = open_product_database(
            dir.path(),
            OpenOptions::default().access_mode(AccessMode::ReadOnly),
        )
        .expect("product database should open");

        match outcome {
            ProductOpenOutcome::Local { db, access_mode } => {
                assert_eq!(access_mode, AccessMode::ReadOnly);
                assert!(!db.is_follower());
                assert_eq!(
                    db.installed_subsystem_names(),
                    vec!["graph", "vector", "search"]
                );
                assert_default_branch_bootstrapped(&db);
                assert_builtin_recipes_seeded(&db);
                db.shutdown().expect("database should shut down");
            }
            other => panic!("expected local product open outcome, got {other:?}"),
        }
    }

    #[test]
    fn follower_product_open_forces_read_only_access_mode() {
        let dir = tempdir().expect("tempdir should succeed");

        let primary = open_product_database(dir.path(), OpenOptions::default())
            .expect("primary product database should open");
        let primary_db = match primary {
            ProductOpenOutcome::Local { db, .. } => db,
            other => panic!("expected local primary, got {other:?}"),
        };

        let follower = open_product_database(
            dir.path(),
            OpenOptions::default()
                .access_mode(AccessMode::ReadWrite)
                .follower(true),
        )
        .expect("follower product database should open");

        match follower {
            ProductOpenOutcome::Local { db, access_mode } => {
                assert_eq!(access_mode, AccessMode::ReadOnly);
                assert!(db.is_follower());
                assert_eq!(
                    db.installed_subsystem_names(),
                    vec!["graph", "vector", "search"]
                );
                db.shutdown().expect("follower should shut down");
            }
            other => panic!("expected local follower outcome, got {other:?}"),
        }

        primary_db.shutdown().expect("primary should shut down");
    }

    #[test]
    fn follower_product_open_does_not_create_default_branch_state() {
        let dir = tempdir().expect("tempdir should succeed");
        let primary = Database::open_runtime(
            OpenSpec::primary(dir.path()).with_subsystems(product_runtime_subsystems()),
        )
        .expect("setup primary should open without product default branch");
        assert!(primary.default_branch_name().is_none());

        let follower = open_product_database(dir.path(), OpenOptions::default().follower(true))
            .expect("follower product database should open");

        match follower {
            ProductOpenOutcome::Local { db, access_mode } => {
                assert_eq!(access_mode, AccessMode::ReadOnly);
                assert!(db.is_follower());
                assert_eq!(db.default_branch_name(), None);
                db.shutdown().expect("follower should shut down");
            }
            other => panic!("expected local follower outcome, got {other:?}"),
        }

        assert_eq!(primary.default_branch_name(), None);
        primary.shutdown().expect("primary should shut down");
    }

    #[test]
    fn disk_product_open_uses_requested_default_branch() {
        let dir = tempdir().expect("tempdir should succeed");

        let outcome =
            open_product_database(dir.path(), OpenOptions::default().default_branch("main"))
                .expect("product database should open");

        match outcome {
            ProductOpenOutcome::Local { db, .. } => {
                assert_eq!(db.default_branch_name().as_deref(), Some("main"));
                assert!(
                    db.branches()
                        .exists("main")
                        .expect("default branch existence check should succeed"),
                    "product open should create the requested default branch"
                );
                db.shutdown().expect("database should shut down");
            }
            other => panic!("expected local product open outcome, got {other:?}"),
        }
    }

    #[test]
    fn cache_product_open_returns_local_read_write_database() {
        let outcome = open_product_cache().expect("cache product database should open");

        match outcome {
            ProductOpenOutcome::Local { db, access_mode } => {
                assert_eq!(access_mode, AccessMode::ReadWrite);
                assert!(!db.is_follower());
                assert_eq!(
                    db.installed_subsystem_names(),
                    vec!["graph", "vector", "search"]
                );
                assert_default_branch_bootstrapped(&db);
                assert_builtin_recipes_seeded(&db);
                db.shutdown().expect("cache should shut down");
            }
            other => panic!("expected local cache outcome, got {other:?}"),
        }
    }

    #[test]
    fn product_open_installs_engine_owned_runtime_order() {
        let outcome = open_product_cache().expect("cache product database should open");

        match outcome {
            ProductOpenOutcome::Local { db, .. } => {
                assert_eq!(
                    db.installed_subsystem_names(),
                    vec!["graph", "vector", "search"]
                );
                db.shutdown().expect("database should shut down");
            }
            other => panic!("expected local cache outcome, got {other:?}"),
        }
    }

    #[test]
    fn product_open_installs_engine_owned_graph_and_vector_runtime_hooks() {
        let outcome = open_product_cache().expect("cache product database should open");

        match outcome {
            ProductOpenOutcome::Local { db, .. } => {
                assert!(
                    db.dag_hook().is_installed(),
                    "product open should install graph DAG hook"
                );
                assert!(
                    db.merge_registry().has_graph(),
                    "product open should register graph merge planning"
                );
                assert!(
                    db.merge_registry().has_vector(),
                    "product open should register vector merge planning"
                );
                db.shutdown().expect("database should shut down");
            }
            other => panic!("expected local cache outcome, got {other:?}"),
        }
    }

    #[test]
    fn disk_product_open_keeps_seed_failure_warning_only() {
        let dir = tempdir().expect("tempdir should succeed");
        let canonical = std::fs::canonicalize(dir.path()).expect("tempdir should canonicalize");
        crate::recipe_store::clear_seed_builtin_recipes_failure_for_test(&canonical);
        crate::recipe_store::inject_seed_builtin_recipes_failure_for_test(
            &canonical,
            "injected recipe seed failure",
        );

        let outcome = open_product_database(dir.path(), OpenOptions::default())
            .expect("recipe seed failure should not fail product open");

        match outcome {
            ProductOpenOutcome::Local { db, access_mode } => {
                assert_eq!(access_mode, AccessMode::ReadWrite);
                assert_default_branch_bootstrapped(&db);
                assert_builtin_recipes_not_seeded(&db);
                db.shutdown().expect("database should shut down");
            }
            other => panic!("expected local product open outcome, got {other:?}"),
        }

        crate::recipe_store::clear_seed_builtin_recipes_failure_for_test(&canonical);
    }

    #[test]
    fn disk_product_open_wraps_ordinary_open_error() {
        let err = open_product_database("", OpenOptions::default())
            .expect_err("empty primary path should fail as an ordinary open error");

        match &err {
            ProductOpenError::Open { data_dir, source } => {
                assert!(data_dir.as_os_str().is_empty());
                assert!(
                    source.to_string().contains("path is required"),
                    "unexpected source: {source}"
                );
                assert!(
                    err.to_string().starts_with("Failed to open database:"),
                    "unexpected display: {err}"
                );
            }
            other => panic!("expected ordinary open error, got {other:?}"),
        }
    }

    #[test]
    fn locked_primary_without_socket_returns_typed_error() {
        let dir = tempdir().expect("tempdir should succeed");
        let _lock_file = hold_database_lock(dir.path());
        let err = open_product_database(dir.path(), OpenOptions::default())
            .expect_err("locked primary without socket should fail");

        let err_message = err.to_string();
        match &err {
            ProductOpenError::LockedWithoutIpcSocket {
                data_dir,
                socket_path,
            } => {
                assert_eq!(data_dir.as_path(), dir.path());
                assert_eq!(socket_path, &dir.path().join("strata.sock"));
                assert_eq!(err_message, LOCKED_WITHOUT_IPC_SOCKET_MESSAGE);
            }
            other => panic!("expected typed lock-without-socket error, got {other:?}"),
        }
    }

    #[test]
    fn locked_primary_with_socket_returns_ipc_outcome() {
        let dir = tempdir().expect("tempdir should succeed");
        let _lock_file = hold_database_lock(dir.path());

        let socket_path = dir.path().join("strata.sock");
        std::fs::write(&socket_path, b"").expect("socket marker should be writable");

        let outcome = open_product_database(
            dir.path(),
            OpenOptions::default().access_mode(AccessMode::ReadOnly),
        )
        .expect("locked primary with socket should classify as IPC fallback");

        match outcome {
            ProductOpenOutcome::Ipc {
                data_dir,
                socket_path: discovered_socket_path,
                access_mode,
            } => {
                assert_eq!(data_dir.as_path(), dir.path());
                assert_eq!(discovered_socket_path, socket_path);
                assert_eq!(access_mode, AccessMode::ReadOnly);
            }
            other => panic!("expected IPC fallback outcome, got {other:?}"),
        }
    }
}
