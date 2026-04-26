use std::path::{Path, PathBuf};
use std::sync::Arc;

use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem};
use strata_graph::GraphSubsystem;
use strata_security::{AccessMode, OpenOptions};
use strata_vector::VectorSubsystem;

use crate::{Error, Result};

enum Backend {
    Local { db: Arc<Database> },
    Ipc { _data_dir: PathBuf },
}

/// Transitional bootstrap handle kept only for the legacy open policy.
pub struct Strata {
    backend: Backend,
    access_mode: AccessMode,
}

impl Strata {
    /// Open a database using the transitional legacy bootstrap policy.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with(path, OpenOptions::default())
    }

    /// Open a database using the transitional legacy bootstrap policy and the
    /// provided open options.
    pub fn open_with<P: AsRef<Path>>(path: P, opts: OpenOptions) -> Result<Self> {
        let data_dir = path.as_ref().to_path_buf();

        if opts.follower {
            let db = Database::open_runtime(default_product_follower_spec(&data_dir))
                .map_err(engine_open_error)?;
            return Ok(Self {
                backend: Backend::Local { db },
                access_mode: AccessMode::ReadOnly,
            });
        }

        let access_mode = opts.access_mode;
        match Database::open_runtime(default_product_spec(&data_dir)) {
            Ok(db) => {
                seed_builtin_recipes(&db);
                if access_mode == AccessMode::ReadWrite {
                    ensure_default_branch(&db)?;
                }
                Ok(Self {
                    backend: Backend::Local { db },
                    access_mode,
                })
            }
            Err(error) => {
                let message = error.to_string();
                if message.contains("already in use by another process") {
                    let socket_path = data_dir.join("strata.sock");
                    if socket_path.exists() {
                        return Ok(Self {
                            backend: Backend::Ipc {
                                _data_dir: data_dir,
                            },
                            access_mode,
                        });
                    }

                    return Err(Error::Internal {
                        reason: "Database is locked by another process. Run `strata up` to enable shared access, or use --follower for read-only access.".to_string(),
                    });
                }

                Err(engine_open_error(error))
            }
        }
    }

    /// Open an ephemeral cache database using the transitional legacy bootstrap
    /// policy.
    pub fn cache() -> Result<Self> {
        let db = Database::open_runtime(default_product_cache_spec()).map_err(|error| {
            Error::Internal {
                reason: format!("Failed to open cache database: {error}"),
            }
        })?;
        seed_builtin_recipes(&db);
        ensure_default_branch(&db)?;
        Ok(Self {
            backend: Backend::Local { db },
            access_mode: AccessMode::ReadWrite,
        })
    }

    /// Return the local database handle for non-IPC bootstrap results.
    ///
    /// # Panics
    ///
    /// Panics if called on an IPC-backed bootstrap handle.
    pub fn database(&self) -> Arc<Database> {
        match &self.backend {
            Backend::Local { db } => db.clone(),
            Backend::Ipc { .. } => panic!("database() not available on IPC-backed Strata handles"),
        }
    }

    /// Return the access mode selected by the bootstrap shell.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    /// Return whether the bootstrap shell resolved to an IPC marker handle.
    pub fn is_ipc(&self) -> bool {
        matches!(self.backend, Backend::Ipc { .. })
    }
}

fn default_product_spec<P: AsRef<Path>>(path: P) -> OpenSpec {
    OpenSpec::primary(path)
        .with_subsystem(GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem)
        .with_default_branch("default")
}

fn default_product_follower_spec<P: AsRef<Path>>(path: P) -> OpenSpec {
    OpenSpec::follower(path)
        .with_subsystem(GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem)
}

fn default_product_cache_spec() -> OpenSpec {
    OpenSpec::cache()
        .with_subsystem(GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem)
        .with_default_branch("default")
}

fn seed_builtin_recipes(db: &Arc<Database>) {
    if let Err(error) = strata_engine::recipe_store::seed_builtin_recipes(db) {
        tracing::warn!(error = %error, "Failed to seed built-in recipes");
    }
}

fn ensure_default_branch(db: &Arc<Database>) -> Result<()> {
    let default_branch = db
        .default_branch_name()
        .unwrap_or_else(|| "default".to_string());
    let branches = db.branches();
    if !branches.exists(&default_branch).map_err(engine_error)? {
        branches.create(&default_branch).map_err(engine_error)?;
    }
    Ok(())
}

fn engine_open_error(error: strata_core::StrataError) -> Error {
    Error::Internal {
        reason: format!("Failed to open database: {error}"),
    }
}

fn engine_error(error: strata_core::StrataError) -> Error {
    match error {
        strata_core::StrataError::Storage { .. } => Error::Io {
            reason: error.to_string(),
        },
        _ => Error::Internal {
            reason: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::Strata;
    use strata_security::{AccessMode, OpenOptions};

    #[test]
    fn cache_returns_local_read_write_handle() {
        let strata = Strata::cache().expect("cache database should open");
        assert!(!strata.is_ipc());
        assert_eq!(strata.access_mode(), AccessMode::ReadWrite);
        assert_eq!(
            strata.database().default_branch_name().as_deref(),
            Some("default")
        );
    }

    #[test]
    fn disk_open_returns_local_handle() {
        let dir = tempfile::tempdir().expect("tempdir should succeed");
        let strata = Strata::open_with(dir.path(), OpenOptions::default())
            .expect("disk database should open through the bootstrap shell");
        assert!(!strata.is_ipc());
        assert_eq!(strata.access_mode(), AccessMode::ReadWrite);
        assert_eq!(
            strata.database().default_branch_name().as_deref(),
            Some("default")
        );
    }
}
