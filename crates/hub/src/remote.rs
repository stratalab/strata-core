//! `RemoteTrackingRef`: where a cloned database came from (coordination
//! doc §3.9, frontier-form amendment).
//!
//! Clone orchestration writes the ref as its last step after
//! [`crate::import_bundle`]; future sync operations overwrite it with
//! each fetch. The record lives inside the cloned database as
//! engine-owned provenance; this module maps the hub wire vocabulary
//! onto the engine's neutral `RemoteOrigin` record.

use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use stratahub_protocol::{BranchName, DatasetName, Hash, Manifest};
use time::OffsetDateTime;

use strata_engine::artifact::{RemoteOrigin, RemoteOriginFrontierEntry};
use strata_engine::{Database, DurableLocalOpenOptions};

/// Where a cloned database came from (M7E2 shape, frontier form).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteTrackingRef {
    /// The hub the dataset was fetched from.
    pub hub_url: String,
    /// The remote dataset name.
    pub dataset: DatasetName,
    /// The branch the clone opened by default.
    pub branch: BranchName,
    /// The fetched bundle's manifest hash.
    pub manifest_hash: Hash,
    /// When the fetch happened (UTC).
    pub fetched_at: OffsetDateTime,
    /// One entry per fetched branch: the bundle's per-branch head-commit
    /// token, plus the local head version once sync records it
    /// (`None` on V1 clones).
    pub base_frontier: Vec<(String, String, Option<u64>)>,
}

impl RemoteTrackingRef {
    /// Builds the ref a clone records: the frontier derives from the
    /// fetched manifest's branch entries.
    #[must_use]
    pub fn for_clone(
        hub_url: String,
        dataset: DatasetName,
        branch: BranchName,
        manifest: &Manifest,
        manifest_hash: Hash,
        fetched_at: OffsetDateTime,
    ) -> Self {
        Self {
            hub_url,
            dataset,
            branch,
            manifest_hash,
            fetched_at,
            base_frontier: manifest
                .branches
                .iter()
                .map(|entry| {
                    (
                        entry.name.as_str().to_owned(),
                        entry.head_commit.clone(),
                        None,
                    )
                })
                .collect(),
        }
    }
}

/// Failure modes of tracking-ref reads and writes.
#[derive(Debug)]
#[non_exhaustive]
pub enum RemoteRefError {
    /// The database failed to open or the write failed.
    Engine {
        /// The engine error code.
        code: String,
    },
    /// The stored record does not parse as hub vocabulary.
    Malformed {
        /// Human-readable defect description.
        detail: String,
    },
    /// The path is not an existing database. A tracking-ref read or write
    /// attaches to a database the caller says already exists; it must not create
    /// one. Reported by hub rather than borrowing an engine code — the engine
    /// never emits a "database not found" for a create-capable open (#2630).
    DatabaseMissing {
        /// The path that was expected to hold a database.
        path: PathBuf,
    },
}

impl fmt::Display for RemoteRefError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine { code } => write!(formatter, "database operation failed: {code}"),
            Self::Malformed { detail } => {
                write!(formatter, "remote tracking ref is malformed: {detail}")
            }
            Self::DatabaseMissing { path } => {
                write!(formatter, "no database at {}", path.display())
            }
        }
    }
}

impl Error for RemoteRefError {}

/// Writes (or overwrites) the tracking ref inside the database at `path`.
///
/// # Errors
///
/// [`RemoteRefError::Engine`] when the database fails to open or commit.
pub fn write_remote_tracking_ref(
    path: &Path,
    tracking_ref: &RemoteTrackingRef,
) -> Result<(), RemoteRefError> {
    let mut db = open_database(path)?;
    let fetched_at_micros =
        u64::try_from(tracking_ref.fetched_at.unix_timestamp_nanos() / 1_000).unwrap_or_default();
    let origin = RemoteOrigin::new(
        tracking_ref.hub_url.clone(),
        tracking_ref.dataset.as_str().to_owned(),
        tracking_ref.branch.as_str().to_owned(),
        tracking_ref.manifest_hash.as_str().to_owned(),
        fetched_at_micros,
        tracking_ref
            .base_frontier
            .iter()
            .map(|(branch, base, local_version)| {
                RemoteOriginFrontierEntry::new(branch.clone(), base.clone(), *local_version)
            })
            .collect(),
    );
    db.set_remote_origin(&origin)
        .map_err(|error| RemoteRefError::Engine {
            code: error.code().to_owned(),
        })
}

/// Reads the tracking ref from the database at `path`, when one exists.
///
/// # Errors
///
/// [`RemoteRefError::Engine`] on open/read failure;
/// [`RemoteRefError::Malformed`] when the stored record does not parse
/// as hub vocabulary.
pub fn read_remote_tracking_ref(path: &Path) -> Result<Option<RemoteTrackingRef>, RemoteRefError> {
    let mut db = open_database(path)?;
    let Some(origin) = db.remote_origin().map_err(|error| RemoteRefError::Engine {
        code: error.code().to_owned(),
    })?
    else {
        return Ok(None);
    };

    let fetched_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(origin.fetched_at_micros()) * 1_000)
            .map_err(|error| malformed(format!("fetched_at out of range: {error}")))?;
    Ok(Some(RemoteTrackingRef {
        hub_url: origin.remote_url().to_owned(),
        dataset: DatasetName::parse(origin.dataset())
            .map_err(|error| malformed(format!("dataset name: {error}")))?,
        branch: BranchName::parse(origin.branch())
            .map_err(|error| malformed(format!("branch name: {error}")))?,
        manifest_hash: Hash::parse(origin.manifest_hash())
            .map_err(|error| malformed(format!("manifest hash: {error}")))?,
        fetched_at,
        base_frontier: origin
            .base_frontier()
            .iter()
            .map(|entry| {
                (
                    entry.branch().to_owned(),
                    entry.base().to_owned(),
                    entry.local_version(),
                )
            })
            .collect(),
    }))
}

fn open_database(path: &Path) -> Result<Database, RemoteRefError> {
    // #2630: a tracking-ref read/write ATTACHES to an existing database; it must
    // not create one. `open_local` is open-or-create, so opening a path that
    // does not exist would materialize wal/, manifest/, snapshots/ and locks/ on
    // disk and this function would then report "not found", leaving that debris
    // behind. Probe first and refuse a missing path without touching the
    // filesystem, and report it with a typed hub error rather than fabricating
    // `not_found.engine.database` — a code the engine never emits.
    if !path.exists() {
        return Err(RemoteRefError::DatabaseMissing {
            path: path.to_owned(),
        });
    }
    let outcome = Database::open_local(path, DurableLocalOpenOptions::new()).map_err(|error| {
        RemoteRefError::Engine {
            code: error.code().to_owned(),
        }
    })?;
    // The path existed but was not a database, so the open created one. Still not
    // an attach target: report it missing (typed), not as an engine "not found".
    if outcome.summary().created() {
        return Err(RemoteRefError::DatabaseMissing {
            path: path.to_owned(),
        });
    }
    Ok(outcome.into_database())
}

fn malformed(detail: String) -> RemoteRefError {
    RemoteRefError::Malformed { detail }
}
