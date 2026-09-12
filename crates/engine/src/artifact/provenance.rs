//! Remote origin provenance — where a cloned database came from.
//!
//! Written by clone orchestration after a successful bundle import and
//! overwritten by future sync operations (each records a new fetch).
//! The record is engine-owned metadata inside the database (dataset
//! clone artifact contract, binding decision 2: clone preserves
//! provenance); the engine attaches no meaning to the remote URL or
//! dataset naming — hosts are provider-neutral.
//!
//! The base frontier is deliberately the wide shape (one entry per
//! fetched branch, with an opaque per-branch base token and an optional
//! local version) so future delta sync can anchor "what changed since
//! this recorded point" without a record migration.

use serde::{Deserialize, Serialize};

/// Layout version for the persisted record.
const REMOTE_ORIGIN_RECORD_VERSION: u32 = 1;

/// One fetched branch in the base frontier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteOriginFrontierEntry {
    branch: String,
    /// Opaque per-branch base token from the fetched bundle (the
    /// manifest's branch head commit token).
    base: String,
    /// Local head commit version at the sync point, when recorded.
    /// Absent in V1 clones; delta sync populates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_version: Option<u64>,
}

impl RemoteOriginFrontierEntry {
    /// Creates a frontier entry.
    #[must_use]
    pub const fn new(branch: String, base: String, local_version: Option<u64>) -> Self {
        Self {
            branch,
            base,
            local_version,
        }
    }

    /// The fetched branch name.
    #[must_use]
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// The opaque per-branch base token.
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The local head commit version at the sync point, when recorded.
    #[must_use]
    pub const fn local_version(&self) -> Option<u64> {
        self.local_version
    }
}

/// The remote origin of a cloned database.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteOrigin {
    record_version: u32,
    remote_url: String,
    dataset: String,
    branch: String,
    manifest_hash: String,
    fetched_at_micros: u64,
    base_frontier: Vec<RemoteOriginFrontierEntry>,
}

impl RemoteOrigin {
    /// Creates a remote origin record.
    #[must_use]
    pub fn new(
        remote_url: String,
        dataset: String,
        branch: String,
        manifest_hash: String,
        fetched_at_micros: u64,
        base_frontier: Vec<RemoteOriginFrontierEntry>,
    ) -> Self {
        Self {
            record_version: REMOTE_ORIGIN_RECORD_VERSION,
            remote_url,
            dataset,
            branch,
            manifest_hash,
            fetched_at_micros,
            base_frontier,
        }
    }

    /// The remote host the database was fetched from.
    #[must_use]
    pub fn remote_url(&self) -> &str {
        &self.remote_url
    }

    /// The remote dataset name.
    #[must_use]
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// The branch the clone opened by default.
    #[must_use]
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// The fetched bundle's manifest hash.
    #[must_use]
    pub fn manifest_hash(&self) -> &str {
        &self.manifest_hash
    }

    /// When the fetch happened (Unix microseconds).
    #[must_use]
    pub const fn fetched_at_micros(&self) -> u64 {
        self.fetched_at_micros
    }

    /// The per-branch base frontier recorded at the sync point.
    #[must_use]
    pub fn base_frontier(&self) -> &[RemoteOriginFrontierEntry] {
        &self.base_frontier
    }
}

pub(crate) fn encode_remote_origin(origin: &RemoteOrigin) -> Vec<u8> {
    serde_json::to_vec(origin).expect("remote origin record serializes")
}

pub(crate) fn decode_remote_origin(bytes: &[u8]) -> Result<RemoteOrigin, crate::api::EngineError> {
    let origin: RemoteOrigin = serde_json::from_slice(bytes).map_err(|error| {
        crate::api::EngineError::corruption(
            "data_loss.engine.control_plane",
            format!("remote origin record failed to decode: {error}"),
        )
    })?;
    if origin.record_version != REMOTE_ORIGIN_RECORD_VERSION {
        return Err(crate::api::EngineError::corruption(
            "data_loss.engine.control_plane",
            "remote origin record version is not supported",
        ));
    }
    Ok(origin)
}
