//! Manifest services for storage-owned durable metadata.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "manifest services are consumed by lifecycle, WAL, and table work added later"
    )
)]

use crate::backend::{
    Backend, BackendError, BackendErrorKind, BackendHandle, PublishError, PublishOutcome,
};
use crate::format::{
    decode_branch_catalog_manifest, decode_manifest, decode_pending_releases_manifest,
    decode_table_manifest, encode_branch_catalog_manifest, encode_manifest,
    encode_pending_releases_manifest, encode_table_manifest, BranchCatalogManifest,
    DatabaseManifest, FormatError, PendingReleasesManifest, TableManifest,
};
use crate::layout::{LayoutError, ObjectLayout, TableObjectClassification};
use crate::object::{ObjectName, ObjectPrefix};
use crate::service::{validate_publish_outcome, ObjectPublisher};
use std::fmt;
use strata_core::CommitVersion;

pub(crate) type ManifestServiceResult<T> = Result<T, ManifestServiceError>;

const DATABASE_MANIFEST_SERVICE: u8 = 0;
const TABLE_MANIFEST_SERVICE: u8 = 1;
const BRANCH_CATALOG_MANIFEST_SERVICE: u8 = 2;
const PENDING_RELEASES_MANIFEST_SERVICE: u8 = 3;
const CREATE_MANIFEST_OBJECT: bool = true;
const REPLACE_MANIFEST_OBJECT: bool = false;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestRole {
    Database,
    Table,
    BranchCatalog,
    PendingReleases,
}

impl ManifestRole {
    const fn name(self) -> &'static str {
        match self {
            Self::Database => "database manifest",
            Self::Table => "table manifest",
            Self::BranchCatalog => "branch catalog manifest",
            Self::PendingReleases => "pending releases manifest",
        }
    }
}

impl fmt::Display for ManifestRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ManifestServiceError {
    Layout {
        role: ManifestRole,
        source: LayoutError,
    },
    Missing {
        role: ManifestRole,
        object: ObjectName,
    },
    Read {
        role: ManifestRole,
        object: ObjectName,
        source: BackendError,
    },
    List {
        role: ManifestRole,
        prefix: ObjectPrefix,
        source: BackendError,
    },
    Decode {
        role: ManifestRole,
        object: ObjectName,
        source: FormatError,
    },
    Encode {
        role: ManifestRole,
        object: ObjectName,
        source: FormatError,
    },
    Publish {
        role: ManifestRole,
        source: PublishError,
    },
    Delete {
        role: ManifestRole,
        source: crate::backend::DeleteError,
    },
    InvalidPublishMetadata {
        role: ManifestRole,
        object: ObjectName,
        field: &'static str,
    },
    CodecMismatch {
        object: ObjectName,
        expected: String,
        actual: String,
    },
    InvalidRecoveryFact {
        role: ManifestRole,
        object: ObjectName,
        field: &'static str,
    },
}

impl fmt::Display for ManifestServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Layout { role, source } => {
                write!(formatter, "failed to build {role} object name: {source}")
            }
            Self::Missing { role, object } => {
                write!(formatter, "{role} object {object} is missing")
            }
            Self::Read {
                role,
                object,
                source,
            } => write!(formatter, "failed to read {role} object {object}: {source}"),
            Self::List {
                role,
                prefix,
                source,
            } => write!(
                formatter,
                "failed to list {role} objects under {prefix}: {source}"
            ),
            Self::Decode {
                role,
                object,
                source,
            } => {
                write!(formatter, "failed to decode {role} {object}: {source}")
            }
            Self::Encode {
                role,
                object,
                source,
            } => {
                write!(formatter, "failed to encode {role} {object}: {source}")
            }
            Self::Publish { role, source } => {
                write!(formatter, "failed to publish {role}: {source}")
            }
            Self::Delete { role, source } => {
                write!(formatter, "failed to delete {role}: {source}")
            }
            Self::InvalidPublishMetadata {
                role,
                object,
                field,
            } => write!(
                formatter,
                "{role} object {object} has invalid publish metadata {field}"
            ),
            Self::CodecMismatch {
                object,
                expected,
                actual,
            } => write!(
                formatter,
                "database manifest {object} codec mismatch: expected {expected}, found {actual}"
            ),
            Self::InvalidRecoveryFact {
                role,
                object,
                field,
            } => {
                write!(
                    formatter,
                    "{role} object {object} has invalid recovery fact {field}"
                )
            }
        }
    }
}

impl std::error::Error for ManifestServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Layout { source, .. } => Some(source),
            Self::Read { source, .. } | Self::List { source, .. } => Some(source),
            Self::Decode { source, .. } | Self::Encode { source, .. } => Some(source),
            Self::Publish { source, .. } => Some(source),
            Self::Delete { source, .. } => Some(source),
            Self::Missing { .. }
            | Self::InvalidPublishMetadata { .. }
            | Self::CodecMismatch { .. }
            | Self::InvalidRecoveryFact { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestWrite<M> {
    manifest: M,
    outcome: PublishOutcome,
}

pub(crate) type DatabaseManifestWrite = ManifestWrite<DatabaseManifest>;
pub(crate) type TableManifestWrite = ManifestWrite<TableManifest>;
pub(crate) type BranchCatalogManifestWrite = ManifestWrite<BranchCatalogManifest>;
pub(crate) type PendingReleasesManifestWrite = ManifestWrite<PendingReleasesManifest>;

impl<M> ManifestWrite<M> {
    fn new(manifest: M, outcome: PublishOutcome) -> Self {
        Self { manifest, outcome }
    }

    pub(crate) const fn manifest(&self) -> &M {
        &self.manifest
    }

    #[cfg_attr(
        all(test, target_arch = "wasm32"),
        expect(
            dead_code,
            reason = "manifest publish outcome is asserted by host-only property tests"
        )
    )]
    pub(crate) const fn outcome(&self) -> &PublishOutcome {
        &self.outcome
    }
}

pub(crate) type DatabaseManifestService<'a> = ManifestService<'a, DATABASE_MANIFEST_SERVICE>;
pub(crate) type TableManifestService<'a> = ManifestService<'a, TABLE_MANIFEST_SERVICE>;
pub(crate) type BranchCatalogManifestService<'a> =
    ManifestService<'a, BRANCH_CATALOG_MANIFEST_SERVICE>;
pub(crate) type PendingReleasesManifestService<'a> =
    ManifestService<'a, PENDING_RELEASES_MANIFEST_SERVICE>;

#[derive(Clone)]
pub(crate) struct ManifestService<'a, const ROLE: u8> {
    backend: BackendHandle<'a>,
}

impl<'a, const ROLE: u8> ManifestService<'a, ROLE> {
    pub(crate) fn new(backend: impl Into<BackendHandle<'a>>) -> Self {
        Self {
            backend: backend.into(),
        }
    }
}

impl ManifestService<'_, DATABASE_MANIFEST_SERVICE> {
    pub(crate) fn load_current(&self) -> ManifestServiceResult<Option<DatabaseManifest>> {
        let object = database_manifest_object()?;
        read_optional(&self.backend, ManifestRole::Database, &object)?
            .map(|bytes| decode_database_manifest(&object, &bytes))
            .transpose()
    }

    pub(crate) fn load_required(&self) -> ManifestServiceResult<DatabaseManifest> {
        let object = database_manifest_object()?;
        self.load_current()?.ok_or(ManifestServiceError::Missing {
            role: ManifestRole::Database,
            object,
        })
    }

    pub(crate) fn load_current_for_codec(
        &self,
        expected_codec_id: &str,
    ) -> ManifestServiceResult<Option<DatabaseManifest>> {
        let object = database_manifest_object()?;
        self.load_current()?
            .map(|manifest| validate_codec(&object, expected_codec_id, manifest))
            .transpose()
    }

    pub(crate) fn create_initial(
        &self,
        database_id: [u8; 16],
        codec_id: impl Into<String>,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        let object = database_manifest_object()?;
        let manifest = DatabaseManifest::new(database_id, codec_id).map_err(|source| {
            ManifestServiceError::Encode {
                role: ManifestRole::Database,
                object: object.clone(),
                source,
            }
        })?;
        publish_database_manifest(&self.backend, &object, &manifest, CREATE_MANIFEST_OBJECT)
    }

    pub(crate) fn publish_current(
        &self,
        manifest: &DatabaseManifest,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        let object = database_manifest_object()?;
        // Raw publish replaces the manifest the caller supplied; it does not
        // merge with current durable state. The persist_* paths below own
        // preservation of unrelated recovery facts.
        publish_database_manifest(&self.backend, &object, manifest, REPLACE_MANIFEST_OBJECT)
    }

    pub(crate) fn persist_active_wal_segment(
        &self,
        active_wal_segment: u64,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        validate_active_wal_segment(active_wal_segment)?;
        let current = self.load_required()?;
        self.persist_active_wal_segment_from_current(current, active_wal_segment)
    }

    pub(crate) fn persist_active_wal_segment_from_current(
        &self,
        current: DatabaseManifest,
        active_wal_segment: u64,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        validate_active_wal_segment(active_wal_segment)?;
        let object = database_manifest_object()?;

        // Active WAL changes must preserve snapshot and flush facts. Dropping
        // either would make a later recovery choose the wrong replay window.
        let snapshot_watermark = current.snapshot_watermark();
        let snapshot_id = current.snapshot_id();
        let flushed_through_commit_id = current.flushed_through_commit_id();
        let updated = current
            .with_recovery_facts(
                active_wal_segment,
                snapshot_watermark,
                snapshot_id,
                flushed_through_commit_id,
            )
            .map_err(|source| ManifestServiceError::Encode {
                role: ManifestRole::Database,
                object: object.clone(),
                source,
            })?;
        publish_database_manifest(&self.backend, &object, &updated, REPLACE_MANIFEST_OBJECT)
    }

    pub(crate) fn persist_snapshot_facts(
        &self,
        snapshot_id: u64,
        snapshot_watermark: CommitVersion,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        self.persist_snapshot_facts_with_flush_boundary(snapshot_id, snapshot_watermark, None)
    }

    /// Records the snapshot facts and, when the checkpoint snapshot is a delta over a durable
    /// table-manifest base, the base floor it sits on (`flushed_through_base` = the highest
    /// durably-flushed commit). Recording the base floor in the SAME manifest write keeps
    /// `snapshot_watermark` from outrunning its anchored base: a later recovery that finds the
    /// base missing can then tell the snapshot is an orphaned delta rather than a self-contained
    /// snapshot. A `None` base preserves the current flushed-through fact — a full, self-contained
    /// snapshot needs no base.
    pub(crate) fn persist_snapshot_facts_with_flush_boundary(
        &self,
        snapshot_id: u64,
        snapshot_watermark: CommitVersion,
        flushed_through_base: Option<CommitVersion>,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        let object = database_manifest_object()?;
        if snapshot_id == 0 {
            return Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object,
                field: "snapshot_id",
            });
        }
        if snapshot_watermark.as_u64() == 0 {
            return Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object,
                field: "snapshot_watermark",
            });
        }

        let current = self.load_required()?;
        // Snapshot facts are a pair: the durable snapshot object id and the commit watermark
        // it covers. They are updated together with the active WAL segment unchanged. The
        // flush fact is advanced to the snapshot's base floor (monotonic — a higher recorded
        // watermark never regresses) when the snapshot is a delta, else preserved.
        let active_wal_segment = current.active_wal_segment();
        let flushed_through_commit_id = match flushed_through_base {
            Some(base) => Some(
                current
                    .flushed_through_commit_id()
                    .map_or(base, |recorded| recorded.max(base)),
            ),
            None => current.flushed_through_commit_id(),
        };
        let updated = current
            .with_recovery_facts(
                active_wal_segment,
                Some(snapshot_watermark.as_u64()),
                Some(snapshot_id),
                flushed_through_commit_id,
            )
            .map_err(|source| ManifestServiceError::Encode {
                role: ManifestRole::Database,
                object: object.clone(),
                source,
            })?;
        publish_database_manifest(&self.backend, &object, &updated, REPLACE_MANIFEST_OBJECT)
    }

    pub(crate) fn persist_flush_watermark(
        &self,
        commit_id: CommitVersion,
    ) -> ManifestServiceResult<DatabaseManifestWrite> {
        let object = database_manifest_object()?;
        if commit_id.as_u64() == 0 {
            return Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object,
                field: "flushed_through_commit_id",
            });
        }

        let current = self.load_required()?;
        // Flush progress never changes the snapshot identity or active WAL
        // segment; it only records how far table state has caught up.
        let active_wal_segment = current.active_wal_segment();
        let snapshot_watermark = current.snapshot_watermark();
        let snapshot_id = current.snapshot_id();
        let updated = current
            .with_recovery_facts(
                active_wal_segment,
                snapshot_watermark,
                snapshot_id,
                Some(commit_id),
            )
            .map_err(|source| ManifestServiceError::Encode {
                role: ManifestRole::Database,
                object: object.clone(),
                source,
            })?;
        publish_database_manifest(&self.backend, &object, &updated, REPLACE_MANIFEST_OBJECT)
    }
}

impl ManifestService<'_, BRANCH_CATALOG_MANIFEST_SERVICE> {
    #[allow(
        dead_code,
        reason = "branch catalog manifest load is consumed by recovery rebuild"
    )]
    pub(crate) fn load_current(&self) -> ManifestServiceResult<Option<BranchCatalogManifest>> {
        let object = branch_catalog_manifest_object()?;
        read_optional(&self.backend, ManifestRole::BranchCatalog, &object)?
            .map(|bytes| decode_branch_catalog(&object, &bytes))
            .transpose()
    }

    #[allow(
        dead_code,
        reason = "publish_replace handles both create and replace via durable replace semantics; create kept for parity with sibling services"
    )]
    pub(crate) fn publish_create(
        &self,
        manifest: &BranchCatalogManifest,
    ) -> ManifestServiceResult<BranchCatalogManifestWrite> {
        let object = branch_catalog_manifest_object()?;
        publish_branch_catalog(&self.backend, &object, manifest, CREATE_MANIFEST_OBJECT)
    }

    pub(crate) fn publish_replace(
        &self,
        manifest: &BranchCatalogManifest,
    ) -> ManifestServiceResult<BranchCatalogManifestWrite> {
        let object = branch_catalog_manifest_object()?;
        publish_branch_catalog(&self.backend, &object, manifest, REPLACE_MANIFEST_OBJECT)
    }
}

impl ManifestService<'_, PENDING_RELEASES_MANIFEST_SERVICE> {
    pub(crate) fn load_current(&self) -> ManifestServiceResult<Option<PendingReleasesManifest>> {
        let object = pending_releases_manifest_object()?;
        read_optional(&self.backend, ManifestRole::PendingReleases, &object)?
            .map(|bytes| decode_pending_releases(&object, &bytes))
            .transpose()
    }

    pub(crate) fn publish_replace(
        &self,
        manifest: &PendingReleasesManifest,
    ) -> ManifestServiceResult<PendingReleasesManifestWrite> {
        let object = pending_releases_manifest_object()?;
        publish_pending_releases(&self.backend, &object, manifest, REPLACE_MANIFEST_OBJECT)
    }
}

impl ManifestService<'_, TABLE_MANIFEST_SERVICE> {
    pub(crate) fn load(&self, branch_id: &str) -> ManifestServiceResult<Option<Vec<u8>>> {
        let object = table_manifest_object(branch_id)?;
        read_optional(&self.backend, ManifestRole::Table, &object)
    }

    pub(crate) fn load_current(
        &self,
        branch_id: strata_core::BranchId,
    ) -> ManifestServiceResult<Option<TableManifest>> {
        let branch_component = branch_id.to_string();
        let object = table_manifest_object(&branch_component)?;
        read_optional(&self.backend, ManifestRole::Table, &object)?
            .map(|bytes| decode_branch_table_manifest(branch_id, &object, &bytes))
            .transpose()
    }

    pub(crate) fn load_all_current(&self) -> ManifestServiceResult<Vec<TableManifest>> {
        let prefix =
            ObjectLayout::table_prefix().map_err(|source| ManifestServiceError::Layout {
                role: ManifestRole::Table,
                source,
            })?;
        let mut objects =
            self.backend
                .list_prefix(&prefix)
                .map_err(|source| ManifestServiceError::List {
                    role: ManifestRole::Table,
                    prefix: prefix.clone(),
                    source,
                })?;
        objects.sort();
        objects
            .into_iter()
            .filter(is_branch_table_manifest_object)
            .map(|object| {
                let branch_id = branch_id_from_table_manifest_object(&object)?;
                read_optional(&self.backend, ManifestRole::Table, &object)?
                    .map(|bytes| decode_branch_table_manifest(branch_id, &object, &bytes))
                    .transpose()?
                    .ok_or(ManifestServiceError::Missing {
                        role: ManifestRole::Table,
                        object,
                    })
            })
            .collect()
    }

    #[allow(
        dead_code,
        reason = "table-manifest recovery tests exercise required-load failures"
    )]
    pub(crate) fn load_required(
        &self,
        branch_id: strata_core::BranchId,
    ) -> ManifestServiceResult<TableManifest> {
        let branch_component = branch_id.to_string();
        let object = table_manifest_object(&branch_component)?;
        self.load_current(branch_id)?
            .ok_or(ManifestServiceError::Missing {
                role: ManifestRole::Table,
                object,
            })
    }

    pub(crate) fn publish_create(
        &self,
        branch_id: &str,
        bytes: &[u8],
    ) -> ManifestServiceResult<PublishOutcome> {
        let object = table_manifest_object(branch_id)?;
        let outcome = ObjectPublisher::new(&self.backend)
            .publish_durable_create(&object, bytes)
            .map_err(|source| ManifestServiceError::Publish {
                role: ManifestRole::Table,
                source,
            })?;
        validate_manifest_publish_outcome(
            ManifestRole::Table,
            &object,
            bytes.len() as u64,
            &outcome,
        )?;
        Ok(outcome)
    }

    pub(crate) fn publish_replace(
        &self,
        branch_id: &str,
        bytes: &[u8],
    ) -> ManifestServiceResult<PublishOutcome> {
        let object = table_manifest_object(branch_id)?;
        let outcome = ObjectPublisher::new(&self.backend)
            .publish_durable_replace(&object, bytes)
            .map_err(|source| ManifestServiceError::Publish {
                role: ManifestRole::Table,
                source,
            })?;
        validate_manifest_publish_outcome(
            ManifestRole::Table,
            &object,
            bytes.len() as u64,
            &outcome,
        )?;
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "table-manifest service tests exercise create publication"
    )]
    pub(crate) fn publish_create_manifest(
        &self,
        branch_id: strata_core::BranchId,
        manifest: &TableManifest,
    ) -> ManifestServiceResult<TableManifestWrite> {
        let branch_component = branch_id.to_string();
        let object = table_manifest_object(&branch_component)?;
        validate_table_manifest_branch(branch_id, &object, manifest)?;
        publish_branch_table_manifest(
            &self.backend,
            branch_id,
            &object,
            manifest,
            CREATE_MANIFEST_OBJECT,
        )
    }

    /// #2833: remove a branch's table-manifest object outright — the
    /// layer-less fork paths (eager/materialized and empty forks) own no
    /// durable tables, and a DELETED predecessor generation's manifest left
    /// on disk becomes the re-created branch's recovery provenance.
    /// Idempotent: an already-absent manifest is success (the backend
    /// classifies absence as `AlreadyMissing`, an Ok outcome — #2810).
    pub(crate) fn remove_manifest(
        &self,
        branch_id: strata_core::BranchId,
    ) -> ManifestServiceResult<()> {
        let branch_component = branch_id.to_string();
        let object = table_manifest_object(&branch_component)?;
        // Absence is SUCCESS at the backend layer already: LocalFs classifies
        // a missing object as `Ok(AlreadyMissing)` (#2810's stat→unlink race
        // fix), so the Ok arm covers the idempotent case and every Err is a
        // real failure. (A NotFound-source guard here was dead code on the
        // shipping backend — the mutation gate proved it.)
        match self.backend.delete_object(&object) {
            Ok(_) => Ok(()),
            Err(source) => Err(ManifestServiceError::Delete {
                role: ManifestRole::Table,
                source,
            }),
        }
    }

    pub(crate) fn publish_replace_manifest(
        &self,
        branch_id: strata_core::BranchId,
        manifest: &TableManifest,
    ) -> ManifestServiceResult<TableManifestWrite> {
        let branch_component = branch_id.to_string();
        let object = table_manifest_object(&branch_component)?;
        validate_table_manifest_branch(branch_id, &object, manifest)?;
        publish_branch_table_manifest(
            &self.backend,
            branch_id,
            &object,
            manifest,
            REPLACE_MANIFEST_OBJECT,
        )
    }
}

fn database_manifest_object() -> ManifestServiceResult<ObjectName> {
    ObjectLayout::database_manifest().map_err(|source| ManifestServiceError::Layout {
        role: ManifestRole::Database,
        source,
    })
}

fn branch_catalog_manifest_object() -> ManifestServiceResult<ObjectName> {
    ObjectLayout::branch_catalog_manifest().map_err(|source| ManifestServiceError::Layout {
        role: ManifestRole::BranchCatalog,
        source,
    })
}

fn pending_releases_manifest_object() -> ManifestServiceResult<ObjectName> {
    ObjectLayout::pending_releases_manifest().map_err(|source| ManifestServiceError::Layout {
        role: ManifestRole::PendingReleases,
        source,
    })
}

fn decode_pending_releases(
    object: &ObjectName,
    bytes: &[u8],
) -> ManifestServiceResult<PendingReleasesManifest> {
    decode_pending_releases_manifest(bytes).map_err(|source| ManifestServiceError::Decode {
        role: ManifestRole::PendingReleases,
        object: object.clone(),
        source,
    })
}

fn publish_pending_releases(
    backend: &dyn Backend,
    object: &ObjectName,
    manifest: &PendingReleasesManifest,
    create_object: bool,
) -> ManifestServiceResult<PendingReleasesManifestWrite> {
    let bytes = encode_pending_releases_manifest(manifest).map_err(|source| {
        ManifestServiceError::Encode {
            role: ManifestRole::PendingReleases,
            object: object.clone(),
            source,
        }
    })?;
    let decoded = decode_pending_releases(object, &bytes)?;
    let publisher = ObjectPublisher::new(backend);
    let outcome = if create_object {
        publisher.publish_durable_create(object, &bytes)
    } else {
        publisher.publish_durable_replace(object, &bytes)
    }
    .map_err(|source| ManifestServiceError::Publish {
        role: ManifestRole::PendingReleases,
        source,
    })?;
    validate_manifest_publish_outcome(
        ManifestRole::PendingReleases,
        object,
        bytes.len() as u64,
        &outcome,
    )?;
    Ok(PendingReleasesManifestWrite::new(decoded, outcome))
}

fn decode_branch_catalog(
    object: &ObjectName,
    bytes: &[u8],
) -> ManifestServiceResult<BranchCatalogManifest> {
    decode_branch_catalog_manifest(bytes).map_err(|source| ManifestServiceError::Decode {
        role: ManifestRole::BranchCatalog,
        object: object.clone(),
        source,
    })
}

fn publish_branch_catalog(
    backend: &dyn Backend,
    object: &ObjectName,
    manifest: &BranchCatalogManifest,
    create_object: bool,
) -> ManifestServiceResult<BranchCatalogManifestWrite> {
    let bytes = encode_branch_catalog_manifest(manifest).map_err(|source| {
        ManifestServiceError::Encode {
            role: ManifestRole::BranchCatalog,
            object: object.clone(),
            source,
        }
    })?;
    let decoded = decode_branch_catalog(object, &bytes)?;
    let publisher = ObjectPublisher::new(backend);
    let outcome = if create_object {
        publisher.publish_durable_create(object, &bytes)
    } else {
        publisher.publish_durable_replace(object, &bytes)
    }
    .map_err(|source| ManifestServiceError::Publish {
        role: ManifestRole::BranchCatalog,
        source,
    })?;
    validate_manifest_publish_outcome(
        ManifestRole::BranchCatalog,
        object,
        bytes.len() as u64,
        &outcome,
    )?;
    Ok(BranchCatalogManifestWrite::new(decoded, outcome))
}

fn validate_active_wal_segment(active_wal_segment: u64) -> ManifestServiceResult<()> {
    if active_wal_segment != 0 {
        return Ok(());
    }

    Err(ManifestServiceError::InvalidRecoveryFact {
        role: ManifestRole::Database,
        object: database_manifest_object()?,
        field: "active_wal_segment",
    })
}

fn table_manifest_object(branch_id: &str) -> ManifestServiceResult<ObjectName> {
    ObjectLayout::branch_table_manifest(branch_id).map_err(|source| ManifestServiceError::Layout {
        role: ManifestRole::Table,
        source,
    })
}

fn is_branch_table_manifest_object(object: &ObjectName) -> bool {
    matches!(
        ObjectLayout::classify_table_object(object),
        Ok(Some(TableObjectClassification::Manifest { .. }))
    )
}

fn branch_id_from_table_manifest_object(
    object: &ObjectName,
) -> ManifestServiceResult<strata_core::BranchId> {
    let Ok(Some(TableObjectClassification::Manifest { branch_id })) =
        ObjectLayout::classify_table_object(object)
    else {
        return Err(ManifestServiceError::InvalidRecoveryFact {
            role: ManifestRole::Table,
            object: object.clone(),
            field: "branch_id",
        });
    };
    strata_core::BranchId::parse_str(branch_id).map_err(|_| {
        ManifestServiceError::InvalidRecoveryFact {
            role: ManifestRole::Table,
            object: object.clone(),
            field: "branch_id",
        }
    })
}

fn read_optional(
    backend: &dyn Backend,
    role: ManifestRole,
    object: &ObjectName,
) -> ManifestServiceResult<Option<Vec<u8>>> {
    match backend.read_object(object) {
        Ok(bytes) => Ok(Some(bytes)),
        // Only NotFound is absence. Other backend failures are not downgraded
        // because open/recovery must distinguish missing metadata from an
        // unavailable or corrupt backend.
        Err(source) if source.kind() == BackendErrorKind::NotFound => Ok(None),
        Err(source) => Err(ManifestServiceError::Read {
            role,
            object: object.clone(),
            source,
        }),
    }
}

fn decode_database_manifest(
    object: &ObjectName,
    bytes: &[u8],
) -> ManifestServiceResult<DatabaseManifest> {
    decode_manifest(bytes).map_err(|source| ManifestServiceError::Decode {
        role: ManifestRole::Database,
        object: object.clone(),
        source,
    })
}

fn publish_database_manifest(
    backend: &dyn Backend,
    object: &ObjectName,
    manifest: &DatabaseManifest,
    create_object: bool,
) -> ManifestServiceResult<DatabaseManifestWrite> {
    let bytes = encode_manifest(manifest).map_err(|source| ManifestServiceError::Encode {
        role: ManifestRole::Database,
        object: object.clone(),
        source,
    })?;
    // Decode our freshly encoded bytes before publish so callers receive the
    // exact durable representation facts, not an unchecked input value.
    let decoded = decode_database_manifest(object, &bytes)?;
    let publisher = ObjectPublisher::new(backend);
    let outcome = if create_object {
        publisher.publish_durable_create(object, &bytes)
    } else {
        publisher.publish_durable_replace(object, &bytes)
    }
    .map_err(|source| ManifestServiceError::Publish {
        role: ManifestRole::Database,
        source,
    })?;
    validate_manifest_publish_outcome(
        ManifestRole::Database,
        object,
        bytes.len() as u64,
        &outcome,
    )?;

    Ok(DatabaseManifestWrite::new(decoded, outcome))
}

fn decode_branch_table_manifest(
    branch_id: strata_core::BranchId,
    object: &ObjectName,
    bytes: &[u8],
) -> ManifestServiceResult<TableManifest> {
    let manifest = decode_table_manifest(bytes).map_err(|source| ManifestServiceError::Decode {
        role: ManifestRole::Table,
        object: object.clone(),
        source,
    })?;
    validate_table_manifest_branch(branch_id, object, &manifest)?;
    Ok(manifest)
}

fn publish_branch_table_manifest(
    backend: &dyn Backend,
    branch_id: strata_core::BranchId,
    object: &ObjectName,
    manifest: &TableManifest,
    create_object: bool,
) -> ManifestServiceResult<TableManifestWrite> {
    let bytes = encode_table_manifest(manifest).map_err(|source| ManifestServiceError::Encode {
        role: ManifestRole::Table,
        object: object.clone(),
        source,
    })?;
    let decoded = decode_branch_table_manifest(branch_id, object, &bytes)?;
    let publisher = ObjectPublisher::new(backend);
    let outcome = if create_object {
        publisher.publish_durable_create(object, &bytes)
    } else {
        publisher.publish_durable_replace(object, &bytes)
    }
    .map_err(|source| ManifestServiceError::Publish {
        role: ManifestRole::Table,
        source,
    })?;
    validate_manifest_publish_outcome(ManifestRole::Table, object, bytes.len() as u64, &outcome)?;
    Ok(TableManifestWrite::new(decoded, outcome))
}

fn validate_table_manifest_branch(
    branch_id: strata_core::BranchId,
    object: &ObjectName,
    manifest: &TableManifest,
) -> ManifestServiceResult<()> {
    if manifest.branch_id() == branch_id {
        Ok(())
    } else {
        Err(ManifestServiceError::InvalidRecoveryFact {
            role: ManifestRole::Table,
            object: object.clone(),
            field: "branch_id",
        })
    }
}

fn validate_manifest_publish_outcome(
    role: ManifestRole,
    object: &ObjectName,
    byte_count: u64,
    outcome: &PublishOutcome,
) -> ManifestServiceResult<()> {
    validate_publish_outcome(object, byte_count, outcome).map_err(|mismatch| {
        ManifestServiceError::InvalidPublishMetadata {
            role,
            object: mismatch.object().clone(),
            field: mismatch.field(),
        }
    })
}

fn validate_codec(
    object: &ObjectName,
    expected_codec_id: &str,
    manifest: DatabaseManifest,
) -> ManifestServiceResult<DatabaseManifest> {
    if manifest.codec_id() == expected_codec_id {
        Ok(manifest)
    } else {
        Err(ManifestServiceError::CodecMismatch {
            object: object.clone(),
            expected: expected_codec_id.to_owned(),
            actual: manifest.codec_id().to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DatabaseManifestService, DatabaseManifestWrite, ManifestRole, ManifestServiceError,
        ManifestServiceResult, TableManifestService,
    };
    #[cfg(all(feature = "localfs", unix))]
    use crate::backend::local_fs::LocalFsBackend;
    use crate::backend::{
        memory::MemoryBackend, Backend, BackendCapabilities, BackendCapability, BackendError,
        BackendErrorKind, BackendMetadata, BackendRange, BackendResult, PublishDurability,
        PublishError, PublishFailureKind, PublishMode, PublishOutcome,
    };
    use crate::branch::facts::BranchLevel;
    use crate::format::{
        encode_manifest, encode_table_manifest, encode_wal_segment_header, DatabaseManifest,
        FormatError, TableManifest, TableManifestLevel, TableManifestTableBounds,
        TableManifestTableFacts, TableManifestTableProvenance, TableManifestTableRef,
        WalSegmentHeader,
    };
    use crate::layout::{LayoutError, ObjectLayout};
    use crate::object::{ObjectName, ObjectNameError, ObjectPrefix, MAX_OBJECT_NAME_BYTES};
    use crate::table::TableIdentity;
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::collection::vec;
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::prelude::{prop_oneof, Just, Strategy};
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::prop_assert_eq;
    #[cfg(not(target_arch = "wasm32"))]
    use proptest::test_runner::{
        Config, FileFailurePersistence, TestCaseError, TestCaseResult, TestRunner,
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use strata_core::{BranchId, CommitVersion, Timestamp};

    const ALL_PUBLISH_FAILURE_KINDS: [PublishFailureKind; 5] = [
        PublishFailureKind::Unsupported,
        PublishFailureKind::PreconditionFailed,
        PublishFailureKind::FailedBeforeVisibility,
        PublishFailureKind::VisibilityUnknown,
        PublishFailureKind::VisibleDurabilityUnconfirmed,
    ];

    #[cfg(not(target_arch = "wasm32"))]
    const MODEL_DATABASE_ID: [u8; 16] = [0x42; 16];
    #[cfg(not(target_arch = "wasm32"))]
    const MODEL_CODEC_ID: &str = "identity";

    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct DatabaseManifestModel {
        database_id: [u8; 16],
        codec_id: &'static str,
        active_wal_segment: u64,
        snapshot_id: Option<u64>,
        snapshot_watermark: Option<u64>,
        flushed_through_commit_id: Option<CommitVersion>,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl DatabaseManifestModel {
        const fn initial() -> Self {
            Self {
                database_id: MODEL_DATABASE_ID,
                codec_id: MODEL_CODEC_ID,
                active_wal_segment: 1,
                snapshot_id: None,
                snapshot_watermark: None,
                flushed_through_commit_id: None,
            }
        }

        fn assert_matches(&self, manifest: &DatabaseManifest) -> TestCaseResult {
            prop_assert_eq!(manifest.database_id(), &self.database_id);
            prop_assert_eq!(manifest.codec_id(), self.codec_id);
            prop_assert_eq!(manifest.active_wal_segment(), self.active_wal_segment);
            prop_assert_eq!(manifest.snapshot_id(), self.snapshot_id);
            prop_assert_eq!(manifest.snapshot_watermark(), self.snapshot_watermark);
            prop_assert_eq!(
                manifest.flushed_through_commit_id(),
                self.flushed_through_commit_id
            );
            Ok(())
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Clone, Debug)]
    enum ManifestUpdate {
        ActiveWalSegment(u64),
        SnapshotFacts {
            snapshot_id: u64,
            snapshot_watermark: CommitVersion,
        },
        FlushWatermark(CommitVersion),
        RejectZeroActiveWalSegment,
        RejectZeroSnapshotId,
        RejectZeroSnapshotWatermark,
        RejectZeroFlushWatermark,
    }

    #[derive(Debug, Default)]
    struct RecordingBackend {
        objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
        outcome_override: Mutex<Option<PublishOutcome>>,
    }

    impl RecordingBackend {
        fn new() -> Self {
            Self::default()
        }

        fn override_next_publish_outcome(&self, outcome: PublishOutcome) {
            *self
                .outcome_override
                .lock()
                .expect("recording backend lock") = Some(outcome);
        }
    }

    impl Backend for RecordingBackend {
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities::from_slice(&[
                BackendCapability::ReadObject,
                BackendCapability::ReadRange,
                BackendCapability::WriteObject,
                BackendCapability::DeleteObject,
                BackendCapability::ListPrefix,
                BackendCapability::ObjectMetadata,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
            ])
        }

        fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
            self.objects
                .lock()
                .expect("recording backend lock")
                .get(name)
                .cloned()
                .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
            let bytes = self.read_object(name)?;
            let start = usize::try_from(range.offset())
                .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
            let end = range.end_offset().ok_or_else(|| {
                BackendError::new(BackendErrorKind::InvalidRange, "range overflow")
            })?;
            let end = usize::try_from(end)
                .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
            if start > bytes.len() {
                return Ok(Vec::new());
            }
            Ok(bytes[start..bytes.len().min(end)].to_vec())
        }

        fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
            self.objects
                .lock()
                .expect("recording backend lock")
                .insert(name.clone(), bytes.to_vec());
            Ok(BackendMetadata::new(bytes.len() as u64, None))
        }

        fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
            let removed = self
                .objects
                .lock()
                .expect("recording backend lock")
                .remove(name)
                .is_some();
            crate::backend::durable_delete_result(name, removed)
        }

        fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
            Ok(self
                .objects
                .lock()
                .expect("recording backend lock")
                .keys()
                .filter(|name| name.as_str().starts_with(prefix.as_str()))
                .cloned()
                .collect())
        }

        fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
            self.objects
                .lock()
                .expect("recording backend lock")
                .get(name)
                .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
                .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn publish_object(
            &self,
            name: &ObjectName,
            bytes: &[u8],
            mode: PublishMode,
        ) -> Result<PublishOutcome, PublishError> {
            let mut objects = self.objects.lock().expect("recording backend lock");
            // The in-memory recording backend mirrors the publish precondition:
            // create must not replace existing bytes, while replace may.
            if mode == PublishMode::Create && objects.contains_key(name) {
                return Err(PublishError::precondition_failed(
                    name,
                    "object already exists",
                ));
            }
            objects.insert(name.clone(), bytes.to_vec());
            if let Some(outcome) = self
                .outcome_override
                .lock()
                .expect("recording backend lock")
                .take()
            {
                return Ok(outcome);
            }
            Ok(PublishOutcome::new(
                name.clone(),
                BackendMetadata::new(bytes.len() as u64, None),
                PublishDurability::Durable,
            ))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn manifest_update_strategy() -> impl Strategy<Value = ManifestUpdate> {
        prop_oneof![
            (1_u64..=1_000_000).prop_map(ManifestUpdate::ActiveWalSegment),
            (1_u64..=1_000_000, 1_u64..=1_000_000).prop_map(|(snapshot_id, snapshot_watermark)| {
                ManifestUpdate::SnapshotFacts {
                    snapshot_id,
                    snapshot_watermark: CommitVersion::new(snapshot_watermark),
                }
            }),
            (1_u64..=1_000_000).prop_map(|commit_id| ManifestUpdate::FlushWatermark(
                CommitVersion::new(commit_id)
            )),
            Just(ManifestUpdate::RejectZeroActiveWalSegment),
            Just(ManifestUpdate::RejectZeroSnapshotId),
            Just(ManifestUpdate::RejectZeroSnapshotWatermark),
            Just(ManifestUpdate::RejectZeroFlushWatermark),
        ]
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn test_failure(context: &str, error: impl std::fmt::Debug) -> TestCaseError {
        TestCaseError::fail(format!("{context}: {error:?}"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn required_current_manifest(
        service: &DatabaseManifestService<'_>,
    ) -> Result<DatabaseManifest, TestCaseError> {
        service
            .load_current()
            .map_err(|error| test_failure("load current manifest", error))?
            .ok_or_else(|| TestCaseError::fail("database manifest is missing"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn assert_persisted_database_manifest(
        backend: &dyn Backend,
        service: &DatabaseManifestService<'_>,
        object: &ObjectName,
        model: &DatabaseManifestModel,
    ) -> TestCaseResult {
        let loaded = required_current_manifest(service)?;
        let stored = backend
            .read_object(object)
            .map_err(|error| test_failure("read stored manifest bytes", error))?;
        let encoded = encode_manifest(&loaded)
            .map_err(|error| test_failure("encode loaded manifest", error))?;

        model.assert_matches(&loaded)?;
        prop_assert_eq!(stored, encoded);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn assert_database_manifest_write(
        write: &super::DatabaseManifestWrite,
        object: &ObjectName,
        model: &DatabaseManifestModel,
    ) -> TestCaseResult {
        let encoded = encode_manifest(write.manifest())
            .map_err(|error| test_failure("encode written manifest", error))?;

        model.assert_matches(write.manifest())?;
        prop_assert_eq!(write.outcome().object(), object);
        prop_assert_eq!(
            write.outcome().metadata().size_bytes(),
            encoded.len() as u64
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn assert_invalid_recovery_fact(
        error: ManifestServiceError,
        object: &ObjectName,
        field: &'static str,
    ) -> TestCaseResult {
        prop_assert_eq!(
            error,
            ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object: object.clone(),
                field,
            }
        );
        Ok(())
    }

    fn database_manifest_bytes_with_recovery_words(
        active_wal_segment: u64,
        snapshot_watermark: u64,
        snapshot_id: u64,
        flushed_through_commit_id: u64,
    ) -> Vec<u8> {
        let codec_id = "identity";
        let manifest = DatabaseManifest::new([0x22; 16], codec_id)
            .expect("manifest")
            .with_recovery_facts(7, Some(42), Some(3), Some(CommitVersion::new(41)))
            .expect("recovery facts");
        let mut bytes = encode_manifest(&manifest).expect("encode manifest");
        let active_offset = 4 + 4 + 16 + 4 + codec_id.len();
        let snapshot_watermark_offset = active_offset + 8;
        let snapshot_id_offset = snapshot_watermark_offset + 8;
        let flush_offset = snapshot_id_offset + 8;

        bytes[active_offset..active_offset + 8].copy_from_slice(&active_wal_segment.to_le_bytes());
        bytes[snapshot_watermark_offset..snapshot_watermark_offset + 8]
            .copy_from_slice(&snapshot_watermark.to_le_bytes());
        bytes[snapshot_id_offset..snapshot_id_offset + 8]
            .copy_from_slice(&snapshot_id.to_le_bytes());
        bytes[flush_offset..flush_offset + 8]
            .copy_from_slice(&flushed_through_commit_id.to_le_bytes());
        refresh_manifest_crc(&mut bytes);
        bytes
    }

    fn database_manifest_bytes_with_version(version: u32) -> Vec<u8> {
        let mut bytes = encode_manifest(
            &DatabaseManifest::new([0x22; 16], "identity").expect("database manifest"),
        )
        .expect("encode manifest");
        bytes[4..8].copy_from_slice(&version.to_le_bytes());
        bytes
    }

    fn database_manifest_bytes_with_checksum_mismatch() -> Vec<u8> {
        let mut bytes = encode_manifest(
            &DatabaseManifest::new([0x22; 16], "identity").expect("database manifest"),
        )
        .expect("encode manifest");
        bytes[8] ^= 0xff;
        bytes
    }

    fn refresh_manifest_crc(bytes: &mut [u8]) {
        let checksum_offset = bytes.len() - 4;
        let checksum = crc32fast::hash(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
    }

    fn write_database_manifest_bytes(backend: &dyn Backend, bytes: &[u8]) -> ObjectName {
        let object = ObjectLayout::database_manifest().expect("manifest name");
        backend
            .write_object(&object, bytes)
            .expect("write manifest bytes");
        object
    }

    fn corrupt_database_manifest_bytes() -> Vec<u8> {
        let mut bytes = encode_manifest(
            &DatabaseManifest::new([0x44; 16], "identity").expect("database manifest"),
        )
        .expect("encode database manifest");
        bytes[0] = b'X';
        bytes
    }

    fn wal_like_bytes() -> Vec<u8> {
        encode_wal_segment_header(&WalSegmentHeader::new(7, [0x55; 16]))
    }

    fn assert_table_layout_error(error: ManifestServiceError, expected: &LayoutError) {
        match error {
            ManifestServiceError::Layout { role, source } => {
                assert_eq!(role, ManifestRole::Table);
                assert_eq!(source, *expected);
            }
            other => panic!("expected table layout error, got {other:?}"),
        }
    }

    fn assert_decode_invalid_value(
        error: ManifestServiceError,
        object: &ObjectName,
        expected_field: &'static str,
    ) {
        match error {
            ManifestServiceError::Decode {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, *object);
                assert_eq!(
                    source,
                    FormatError::InvalidValue {
                        field: expected_field
                    }
                );
            }
            other => panic!("expected decode invalid value, got {other:?}"),
        }
    }

    fn assert_database_decode_error(error: ManifestServiceError, object: &ObjectName) {
        match error {
            ManifestServiceError::Decode {
                role,
                object: actual,
                source: _,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, *object);
            }
            other => panic!("expected database decode error, got {other:?}"),
        }
    }

    fn assert_encode_error(
        error: ManifestServiceError,
        object: &ObjectName,
        expected_source: &FormatError,
    ) {
        match error {
            ManifestServiceError::Encode {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, *object);
                assert_eq!(source, *expected_source);
            }
            other => panic!("expected encode error, got {other:?}"),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn assert_rejected_update_preserves_stored_manifest(
        backend: &dyn Backend,
        service: &DatabaseManifestService<'_>,
        object: &ObjectName,
        before_bytes: Vec<u8>,
        model: &DatabaseManifestModel,
    ) -> TestCaseResult {
        let after_bytes = backend
            .read_object(object)
            .map_err(|error| test_failure("read stored manifest bytes after rejection", error))?;
        prop_assert_eq!(after_bytes, before_bytes);
        assert_persisted_database_manifest(backend, service, object, model)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn run_manifest_update_sequence(updates: Vec<ManifestUpdate>) -> TestCaseResult {
        let backend = RecordingBackend::new();
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let mut model = DatabaseManifestModel::initial();

        let write = service
            .create_initial(model.database_id, model.codec_id)
            .map_err(|error| test_failure("create initial manifest", error))?;
        assert_database_manifest_write(&write, &object, &model)?;
        assert_persisted_database_manifest(&backend, &service, &object, &model)?;

        for update in updates {
            let before_bytes = backend
                .read_object(&object)
                .map_err(|error| test_failure("read manifest bytes before update", error))?;
            match update {
                ManifestUpdate::ActiveWalSegment(segment) => {
                    let write = service
                        .persist_active_wal_segment(segment)
                        .map_err(|error| test_failure("persist active WAL segment", error))?;
                    model.active_wal_segment = segment;
                    assert_database_manifest_write(&write, &object, &model)?;
                    assert_persisted_database_manifest(&backend, &service, &object, &model)?;
                }
                ManifestUpdate::SnapshotFacts {
                    snapshot_id,
                    snapshot_watermark,
                } => {
                    let write = service
                        .persist_snapshot_facts(snapshot_id, snapshot_watermark)
                        .map_err(|error| test_failure("persist snapshot facts", error))?;
                    model.snapshot_id = Some(snapshot_id);
                    model.snapshot_watermark = Some(snapshot_watermark.as_u64());
                    assert_database_manifest_write(&write, &object, &model)?;
                    assert_persisted_database_manifest(&backend, &service, &object, &model)?;
                }
                ManifestUpdate::FlushWatermark(commit_id) => {
                    let write = service
                        .persist_flush_watermark(commit_id)
                        .map_err(|error| test_failure("persist flush watermark", error))?;
                    model.flushed_through_commit_id = Some(commit_id);
                    assert_database_manifest_write(&write, &object, &model)?;
                    assert_persisted_database_manifest(&backend, &service, &object, &model)?;
                }
                ManifestUpdate::RejectZeroActiveWalSegment => {
                    let error = service
                        .persist_active_wal_segment(0)
                        .expect_err("zero active WAL segment should be rejected");
                    assert_invalid_recovery_fact(error, &object, "active_wal_segment")?;
                    assert_rejected_update_preserves_stored_manifest(
                        &backend,
                        &service,
                        &object,
                        before_bytes,
                        &model,
                    )?;
                }
                ManifestUpdate::RejectZeroSnapshotId => {
                    let error = service
                        .persist_snapshot_facts(0, CommitVersion::new(7))
                        .expect_err("zero snapshot id should be rejected");
                    assert_invalid_recovery_fact(error, &object, "snapshot_id")?;
                    assert_rejected_update_preserves_stored_manifest(
                        &backend,
                        &service,
                        &object,
                        before_bytes,
                        &model,
                    )?;
                }
                ManifestUpdate::RejectZeroSnapshotWatermark => {
                    let error = service
                        .persist_snapshot_facts(7, CommitVersion::ZERO)
                        .expect_err("zero snapshot watermark should be rejected");
                    assert_invalid_recovery_fact(error, &object, "snapshot_watermark")?;
                    assert_rejected_update_preserves_stored_manifest(
                        &backend,
                        &service,
                        &object,
                        before_bytes,
                        &model,
                    )?;
                }
                ManifestUpdate::RejectZeroFlushWatermark => {
                    let error = service
                        .persist_flush_watermark(CommitVersion::ZERO)
                        .expect_err("zero flush watermark should be rejected");
                    assert_invalid_recovery_fact(error, &object, "flushed_through_commit_id")?;
                    assert_rejected_update_preserves_stored_manifest(
                        &backend,
                        &service,
                        &object,
                        before_bytes,
                        &model,
                    )?;
                }
            }
        }

        Ok(())
    }

    #[derive(Debug)]
    struct ReadFailureBackend {
        kind: BackendErrorKind,
    }

    impl ReadFailureBackend {
        const fn new(kind: BackendErrorKind) -> Self {
            Self { kind }
        }
    }

    impl Backend for ReadFailureBackend {
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities::from_slice(&[
                BackendCapability::ReadObject,
                BackendCapability::ReadRange,
                BackendCapability::WriteObject,
                BackendCapability::DeleteObject,
                BackendCapability::ListPrefix,
                BackendCapability::ObjectMetadata,
            ])
        }

        fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
            Err(BackendError::new(self.kind, "read failure"))
        }

        fn read_range(&self, _name: &ObjectName, _range: BackendRange) -> BackendResult<Vec<u8>> {
            Err(BackendError::new(self.kind, "read failure"))
        }

        fn write_object(
            &self,
            _name: &ObjectName,
            _bytes: &[u8],
        ) -> BackendResult<BackendMetadata> {
            Err(BackendError::new(
                BackendErrorKind::UnsupportedOperation,
                "write is not supported",
            ))
        }

        fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
            crate::backend::failed_delete_result(
                name,
                BackendError::new(
                    BackendErrorKind::UnsupportedOperation,
                    "delete is not supported",
                ),
            )
        }

        fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
            Ok(Vec::new())
        }

        fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
            Err(BackendError::new(BackendErrorKind::NotFound, "not found"))
        }
    }

    #[derive(Debug)]
    struct PublishFailureBackend {
        kind: PublishFailureKind,
    }

    impl PublishFailureBackend {
        const fn new(kind: PublishFailureKind) -> Self {
            Self { kind }
        }
    }

    impl Backend for PublishFailureBackend {
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities::from_slice(&[
                BackendCapability::ReadObject,
                BackendCapability::ReadRange,
                BackendCapability::WriteObject,
                BackendCapability::DeleteObject,
                BackendCapability::ListPrefix,
                BackendCapability::ObjectMetadata,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
            ])
        }

        fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
            Err(BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn read_range(&self, _name: &ObjectName, _range: BackendRange) -> BackendResult<Vec<u8>> {
            Err(BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn write_object(
            &self,
            _name: &ObjectName,
            _bytes: &[u8],
        ) -> BackendResult<BackendMetadata> {
            Err(BackendError::new(
                BackendErrorKind::UnsupportedOperation,
                "write is not supported",
            ))
        }

        fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
            crate::backend::failed_delete_result(
                name,
                BackendError::new(
                    BackendErrorKind::UnsupportedOperation,
                    "delete is not supported",
                ),
            )
        }

        fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
            Ok(Vec::new())
        }

        fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
            Err(BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn publish_object(
            &self,
            name: &ObjectName,
            _bytes: &[u8],
            _mode: PublishMode,
        ) -> Result<PublishOutcome, PublishError> {
            // This backend never stores bytes; it only checks that each
            // PublishFailureKind is preserved through the service error.
            Err(PublishError::new(
                name.clone(),
                self.kind,
                BackendError::new(BackendErrorKind::Interrupted, "publish failed"),
            ))
        }
    }

    #[derive(Debug)]
    struct StoredPublishFailureBackend {
        kind: PublishFailureKind,
        objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    }

    impl StoredPublishFailureBackend {
        fn with_object(kind: PublishFailureKind, name: ObjectName, bytes: &[u8]) -> Self {
            let mut objects = BTreeMap::new();
            objects.insert(name, bytes.to_vec());
            Self {
                kind,
                objects: Mutex::new(objects),
            }
        }
    }

    impl Backend for StoredPublishFailureBackend {
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities::from_slice(&[
                BackendCapability::ReadObject,
                BackendCapability::ReadRange,
                BackendCapability::WriteObject,
                BackendCapability::DeleteObject,
                BackendCapability::ListPrefix,
                BackendCapability::ObjectMetadata,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
            ])
        }

        fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
            self.objects
                .lock()
                .expect("stored publish backend lock")
                .get(name)
                .cloned()
                .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
            let bytes = self.read_object(name)?;
            let start = usize::try_from(range.offset())
                .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
            let end = range.end_offset().ok_or_else(|| {
                BackendError::new(BackendErrorKind::InvalidRange, "range overflow")
            })?;
            let end = usize::try_from(end)
                .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
            if start > bytes.len() {
                return Ok(Vec::new());
            }
            Ok(bytes[start..bytes.len().min(end)].to_vec())
        }

        fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
            self.objects
                .lock()
                .expect("stored publish backend lock")
                .insert(name.clone(), bytes.to_vec());
            Ok(BackendMetadata::new(bytes.len() as u64, None))
        }

        fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
            let removed = self
                .objects
                .lock()
                .expect("stored publish backend lock")
                .remove(name)
                .is_some();
            crate::backend::durable_delete_result(name, removed)
        }

        fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
            Ok(self
                .objects
                .lock()
                .expect("stored publish backend lock")
                .keys()
                .filter(|name| name.as_str().starts_with(prefix.as_str()))
                .cloned()
                .collect())
        }

        fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
            self.objects
                .lock()
                .expect("stored publish backend lock")
                .get(name)
                .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
                .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
        }

        fn publish_object(
            &self,
            name: &ObjectName,
            _bytes: &[u8],
            _mode: PublishMode,
        ) -> Result<PublishOutcome, PublishError> {
            // Existing bytes stay authoritative for failure kinds that promise
            // no visible replacement. Tests assert this separately from the
            // service's error mapping.
            Err(PublishError::new(
                name.clone(),
                self.kind,
                BackendError::new(BackendErrorKind::Interrupted, "publish failed"),
            ))
        }
    }

    const fn publish_failure_implies_no_visible_replacement(kind: PublishFailureKind) -> bool {
        matches!(
            kind,
            PublishFailureKind::Unsupported
                | PublishFailureKind::PreconditionFailed
                | PublishFailureKind::FailedBeforeVisibility
        )
    }

    #[test]
    fn database_manifest_load_current_reports_absent_manifest() {
        let backend = MemoryBackend::new();
        let service = DatabaseManifestService::new(&backend);

        assert_eq!(service.load_current(), Ok(None));
    }

    #[test]
    fn database_manifest_load_current_preserves_backend_read_failure() {
        let backend = ReadFailureBackend::new(BackendErrorKind::Unavailable);
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");

        let error = service
            .load_current()
            .expect_err("backend read failure must not become missing");

        match error {
            ManifestServiceError::Read {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, object);
                assert_eq!(source.kind(), BackendErrorKind::Unavailable);
            }
            other => panic!("expected database read error, got {other:?}"),
        }
    }

    #[test]
    fn database_manifest_load_required_reports_missing_manifest() {
        let backend = MemoryBackend::new();
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");

        assert_eq!(
            service.load_required(),
            Err(ManifestServiceError::Missing {
                role: ManifestRole::Database,
                object
            })
        );
    }

    #[test]
    fn database_manifest_publish_rejects_invalid_publish_outcome_metadata() {
        let backend = RecordingBackend::new();
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");
        backend.override_next_publish_outcome(PublishOutcome::new(
            object.clone(),
            BackendMetadata::new(1, None),
            PublishDurability::Durable,
        ));

        let error = service
            .create_initial([0x22; 16], "identity")
            .expect_err("bad publish metadata should fail");
        assert_eq!(
            error,
            ManifestServiceError::InvalidPublishMetadata {
                role: ManifestRole::Database,
                object,
                field: "size_bytes",
            }
        );
    }

    #[test]
    fn database_manifest_load_for_codec_reports_absent_manifest() {
        let backend = MemoryBackend::new();
        let service = DatabaseManifestService::new(&backend);

        assert_eq!(service.load_current_for_codec("identity"), Ok(None));
    }

    #[test]
    fn database_manifest_load_for_codec_preserves_backend_read_failure() {
        let backend = ReadFailureBackend::new(BackendErrorKind::Interrupted);
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");

        let error = service
            .load_current_for_codec("identity")
            .expect_err("codec load must preserve read failure");

        match error {
            ManifestServiceError::Read {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, object);
                assert_eq!(source.kind(), BackendErrorKind::Interrupted);
            }
            other => panic!("expected database read error, got {other:?}"),
        }
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn database_manifest_create_and_load_round_trips_v1_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");

        let write = service
            .create_initial([0x22; 16], "identity")
            .expect("create manifest");
        let stored = backend.read_object(&object).expect("manifest bytes");
        let expected = encode_manifest(write.manifest()).expect("encode manifest");

        assert_eq!(write.manifest().database_id(), &[0x22; 16]);
        assert_eq!(write.manifest().codec_id(), "identity");
        assert_eq!(write.manifest().active_wal_segment(), 1);
        assert_eq!(write.outcome().object(), &object);
        assert_eq!(
            write.outcome().metadata().size_bytes(),
            expected.len() as u64
        );
        assert_eq!(write.outcome().durability(), PublishDurability::Durable);
        assert_eq!(stored, expected);
        assert_eq!(
            service.load_current().expect("load current"),
            Some(write.manifest().clone())
        );
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn database_manifest_create_refuses_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = DatabaseManifestService::new(&backend);

        let first = service
            .create_initial([0x22; 16], "identity")
            .expect("first create");
        let error = service
            .create_initial([0x33; 16], "identity")
            .expect_err("second create should fail");

        assert_publish_error(
            error,
            ManifestRole::Database,
            PublishFailureKind::PreconditionFailed,
        );
        assert_eq!(
            service.load_current().expect("load current"),
            Some(first.manifest().clone())
        );
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn database_manifest_active_wal_update_preserves_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = DatabaseManifestService::new(&backend);
        service
            .create_initial([0x22; 16], "identity")
            .expect("create manifest");

        let write = service
            .persist_active_wal_segment(7)
            .expect("active WAL update");

        assert_eq!(write.manifest().database_id(), &[0x22; 16]);
        assert_eq!(write.manifest().codec_id(), "identity");
        assert_eq!(write.manifest().active_wal_segment(), 7);
        assert_eq!(
            service.load_current().expect("load current"),
            Some(write.manifest().clone())
        );
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn database_manifest_rejects_zero_active_wal_without_publish() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let initial = service
            .create_initial([0x22; 16], "identity")
            .expect("create manifest");

        let error = service
            .persist_active_wal_segment(0)
            .expect_err("zero active WAL segment rejected");

        assert_eq!(
            error,
            ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object,
                field: "active_wal_segment"
            }
        );
        assert_eq!(
            service.load_current().expect("load current"),
            Some(initial.manifest().clone())
        );
    }

    #[test]
    fn database_manifest_rejects_invalid_recovery_facts_before_backend_access() {
        let backend = ReadFailureBackend::new(BackendErrorKind::Unavailable);
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");

        assert_eq!(
            service.persist_active_wal_segment(0),
            Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object: object.clone(),
                field: "active_wal_segment"
            })
        );
        assert_eq!(
            service.persist_snapshot_facts(0, CommitVersion::new(42)),
            Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object: object.clone(),
                field: "snapshot_id"
            })
        );
        assert_eq!(
            service.persist_snapshot_facts(3, CommitVersion::ZERO),
            Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object: object.clone(),
                field: "snapshot_watermark"
            })
        );
        assert_eq!(
            service.persist_flush_watermark(CommitVersion::ZERO),
            Err(ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object,
                field: "flushed_through_commit_id"
            })
        );
    }

    #[test]
    fn database_manifest_decode_rejects_invalid_recovery_fact_bytes() {
        for (bytes, expected_field) in [
            (
                database_manifest_bytes_with_recovery_words(0, 0, 0, 0),
                "active_wal_segment",
            ),
            (
                database_manifest_bytes_with_recovery_words(1, 42, 0, 0),
                "snapshot_recovery_pair",
            ),
            (
                database_manifest_bytes_with_recovery_words(1, 0, 3, 0),
                "snapshot_recovery_pair",
            ),
        ] {
            let backend = MemoryBackend::new();
            let object = write_database_manifest_bytes(&backend, &bytes);
            let service = DatabaseManifestService::new(&backend);

            let error = service
                .load_current()
                .expect_err("invalid recovery fact bytes should fail");

            assert_decode_invalid_value(error, &object, expected_field);
        }
    }

    #[test]
    fn database_manifest_load_routes_version_and_checksum_failures_to_decode() {
        let cases = [
            (
                database_manifest_bytes_with_version(2),
                FormatError::PreV1Format {
                    format: "database_manifest",
                    version: 2,
                },
            ),
            (
                database_manifest_bytes_with_version(9),
                FormatError::FutureFormat {
                    format: "database_manifest",
                    version: 9,
                    max_supported: 1,
                },
            ),
            (
                database_manifest_bytes_with_checksum_mismatch(),
                FormatError::ChecksumMismatch {
                    format: "database_manifest",
                    expected: 0,
                    computed: 0,
                },
            ),
        ];

        for (bytes, expected_source) in cases {
            let backend = MemoryBackend::new();
            let object = write_database_manifest_bytes(&backend, &bytes);
            let service = DatabaseManifestService::new(&backend);

            let error = service
                .load_current()
                .expect_err("malformed current manifest should fail closed");

            match error {
                ManifestServiceError::Decode {
                    role,
                    object: actual,
                    source,
                } => {
                    assert_eq!(role, ManifestRole::Database);
                    assert_eq!(actual, object);
                    match expected_source {
                        FormatError::ChecksumMismatch { format, .. } => {
                            assert!(matches!(
                                source,
                                FormatError::ChecksumMismatch {
                                    format: actual_format,
                                    ..
                                } if actual_format == format
                            ));
                        }
                        other => assert_eq!(source, other),
                    }
                }
                other => panic!("expected decode error, got {other:?}"),
            }
        }
    }

    #[test]
    fn database_manifest_persists_recovery_fact_boundaries() {
        let backend = RecordingBackend::new();
        let service = DatabaseManifestService::new(&backend);
        service
            .create_initial([0x22; 16], "identity")
            .expect("create manifest");

        let min_active = service
            .persist_active_wal_segment(1)
            .expect("minimum active WAL segment");
        assert_eq!(min_active.manifest().active_wal_segment(), 1);

        let max_active = service
            .persist_active_wal_segment(u64::MAX)
            .expect("maximum active WAL segment");
        assert_eq!(max_active.manifest().active_wal_segment(), u64::MAX);

        let min_snapshot = service
            .persist_snapshot_facts(1, CommitVersion::new(1))
            .expect("minimum snapshot facts");
        assert_eq!(min_snapshot.manifest().snapshot_id(), Some(1));
        assert_eq!(min_snapshot.manifest().snapshot_watermark(), Some(1));

        let max_snapshot = service
            .persist_snapshot_facts(u64::MAX, CommitVersion::MAX)
            .expect("maximum snapshot facts");
        assert_eq!(max_snapshot.manifest().snapshot_id(), Some(u64::MAX));
        assert_eq!(max_snapshot.manifest().snapshot_watermark(), Some(u64::MAX));

        let min_flush = service
            .persist_flush_watermark(CommitVersion::new(1))
            .expect("minimum flush watermark");
        assert_eq!(
            min_flush.manifest().flushed_through_commit_id(),
            Some(CommitVersion::new(1))
        );

        let max_flush = service
            .persist_flush_watermark(CommitVersion::MAX)
            .expect("maximum flush watermark");
        assert_eq!(max_flush.manifest().database_id(), &[0x22; 16]);
        assert_eq!(max_flush.manifest().codec_id(), "identity");
        assert_eq!(max_flush.manifest().active_wal_segment(), u64::MAX);
        assert_eq!(max_flush.manifest().snapshot_id(), Some(u64::MAX));
        assert_eq!(max_flush.manifest().snapshot_watermark(), Some(u64::MAX));
        assert_eq!(
            max_flush.manifest().flushed_through_commit_id(),
            Some(CommitVersion::MAX)
        );
        assert_eq!(
            service.load_current().expect("load current"),
            Some(max_flush.manifest().clone())
        );
    }

    #[test]
    fn database_manifest_create_invalid_codec_ids_return_encode_before_publish() {
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let cases = [
            (
                String::new(),
                FormatError::InvalidLength { field: "codec_id" },
            ),
            (
                "x".repeat(257),
                FormatError::InvalidLength { field: "codec_id" },
            ),
            (
                "bad\0codec".to_string(),
                FormatError::InvalidUtf8 { field: "codec_id" },
            ),
        ];

        for (codec_id, expected_source) in cases {
            let backend = PublishFailureBackend::new(PublishFailureKind::FailedBeforeVisibility);
            let service = DatabaseManifestService::new(&backend);

            let error = service
                .create_initial([0x22; 16], codec_id)
                .expect_err("invalid codec id should fail before publish");

            assert_encode_error(error, &object, &expected_source);
        }
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn database_manifest_snapshot_and_flush_updates_are_full_replacements() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = DatabaseManifestService::new(&backend);
        service
            .create_initial([0x22; 16], "identity")
            .expect("create manifest");

        service
            .persist_snapshot_facts(3, CommitVersion::new(42))
            .expect("snapshot facts");
        let write = service
            .persist_flush_watermark(CommitVersion::new(41))
            .expect("flush watermark");

        assert_eq!(write.manifest().database_id(), &[0x22; 16]);
        assert_eq!(write.manifest().codec_id(), "identity");
        assert_eq!(write.manifest().active_wal_segment(), 1);
        assert_eq!(write.manifest().snapshot_id(), Some(3));
        assert_eq!(write.manifest().snapshot_watermark(), Some(42));
        assert_eq!(
            write.manifest().flushed_through_commit_id(),
            Some(CommitVersion::new(41))
        );
        assert_eq!(
            service.load_current().expect("load current"),
            Some(write.manifest().clone())
        );
    }

    #[test]
    fn database_manifest_publish_current_publishes_supplied_manifest_without_preserving_current_facts(
    ) {
        let backend = RecordingBackend::new();
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");
        service
            .create_initial([0x22; 16], "identity")
            .expect("create manifest");
        service
            .persist_snapshot_facts(3, CommitVersion::new(42))
            .expect("snapshot facts");
        service
            .persist_flush_watermark(CommitVersion::new(41))
            .expect("flush watermark");
        let supplied = DatabaseManifest::new([0x77; 16], "raw")
            .expect("raw manifest")
            .with_recovery_facts(9, None, None, Some(CommitVersion::new(4)))
            .expect("raw recovery facts");

        let write = service
            .publish_current(&supplied)
            .expect("publish supplied manifest");
        let stored = backend.read_object(&object).expect("manifest bytes");
        let expected = encode_manifest(&supplied).expect("encode supplied manifest");

        assert_eq!(write.manifest(), &supplied);
        assert_eq!(stored, expected);
        assert_eq!(
            service.load_current().expect("load current"),
            Some(supplied)
        );
    }

    #[test]
    fn database_manifest_publish_current_ignores_missing_or_corrupt_current_manifest() {
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let supplied = DatabaseManifest::new([0x77; 16], "raw")
            .expect("raw manifest")
            .with_recovery_facts(9, None, None, Some(CommitVersion::new(4)))
            .expect("raw recovery facts");
        let expected = encode_manifest(&supplied).expect("encode supplied manifest");

        let missing_backend = RecordingBackend::new();
        let missing_service = DatabaseManifestService::new(&missing_backend);
        let missing_write = missing_service
            .publish_current(&supplied)
            .expect("raw publish should not require current manifest");
        assert_eq!(missing_write.manifest(), &supplied);
        assert_eq!(
            missing_backend
                .read_object(&object)
                .expect("raw published bytes"),
            expected
        );

        let corrupt_backend = RecordingBackend::new();
        corrupt_backend
            .write_object(&object, b"not a manifest")
            .expect("write corrupt current manifest");
        let corrupt_service = DatabaseManifestService::new(&corrupt_backend);
        let corrupt_write = corrupt_service
            .publish_current(&supplied)
            .expect("raw publish should not decode current manifest");
        assert_eq!(corrupt_write.manifest(), &supplied);
        assert_eq!(
            corrupt_backend
                .read_object(&object)
                .expect("raw published bytes over corrupt current"),
            expected
        );
    }

    #[test]
    fn database_manifest_current_state_updates_fail_closed_on_missing_or_corrupt_current() {
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let missing_backend = MemoryBackend::new();
        let missing_service = DatabaseManifestService::new(&missing_backend);

        assert_eq!(
            missing_service.persist_active_wal_segment(2),
            Err(ManifestServiceError::Missing {
                role: ManifestRole::Database,
                object: object.clone(),
            })
        );
        assert_eq!(
            missing_service.persist_snapshot_facts(3, CommitVersion::new(4)),
            Err(ManifestServiceError::Missing {
                role: ManifestRole::Database,
                object: object.clone(),
            })
        );
        assert_eq!(
            missing_service.persist_flush_watermark(CommitVersion::new(5)),
            Err(ManifestServiceError::Missing {
                role: ManifestRole::Database,
                object: object.clone(),
            })
        );

        let corrupt_backend = RecordingBackend::new();
        corrupt_backend
            .write_object(&object, b"not a manifest")
            .expect("write corrupt manifest");
        let corrupt_service = DatabaseManifestService::new(&corrupt_backend);

        let corrupt_bytes = corrupt_backend
            .read_object(&object)
            .expect("corrupt manifest bytes");
        let error = corrupt_service
            .persist_active_wal_segment(2)
            .expect_err("active update should fail on corrupt current manifest");
        assert_database_decode_error(error, &object);
        assert_eq!(
            corrupt_backend
                .read_object(&object)
                .expect("manifest bytes after failed active update"),
            corrupt_bytes
        );

        let error = corrupt_service
            .persist_snapshot_facts(3, CommitVersion::new(4))
            .expect_err("snapshot update should fail on corrupt current manifest");
        assert_database_decode_error(error, &object);
        assert_eq!(
            corrupt_backend
                .read_object(&object)
                .expect("manifest bytes after failed snapshot update"),
            corrupt_bytes
        );

        let error = corrupt_service
            .persist_flush_watermark(CommitVersion::new(5))
            .expect_err("flush update should fail on corrupt current manifest");
        assert_database_decode_error(error, &object);
        assert_eq!(
            corrupt_backend
                .read_object(&object)
                .expect("manifest bytes after failed flush update"),
            corrupt_bytes
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn database_manifest_state_machine_preserves_recovery_facts() {
        let mut runner = TestRunner::new(Config {
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
                "proptest-regressions/manifest_state_machine.txt",
            ))),
            ..Config::default()
        });
        let strategy = vec(manifest_update_strategy(), 1..=64);

        runner
            .run(&strategy, run_manifest_update_sequence)
            .expect("manifest update sequence should match model");
    }

    #[test]
    fn database_manifest_corrupt_bytes_return_decode_error() {
        let backend = MemoryBackend::new();
        let object = ObjectLayout::database_manifest().expect("manifest name");
        backend
            .write_object(&object, b"not a manifest")
            .expect("write corrupt bytes");
        let service = DatabaseManifestService::new(&backend);

        let error = service
            .load_current()
            .expect_err("corrupt manifest should fail");

        match error {
            ManifestServiceError::Decode {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, object);
                assert!(matches!(source, FormatError::InsufficientBytes { .. }));
            }
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    #[test]
    fn database_manifest_codec_mismatch_is_typed() {
        let backend = MemoryBackend::new();
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let manifest = DatabaseManifest::new([0x22; 16], "identity").expect("manifest");
        backend
            .write_object(
                &object,
                &encode_manifest(&manifest).expect("encode manifest"),
            )
            .expect("write manifest");
        let service = DatabaseManifestService::new(&backend);

        let error = service
            .load_current_for_codec("zstd")
            .expect_err("codec mismatch");

        assert_eq!(
            error,
            ManifestServiceError::CodecMismatch {
                object,
                expected: "zstd".to_string(),
                actual: "identity".to_string()
            }
        );
    }

    #[test]
    fn database_manifest_load_for_codec_returns_matching_manifest() {
        let backend = MemoryBackend::new();
        let manifest = DatabaseManifest::new([0x22; 16], "identity").expect("manifest");
        let object = write_database_manifest_bytes(
            &backend,
            &encode_manifest(&manifest).expect("encode manifest"),
        );
        let service = DatabaseManifestService::new(&backend);

        let loaded = service
            .load_current_for_codec("identity")
            .expect("load for matching codec")
            .expect("manifest present");

        assert_eq!(object.as_str(), "manifest/current");
        assert_eq!(loaded, manifest);
    }

    #[test]
    fn database_manifest_load_for_codec_decodes_corrupt_bytes_before_codec_validation() {
        let backend = MemoryBackend::new();
        let object = write_database_manifest_bytes(&backend, b"not a manifest");
        let service = DatabaseManifestService::new(&backend);

        let error = service
            .load_current_for_codec("identity")
            .expect_err("corrupt bytes should fail before codec comparison");

        match error {
            ManifestServiceError::Decode {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(actual, object);
                assert!(matches!(source, FormatError::InsufficientBytes { .. }));
            }
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    #[test]
    fn database_manifest_durable_publish_rejects_memory_backend() {
        let backend = MemoryBackend::new();
        let service = DatabaseManifestService::new(&backend);
        let object = ObjectLayout::database_manifest().expect("manifest name");

        let error = service
            .create_initial([0x22; 16], "identity")
            .expect_err("memory backend is non-durable");

        let source = assert_publish_error(
            error,
            ManifestRole::Database,
            PublishFailureKind::Unsupported,
        );
        assert_eq!(source.object(), &object);
        assert_eq!(
            source.source_error().kind(),
            BackendErrorKind::UnsupportedOperation
        );
    }

    #[test]
    fn database_manifest_replace_rejects_memory_backend() {
        let backend = MemoryBackend::new();
        let service = DatabaseManifestService::new(&backend);
        let manifest = DatabaseManifest::new([0x22; 16], "identity").expect("manifest");
        let object = ObjectLayout::database_manifest().expect("manifest name");

        let error = service
            .publish_current(&manifest)
            .expect_err("memory backend is non-durable");

        let source = assert_publish_error(
            error,
            ManifestRole::Database,
            PublishFailureKind::Unsupported,
        );
        assert_eq!(source.object(), &object);
        assert_eq!(
            source.source_error().kind(),
            BackendErrorKind::UnsupportedOperation
        );
    }

    #[test]
    fn database_manifest_create_publish_failures_preserve_kind_and_source() {
        for kind in ALL_PUBLISH_FAILURE_KINDS {
            let backend = PublishFailureBackend::new(kind);
            let service = DatabaseManifestService::new(&backend);
            let object = ObjectLayout::database_manifest().expect("manifest name");

            let error = service
                .create_initial([0x22; 16], "identity")
                .expect_err("publish should fail");

            let source = assert_publish_error(error, ManifestRole::Database, kind);
            assert_eq!(source.object(), &object);
            assert_eq!(source.source_error().kind(), BackendErrorKind::Interrupted);
        }
    }

    fn assert_current_state_update_publish_failure(
        kind: PublishFailureKind,
        update: impl FnOnce(
            &DatabaseManifestService<'_>,
        ) -> ManifestServiceResult<DatabaseManifestWrite>,
    ) {
        let old_manifest = DatabaseManifest::new([0x11; 16], "old-codec")
            .expect("old manifest")
            .with_recovery_facts(7, Some(42), Some(3), Some(CommitVersion::new(41)))
            .expect("old recovery facts");
        let old_bytes = encode_manifest(&old_manifest).expect("encode old manifest");
        let object = ObjectLayout::database_manifest().expect("manifest name");
        let backend = StoredPublishFailureBackend::with_object(kind, object.clone(), &old_bytes);
        let service = DatabaseManifestService::new(&backend);

        let error = update(&service).expect_err("current-state update publish should fail");
        let source = assert_publish_error(error, ManifestRole::Database, kind);
        assert_eq!(source.object(), &object);

        if publish_failure_implies_no_visible_replacement(kind) {
            assert_eq!(
                backend
                    .read_object(&object)
                    .expect("manifest bytes after failed current-state update"),
                old_bytes
            );
            assert_eq!(
                service.load_current().expect("load current"),
                Some(old_manifest)
            );
        }
    }

    #[test]
    fn database_manifest_current_state_update_publish_failures_preserve_old_bytes() {
        for kind in ALL_PUBLISH_FAILURE_KINDS {
            assert_current_state_update_publish_failure(kind, |service| {
                service.persist_active_wal_segment(8)
            });
            assert_current_state_update_publish_failure(kind, |service| {
                service.persist_snapshot_facts(9, CommitVersion::new(43))
            });
            assert_current_state_update_publish_failure(kind, |service| {
                service.persist_flush_watermark(CommitVersion::new(44))
            });
        }
    }

    #[test]
    fn database_manifest_replace_publish_failures_preserve_kind_and_old_bytes() {
        let old_manifest = DatabaseManifest::new([0x11; 16], "old-codec").expect("old manifest");
        let old_bytes = encode_manifest(&old_manifest).expect("encode old manifest");
        let new_manifest = DatabaseManifest::new([0x22; 16], "new-codec").expect("new manifest");

        for kind in ALL_PUBLISH_FAILURE_KINDS {
            let object = ObjectLayout::database_manifest().expect("manifest name");
            let backend =
                StoredPublishFailureBackend::with_object(kind, object.clone(), &old_bytes);
            let service = DatabaseManifestService::new(&backend);

            let error = service
                .publish_current(&new_manifest)
                .expect_err("replace publish should fail");

            let source = assert_publish_error(error, ManifestRole::Database, kind);
            assert_eq!(source.object(), &object);
            assert_eq!(source.source_error().kind(), BackendErrorKind::Interrupted);
            if publish_failure_implies_no_visible_replacement(kind) {
                assert_eq!(
                    backend.read_object(&object).expect("stored manifest bytes"),
                    old_bytes
                );
                assert_eq!(
                    service.load_current().expect("load current manifest"),
                    Some(old_manifest.clone())
                );
            }
        }
    }

    #[test]
    fn table_manifest_load_reports_absent_manifest() {
        let backend = MemoryBackend::new();
        let service = TableManifestService::new(&backend);

        assert_eq!(service.load("branch").expect("load table manifest"), None);
    }

    #[test]
    fn table_manifest_service_load_decodes_present_manifest() {
        let backend = RecordingBackend::new();
        let branch = branch_id(0x51);
        let manifest = typed_table_manifest(branch, 7, "service-load-table");
        let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest");
        backend
            .write_object(
                &object,
                &encode_table_manifest(&manifest).expect("manifest bytes"),
            )
            .expect("write manifest");
        let service = TableManifestService::new(&backend);

        let loaded = service
            .load_current(branch)
            .expect("load typed manifest")
            .expect("present manifest");

        assert_eq!(loaded, manifest);
    }

    #[test]
    fn table_manifest_service_load_rejects_corrupt_manifest() {
        let backend = RecordingBackend::new();
        let branch = branch_id(0x52);
        let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest");
        backend
            .write_object(&object, b"not a typed table manifest")
            .expect("write corrupt manifest");
        let service = TableManifestService::new(&backend);

        let error = service
            .load_current(branch)
            .expect_err("corrupt typed manifest rejects");

        match error {
            ManifestServiceError::Decode {
                role,
                object: actual,
                source: _,
            } => {
                assert_eq!(role, ManifestRole::Table);
                assert_eq!(actual, object);
            }
            other => panic!("expected typed table decode error, got {other:?}"),
        }
    }

    #[test]
    fn table_manifest_service_load_rejects_branch_object_payload_mismatch() {
        let backend = RecordingBackend::new();
        let object_branch = branch_id(0x53);
        let payload_branch = branch_id(0x54);
        let object =
            ObjectLayout::branch_table_manifest(&object_branch.to_string()).expect("manifest");
        backend
            .write_object(
                &object,
                &encode_table_manifest(&typed_table_manifest(
                    payload_branch,
                    8,
                    "service-mismatch-table",
                ))
                .expect("manifest bytes"),
            )
            .expect("write mismatched manifest");
        let service = TableManifestService::new(&backend);

        assert_eq!(
            service
                .load_current(object_branch)
                .expect_err("branch mismatch rejects"),
            ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Table,
                object,
                field: "branch_id",
            }
        );
    }

    #[test]
    fn table_manifest_service_publish_create_writes_canonical_bytes() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let branch = branch_id(0x55);
        let manifest = typed_table_manifest(branch, 9, "service-create-table");
        let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest");

        let write = service
            .publish_create_manifest(branch, &manifest)
            .expect("create typed manifest");

        assert_eq!(write.manifest(), &manifest);
        assert_eq!(write.outcome().object(), &object);
        assert_eq!(write.outcome().durability(), PublishDurability::Durable);
        assert_eq!(
            backend.read_object(&object).expect("stored manifest"),
            encode_table_manifest(&manifest).expect("canonical manifest bytes")
        );
    }

    /// #2833: removal deletes a present manifest and is idempotent when the
    /// manifest never existed (the fresh-name eager fork's no-op arm).
    #[test]
    fn table_manifest_service_remove_deletes_and_tolerates_absent() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let branch = branch_id(0x57);
        let manifest = typed_table_manifest(branch, 12, "service-remove-table");
        let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest");
        service
            .publish_create_manifest(branch, &manifest)
            .expect("create manifest");
        assert!(backend.read_object(&object).is_ok());

        service.remove_manifest(branch).expect("remove present");
        assert!(
            backend.read_object(&object).is_err(),
            "the manifest object is gone after removal"
        );

        service
            .remove_manifest(branch)
            .expect("removing an absent manifest is idempotent success");
    }

    /// The absent-manifest arm against the REAL backend: `LocalFsBackend`
    /// reports a missing object as an error whose source kind is `NotFound`
    /// (unlike the in-memory fixtures' `AlreadyMissing` success), so this
    /// leg exercises the guard itself.
    #[cfg(feature = "localfs")]
    #[test]
    fn table_manifest_service_remove_tolerates_absent_on_local_fs() {
        let dir = tempfile::tempdir().expect("tmp");
        let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());
        let service = TableManifestService::new(&backend);
        let branch = branch_id(0x58);

        service
            .remove_manifest(branch)
            .expect("absent manifest on local fs is idempotent success");
    }

    /// A NON-absence delete failure must surface as a typed `Delete` error
    /// with its source retained — never be swallowed by the absence guard.
    #[test]
    fn table_manifest_service_remove_surfaces_real_delete_failures() {
        struct DeleteFailureBackend;
        impl Backend for DeleteFailureBackend {
            fn capabilities(&self) -> BackendCapabilities {
                BackendCapabilities::from_slice(&[BackendCapability::DeleteObject])
            }
            fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
                unreachable!("remove path never reads")
            }
            fn read_range(
                &self,
                _name: &ObjectName,
                _range: BackendRange,
            ) -> BackendResult<Vec<u8>> {
                unreachable!("remove path never reads")
            }
            fn write_object(
                &self,
                _name: &ObjectName,
                _bytes: &[u8],
            ) -> BackendResult<BackendMetadata> {
                unreachable!("remove path never writes")
            }
            fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
                Err(crate::backend::DeleteError::new(
                    name.clone(),
                    crate::backend::DeleteFailureKind::RemovalUnknown,
                    BackendError::new(BackendErrorKind::Unavailable, "disk unavailable"),
                ))
            }
            fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
                unreachable!("remove path never lists")
            }
            fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
                unreachable!("remove path never stats")
            }
        }

        let service = TableManifestService::new(&DeleteFailureBackend);
        let error = service
            .remove_manifest(branch_id(0x59))
            .expect_err("a real delete failure surfaces");
        match &error {
            ManifestServiceError::Delete { role, source } => {
                assert_eq!(*role, ManifestRole::Table);
                assert_eq!(source.source_error().kind(), BackendErrorKind::Unavailable);
            }
            other => panic!("expected delete error, got {other:?}"),
        }
        // Chain retention (#2771 lesson): source() and Display stay wired.
        let source = std::error::Error::source(&error).expect("delete error retains its source");
        assert!(!source.to_string().is_empty());
        assert!(!error.to_string().is_empty());
    }

    #[test]
    fn table_manifest_service_publish_replace_writes_canonical_bytes() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let branch = branch_id(0x56);
        let old = typed_table_manifest(branch, 10, "service-old-table");
        let new = typed_table_manifest(branch, 11, "service-new-table");
        let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest");
        service
            .publish_create_manifest(branch, &old)
            .expect("create old manifest");

        let write = service
            .publish_replace_manifest(branch, &new)
            .expect("replace typed manifest");

        assert_eq!(write.manifest(), &new);
        assert_eq!(
            backend.read_object(&object).expect("stored manifest"),
            encode_table_manifest(&new).expect("canonical manifest bytes")
        );
    }

    #[test]
    fn table_manifest_service_publish_rejects_payload_branch_mismatch_before_publish() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let object_branch = branch_id(0x57);
        let payload_branch = branch_id(0x58);
        let manifest = typed_table_manifest(payload_branch, 12, "service-wrong-branch-table");
        let object =
            ObjectLayout::branch_table_manifest(&object_branch.to_string()).expect("manifest");

        assert_eq!(
            service
                .publish_replace_manifest(object_branch, &manifest)
                .expect_err("wrong branch publish rejects"),
            ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Table,
                object: object.clone(),
                field: "branch_id",
            }
        );
        assert_eq!(
            backend
                .read_object(&object)
                .expect_err("no manifest publish")
                .kind(),
            BackendErrorKind::NotFound
        );
    }

    #[test]
    fn table_manifest_service_does_not_accept_database_manifest_bytes() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let branch = branch_id(0x59);
        let object = ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest");
        let database = DatabaseManifest::new([0x55; 16], "identity").expect("database manifest");
        backend
            .write_object(
                &object,
                &encode_manifest(&database).expect("database manifest bytes"),
            )
            .expect("write database bytes under table manifest object");

        let error = service
            .load_current(branch)
            .expect_err("database manifest bytes rejected as table manifest");

        match error {
            ManifestServiceError::Decode {
                role,
                object: actual,
                source: FormatError::InvalidMagic { format },
            } => {
                assert_eq!(role, ManifestRole::Table);
                assert_eq!(actual, object);
                assert_eq!(format, "table_manifest");
            }
            other => panic!("expected table-manifest decode error, got {other:?}"),
        }
    }

    #[test]
    fn table_manifest_load_preserves_backend_read_failure() {
        let backend = ReadFailureBackend::new(BackendErrorKind::PermissionDenied);
        let service = TableManifestService::new(&backend);
        let object = ObjectLayout::branch_table_manifest("branch").expect("manifest name");

        let error = service
            .load("branch")
            .expect_err("table manifest read failure must not become missing");

        match error {
            ManifestServiceError::Read {
                role,
                object: actual,
                source,
            } => {
                assert_eq!(role, ManifestRole::Table);
                assert_eq!(actual, object);
                assert_eq!(source.kind(), BackendErrorKind::PermissionDenied);
            }
            other => panic!("expected table read error, got {other:?}"),
        }
    }

    #[test]
    fn table_manifest_payloads_round_trip_without_database_decoding() {
        let payloads = [
            Vec::new(),
            vec![0xff, 0x00, 0x80, 0x7f, b'/', b':', b'\\'],
            corrupt_database_manifest_bytes(),
            wal_like_bytes(),
            b"contains\0embedded\0nul".to_vec(),
            (0_u32..65_536)
                .map(|value| u8::try_from(value % 251).expect("modulo fits in u8"))
                .collect(),
        ];

        for (index, payload) in payloads.iter().enumerate() {
            let backend = RecordingBackend::new();
            let service = TableManifestService::new(&backend);
            let branch_id = format!("branch-{index}");
            let object = ObjectLayout::branch_table_manifest(&branch_id).expect("manifest name");

            let outcome = service
                .publish_replace(&branch_id, payload)
                .expect("publish opaque table manifest");

            assert_eq!(outcome.object(), &object);
            assert_eq!(outcome.metadata().size_bytes(), payload.len() as u64);
            assert_eq!(
                backend.read_object(&object).expect("stored table bytes"),
                *payload
            );
            assert_eq!(
                service.load(&branch_id).expect("load table manifest"),
                Some(payload.clone())
            );
        }
    }

    #[test]
    fn table_manifest_create_and_replace_own_only_opaque_payload_bytes() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let object = ObjectLayout::branch_table_manifest("branch").expect("manifest name");

        service
            .publish_create("branch", b"created")
            .expect("create table manifest");
        assert_eq!(
            backend.read_object(&object).expect("created table bytes"),
            b"created"
        );

        let error = service
            .publish_create("branch", b"duplicate")
            .expect_err("create must refuse existing table manifest");
        assert_publish_error(
            error,
            ManifestRole::Table,
            PublishFailureKind::PreconditionFailed,
        );
        assert_eq!(
            backend
                .read_object(&object)
                .expect("table bytes after failed create"),
            b"created"
        );

        service
            .publish_replace("branch", b"replaced")
            .expect("replace table manifest");
        assert_eq!(
            service
                .load("branch")
                .expect("load replaced table manifest"),
            Some(b"replaced".to_vec())
        );

        let missing_object =
            ObjectLayout::branch_table_manifest("missing-branch").expect("manifest name");
        service
            .publish_replace("missing-branch", b"created-by-replace")
            .expect("replace may create a missing table manifest");
        assert_eq!(
            backend
                .read_object(&missing_object)
                .expect("replace-created table bytes"),
            b"created-by-replace"
        );
    }

    #[test]
    fn table_manifest_publish_rejects_invalid_publish_outcome_metadata() {
        let backend = RecordingBackend::new();
        let service = TableManifestService::new(&backend);
        let object = ObjectLayout::branch_table_manifest("branch").expect("manifest name");
        backend.override_next_publish_outcome(PublishOutcome::new(
            object.clone(),
            BackendMetadata::new(1, None),
            PublishDurability::Durable,
        ));

        assert_eq!(
            service.publish_create("branch", b"created"),
            Err(ManifestServiceError::InvalidPublishMetadata {
                role: ManifestRole::Table,
                object,
                field: "size_bytes",
            })
        );
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn table_manifest_publish_and_load_round_trips_opaque_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = TableManifestService::new(&backend);
        let object = ObjectLayout::branch_table_manifest("branch").expect("manifest name");

        let outcome = service
            .publish_replace("branch", b"opaque table manifest")
            .expect("publish table manifest");
        let stored = backend.read_object(&object).expect("table manifest bytes");

        assert_eq!(outcome.object(), &object);
        assert_eq!(
            outcome.metadata().size_bytes(),
            b"opaque table manifest".len() as u64
        );
        assert_eq!(outcome.durability(), PublishDurability::Durable);
        assert_eq!(stored, b"opaque table manifest");
        assert_eq!(
            service.load("branch").expect("load table manifest"),
            Some(b"opaque table manifest".to_vec())
        );
    }

    #[test]
    fn table_manifest_publish_failure_preserves_publish_kind() {
        let backend = MemoryBackend::new();
        let service = TableManifestService::new(&backend);
        let expected_object = ObjectLayout::branch_table_manifest("branch").expect("manifest");

        let error = service
            .publish_replace("branch", b"opaque")
            .expect_err("memory backend is non-durable");

        match error {
            ManifestServiceError::Publish { role, source } => {
                assert_eq!(role, ManifestRole::Table);
                assert_eq!(source.kind(), PublishFailureKind::Unsupported);
                assert_eq!(source.object(), &expected_object);
                assert_eq!(
                    source.source_error().kind(),
                    BackendErrorKind::UnsupportedOperation
                );
            }
            other => panic!("expected publish error, got {other:?}"),
        }
    }

    #[test]
    fn table_manifest_create_failure_preserves_publish_kind() {
        let object = ObjectLayout::branch_table_manifest("branch").expect("manifest");

        for kind in ALL_PUBLISH_FAILURE_KINDS {
            let backend = PublishFailureBackend::new(kind);
            let service = TableManifestService::new(&backend);

            let error = service
                .publish_create("branch", b"opaque")
                .expect_err("table create publish should fail");

            let source = assert_publish_error(error, ManifestRole::Table, kind);
            assert_eq!(source.object(), &object);
            assert_eq!(source.source_error().kind(), BackendErrorKind::Interrupted);
        }
    }

    #[test]
    fn table_manifest_replace_publish_failures_preserve_kind_and_old_bytes() {
        let old_bytes = b"old opaque table manifest";
        let new_bytes = b"new opaque table manifest";

        for kind in ALL_PUBLISH_FAILURE_KINDS {
            let object = ObjectLayout::branch_table_manifest("branch").expect("manifest");
            let backend = StoredPublishFailureBackend::with_object(kind, object.clone(), old_bytes);
            let service = TableManifestService::new(&backend);

            let error = service
                .publish_replace("branch", new_bytes)
                .expect_err("table replace publish should fail");

            let source = assert_publish_error(error, ManifestRole::Table, kind);
            assert_eq!(source.object(), &object);
            assert_eq!(source.source_error().kind(), BackendErrorKind::Interrupted);
            if publish_failure_implies_no_visible_replacement(kind) {
                assert_eq!(
                    backend
                        .read_object(&object)
                        .expect("stored table manifest bytes"),
                    old_bytes
                );
                assert_eq!(
                    service.load("branch").expect("load table manifest"),
                    Some(old_bytes.to_vec())
                );
            }
        }
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn table_manifest_create_refuses_existing_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = TableManifestService::new(&backend);

        service
            .publish_create("branch", b"old")
            .expect("initial table manifest");
        let error = service
            .publish_create("branch", b"new")
            .expect_err("create should refuse existing table manifest");

        assert_publish_error(
            error,
            ManifestRole::Table,
            PublishFailureKind::PreconditionFailed,
        );
        assert_eq!(
            service.load("branch").expect("load table manifest"),
            Some(b"old".to_vec())
        );
    }

    #[test]
    fn table_manifest_invalid_branch_id_returns_layout_error() {
        let backend = MemoryBackend::new();
        let service = TableManifestService::new(&backend);

        let error = service.load("bad/branch").expect_err("invalid branch id");

        match error {
            ManifestServiceError::Layout {
                role,
                source: LayoutError::ComponentContainsSeparator { role: "branch" },
            } => assert_eq!(role, ManifestRole::Table),
            other => panic!("expected layout error, got {other:?}"),
        }
    }

    #[test]
    fn table_manifest_invalid_branch_ids_return_precise_layout_errors_before_backend_access() {
        let component_too_long = "a".repeat(MAX_OBJECT_NAME_BYTES + 1);
        let assembled_too_long = "a".repeat(MAX_OBJECT_NAME_BYTES - "tables//manifest".len() + 1);
        let cases = [
            ("", LayoutError::EmptyComponent { role: "branch" }),
            (
                "bad/branch",
                LayoutError::ComponentContainsSeparator { role: "branch" },
            ),
            (
                "bad\0branch",
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::InvalidByte(0),
                },
            ),
            (
                "bad branch",
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::InvalidByte(b' '),
                },
            ),
            (
                "bad:branch",
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::InvalidByte(b':'),
                },
            ),
            (
                "\u{00e9}",
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::InvalidByte(0xc3),
                },
            ),
            (
                component_too_long.as_str(),
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::TooLong {
                        actual: MAX_OBJECT_NAME_BYTES + 1,
                        max: MAX_OBJECT_NAME_BYTES,
                    },
                },
            ),
            (
                assembled_too_long.as_str(),
                LayoutError::InvalidObjectName(ObjectNameError::TooLong {
                    actual: MAX_OBJECT_NAME_BYTES + 1,
                    max: MAX_OBJECT_NAME_BYTES,
                }),
            ),
            (
                ".",
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::TraversalComponent,
                },
            ),
            (
                "..",
                LayoutError::InvalidComponent {
                    role: "branch",
                    source: ObjectNameError::TraversalComponent,
                },
            ),
        ];

        for (branch_id, expected) in cases {
            let read_backend = ReadFailureBackend::new(BackendErrorKind::Unavailable);
            let read_service = TableManifestService::new(&read_backend);
            let read_error = read_service
                .load(branch_id)
                .expect_err("invalid branch id should fail before read");
            assert_table_layout_error(read_error, &expected);

            let publish_backend = PublishFailureBackend::new(PublishFailureKind::VisibilityUnknown);
            let publish_service = TableManifestService::new(&publish_backend);
            let publish_error = publish_service
                .publish_replace(branch_id, b"opaque")
                .expect_err("invalid branch id should fail before publish");
            assert_table_layout_error(publish_error, &expected);
        }
    }

    #[test]
    fn manifest_errors_preserve_roles_objects_and_fields() {
        let database_object = ObjectLayout::database_manifest().expect("database manifest");
        let table_object = ObjectLayout::branch_table_manifest("branch").expect("table manifest");

        let database_read =
            DatabaseManifestService::new(&ReadFailureBackend::new(BackendErrorKind::Unavailable))
                .load_current()
                .expect_err("database read should fail");
        assert_eq!(
            database_read,
            ManifestServiceError::Read {
                role: ManifestRole::Database,
                object: database_object.clone(),
                source: BackendError::new(BackendErrorKind::Unavailable, "read failure"),
            }
        );

        let table_read =
            TableManifestService::new(&ReadFailureBackend::new(BackendErrorKind::PermissionDenied))
                .load("branch")
                .expect_err("table read should fail");
        assert_eq!(
            table_read,
            ManifestServiceError::Read {
                role: ManifestRole::Table,
                object: table_object.clone(),
                source: BackendError::new(BackendErrorKind::PermissionDenied, "read failure"),
            }
        );

        let database_decode_backend = MemoryBackend::new();
        database_decode_backend
            .write_object(&database_object, b"not a database manifest")
            .expect("write corrupt database manifest");
        let database_decode = DatabaseManifestService::new(&database_decode_backend)
            .load_current()
            .expect_err("database decode should fail");
        match database_decode {
            ManifestServiceError::Decode {
                role,
                object,
                source,
            } => {
                assert_eq!(role, ManifestRole::Database);
                assert_eq!(object, database_object);
                assert!(matches!(source, FormatError::InsufficientBytes { .. }));
            }
            other => panic!("expected decode error, got {other:?}"),
        }

        let database_encode = DatabaseManifestService::new(&PublishFailureBackend::new(
            PublishFailureKind::FailedBeforeVisibility,
        ))
        .create_initial([0x22; 16], "")
        .expect_err("database encode should fail");
        assert_encode_error(
            database_encode,
            &ObjectLayout::database_manifest().expect("database manifest"),
            &FormatError::InvalidLength { field: "codec_id" },
        );

        let codec_backend = MemoryBackend::new();
        let codec_manifest = DatabaseManifest::new([0x22; 16], "identity").expect("manifest");
        write_database_manifest_bytes(
            &codec_backend,
            &encode_manifest(&codec_manifest).expect("encode manifest"),
        );
        let codec_mismatch = DatabaseManifestService::new(&codec_backend)
            .load_current_for_codec("zstd")
            .expect_err("codec mismatch should fail");
        assert_eq!(
            codec_mismatch,
            ManifestServiceError::CodecMismatch {
                object: ObjectLayout::database_manifest().expect("database manifest"),
                expected: "zstd".to_string(),
                actual: "identity".to_string(),
            }
        );

        let invalid_fact = DatabaseManifestService::new(&MemoryBackend::new())
            .persist_flush_watermark(CommitVersion::ZERO)
            .expect_err("invalid fact should fail");
        assert_eq!(
            invalid_fact,
            ManifestServiceError::InvalidRecoveryFact {
                role: ManifestRole::Database,
                object: ObjectLayout::database_manifest().expect("database manifest"),
                field: "flushed_through_commit_id",
            }
        );

        let table_layout = TableManifestService::new(&MemoryBackend::new())
            .load("bad/branch")
            .expect_err("table layout should fail");
        assert_table_layout_error(
            table_layout,
            &LayoutError::ComponentContainsSeparator { role: "branch" },
        );
    }

    fn assert_publish_error(
        error: ManifestServiceError,
        role: ManifestRole,
        kind: PublishFailureKind,
    ) -> PublishError {
        match error {
            ManifestServiceError::Publish {
                role: actual_role,
                source,
            } => {
                assert_eq!(actual_role, role);
                assert_eq!(source.kind(), kind);
                source
            }
            other => panic!("expected publish error, got {other:?}"),
        }
    }

    fn typed_table_manifest(branch: BranchId, sequence: u64, identity: &str) -> TableManifest {
        TableManifest::new(
            branch,
            None,
            sequence,
            vec![TableManifestLevel::new(
                BranchLevel::ZERO,
                vec![typed_table_ref(branch, identity, 0)],
            )
            .expect("level")],
            Vec::new(),
            Vec::new(),
        )
        .expect("table manifest")
    }

    fn typed_table_ref(branch: BranchId, identity: &str, order: u32) -> TableManifestTableRef {
        let object = format!("tables/{branch}/l0000/{identity}");
        TableManifestTableRef::new(
            TableIdentity::new(identity).expect("table identity"),
            ObjectName::new(object).expect("table object"),
            order,
            TableManifestTableFacts::new(
                128,
                4,
                1,
                CommitVersion::new(1),
                CommitVersion::new(4),
                Some(Timestamp::from_micros(10)),
                Some(Timestamp::from_micros(40)),
            )
            .expect("table facts"),
            TableManifestTableBounds::new(
                b"physical-a".to_vec(),
                b"physical-z".to_vec(),
                b"internal-a".to_vec(),
                b"internal-z".to_vec(),
            )
            .expect("table bounds"),
            TableManifestTableProvenance::Flush,
        )
        .expect("table ref")
    }

    fn branch_id(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; BranchId::BYTE_LEN])
    }
}
