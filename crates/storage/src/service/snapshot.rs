//! Snapshot service mechanics over storage backend objects.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "snapshot service is consumed by later lifecycle and recovery layers"
    )
)]

use crate::backend::{
    Backend, BackendCapability, BackendError, BackendErrorKind, BackendHandle, PublishError,
    PublishOutcome,
};
use crate::format::FormatError;
use crate::format::{
    decode_snapshot_container, encode_snapshot_container, visit_snapshot_container_sections,
    SnapshotContainer, SnapshotHeader, SnapshotSection, SnapshotSectionRef,
};
use crate::layout::{LayoutError, ObjectLayout};
use crate::object::ObjectName;
use crate::service::{validate_publish_outcome, ObjectPublisher};
use std::fmt;
use strata_core::{CommitVersion, Timestamp};

pub(crate) type SnapshotServiceResult<T> = Result<T, SnapshotServiceError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotServiceError {
    InvalidSnapshotFact {
        snapshot_id: u64,
        field: &'static str,
    },
    UnsupportedCapability {
        capability: BackendCapability,
    },
    Layout {
        source: LayoutError,
    },
    List {
        source: BackendError,
    },
    InvalidListedObject {
        object: ObjectName,
        source: BackendError,
    },
    Missing {
        object: ObjectName,
        snapshot_id: u64,
    },
    Read {
        object: ObjectName,
        snapshot_id: u64,
        source: BackendError,
    },
    Decode {
        object: ObjectName,
        snapshot_id: u64,
        source: FormatError,
    },
    Visit {
        object: ObjectName,
        snapshot_id: u64,
        source: FormatError,
    },
    Encode {
        object: ObjectName,
        snapshot_id: u64,
        source: FormatError,
    },
    Publish {
        snapshot_id: u64,
        source: PublishError,
    },
    InvalidPublishMetadata {
        object: ObjectName,
        snapshot_id: u64,
        field: &'static str,
    },
    CodecMismatch {
        object: ObjectName,
        snapshot_id: u64,
        expected: String,
        actual: String,
    },
    DatabaseMismatch {
        object: ObjectName,
        snapshot_id: u64,
        expected: [u8; 16],
        actual: [u8; 16],
    },
    SnapshotIdMismatch {
        object: ObjectName,
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for SnapshotServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSnapshotFact { snapshot_id, field } => {
                write!(formatter, "snapshot {snapshot_id} has invalid fact {field}")
            }
            Self::UnsupportedCapability { capability } => {
                write!(formatter, "backend does not support {capability}")
            }
            Self::Layout { source } => {
                write!(formatter, "failed to build snapshot object name: {source}")
            }
            Self::List { source } => {
                write!(formatter, "failed to list snapshot objects: {source}")
            }
            Self::InvalidListedObject { object, source } => {
                write!(
                    formatter,
                    "snapshot listing contains invalid object {object}: {source}"
                )
            }
            Self::Missing {
                object,
                snapshot_id,
            } => write!(formatter, "snapshot {snapshot_id} object {object} is missing"),
            Self::Read {
                object,
                snapshot_id,
                source,
            } => write!(
                formatter,
                "failed to read snapshot {snapshot_id} object {object}: {source}"
            ),
            Self::Decode {
                object,
                snapshot_id,
                source,
            } => write!(
                formatter,
                "failed to decode snapshot {snapshot_id} object {object}: {source}"
            ),
            Self::Visit {
                object,
                snapshot_id,
                source,
            } => write!(
                formatter,
                "snapshot {snapshot_id} object {object} section visit failed: {source}"
            ),
            Self::Encode {
                object,
                snapshot_id,
                source,
            } => write!(
                formatter,
                "failed to encode snapshot {snapshot_id} object {object}: {source}"
            ),
            Self::Publish {
                snapshot_id,
                source,
            } => write!(formatter, "failed to publish snapshot {snapshot_id}: {source}"),
            Self::InvalidPublishMetadata {
                object,
                snapshot_id,
                field,
            } => write!(
                formatter,
                "snapshot {snapshot_id} object {object} has invalid publish metadata {field}"
            ),
            Self::CodecMismatch {
                object,
                snapshot_id,
                expected,
                actual,
            } => write!(
                formatter,
                "snapshot {snapshot_id} object {object} codec mismatch: expected {expected}, found {actual}"
            ),
            Self::DatabaseMismatch {
                object,
                snapshot_id,
                expected,
                actual,
            } => write!(
                formatter,
                "snapshot {snapshot_id} object {object} database mismatch: expected {expected:?}, found {actual:?}"
            ),
            Self::SnapshotIdMismatch {
                object,
                expected,
                actual,
            } => write!(
                formatter,
                "snapshot object {object} id mismatch: expected {expected}, found {actual}"
            ),
        }
    }
}

impl std::error::Error for SnapshotServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Layout { source } => Some(source),
            Self::List { source }
            | Self::InvalidListedObject { source, .. }
            | Self::Read { source, .. } => Some(source),
            Self::Decode { source, .. }
            | Self::Visit { source, .. }
            | Self::Encode { source, .. } => Some(source),
            Self::Publish { source, .. } => Some(source),
            Self::InvalidSnapshotFact { .. }
            | Self::UnsupportedCapability { .. }
            | Self::Missing { .. }
            | Self::InvalidPublishMetadata { .. }
            | Self::CodecMismatch { .. }
            | Self::DatabaseMismatch { .. }
            | Self::SnapshotIdMismatch { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotPublishRequest {
    snapshot_id: u64,
    watermark_commit_version: CommitVersion,
    created_at: Timestamp,
    database_id: [u8; 16],
    codec_id: String,
    sections: Vec<SnapshotSection>,
}

impl SnapshotPublishRequest {
    pub(crate) fn new(
        snapshot_id: u64,
        watermark_commit_version: CommitVersion,
        created_at: Timestamp,
        database_id: [u8; 16],
        codec_id: impl Into<String>,
        sections: Vec<SnapshotSection>,
    ) -> Self {
        Self {
            snapshot_id,
            watermark_commit_version,
            created_at,
            database_id,
            codec_id: codec_id.into(),
            sections,
        }
    }
}

pub(crate) type SnapshotWrite = (SnapshotContainer, PublishOutcome);

#[derive(Clone)]
pub(crate) struct SnapshotService<'a> {
    backend: BackendHandle<'a>,
}

impl<'a> SnapshotService<'a> {
    pub(crate) fn new(backend: impl Into<BackendHandle<'a>>) -> Self {
        Self {
            backend: backend.into(),
        }
    }

    pub(crate) fn publish_create(
        &self,
        request: SnapshotPublishRequest,
    ) -> SnapshotServiceResult<SnapshotWrite> {
        validate_snapshot_facts(request.snapshot_id, request.watermark_commit_version)?;
        let object = snapshot_object(request.snapshot_id)?;
        let header = SnapshotHeader::new(
            request.snapshot_id,
            request.watermark_commit_version,
            request.created_at,
            request.database_id,
            request.codec_id,
        )
        .map_err(|source| SnapshotServiceError::Encode {
            object: object.clone(),
            snapshot_id: request.snapshot_id,
            source,
        })?;
        let container = SnapshotContainer::new(header, request.sections);
        publish_snapshot_container(&self.backend, &object, &container)
    }

    pub(crate) fn load_optional(
        &self,
        snapshot_id: u64,
    ) -> SnapshotServiceResult<Option<SnapshotContainer>> {
        validate_snapshot_id(snapshot_id)?;
        let object = snapshot_object(snapshot_id)?;
        read_snapshot_optional(&self.backend, &object, snapshot_id)?
            .map(|bytes| decode_snapshot(&object, snapshot_id, &bytes))
            .transpose()
    }

    pub(crate) fn load_required(
        &self,
        snapshot_id: u64,
    ) -> SnapshotServiceResult<SnapshotContainer> {
        validate_snapshot_id(snapshot_id)?;
        let object = snapshot_object(snapshot_id)?;
        self.load_optional(snapshot_id)?
            .ok_or(SnapshotServiceError::Missing {
                object,
                snapshot_id,
            })
    }

    pub(crate) fn load_required_for_codec(
        &self,
        snapshot_id: u64,
        expected_database_id: [u8; 16],
        expected_codec_id: &str,
    ) -> SnapshotServiceResult<SnapshotContainer> {
        validate_snapshot_id(snapshot_id)?;
        let object = snapshot_object(snapshot_id)?;
        let container = self.load_required(snapshot_id)?;
        validate_snapshot_identity(
            &object,
            snapshot_id,
            expected_database_id,
            expected_codec_id,
            container,
        )
    }

    pub(crate) fn visit_sections(
        &self,
        snapshot_id: u64,
        expected_database_id: [u8; 16],
        expected_codec_id: &str,
        max_sections: usize,
        mut visit_section: impl FnMut(SnapshotSectionRef<'_>) -> Result<(), FormatError>,
    ) -> SnapshotServiceResult<SnapshotHeader> {
        validate_snapshot_id(snapshot_id)?;
        let object = snapshot_object(snapshot_id)?;
        let Some(bytes) = read_snapshot_optional(&self.backend, &object, snapshot_id)? else {
            return Err(SnapshotServiceError::Missing {
                object,
                snapshot_id,
            });
        };

        // Validate the full container and header identity before exposing
        // borrowed section bytes to the caller. This keeps wrong-database or
        // wrong-codec snapshots from leaking into upper-layer recovery logic.
        let header = visit_snapshot_container_sections(&bytes, max_sections, |_| Ok(())).map_err(
            |source| SnapshotServiceError::Decode {
                object: object.clone(),
                snapshot_id,
                source,
            },
        )?;
        validate_loaded_snapshot_header(&object, snapshot_id, &header)?;
        validate_snapshot_header_identity(
            &object,
            snapshot_id,
            expected_database_id,
            expected_codec_id,
            &header,
        )?;

        let mut callback_error = None;
        let header = visit_snapshot_container_sections(&bytes, max_sections, |section| {
            visit_section(section).inspect_err(|source| {
                callback_error = Some(source.clone());
            })
        })
        .map_err(|source| match callback_error {
            Some(source) => SnapshotServiceError::Visit {
                object: object.clone(),
                snapshot_id,
                source,
            },
            None => SnapshotServiceError::Decode {
                object: object.clone(),
                snapshot_id,
                source,
            },
        })?;

        Ok(header)
    }
}

fn validate_snapshot_facts(
    snapshot_id: u64,
    watermark_commit_version: CommitVersion,
) -> SnapshotServiceResult<()> {
    validate_snapshot_id(snapshot_id)?;
    if watermark_commit_version.as_u64() == 0 {
        return Err(SnapshotServiceError::InvalidSnapshotFact {
            snapshot_id,
            field: "snapshot_watermark",
        });
    }
    Ok(())
}

fn validate_snapshot_id(snapshot_id: u64) -> SnapshotServiceResult<()> {
    if snapshot_id == 0 {
        return Err(SnapshotServiceError::InvalidSnapshotFact {
            snapshot_id,
            field: "snapshot_id",
        });
    }
    Ok(())
}

fn snapshot_object(snapshot_id: u64) -> SnapshotServiceResult<ObjectName> {
    ObjectLayout::snapshot(snapshot_id).map_err(|source| SnapshotServiceError::Layout { source })
}

fn read_snapshot_optional(
    backend: &dyn Backend,
    object: &ObjectName,
    snapshot_id: u64,
) -> SnapshotServiceResult<Option<Vec<u8>>> {
    require_capability(backend, BackendCapability::ReadObject)?;
    match backend.read_object(object) {
        Ok(bytes) => Ok(Some(bytes)),
        // Only NotFound is absence. Other failures mean the snapshot object
        // cannot be trusted or reached and must stay visible to recovery.
        Err(source) if source.kind() == BackendErrorKind::NotFound => Ok(None),
        Err(source) => Err(SnapshotServiceError::Read {
            object: object.clone(),
            snapshot_id,
            source,
        }),
    }
}

pub(super) fn require_capability(
    backend: &dyn Backend,
    capability: BackendCapability,
) -> SnapshotServiceResult<()> {
    if backend.capabilities().contains(capability) {
        return Ok(());
    }
    Err(SnapshotServiceError::UnsupportedCapability { capability })
}

fn decode_snapshot(
    object: &ObjectName,
    snapshot_id: u64,
    bytes: &[u8],
) -> SnapshotServiceResult<SnapshotContainer> {
    let container =
        decode_snapshot_container(bytes).map_err(|source| SnapshotServiceError::Decode {
            object: object.clone(),
            snapshot_id,
            source,
        })?;
    validate_loaded_snapshot(object, snapshot_id, container)
}

fn validate_loaded_snapshot(
    object: &ObjectName,
    snapshot_id: u64,
    container: SnapshotContainer,
) -> SnapshotServiceResult<SnapshotContainer> {
    let header = container.header();
    validate_loaded_snapshot_header(object, snapshot_id, header)?;
    Ok(container)
}

fn validate_loaded_snapshot_header(
    object: &ObjectName,
    snapshot_id: u64,
    header: &SnapshotHeader,
) -> SnapshotServiceResult<()> {
    if header.snapshot_id() != snapshot_id {
        return Err(SnapshotServiceError::SnapshotIdMismatch {
            object: object.clone(),
            expected: snapshot_id,
            actual: header.snapshot_id(),
        });
    }
    if header.watermark_commit_version().as_u64() == 0 {
        return Err(SnapshotServiceError::Decode {
            object: object.clone(),
            snapshot_id,
            source: FormatError::InvalidValue {
                field: "snapshot_watermark",
            },
        });
    }
    Ok(())
}

fn publish_snapshot_container(
    backend: &dyn Backend,
    object: &ObjectName,
    container: &SnapshotContainer,
) -> SnapshotServiceResult<SnapshotWrite> {
    let snapshot_id = container.header().snapshot_id();
    let bytes =
        encode_snapshot_container(container).map_err(|source| SnapshotServiceError::Encode {
            object: object.clone(),
            snapshot_id,
            source,
        })?;
    // Decode our own bytes before publishing so the returned container matches
    // the durable representation rather than unchecked caller-owned input.
    let decoded = decode_snapshot(object, snapshot_id, &bytes)?;
    let outcome = ObjectPublisher::new(backend)
        .publish_durable_create(object, &bytes)
        .map_err(|source| SnapshotServiceError::Publish {
            snapshot_id,
            source,
        })?;
    validate_publish_outcome(object, bytes.len() as u64, &outcome).map_err(|mismatch| {
        SnapshotServiceError::InvalidPublishMetadata {
            object: mismatch.object().clone(),
            snapshot_id,
            field: mismatch.field(),
        }
    })?;
    Ok((decoded, outcome))
}

fn validate_snapshot_identity(
    object: &ObjectName,
    snapshot_id: u64,
    expected_database_id: [u8; 16],
    expected_codec_id: &str,
    container: SnapshotContainer,
) -> SnapshotServiceResult<SnapshotContainer> {
    let header = container.header();
    validate_snapshot_header_identity(
        object,
        snapshot_id,
        expected_database_id,
        expected_codec_id,
        header,
    )?;
    Ok(container)
}

fn validate_snapshot_header_identity(
    object: &ObjectName,
    snapshot_id: u64,
    expected_database_id: [u8; 16],
    expected_codec_id: &str,
    header: &SnapshotHeader,
) -> SnapshotServiceResult<()> {
    if header.database_id() != &expected_database_id {
        return Err(SnapshotServiceError::DatabaseMismatch {
            object: object.clone(),
            snapshot_id,
            expected: expected_database_id,
            actual: *header.database_id(),
        });
    }
    if header.codec_id() != expected_codec_id {
        return Err(SnapshotServiceError::CodecMismatch {
            object: object.clone(),
            snapshot_id,
            expected: expected_codec_id.to_owned(),
            actual: header.codec_id().to_owned(),
        });
    }
    Ok(())
}

mod listing;
pub(crate) use listing::SnapshotDeleteReport;
pub(crate) use listing::{SnapshotDeleteFailure, SnapshotDeleteOutcome, SnapshotObject};

#[cfg(test)]
mod listing_tests;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod listing_property_tests;

#[cfg(test)]
mod publish_load_tests;

#[cfg(test)]
mod publish_fault_tests;

#[cfg(test)]
mod tests {
    use super::{SnapshotPublishRequest, SnapshotService, SnapshotServiceError};
    #[cfg(all(feature = "localfs", unix))]
    use crate::backend::local_fs::LocalFsBackend;
    use crate::backend::{
        memory::MemoryBackend, Backend, BackendCapabilities, BackendCapability, BackendError,
        BackendErrorKind, BackendMetadata, BackendRange, BackendResult, PublishDurability,
        PublishError, PublishFailureKind, PublishMode, PublishOutcome,
    };
    use crate::format::{
        encode_snapshot_container, FormatError, SnapshotContainer, SnapshotHeader, SnapshotSection,
    };
    use crate::layout::ObjectLayout;
    use crate::object::{ObjectName, ObjectPrefix};
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use strata_core::{CommitVersion, Timestamp};

    const DATABASE_ID: [u8; 16] = [0x11; 16];
    const SNAPSHOT_ID: u64 = 7;
    const CODEC_ID: &str = "identity";

    const ALL_PUBLISH_FAILURE_KINDS: [PublishFailureKind; 5] = [
        PublishFailureKind::Unsupported,
        PublishFailureKind::PreconditionFailed,
        PublishFailureKind::FailedBeforeVisibility,
        PublishFailureKind::VisibilityUnknown,
        PublishFailureKind::VisibleDurabilityUnconfirmed,
    ];

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
            // Create-only snapshot publication must not overwrite an existing
            // immutable snapshot object.
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

    #[test]
    fn snapshot_publish_rejects_invalid_publish_outcome_metadata() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");
        backend.override_next_publish_outcome(PublishOutcome::new(
            object.clone(),
            BackendMetadata::new(1, None),
            PublishDurability::Durable,
        ));

        assert_eq!(
            service.publish_create(request(SNAPSHOT_ID)),
            Err(SnapshotServiceError::InvalidPublishMetadata {
                object,
                snapshot_id: SNAPSHOT_ID,
                field: "size_bytes",
            })
        );
    }

    struct PublishFailureBackend {
        kind: PublishFailureKind,
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
            Ok(BackendMetadata::new(0, None))
        }

        fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
            crate::backend::durable_delete_result(name, false)
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
            bytes: &[u8],
            _mode: PublishMode,
        ) -> Result<PublishOutcome, PublishError> {
            // Visible-but-unconfirmed failures deliberately leave bytes
            // observable in real backends; this fake only proves service error
            // classification and leaves visibility assertions to publisher
            // fault-window tests.
            let source = if self.kind == PublishFailureKind::PreconditionFailed {
                BackendError::new(BackendErrorKind::AlreadyExists, "precondition failed")
            } else {
                BackendError::new(
                    BackendErrorKind::Unavailable,
                    format!("injected publish failure after {} bytes", bytes.len()),
                )
            };
            Err(PublishError::new(name.clone(), self.kind, source))
        }
    }

    fn section(payload: &'static [u8]) -> SnapshotSection {
        SnapshotSection::new(0x01, payload.to_vec()).expect("snapshot section")
    }

    fn request(snapshot_id: u64) -> SnapshotPublishRequest {
        SnapshotPublishRequest::new(
            snapshot_id,
            CommitVersion::new(42),
            Timestamp::from_micros(1_700),
            DATABASE_ID,
            CODEC_ID,
            vec![section(b"rows")],
        )
    }

    fn request_with_watermark(
        snapshot_id: u64,
        watermark: CommitVersion,
    ) -> SnapshotPublishRequest {
        SnapshotPublishRequest::new(
            snapshot_id,
            watermark,
            Timestamp::from_micros(1_700),
            DATABASE_ID,
            CODEC_ID,
            vec![section(b"rows")],
        )
    }

    fn container() -> SnapshotContainer {
        SnapshotContainer::new(
            SnapshotHeader::new(
                SNAPSHOT_ID,
                CommitVersion::new(42),
                Timestamp::from_micros(1_700),
                DATABASE_ID,
                CODEC_ID,
            )
            .expect("snapshot header"),
            vec![section(b"rows")],
        )
    }

    fn container_with_facts(snapshot_id: u64, watermark: CommitVersion) -> SnapshotContainer {
        SnapshotContainer::new(
            SnapshotHeader::new(
                snapshot_id,
                watermark,
                Timestamp::from_micros(1_700),
                DATABASE_ID,
                CODEC_ID,
            )
            .expect("snapshot header"),
            vec![section(b"rows")],
        )
    }

    fn write_snapshot_bytes(backend: &dyn Backend, bytes: &[u8]) -> ObjectName {
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");
        backend
            .write_object(&object, bytes)
            .expect("write snapshot");
        object
    }

    #[test]
    fn snapshot_optional_load_returns_missing_on_memory_backend() {
        let backend = MemoryBackend::new();
        let service = SnapshotService::new(&backend);

        assert_eq!(service.load_optional(SNAPSHOT_ID), Ok(None));
    }

    #[test]
    fn snapshot_required_load_returns_typed_missing() {
        let backend = MemoryBackend::new();
        let service = SnapshotService::new(&backend);
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");

        assert_eq!(
            service.load_required(SNAPSHOT_ID),
            Err(SnapshotServiceError::Missing {
                object,
                snapshot_id: SNAPSHOT_ID,
            })
        );
    }

    #[test]
    fn snapshot_publish_rejects_memory_backend_as_unsupported_durable_publish() {
        let backend = MemoryBackend::new();
        let service = SnapshotService::new(&backend);

        let error = service
            .publish_create(request(SNAPSHOT_ID))
            .expect_err("memory backend cannot publish durable snapshots");

        match error {
            SnapshotServiceError::Publish {
                snapshot_id,
                source,
            } => {
                assert_eq!(snapshot_id, SNAPSHOT_ID);
                assert_eq!(source.kind(), PublishFailureKind::Unsupported);
                assert_eq!(
                    source.source_error().kind(),
                    BackendErrorKind::UnsupportedOperation
                );
            }
            other => panic!("expected publish error, got {other:?}"),
        }
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn snapshot_create_and_load_round_trips_v1_bytes_on_localfs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let service = SnapshotService::new(&backend);
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");

        let write = service
            .publish_create(request(SNAPSHOT_ID))
            .expect("publish snapshot");
        let loaded = service
            .load_required_for_codec(SNAPSHOT_ID, DATABASE_ID, CODEC_ID)
            .expect("load snapshot");
        let stored = backend.read_object(&object).expect("read stored snapshot");
        let (container, outcome) = &write;

        assert_eq!(container, &loaded);
        assert_eq!(outcome.object(), &object);
        assert_eq!(outcome.durability(), PublishDurability::Durable);
        assert_eq!(outcome.metadata().size_bytes(), stored.len() as u64);
        assert_eq!(
            encode_snapshot_container(&loaded).expect("encode loaded snapshot"),
            stored
        );
    }

    #[test]
    fn snapshot_publish_rejects_zero_snapshot_id_before_backend_access() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);

        assert_eq!(
            service.publish_create(request(0)),
            Err(SnapshotServiceError::InvalidSnapshotFact {
                snapshot_id: 0,
                field: "snapshot_id",
            })
        );
        assert!(backend
            .objects
            .lock()
            .expect("recording backend lock")
            .is_empty());
    }

    #[test]
    fn snapshot_publish_rejects_zero_watermark_before_backend_access() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);

        assert_eq!(
            service.publish_create(request_with_watermark(SNAPSHOT_ID, CommitVersion::ZERO)),
            Err(SnapshotServiceError::InvalidSnapshotFact {
                snapshot_id: SNAPSHOT_ID,
                field: "snapshot_watermark",
            })
        );
        assert!(backend
            .objects
            .lock()
            .expect("recording backend lock")
            .is_empty());
    }

    #[test]
    fn snapshot_create_refuses_existing_snapshot_and_preserves_old_bytes() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");
        let first = service
            .publish_create(request(SNAPSHOT_ID))
            .expect("initial snapshot");
        let old_bytes = backend.read_object(&object).expect("read old snapshot");

        let error = service
            .publish_create(SnapshotPublishRequest::new(
                SNAPSHOT_ID,
                CommitVersion::new(99),
                Timestamp::from_micros(2_000),
                DATABASE_ID,
                CODEC_ID,
                vec![section(b"different")],
            ))
            .expect_err("duplicate snapshot create should fail");

        match error {
            SnapshotServiceError::Publish {
                snapshot_id,
                source,
            } => {
                assert_eq!(snapshot_id, SNAPSHOT_ID);
                assert_eq!(source.kind(), PublishFailureKind::PreconditionFailed);
            }
            other => panic!("expected publish error, got {other:?}"),
        }
        assert_eq!(
            backend.read_object(&object).expect("read snapshot"),
            old_bytes
        );
        assert_eq!(
            encode_snapshot_container(&first.0).expect("encode first"),
            old_bytes
        );
    }

    #[test]
    fn snapshot_corrupt_bytes_return_typed_decode_error() {
        let backend = MemoryBackend::new();
        let object = write_snapshot_bytes(&backend, b"not a snapshot");
        let service = SnapshotService::new(&backend);

        match service
            .load_required(SNAPSHOT_ID)
            .expect_err("corrupt snapshot should fail closed")
        {
            SnapshotServiceError::Decode {
                object: actual,
                snapshot_id,
                source: _,
            } => {
                assert_eq!(actual, object);
                assert_eq!(snapshot_id, SNAPSHOT_ID);
            }
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_load_rejects_header_id_that_disagrees_with_object_id() {
        let backend = MemoryBackend::new();
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");
        let bytes = encode_snapshot_container(&container_with_facts(
            SNAPSHOT_ID + 1,
            CommitVersion::new(42),
        ))
        .expect("encode snapshot");
        backend
            .write_object(&object, &bytes)
            .expect("write snapshot");
        let service = SnapshotService::new(&backend);

        assert_eq!(
            service.load_required(SNAPSHOT_ID),
            Err(SnapshotServiceError::SnapshotIdMismatch {
                object,
                expected: SNAPSHOT_ID,
                actual: SNAPSHOT_ID + 1,
            })
        );
    }

    #[test]
    fn snapshot_load_rejects_zero_watermark_after_decode() {
        let backend = MemoryBackend::new();
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");
        let bytes =
            encode_snapshot_container(&container_with_facts(SNAPSHOT_ID, CommitVersion::ZERO))
                .expect("encode snapshot");
        backend
            .write_object(&object, &bytes)
            .expect("write snapshot");
        let service = SnapshotService::new(&backend);

        assert_eq!(
            service.load_required(SNAPSHOT_ID),
            Err(SnapshotServiceError::Decode {
                object,
                snapshot_id: SNAPSHOT_ID,
                source: FormatError::InvalidValue {
                    field: "snapshot_watermark",
                },
            })
        );
    }

    #[test]
    fn snapshot_codec_mismatch_is_typed() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        service
            .publish_create(request(SNAPSHOT_ID))
            .expect("publish snapshot");
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");

        assert_eq!(
            service.load_required_for_codec(SNAPSHOT_ID, DATABASE_ID, "zstd"),
            Err(SnapshotServiceError::CodecMismatch {
                object,
                snapshot_id: SNAPSHOT_ID,
                expected: "zstd".to_string(),
                actual: CODEC_ID.to_string(),
            })
        );
    }

    #[test]
    fn snapshot_database_mismatch_is_typed() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        service
            .publish_create(request(SNAPSHOT_ID))
            .expect("publish snapshot");
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");
        let expected = [0x22; 16];

        assert_eq!(
            service.load_required_for_codec(SNAPSHOT_ID, expected, CODEC_ID),
            Err(SnapshotServiceError::DatabaseMismatch {
                object,
                snapshot_id: SNAPSHOT_ID,
                expected,
                actual: DATABASE_ID,
            })
        );
    }

    #[test]
    fn snapshot_visit_sections_visits_borrowed_sections_after_identity_validation() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        service
            .publish_create(SnapshotPublishRequest::new(
                SNAPSHOT_ID,
                CommitVersion::new(42),
                Timestamp::from_micros(1_700),
                DATABASE_ID,
                CODEC_ID,
                vec![
                    SnapshotSection::new(0x01, b"rows".to_vec()).expect("section"),
                    SnapshotSection::new(0x02, b"index".to_vec()).expect("section"),
                ],
            ))
            .expect("publish snapshot");
        let mut visited = Vec::new();

        let header = service
            .visit_sections(SNAPSHOT_ID, DATABASE_ID, CODEC_ID, 4, |section| {
                visited.push((section.section_kind(), section.payload().to_vec()));
                Ok(())
            })
            .expect("visit snapshot");

        assert_eq!(header.snapshot_id(), SNAPSHOT_ID);
        assert_eq!(header.database_id(), &DATABASE_ID);
        assert_eq!(header.codec_id(), CODEC_ID);
        assert_eq!(
            visited,
            vec![(0x01, b"rows".to_vec()), (0x02, b"index".to_vec())]
        );
    }

    #[test]
    fn snapshot_visit_sections_validates_footer_crc_before_callback() {
        let backend = MemoryBackend::new();
        let mut bytes = encode_snapshot_container(&container()).expect("encode snapshot");
        let crc_byte = bytes.len() - 1;
        bytes[crc_byte] ^= 0xff;
        let object = write_snapshot_bytes(&backend, &bytes);
        let service = SnapshotService::new(&backend);
        let visits = Cell::new(0);

        let error = service
            .visit_sections(SNAPSHOT_ID, DATABASE_ID, CODEC_ID, 4, |_| {
                visits.set(visits.get() + 1);
                Ok(())
            })
            .expect_err("crc mismatch should fail before callback");

        assert_eq!(visits.get(), 0);
        assert!(matches!(
            error,
            SnapshotServiceError::Decode {
                object: actual,
                snapshot_id: SNAPSHOT_ID,
                source: FormatError::ChecksumMismatch { .. }
            } if actual == object
        ));
    }

    #[test]
    fn snapshot_visit_sections_validates_identity_before_callback() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        service
            .publish_create(request(SNAPSHOT_ID))
            .expect("publish snapshot");
        let visits = Cell::new(0);
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");

        let error = service
            .visit_sections(SNAPSHOT_ID, DATABASE_ID, "zstd", 4, |_| {
                visits.set(visits.get() + 1);
                Ok(())
            })
            .expect_err("codec mismatch should fail before callback");

        assert_eq!(visits.get(), 0);
        assert_eq!(
            error,
            SnapshotServiceError::CodecMismatch {
                object,
                snapshot_id: SNAPSHOT_ID,
                expected: "zstd".to_string(),
                actual: CODEC_ID.to_string(),
            }
        );
    }

    #[test]
    fn snapshot_visit_sections_propagates_callback_error_without_decode_recast() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        service
            .publish_create(request(SNAPSHOT_ID))
            .expect("publish snapshot");
        let object = ObjectLayout::snapshot(SNAPSHOT_ID).expect("snapshot object");

        let error = service
            .visit_sections(SNAPSHOT_ID, DATABASE_ID, CODEC_ID, 4, |section| {
                assert_eq!(section.payload(), b"rows");
                Err(FormatError::InvalidValue {
                    field: "test_callback",
                })
            })
            .expect_err("callback failure should propagate");

        assert_eq!(
            error,
            SnapshotServiceError::Visit {
                object,
                snapshot_id: SNAPSHOT_ID,
                source: FormatError::InvalidValue {
                    field: "test_callback",
                },
            }
        );
    }

    #[test]
    fn snapshot_publish_failures_preserve_publish_kind() {
        for kind in ALL_PUBLISH_FAILURE_KINDS {
            let backend = PublishFailureBackend { kind };
            let service = SnapshotService::new(&backend);

            let error = service
                .publish_create(request(SNAPSHOT_ID))
                .expect_err("publish failure should propagate");

            match error {
                SnapshotServiceError::Publish {
                    snapshot_id,
                    source,
                } => {
                    assert_eq!(snapshot_id, SNAPSHOT_ID);
                    assert_eq!(source.kind(), kind);
                    assert_eq!(
                        source.object(),
                        &ObjectLayout::snapshot(SNAPSHOT_ID).unwrap()
                    );
                }
                other => panic!("expected publish error, got {other:?}"),
            }
        }
    }

    #[test]
    fn snapshot_service_decodes_freshly_encoded_container_before_returning_write() {
        let backend = RecordingBackend::new();
        let service = SnapshotService::new(&backend);
        let expected = container();

        let write = service
            .publish_create(request(SNAPSHOT_ID))
            .expect("publish snapshot");
        let (container, outcome) = &write;

        assert_eq!(container, &expected);
        assert_eq!(
            encode_snapshot_container(container).expect("encode write"),
            backend
                .read_object(outcome.object())
                .expect("read stored snapshot")
        );
    }
}
