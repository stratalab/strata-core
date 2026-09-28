use super::super::{
    CheckpointRequest, ManifestRole, ManifestServiceError, SnapshotPublishRequest,
    TableObjectServiceError,
};
use crate::backend::memory::MemoryBackend;
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendCapability, BackendError, BackendMetadata,
    BackendRange, BackendResult, PublishError, PublishFailureKind, PublishMode, PublishOutcome,
    PublishResult,
};
use crate::format::quarantine::{
    encode_quarantine_inventory, QuarantineEntry, QuarantineInventory,
};
use crate::format::{
    encode_immutable_table, encode_manifest, DatabaseManifest, SnapshotSection, TableCompression,
    WalCommitPayload, WalRecord,
};
use crate::layout::{ObjectFamily, ObjectLayout};
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use std::collections::BTreeMap;
use std::sync::Mutex;
use strata_core::{BranchId, CommitVersion, Timestamp};

pub(super) const DATABASE_ID: [u8; 16] = [0x53; 16];
pub(super) const CODEC_ID: &str = "identity";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    Read(ObjectName),
    Range(ObjectName),
    List(String),
    Metadata(ObjectName),
    Write(ObjectName),
    Delete(ObjectName),
    Append(ObjectName),
    Sync(ObjectName),
    Publish(ObjectName, PublishMode),
    Lock(ObjectName),
}

#[derive(Debug, Default)]
pub(super) struct ObservingCacheBackend {
    inner: MemoryBackend,
    operations: Mutex<Vec<Operation>>,
}

impl ObservingCacheBackend {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn seed(&self, name: &ObjectName, bytes: &[u8]) {
        self.inner
            .write_object(name, bytes)
            .expect("seed cache backend object");
    }

    pub(super) fn read_seeded(&self, name: &ObjectName) -> Vec<u8> {
        self.inner.read_object(name).expect("read seeded object")
    }

    pub(super) fn operations(&self) -> Vec<Operation> {
        self.operations.lock().expect("operation lock").clone()
    }

    pub(super) fn clear_operations(&self) {
        self.operations.lock().expect("operation lock").clear();
    }

    fn record(&self, operation: Operation) {
        self.operations
            .lock()
            .expect("operation lock")
            .push(operation);
    }
}

impl Backend for ObservingCacheBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.record(Operation::Read(name.clone()));
        self.inner.read_object(name)
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.record(Operation::Range(name.clone()));
        self.inner.read_range(name, range)
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.record(Operation::Write(name.clone()));
        self.inner.write_object(name, bytes)
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        self.record(Operation::Delete(name.clone()));
        self.inner.delete_object(name)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.record(Operation::List(prefix.as_str().to_owned()));
        self.inner.list_prefix(prefix)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.record(Operation::Metadata(name.clone()));
        self.inner.object_metadata(name)
    }

    fn acquire_writer_lock(
        &self,
        name: &ObjectName,
    ) -> BackendResult<crate::backend::BackendWriterGuard> {
        self.record(Operation::Lock(name.clone()));
        Err(BackendError::unsupported(
            BackendCapability::SingleWriterLock,
        ))
    }

    fn append_object(&self, name: &ObjectName, _bytes: &[u8]) -> BackendResult<BackendAppend> {
        self.record(Operation::Append(name.clone()));
        Err(BackendError::unsupported(BackendCapability::AppendObject))
    }

    fn sync_object(&self, name: &ObjectName) -> crate::backend::BackendResult<()> {
        self.record(Operation::Sync(name.clone()));
        Err(BackendError::unsupported(BackendCapability::DurableSync))
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        self.record(Operation::Publish(name.clone(), mode));
        self.inner.publish_object(name, bytes, mode)
    }
}

pub(super) fn branch_id() -> BranchId {
    BranchId::from_bytes([0x42; 16])
}

pub(super) fn wal_record(version: u64) -> WalRecord {
    let branch_id = branch_id();
    let commit_version = CommitVersion::new(version);
    let commit_timestamp = Timestamp::from_micros(version);
    let physical_key = PhysicalKey::new(
        branch_id,
        "default",
        StorageSpaceId::engine(0x20).expect("engine storage space"),
        version.to_le_bytes(),
    )
    .expect("physical key");
    let row = StorageRow::put(
        physical_key,
        commit_version,
        commit_timestamp,
        Timestamp::EPOCH,
        b"cache probe".to_vec(),
    );
    let payload = WalCommitPayload::new(vec![row]).expect("WAL commit payload");
    WalRecord::new(commit_version, branch_id, commit_timestamp, payload).expect("WAL record")
}

pub(super) fn cache_object() -> ObjectName {
    ObjectName::new("cache/runtime-object").expect("cache object name")
}

pub(super) fn table_branch() -> String {
    branch_id().to_string()
}

pub(super) fn snapshot_request() -> SnapshotPublishRequest {
    SnapshotPublishRequest::new(
        7,
        CommitVersion::new(11),
        Timestamp::from_micros(1_700_000),
        DATABASE_ID,
        CODEC_ID,
        vec![SnapshotSection::new(1, b"section".to_vec()).expect("snapshot section")],
    )
}

pub(super) fn checkpoint_request() -> CheckpointRequest {
    CheckpointRequest::new(
        DATABASE_ID,
        CODEC_ID,
        3,
        9,
        CommitVersion::new(44),
        Timestamp::from_micros(1_800_000),
        vec![SnapshotSection::new(1, b"checkpoint".to_vec()).expect("snapshot section")],
    )
}

pub(super) fn valid_manifest_bytes() -> Vec<u8> {
    let manifest = DatabaseManifest::new(DATABASE_ID, CODEC_ID).expect("database metadata");
    encode_manifest(&manifest).expect("encode database metadata")
}

pub(super) fn valid_table_object_bytes() -> Vec<u8> {
    let branch_id = branch_id();
    let physical_key = PhysicalKey::new(
        branch_id,
        "default",
        StorageSpaceId::engine(0x20).expect("engine storage space"),
        b"cache-table-row".to_vec(),
    )
    .expect("physical key");
    let row = StorageRow::put(
        physical_key,
        CommitVersion::new(7),
        Timestamp::from_micros(2_000_000),
        Timestamp::EPOCH,
        b"table bytes".to_vec(),
    );
    encode_immutable_table(&[row], 4096, 1, TableCompression::Uncompressed)
        .expect("encode table object")
}

pub(super) fn quarantine_inventory_fixture() -> (ObjectName, ObjectName, ObjectName, Vec<u8>) {
    let branch_id = branch_id();
    let source_object = table_source_object("table0001");
    let quarantine_object = ObjectLayout::quarantine_object(&branch_id.to_string(), "table0001")
        .expect("quarantine object");
    let inventory_object =
        ObjectLayout::quarantine_manifest(&branch_id.to_string()).expect("inventory object");
    let entry = QuarantineEntry::new(
        "table0001".to_owned(),
        source_object.clone(),
        5,
        Timestamp::from_micros(2_000_000),
    )
    .expect("quarantine entry");
    let inventory = QuarantineInventory::new(DATABASE_ID, branch_id, CODEC_ID, vec![entry])
        .expect("quarantine inventory");
    let inventory_bytes = encode_quarantine_inventory(&inventory).expect("encode inventory");

    (
        source_object,
        quarantine_object,
        inventory_object,
        inventory_bytes,
    )
}

pub(super) fn durable_snapshot(backend: &dyn Backend) -> BTreeMap<ObjectName, Vec<u8>> {
    all_visible_objects(backend)
        .into_iter()
        .filter(|(object, _)| is_durable_family(object))
        .collect()
}

pub(super) fn assert_durable_snapshot_unchanged(
    backend: &ObservingCacheBackend,
    before: &BTreeMap<ObjectName, Vec<u8>>,
) {
    assert_eq!(
        durable_snapshot(backend),
        *before,
        "cache path changed a durable object family"
    );
}

pub(super) fn assert_no_mutating_operations(backend: &ObservingCacheBackend) {
    for operation in backend.operations() {
        assert!(
            !matches!(
                operation,
                Operation::Write(_)
                    | Operation::Delete(_)
                    | Operation::Append(_)
                    | Operation::Sync(_)
                    | Operation::Publish(_, _)
                    | Operation::Lock(_)
            ),
            "unexpected cache-mode mutation operation: {operation:?}"
        );
    }
}

pub(super) fn assert_publish_unsupported(
    error: &PublishError,
    object: &ObjectName,
    missing: BackendCapability,
) {
    assert_eq!(error.object(), object);
    assert_eq!(error.kind(), PublishFailureKind::Unsupported);
    assert!(
        error.source_error().to_string().contains(missing.name()),
        "publish error did not name missing capability {missing:?}: {error}"
    );
}

pub(super) fn assert_manifest_publish_unsupported(
    error: ManifestServiceError,
    role: ManifestRole,
    object: &ObjectName,
) {
    match error {
        ManifestServiceError::Publish {
            role: actual_role,
            source,
        } => {
            assert_eq!(actual_role, role);
            assert_publish_unsupported(&source, object, BackendCapability::DurablePublish);
        }
        other => panic!("expected manifest publish error, got {other:?}"),
    }
}

pub(super) fn assert_table_object_publish_unsupported(
    error: TableObjectServiceError,
    object: &ObjectName,
) {
    match error {
        TableObjectServiceError::Publish {
            object: actual_object,
            source,
        } => {
            assert_eq!(&actual_object, object);
            assert_publish_unsupported(&source, object, BackendCapability::DurablePublish);
        }
        other => panic!("expected table object publish error, got {other:?}"),
    }
}

fn table_source_object(object_id: &str) -> ObjectName {
    ObjectLayout::table_object(&table_branch(), 0, object_id).expect("table object")
}

fn all_visible_objects(backend: &dyn Backend) -> BTreeMap<ObjectName, Vec<u8>> {
    let prefix = ObjectPrefix::new("").expect("all-object prefix");
    backend
        .list_prefix(&prefix)
        .expect("list objects")
        .into_iter()
        .map(|object| {
            let bytes = backend.read_object(&object).expect("read listed object");
            (object, bytes)
        })
        .collect()
}

fn is_durable_family(object: &ObjectName) -> bool {
    matches!(
        ObjectFamily::from_object_name(object),
        Some(
            ObjectFamily::Manifest
                | ObjectFamily::Wal
                | ObjectFamily::Tables
                | ObjectFamily::Snapshots
                | ObjectFamily::Timeline
                | ObjectFamily::Temporary
                | ObjectFamily::Quarantine
                | ObjectFamily::Locks
                | ObjectFamily::Meta
        )
    )
}
