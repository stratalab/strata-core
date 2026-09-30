use super::{
    QuarantineGate, QuarantineInventoryCorruption, QuarantineInventoryLoad,
    QuarantineObjectRequest, QuarantineObjectStatus, QuarantinePurgeRequest,
    QuarantineReconciliationKind, QuarantineService, QuarantineServiceError,
    QuarantineServiceResult,
};
use crate::backend::{
    memory::MemoryBackend, Backend, BackendCapabilities, BackendCapability, BackendError,
    BackendErrorKind, BackendMetadata, BackendRange, BackendResult, PublishDurability,
    PublishError, PublishFailureKind, PublishMode, PublishOutcome,
};
use crate::format::quarantine::{
    encode_quarantine_inventory, QuarantineEntry, QuarantineInventory,
};
use crate::format::FormatError;
use crate::layout::ObjectLayout;
use crate::object::{ObjectName, ObjectPrefix};
use std::collections::BTreeMap;
use std::sync::Mutex;
use strata_core::{BranchId, Timestamp};

// This module stays together for inventory-service coverage because the fake
// backends, identity fixtures, and publish-failure matrix are shared by every
// case below. Split before adding quarantine-object, purge, or reconciliation
// behavior so independent fault-window suites do not accrete here.

const DATABASE_ID: [u8; 16] = [0x51; 16];
const OTHER_DATABASE_ID: [u8; 16] = [0x52; 16];
const CODEC_ID: &str = "identity";

const ALL_PUBLISH_FAILURE_KINDS: [PublishFailureKind; 5] = [
    PublishFailureKind::Unsupported,
    PublishFailureKind::PreconditionFailed,
    PublishFailureKind::FailedBeforeVisibility,
    PublishFailureKind::VisibilityUnknown,
    PublishFailureKind::VisibleDurabilityUnconfirmed,
];

fn branch_id() -> BranchId {
    BranchId::from_bytes([0x31; BranchId::BYTE_LEN])
}

fn other_branch_id() -> BranchId {
    BranchId::from_bytes([0x32; BranchId::BYTE_LEN])
}

fn inventory_object(branch_id: BranchId) -> ObjectName {
    ObjectLayout::quarantine_manifest(&branch_id.to_string()).expect("quarantine manifest")
}

fn table_source_object(table_id: &str) -> ObjectName {
    ObjectLayout::table_object("main", 0, table_id).expect("table source object")
}

fn inventory(branch_id: BranchId, entries: Vec<QuarantineEntry>) -> QuarantineInventory {
    QuarantineInventory::new(DATABASE_ID, branch_id, CODEC_ID, entries)
        .expect("quarantine inventory")
}

fn one_entry_inventory(branch_id: BranchId) -> QuarantineInventory {
    inventory(
        branch_id,
        vec![QuarantineEntry::new(
            "table0001",
            table_source_object("table0001"),
            128,
            Timestamp::from_micros(1_700_000_000_000_000),
        )
        .expect("quarantine entry")],
    )
}

fn encode_inventory(inventory: &QuarantineInventory) -> Vec<u8> {
    encode_quarantine_inventory(inventory).expect("encode quarantine inventory")
}

fn assert_empty_load(load: &QuarantineInventoryLoad, branch_id: BranchId, object: &ObjectName) {
    assert!(!load.is_present());
    assert_eq!(load.object(), object);
    assert_eq!(load.branch_id(), branch_id);
    assert_eq!(load.entry_count(), 0);
    assert_eq!(load.byte_count(), 0);
    assert!(load.inventory().is_empty());
    assert_eq!(load.inventory().database_id(), &DATABASE_ID);
    assert_eq!(load.inventory().codec_id(), CODEC_ID);
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

    fn with_object(name: ObjectName, bytes: Vec<u8>) -> Self {
        let mut objects = BTreeMap::new();
        objects.insert(name, bytes);
        Self {
            objects: Mutex::new(objects),
            outcome_override: Mutex::new(None),
        }
    }

    fn stored_bytes(&self, name: &ObjectName) -> Vec<u8> {
        self.read_object(name).expect("stored object")
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
        let end = range
            .end_offset()
            .ok_or_else(|| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
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

struct ReadFailureBackend;

impl Backend for ReadFailureBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::from_slice(&[BackendCapability::ReadObject])
    }

    fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
        Err(BackendError::new(
            BackendErrorKind::Unavailable,
            "read unavailable",
        ))
    }

    fn read_range(&self, _name: &ObjectName, _range: BackendRange) -> BackendResult<Vec<u8>> {
        Err(BackendError::new(
            BackendErrorKind::UnsupportedOperation,
            "range unsupported",
        ))
    }

    fn write_object(&self, _name: &ObjectName, _bytes: &[u8]) -> BackendResult<BackendMetadata> {
        Err(BackendError::new(
            BackendErrorKind::UnsupportedOperation,
            "write unsupported",
        ))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        crate::backend::failed_delete_result(
            name,
            BackendError::new(BackendErrorKind::UnsupportedOperation, "delete unsupported"),
        )
    }

    fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        Ok(Vec::new())
    }

    fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
        Err(BackendError::new(
            BackendErrorKind::UnsupportedOperation,
            "metadata unsupported",
        ))
    }
}

struct MissingReadCapabilityBackend;

impl Backend for MissingReadCapabilityBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::empty()
    }

    fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
        panic!("read_object should be preflighted when read capability is missing")
    }

    fn read_range(&self, _name: &ObjectName, _range: BackendRange) -> BackendResult<Vec<u8>> {
        panic!("read_range should not be called by inventory load")
    }

    fn write_object(&self, _name: &ObjectName, _bytes: &[u8]) -> BackendResult<BackendMetadata> {
        panic!("write_object should not be called by inventory load")
    }

    fn delete_object(&self, _name: &ObjectName) -> crate::backend::DeleteResult {
        panic!("delete_object should not be called by inventory load")
    }

    fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        panic!("list_prefix should not be called by inventory load")
    }

    fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
        panic!("object_metadata should not be called by inventory load")
    }
}

struct MissingDurableSyncBackend;

impl Backend for MissingDurableSyncBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::from_slice(&[BackendCapability::DurablePublish])
    }

    fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
        panic!("read_object should not be called by inventory publish preflight")
    }

    fn read_range(&self, _name: &ObjectName, _range: BackendRange) -> BackendResult<Vec<u8>> {
        panic!("read_range should not be called by inventory publish preflight")
    }

    fn write_object(&self, _name: &ObjectName, _bytes: &[u8]) -> BackendResult<BackendMetadata> {
        panic!("write_object should not be called by inventory publish preflight")
    }

    fn delete_object(&self, _name: &ObjectName) -> crate::backend::DeleteResult {
        panic!("delete_object should not be called by inventory publish preflight")
    }

    fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        panic!("list_prefix should not be called by inventory publish preflight")
    }

    fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
        panic!("object_metadata should not be called by inventory publish preflight")
    }

    fn publish_object(
        &self,
        _name: &ObjectName,
        _bytes: &[u8],
        _mode: PublishMode,
    ) -> Result<PublishOutcome, PublishError> {
        panic!("publish_object should not be called without durable sync capability")
    }
}

#[derive(Debug)]
struct PublishFailureBackend {
    kind: PublishFailureKind,
    visible_replacement: bool,
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
}

impl PublishFailureBackend {
    fn with_object(kind: PublishFailureKind, name: ObjectName, bytes: &[u8]) -> Self {
        let mut objects = BTreeMap::new();
        objects.insert(name, bytes.to_vec());
        Self {
            kind,
            visible_replacement: false,
            objects: Mutex::new(objects),
        }
    }

    fn visible_after_replace(kind: PublishFailureKind, name: ObjectName, bytes: &[u8]) -> Self {
        let mut backend = Self::with_object(kind, name, bytes);
        backend.visible_replacement = true;
        backend
    }

    fn stored_bytes(&self, name: &ObjectName) -> Vec<u8> {
        self.read_object(name).expect("stored object")
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

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.objects
            .lock()
            .expect("publish failure backend lock")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset())
            .map_err(|_| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
        let end = range
            .end_offset()
            .ok_or_else(|| BackendError::new(BackendErrorKind::InvalidRange, "range overflow"))?;
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
            .expect("publish failure backend lock")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        let removed = self
            .objects
            .lock()
            .expect("publish failure backend lock")
            .remove(name)
            .is_some();
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        Ok(self
            .objects
            .lock()
            .expect("publish failure backend lock")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect())
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("publish failure backend lock")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "not found"))
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        _mode: PublishMode,
    ) -> Result<PublishOutcome, PublishError> {
        if self.visible_replacement {
            self.objects
                .lock()
                .expect("publish failure backend lock")
                .insert(name.clone(), bytes.to_vec());
        }
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

fn assert_publish_error(
    error: QuarantineServiceError,
    object: &ObjectName,
    kind: PublishFailureKind,
) {
    match error {
        QuarantineServiceError::Publish {
            object: actual,
            source,
        } => {
            assert_eq!(&actual, object);
            assert_eq!(source.object(), object);
            assert_eq!(source.kind(), kind);
            assert_eq!(source.source_error().kind(), BackendErrorKind::Interrupted);
        }
        other => panic!("expected publish error, got {other:?}"),
    }
}

fn load_inventory(
    service: &QuarantineService<'_>,
    branch_id: BranchId,
) -> QuarantineServiceResult<QuarantineInventoryLoad> {
    service.load_inventory(branch_id, DATABASE_ID, CODEC_ID)
}

#[test]
fn optional_inventory_load_on_absent_memory_state_returns_empty() {
    let backend = MemoryBackend::new();
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let object = inventory_object(branch_id);

    let load = load_inventory(&service, branch_id).expect("load absent inventory as empty");

    assert_empty_load(&load, branch_id, &object);
}

#[test]
fn inventory_load_requires_read_capability_before_backend_access() {
    let backend = MissingReadCapabilityBackend;
    let service = QuarantineService::new(&backend);

    assert_eq!(
        load_inventory(&service, branch_id()),
        Err(QuarantineServiceError::UnsupportedCapability {
            capability: BackendCapability::ReadObject,
        })
    );
}

#[test]
fn present_inventory_load_reports_branch_object_count_and_bytes() {
    let branch_id = branch_id();
    let object = inventory_object(branch_id);
    let inventory = one_entry_inventory(branch_id);
    let bytes = encode_inventory(&inventory);
    let backend = RecordingBackend::with_object(object.clone(), bytes.clone());
    let service = QuarantineService::new(&backend);

    let load = service
        .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect("load present inventory");

    assert!(load.is_present());
    assert_eq!(load.object(), &object);
    assert_eq!(load.branch_id(), branch_id);
    assert_eq!(load.entry_count(), 1);
    assert_eq!(load.byte_count(), bytes.len() as u64);
    assert_eq!(load.inventory(), &inventory);
}

#[test]
fn required_inventory_load_distinguishes_absent_and_corrupt_inventory() {
    let branch_id = branch_id();
    let object = inventory_object(branch_id);
    let absent_backend = MemoryBackend::new();
    let absent_service = QuarantineService::new(&absent_backend);

    assert_eq!(
        absent_service.load_required_inventory(branch_id, DATABASE_ID, CODEC_ID),
        Err(QuarantineServiceError::Missing {
            object: object.clone()
        })
    );

    absent_backend
        .write_object(&object, b"not-an-inventory")
        .expect("write corrupt inventory");
    let error = absent_service
        .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect_err("corrupt inventory must not be absent");

    assert!(matches!(
        error,
        QuarantineServiceError::Decode {
            object: actual,
            source: FormatError::InsufficientBytes { .. },
        } if actual == object
    ));
}

#[test]
fn inventory_load_preserves_backend_read_failure() {
    let backend = ReadFailureBackend;
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let object = inventory_object(branch_id);

    let error = load_inventory(&service, branch_id).expect_err("read should fail");

    assert!(matches!(
        error,
        QuarantineServiceError::Read {
            object: actual,
            source,
        } if actual == object && source.kind() == BackendErrorKind::Unavailable
    ));
}

#[test]
fn inventory_load_rejects_identity_mismatches() {
    let branch_id = branch_id();
    let object = inventory_object(branch_id);

    let wrong_database =
        QuarantineInventory::new(OTHER_DATABASE_ID, branch_id, CODEC_ID, Vec::new())
            .expect("wrong database inventory");
    let backend = RecordingBackend::with_object(object.clone(), encode_inventory(&wrong_database));
    let service = QuarantineService::new(&backend);
    assert_eq!(
        load_inventory(&service, branch_id),
        Err(QuarantineServiceError::DatabaseMismatch {
            object: object.clone(),
            expected: DATABASE_ID,
            actual: OTHER_DATABASE_ID,
        })
    );

    let wrong_branch = inventory(other_branch_id(), Vec::new());
    let backend = RecordingBackend::with_object(object.clone(), encode_inventory(&wrong_branch));
    let service = QuarantineService::new(&backend);
    assert_eq!(
        load_inventory(&service, branch_id),
        Err(QuarantineServiceError::BranchMismatch {
            object: object.clone(),
            expected: branch_id,
            actual: other_branch_id(),
        })
    );

    let wrong_codec = QuarantineInventory::new(DATABASE_ID, branch_id, "other-codec", Vec::new())
        .expect("wrong codec inventory");
    let backend = RecordingBackend::with_object(object.clone(), encode_inventory(&wrong_codec));
    let service = QuarantineService::new(&backend);
    assert_eq!(
        load_inventory(&service, branch_id),
        Err(QuarantineServiceError::CodecMismatch {
            object,
            expected: CODEC_ID.to_owned(),
            actual: "other-codec".to_owned(),
        })
    );
}

#[test]
fn inventory_publish_replace_creates_missing_inventory_object() {
    let backend = RecordingBackend::new();
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let inventory = one_entry_inventory(branch_id);
    let object = inventory_object(branch_id);
    let expected_bytes = encode_inventory(&inventory);

    let write = service
        .publish_inventory_replace(&inventory)
        .expect("publish inventory");

    assert_eq!(write.object(), &object);
    assert_eq!(write.branch_id(), branch_id);
    assert_eq!(write.entry_count(), 1);
    assert_eq!(write.byte_count(), expected_bytes.len() as u64);
    assert_eq!(write.outcome().object(), &object);
    assert_eq!(write.inventory(), &inventory);
    assert_eq!(backend.stored_bytes(&object), expected_bytes);
}

#[test]
fn inventory_publish_replace_replaces_existing_inventory_object() {
    let branch_id = branch_id();
    let object = inventory_object(branch_id);
    let old_inventory = inventory(branch_id, Vec::new());
    let new_inventory = one_entry_inventory(branch_id);
    let backend = RecordingBackend::with_object(object.clone(), encode_inventory(&old_inventory));
    let service = QuarantineService::new(&backend);

    let write = service
        .publish_inventory_replace(&new_inventory)
        .expect("replace inventory");

    assert_eq!(write.inventory(), &new_inventory);
    assert_eq!(
        backend.stored_bytes(&object),
        encode_inventory(&new_inventory)
    );
}

#[test]
fn empty_inventory_publish_is_valid_and_reports_facts() {
    let backend = RecordingBackend::new();
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let inventory = inventory(branch_id, Vec::new());
    let object = inventory_object(branch_id);
    let expected_bytes = encode_inventory(&inventory);

    let write = service
        .publish_inventory_replace(&inventory)
        .expect("publish empty inventory");

    assert_eq!(write.object(), &object);
    assert_eq!(write.branch_id(), branch_id);
    assert_eq!(write.entry_count(), 0);
    assert_eq!(write.byte_count(), expected_bytes.len() as u64);
    assert_eq!(
        write.outcome().metadata().size_bytes(),
        expected_bytes.len() as u64
    );
    assert_eq!(backend.stored_bytes(&object), expected_bytes);
}

#[test]
fn inventory_publish_rejects_invalid_publish_outcome_metadata() {
    let backend = RecordingBackend::new();
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let inventory = one_entry_inventory(branch_id);
    let object = inventory_object(branch_id);
    backend.override_next_publish_outcome(PublishOutcome::new(
        object.clone(),
        BackendMetadata::new(1, None),
        PublishDurability::Durable,
    ));

    assert_eq!(
        service.publish_inventory_replace(&inventory),
        Err(QuarantineServiceError::InvalidPublishMetadata {
            object,
            field: "size_bytes",
        })
    );
}

#[test]
fn durable_inventory_publish_on_memory_backend_returns_unsupported_publish() {
    let backend = MemoryBackend::new();
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let inventory = inventory(branch_id, Vec::new());
    let object = inventory_object(branch_id);

    let error = service
        .publish_inventory_replace(&inventory)
        .expect_err("memory backend cannot publish durably");

    match error {
        QuarantineServiceError::Publish {
            object: actual,
            source,
        } => {
            assert_eq!(actual, object);
            assert_eq!(source.object(), &object);
            assert_eq!(source.kind(), PublishFailureKind::Unsupported);
            assert_eq!(
                source.source_error().kind(),
                BackendErrorKind::UnsupportedOperation
            );
        }
        other => panic!("expected publish error, got {other:?}"),
    }
}

#[test]
fn inventory_publish_requires_durable_sync_before_backend_publish() {
    let backend = MissingDurableSyncBackend;
    let service = QuarantineService::new(&backend);
    let branch_id = branch_id();
    let inventory = inventory(branch_id, Vec::new());
    let object = inventory_object(branch_id);

    let error = service
        .publish_inventory_replace(&inventory)
        .expect_err("durable sync is required before backend publish");

    match error {
        QuarantineServiceError::Publish {
            object: actual,
            source,
        } => {
            assert_eq!(actual, object);
            assert_eq!(source.object(), &object);
            assert_eq!(source.kind(), PublishFailureKind::Unsupported);
            assert_eq!(
                source.source_error().kind(),
                BackendErrorKind::UnsupportedOperation
            );
            assert!(source.source_error().to_string().contains("durable_sync"));
        }
        other => panic!("expected publish error, got {other:?}"),
    }
}

#[test]
fn inventory_publish_failures_preserve_kind_and_old_bytes_when_not_visible() {
    let branch_id = branch_id();
    let object = inventory_object(branch_id);
    let old_inventory = inventory(branch_id, Vec::new());
    let old_bytes = encode_inventory(&old_inventory);
    let new_inventory = one_entry_inventory(branch_id);

    for kind in ALL_PUBLISH_FAILURE_KINDS {
        let backend = PublishFailureBackend::with_object(kind, object.clone(), &old_bytes);
        let service = QuarantineService::new(&backend);

        let error = service
            .publish_inventory_replace(&new_inventory)
            .expect_err("publish should fail");

        assert_publish_error(error, &object, kind);
        if publish_failure_implies_no_visible_replacement(kind) {
            assert_eq!(backend.stored_bytes(&object), old_bytes);
            assert_eq!(
                service
                    .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
                    .expect("load old inventory")
                    .inventory(),
                &old_inventory
            );
        }
    }
}

#[test]
fn inventory_visible_unconfirmed_publish_returns_error_with_new_bytes_visible() {
    let branch_id = branch_id();
    let object = inventory_object(branch_id);
    let old_inventory = inventory(branch_id, Vec::new());
    let old_bytes = encode_inventory(&old_inventory);
    let new_inventory = one_entry_inventory(branch_id);
    let new_bytes = encode_inventory(&new_inventory);
    let backend = PublishFailureBackend::visible_after_replace(
        PublishFailureKind::VisibleDurabilityUnconfirmed,
        object.clone(),
        &old_bytes,
    );
    let service = QuarantineService::new(&backend);

    let error = service
        .publish_inventory_replace(&new_inventory)
        .expect_err("visible but unconfirmed publish must not return write facts");

    assert_publish_error(
        error,
        &object,
        PublishFailureKind::VisibleDurabilityUnconfirmed,
    );
    assert_eq!(backend.stored_bytes(&object), new_bytes);
    assert_eq!(
        service
            .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
            .expect("load visible unconfirmed inventory")
            .inventory(),
        &new_inventory
    );
}

mod inventory;
#[cfg(all(unix, feature = "localfs"))]
mod local_fs_link;
mod mutation;
mod reconcile;
#[cfg(not(target_arch = "wasm32"))]
mod reconcile_property;
