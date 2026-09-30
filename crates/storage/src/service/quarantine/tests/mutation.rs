use super::*;
use crate::backend::{
    Backend, BackendCapabilities, BackendCapability, BackendError, BackendErrorKind,
    BackendMetadata, BackendRange, BackendResult, DeleteDurability, DeleteOutcome,
    PublishDurability, PublishError, PublishFailureKind, PublishMode, PublishOutcome,
};
use crate::object::{ObjectName, ObjectPrefix};
use std::collections::BTreeMap;
use std::sync::Mutex;

// The quarantine-object mutation suite keeps one faulting backend so publish,
// metadata, read, and delete windows are exercised against the same operation
// log. Existing-state and purge cases split into child modules to keep each
// fault-window group independently reviewable.
mod existing;
mod link;
mod purge;
mod request;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Operation {
    Metadata(ObjectName),
    Publish(ObjectName, PublishMode),
    Delete(ObjectName),
    /// #3721: `link_object(from, to)`.
    Link(ObjectName, ObjectName),
}

#[derive(Debug)]
struct MutationBackend {
    capabilities: BackendCapabilities,
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    reads: Mutex<BTreeMap<ObjectName, usize>>,
    read_failures: Mutex<BTreeMap<ObjectName, BackendErrorKind>>,
    publish_failures: Mutex<BTreeMap<ObjectName, (PublishFailureKind, bool)>>,
    delete_failures: Mutex<BTreeMap<ObjectName, BackendErrorKind>>,
    non_durable_deletes: Mutex<BTreeMap<ObjectName, bool>>,
    metadata_failures: Mutex<BTreeMap<ObjectName, BackendErrorKind>>,
    metadata_override: Mutex<Option<(ObjectName, u64)>>,
    /// #3721: `to` -> (failure kind, source error kind, whether `to` becomes
    /// visible anyway).
    #[allow(
        clippy::type_complexity,
        reason = "a test backend's fault table; a named type adds nothing"
    )]
    link_failures: Mutex<BTreeMap<ObjectName, (PublishFailureKind, BackendErrorKind, bool)>>,
    operations: Mutex<Vec<Operation>>,
}

impl MutationBackend {
    fn durable() -> Self {
        Self {
            capabilities: BackendCapabilities::from_slice(&[
                BackendCapability::ReadObject,
                BackendCapability::ReadRange,
                BackendCapability::WriteObject,
                BackendCapability::DeleteObject,
                BackendCapability::ListPrefix,
                BackendCapability::ObjectMetadata,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
            ]),
            objects: Mutex::new(BTreeMap::new()),
            reads: Mutex::new(BTreeMap::new()),
            read_failures: Mutex::new(BTreeMap::new()),
            publish_failures: Mutex::new(BTreeMap::new()),
            delete_failures: Mutex::new(BTreeMap::new()),
            non_durable_deletes: Mutex::new(BTreeMap::new()),
            metadata_failures: Mutex::new(BTreeMap::new()),
            metadata_override: Mutex::new(None),
            link_failures: Mutex::new(BTreeMap::new()),
            operations: Mutex::new(Vec::new()),
        }
    }

    /// #3721: advertise a durable link, as the local-filesystem backend does.
    fn with_link(mut self) -> Self {
        self.capabilities.insert(BackendCapability::DurableLink);
        self
    }

    fn fail_link(
        &self,
        object: ObjectName,
        kind: PublishFailureKind,
        source: BackendErrorKind,
        visible: bool,
    ) {
        self.link_failures
            .lock()
            .expect("mutation backend lock")
            .insert(object, (kind, source, visible));
    }

    fn with_object(self, object: ObjectName, bytes: &[u8]) -> Self {
        self.objects
            .lock()
            .expect("mutation backend lock")
            .insert(object, bytes.to_vec());
        self
    }

    fn fail_publish(&self, object: ObjectName, kind: PublishFailureKind, visible: bool) {
        self.publish_failures
            .lock()
            .expect("mutation backend lock")
            .insert(object, (kind, visible));
    }

    fn fail_read(&self, object: ObjectName, kind: BackendErrorKind) {
        self.read_failures
            .lock()
            .expect("mutation backend lock")
            .insert(object, kind);
    }

    fn fail_delete(&self, object: ObjectName) {
        self.fail_delete_with(object, BackendErrorKind::Interrupted);
    }

    fn fail_delete_with(&self, object: ObjectName, kind: BackendErrorKind) {
        self.delete_failures
            .lock()
            .expect("mutation backend lock")
            .insert(object, kind);
    }

    fn make_delete_non_durable(&self, object: ObjectName) {
        self.non_durable_deletes
            .lock()
            .expect("mutation backend lock")
            .insert(object, true);
    }

    fn fail_metadata(&self, object: ObjectName, kind: BackendErrorKind) {
        self.metadata_failures
            .lock()
            .expect("mutation backend lock")
            .insert(object, kind);
    }

    fn override_metadata_size(&self, object: ObjectName, size: u64) {
        *self
            .metadata_override
            .lock()
            .expect("mutation backend lock") = Some((object, size));
    }

    fn contains(&self, object: &ObjectName) -> bool {
        self.objects
            .lock()
            .expect("mutation backend lock")
            .contains_key(object)
    }

    fn bytes(&self, object: &ObjectName) -> Vec<u8> {
        self.read_object(object).expect("stored object")
    }

    fn read_count(&self, object: &ObjectName) -> usize {
        self.reads
            .lock()
            .expect("mutation backend lock")
            .get(object)
            .copied()
            .unwrap_or(0)
    }

    fn operations(&self) -> Vec<Operation> {
        self.operations
            .lock()
            .expect("mutation backend lock")
            .clone()
    }
}

impl Backend for MutationBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        *self
            .reads
            .lock()
            .expect("mutation backend lock")
            .entry(name.clone())
            .or_insert(0) += 1;
        if let Some(kind) = self
            .read_failures
            .lock()
            .expect("mutation backend lock")
            .get(name)
            .copied()
        {
            return Err(BackendError::new(kind, "read failed"));
        }
        self.objects
            .lock()
            .expect("mutation backend lock")
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
            .expect("mutation backend lock")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        self.operations
            .lock()
            .expect("mutation backend lock")
            .push(Operation::Delete(name.clone()));
        if let Some(kind) = self
            .delete_failures
            .lock()
            .expect("mutation backend lock")
            .get(name)
            .copied()
        {
            if kind == BackendErrorKind::NotFound {
                self.objects
                    .lock()
                    .expect("mutation backend lock")
                    .remove(name);
            }
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(kind, "delete failed"),
            );
        }
        let removed = self
            .objects
            .lock()
            .expect("mutation backend lock")
            .remove(name)
            .is_some();
        if self
            .non_durable_deletes
            .lock()
            .expect("mutation backend lock")
            .contains_key(name)
        {
            return Ok(DeleteOutcome::from_removed(
                name.clone(),
                DeleteDurability::NonDurable,
                removed,
            ));
        }
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        Ok(self
            .objects
            .lock()
            .expect("mutation backend lock")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect())
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.operations
            .lock()
            .expect("mutation backend lock")
            .push(Operation::Metadata(name.clone()));
        if let Some(kind) = self
            .metadata_failures
            .lock()
            .expect("mutation backend lock")
            .get(name)
            .copied()
        {
            return Err(BackendError::new(kind, "metadata failed"));
        }
        if let Some((_, size)) = self
            .metadata_override
            .lock()
            .expect("mutation backend lock")
            .as_ref()
            .filter(|(object, _)| object == name)
        {
            return Ok(BackendMetadata::new(*size, None));
        }
        self.objects
            .lock()
            .expect("mutation backend lock")
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
        self.operations
            .lock()
            .expect("mutation backend lock")
            .push(Operation::Publish(name.clone(), mode));
        if let Some((kind, visible)) = self
            .publish_failures
            .lock()
            .expect("mutation backend lock")
            .get(name)
            .copied()
        {
            if visible {
                self.objects
                    .lock()
                    .expect("mutation backend lock")
                    .insert(name.clone(), bytes.to_vec());
            }
            return Err(PublishError::new(
                name.clone(),
                kind,
                BackendError::new(BackendErrorKind::Interrupted, "publish failed"),
            ));
        }
        if mode == PublishMode::Create
            && self
                .objects
                .lock()
                .expect("mutation backend lock")
                .contains_key(name)
        {
            return Err(PublishError::precondition_failed(
                name,
                "object already exists",
            ));
        }
        self.objects
            .lock()
            .expect("mutation backend lock")
            .insert(name.clone(), bytes.to_vec());
        Ok(PublishOutcome::new(
            name.clone(),
            BackendMetadata::new(bytes.len() as u64, None),
            PublishDurability::Durable,
        ))
    }

    // #3721: models a hard link — `to` gets `from`'s bytes with no read of
    // them recorded, create-mode no-clobber, and injectable failures.
    fn link_object(
        &self,
        from: &ObjectName,
        to: &ObjectName,
    ) -> Result<PublishOutcome, PublishError> {
        self.operations
            .lock()
            .expect("mutation backend lock")
            .push(Operation::Link(from.clone(), to.clone()));
        let mut objects = self.objects.lock().expect("mutation backend lock");
        let Some(bytes) = objects.get(from).cloned() else {
            return Err(PublishError::new(
                to.clone(),
                PublishFailureKind::FailedBeforeVisibility,
                BackendError::new(BackendErrorKind::NotFound, "link source not found"),
            ));
        };
        if let Some((kind, source, visible)) = self
            .link_failures
            .lock()
            .expect("mutation backend lock")
            .get(to)
            .copied()
        {
            if visible {
                objects.insert(to.clone(), bytes);
            }
            return Err(PublishError::new(
                to.clone(),
                kind,
                BackendError::new(source, "link failed"),
            ));
        }
        if objects.contains_key(to) {
            return Err(PublishError::precondition_failed(
                to,
                "object already exists",
            ));
        }
        let size = bytes.len() as u64;
        objects.insert(to.clone(), bytes);
        Ok(PublishOutcome::new(
            to.clone(),
            BackendMetadata::new(size, None),
            PublishDurability::Durable,
        ))
    }
}

fn request(
    branch_id: BranchId,
    object_id: &str,
    source_object: ObjectName,
) -> QuarantineObjectRequest {
    QuarantineObjectRequest::new(
        branch_id,
        DATABASE_ID,
        CODEC_ID,
        object_id,
        source_object,
        Timestamp::from_micros(1_700_000_000_000_000),
        QuarantineGate::Safe,
    )
}

fn quarantine_object(branch_id: BranchId, object_id: &str) -> ObjectName {
    ObjectLayout::quarantine_object(&branch_id.to_string(), object_id).expect("quarantine object")
}

fn source_object() -> ObjectName {
    table_source_object("table0002")
}

const fn publish_failure_is_uncertain(kind: PublishFailureKind) -> bool {
    matches!(
        kind,
        PublishFailureKind::VisibilityUnknown | PublishFailureKind::VisibleDurabilityUnconfirmed
    )
}

fn mutation_capabilities_without(missing: BackendCapability) -> BackendCapabilities {
    BackendCapabilities::from_slice(
        &[
            BackendCapability::ReadObject,
            BackendCapability::ObjectMetadata,
            BackendCapability::DurablePublish,
            BackendCapability::DurableSync,
            BackendCapability::DeleteObject,
        ]
        .into_iter()
        .filter(|capability| *capability != missing)
        .collect::<Vec<_>>(),
    )
}

#[test]
fn quarantine_publishes_inventory_then_copy_then_deletes_source() {
    let branch_id = branch_id();
    let source_object = source_object();
    let source_bytes = b"table-bytes";
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable().with_object(source_object.clone(), source_bytes);
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("quarantine object");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
    assert_eq!(report.branch_id(), branch_id);
    assert_eq!(report.object_id(), "table0002");
    assert_eq!(report.source_object(), &source_object);
    assert_eq!(report.quarantine_object(), &quarantine_object);
    assert_eq!(report.byte_count(), source_bytes.len() as u64);
    assert_eq!(report.entry_count(), 1);
    let source_delete = report.source_delete().expect("delete");
    assert!(source_delete.deleted_flag());
    assert_eq!(
        source_delete
            .outcome()
            .expect("source delete outcome")
            .durability(),
        DeleteDurability::Durable
    );
    assert_eq!(backend.read_count(&source_object), 1);
    assert_eq!(backend.read_count(&quarantine_object), 0);
    assert!(!backend.contains(&source_object));
    assert_eq!(backend.bytes(&quarantine_object), source_bytes);
    let loaded = service
        .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect("load inventory");
    let entry = &loaded.inventory().entries()[0];
    assert_eq!(entry.source_object(), &source_object);
    assert_eq!(entry.byte_count(), source_bytes.len() as u64);

    let operations = backend.operations();
    assert_eq!(
        operations,
        vec![
            Operation::Metadata(quarantine_object.clone()),
            Operation::Metadata(source_object.clone()),
            Operation::Publish(inventory_object, PublishMode::Replace),
            Operation::Publish(quarantine_object, PublishMode::Create),
            Operation::Delete(source_object),
        ]
    );
}

#[test]
fn quarantine_non_durable_source_delete_is_health_debt() {
    let branch_id = branch_id();
    let source_object = source_object();
    let backend = MutationBackend::durable().with_object(source_object.clone(), b"table-bytes");
    backend.make_delete_non_durable(source_object.clone());
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("quarantine object");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleteFailed
    );
    let source_delete = report.source_delete().expect("source delete");
    assert!(!source_delete.deleted_flag());
    assert_eq!(
        source_delete.delete_error().expect("delete error").kind(),
        crate::backend::DeleteFailureKind::RemovedDurabilityUnconfirmed
    );
    assert!(!backend.contains(&source_object));
}

#[test]
fn quarantine_rejects_each_missing_mutation_capability_before_backend_access() {
    let branch_id = branch_id();
    for missing in [
        BackendCapability::ReadObject,
        BackendCapability::ObjectMetadata,
        BackendCapability::DurablePublish,
        BackendCapability::DurableSync,
        BackendCapability::DeleteObject,
    ] {
        let source_object = source_object();
        let inventory_object = inventory_object(branch_id);
        let quarantine_object = quarantine_object(branch_id, "table0002");
        let mut backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
        backend.capabilities = mutation_capabilities_without(missing);
        let service = QuarantineService::new(&backend);

        assert_eq!(
            service.quarantine_object(&request(branch_id, "table0002", source_object.clone())),
            Err(QuarantineServiceError::UnsupportedCapability {
                capability: missing,
            })
        );
        assert_eq!(backend.read_count(&source_object), 0);
        assert!(!backend.contains(&inventory_object));
        assert!(!backend.contains(&quarantine_object));
        assert!(backend.operations().is_empty());
    }
}

#[test]
fn quarantine_rejects_reserved_or_malformed_object_id_before_backend_access() {
    let branch_id = branch_id();
    let inventory_object = inventory_object(branch_id);
    let reserved_object_id = inventory_object
        .as_str()
        .rsplit('/')
        .next()
        .expect("inventory object id");
    for object_id in [reserved_object_id, "bad/object"] {
        let source_object = source_object();
        let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
        let service = QuarantineService::new(&backend);

        assert!(matches!(
            service.quarantine_object(&request(branch_id, object_id, source_object.clone())),
            Err(
                QuarantineServiceError::InvalidRequest { field: "object_id" }
                    | QuarantineServiceError::Layout { .. }
            )
        ));
        assert_eq!(backend.read_count(&source_object), 0);
        assert!(backend.operations().is_empty());
    }
}

#[test]
fn quarantine_missing_source_fails_before_mutation() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable();
    let service = QuarantineService::new(&backend);

    assert_eq!(
        service.quarantine_object(&request(branch_id, "table0002", source_object.clone())),
        Err(QuarantineServiceError::Missing {
            object: source_object.clone()
        })
    );
    assert!(!backend.contains(&source_object));
    assert!(!backend.contains(&quarantine_object));
    assert!(!backend.contains(&inventory_object));
    assert_eq!(
        backend.operations(),
        vec![Operation::Metadata(quarantine_object)]
    );
}

#[test]
fn quarantine_source_read_failure_fails_before_mutation() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
    backend.fail_read(source_object.clone(), BackendErrorKind::Unavailable);
    let service = QuarantineService::new(&backend);

    assert!(matches!(
        service.quarantine_object(&request(branch_id, "table0002", source_object.clone())),
        Err(QuarantineServiceError::Read { object, source })
            if object == source_object && source.kind() == BackendErrorKind::Unavailable
    ));
    assert!(!backend.contains(&quarantine_object));
    assert!(!backend.contains(&inventory_object));
    assert_eq!(
        backend.operations(),
        vec![Operation::Metadata(quarantine_object)]
    );
}

#[test]
fn quarantine_corrupt_inventory_fails_before_mutation() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable()
        .with_object(source_object.clone(), b"table")
        .with_object(inventory_object.clone(), b"not an inventory");
    let service = QuarantineService::new(&backend);

    assert!(matches!(
        service.quarantine_object(&request(branch_id, "table0002", source_object.clone())),
        Err(QuarantineServiceError::Decode { object, .. }) if object == inventory_object
    ));
    assert!(backend.contains(&source_object));
    assert!(!backend.contains(&quarantine_object));
    assert!(backend.operations().is_empty());
}

#[test]
fn quarantine_metadata_failure_fails_before_mutation() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
    backend.fail_metadata(source_object.clone(), BackendErrorKind::Unavailable);
    let service = QuarantineService::new(&backend);

    assert!(matches!(
        service.quarantine_object(&request(branch_id, "table0002", source_object.clone())),
        Err(QuarantineServiceError::Metadata { object, source })
            if object == source_object && source.kind() == BackendErrorKind::Unavailable
    ));
    assert!(backend.contains(&source_object));
    assert!(!backend.contains(&quarantine_object));
    assert!(!backend.contains(&inventory_object));
    assert_eq!(
        backend.operations(),
        vec![
            Operation::Metadata(quarantine_object),
            Operation::Metadata(source_object),
        ]
    );
}

#[test]
fn quarantine_unsafe_gate_fails_before_backend_access() {
    let branch_id = branch_id();
    for gate in [
        QuarantineGate::Referenced,
        QuarantineGate::UnsafeRecovery,
        QuarantineGate::ProofIncomplete,
    ] {
        let source_object = source_object();
        let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
        let service = QuarantineService::new(&backend);
        let request = QuarantineObjectRequest::new(
            branch_id,
            DATABASE_ID,
            CODEC_ID,
            "table0002",
            source_object,
            Timestamp::from_micros(1),
            gate,
        );

        assert_eq!(
            service.quarantine_object(&request),
            Err(QuarantineServiceError::UnsafeGate { gate })
        );
        assert!(backend.operations().is_empty());
    }
}

#[test]
fn quarantine_epoch_timestamp_requires_explicit_request_flag() {
    let branch_id = branch_id();
    let source_object = source_object();
    let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
    let service = QuarantineService::new(&backend);
    let request = QuarantineObjectRequest::new(
        branch_id,
        DATABASE_ID,
        CODEC_ID,
        "table0002",
        source_object.clone(),
        Timestamp::EPOCH,
        QuarantineGate::Safe,
    );

    assert_eq!(request.branch_id(), branch_id);
    assert_eq!(request.object_id(), "table0002");
    assert_eq!(request.source_object(), &source_object);
    assert_eq!(
        service.quarantine_object(&request),
        Err(QuarantineServiceError::InvalidRequest {
            field: "quarantined_at",
        })
    );

    let report = service
        .quarantine_object(&request.allow_epoch_timestamp())
        .expect("epoch timestamp explicitly allowed");
    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
}

#[test]
fn quarantine_inventory_publish_failure_does_not_copy_or_delete_source() {
    let branch_id = branch_id();
    for kind in ALL_PUBLISH_FAILURE_KINDS {
        let source_object = source_object();
        let quarantine_object = quarantine_object(branch_id, "table0002");
        let inventory_object = inventory_object(branch_id);
        let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
        let visible = kind == PublishFailureKind::VisibleDurabilityUnconfirmed;
        backend.fail_publish(inventory_object.clone(), kind, visible);
        let service = QuarantineService::new(&backend);

        let report = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("publish failure report");

        let expected_status = if publish_failure_is_uncertain(kind) {
            QuarantineObjectStatus::InventoryPublishUncertain
        } else {
            QuarantineObjectStatus::InventoryPublishFailed
        };
        assert_eq!(report.status(), expected_status);
        let failure = report
            .inventory_publish_failure()
            .expect("inventory publish failure");
        assert_eq!(failure.object(), &inventory_object);
        assert_eq!(failure.source().kind(), kind);
        assert!(backend.contains(&source_object));
        assert!(!backend.contains(&quarantine_object));
        assert_eq!(backend.contains(&inventory_object), visible);
        assert_eq!(
            backend.operations(),
            vec![
                Operation::Metadata(quarantine_object),
                Operation::Metadata(source_object),
                Operation::Publish(inventory_object, PublishMode::Replace),
            ]
        );
    }
}

#[test]
fn quarantine_inventory_visibility_unknown_may_leave_inventory_visible_without_copy() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
    backend.fail_publish(
        inventory_object.clone(),
        PublishFailureKind::VisibilityUnknown,
        true,
    );
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("inventory publish uncertainty report");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::InventoryPublishUncertain
    );
    assert!(report.inventory_write().is_none());
    assert_eq!(
        report
            .inventory_publish_failure()
            .expect("inventory publish failure")
            .source()
            .kind(),
        PublishFailureKind::VisibilityUnknown
    );
    assert!(backend.contains(&source_object));
    assert!(backend.contains(&inventory_object));
    assert!(!backend.contains(&quarantine_object));
    assert_eq!(
        service
            .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
            .expect("load visible inventory")
            .inventory()
            .entries()
            .len(),
        1
    );
    assert_eq!(
        backend.operations(),
        vec![
            Operation::Metadata(quarantine_object),
            Operation::Metadata(source_object),
            Operation::Publish(inventory_object, PublishMode::Replace),
        ]
    );
}

#[test]
fn quarantine_copy_publish_failure_leaves_source_and_published_inventory() {
    let branch_id = branch_id();
    for kind in [
        PublishFailureKind::Unsupported,
        PublishFailureKind::PreconditionFailed,
        PublishFailureKind::FailedBeforeVisibility,
    ] {
        let source_object = source_object();
        let quarantine_object = quarantine_object(branch_id, "table0002");
        let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
        backend.fail_publish(quarantine_object.clone(), kind, false);
        let service = QuarantineService::new(&backend);

        let report = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("copy failure report");

        assert_eq!(
            report.status(),
            QuarantineObjectStatus::QuarantinePublishFailed
        );
        assert!(report.inventory_write().is_some());
        assert_eq!(
            report
                .quarantine_publish_failure()
                .expect("copy publish failure")
                .object(),
            &quarantine_object
        );
        assert_eq!(
            report
                .quarantine_publish_failure()
                .expect("copy publish failure")
                .source()
                .kind(),
            kind
        );
        assert!(backend.contains(&source_object));
        assert!(!backend.contains(&quarantine_object));
        assert_eq!(
            service
                .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
                .expect("load inventory")
                .inventory()
                .entries()
                .len(),
            1
        );
        let reconciliation = service
            .reconcile_branch_quarantine(branch_id, DATABASE_ID, CODEC_ID)
            .expect("reconcile after missing copy");
        assert_eq!(
            reconciliation.kind(),
            QuarantineReconciliationKind::MissingQuarantineObject
        );
        assert_eq!(reconciliation.missing_objects().len(), 1);
        assert_eq!(
            reconciliation.missing_objects()[0].object(),
            &quarantine_object
        );
    }
}

#[test]
fn quarantine_copy_publish_uncertainty_keeps_source_for_reconciliation() {
    let branch_id = branch_id();
    for (kind, visible) in [
        (PublishFailureKind::VisibilityUnknown, false),
        (PublishFailureKind::VisibilityUnknown, true),
        (PublishFailureKind::VisibleDurabilityUnconfirmed, true),
    ] {
        let source_object = source_object();
        let quarantine_object = quarantine_object(branch_id, "table0002");
        let backend = MutationBackend::durable().with_object(source_object.clone(), b"table");
        backend.fail_publish(quarantine_object.clone(), kind, visible);
        let service = QuarantineService::new(&backend);

        let report = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("copy uncertainty report");

        assert_eq!(
            report.status(),
            QuarantineObjectStatus::QuarantinePublishUncertain
        );
        assert_eq!(
            report
                .quarantine_publish_failure()
                .expect("copy publish failure")
                .source()
                .kind(),
            kind
        );
        assert!(backend.contains(&source_object));
        assert_eq!(backend.contains(&quarantine_object), visible);
        assert!(backend
            .operations()
            .iter()
            .all(|operation| !matches!(operation, Operation::Delete(_))));
        let reconciliation = service
            .reconcile_branch_quarantine(branch_id, DATABASE_ID, CODEC_ID)
            .expect("reconcile copy uncertainty");
        if visible {
            assert_eq!(
                reconciliation.kind(),
                QuarantineReconciliationKind::CleanInventory
            );
            assert_eq!(reconciliation.listed_objects().len(), 1);
        } else {
            assert_eq!(
                reconciliation.kind(),
                QuarantineReconciliationKind::MissingQuarantineObject
            );
            assert_eq!(reconciliation.missing_objects().len(), 1);
        }
    }
}
