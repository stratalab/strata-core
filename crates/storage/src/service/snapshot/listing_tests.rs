use super::{SnapshotObject, SnapshotService, SnapshotServiceError};
use crate::backend::{
    memory::MemoryBackend, Backend, BackendCapabilities, BackendCapability, BackendError,
    BackendErrorKind, BackendMetadata, BackendRange, BackendResult, DeleteDurability,
    DeleteFailureKind, DeleteOutcome, DeleteResult, DeleteStatus,
    BASIC_OBJECT_BACKEND_CAPABILITIES,
};
use crate::layout::{ObjectFamily, ObjectLayout};
use crate::object::{ObjectName, ObjectPrefix};
use std::collections::BTreeSet;
use std::sync::Mutex;

#[test]
fn snapshot_list_returns_empty_when_family_is_absent() {
    let backend = MemoryBackend::new();
    let service = SnapshotService::new(&backend);

    let snapshots = service.list_snapshots().expect("list snapshots");

    assert!(snapshots.is_empty());
}

#[test]
fn snapshot_list_sorts_by_numeric_snapshot_id() {
    let backend = MemoryBackend::new();
    write_placeholder_snapshot(&backend, 10);
    write_placeholder_snapshot(&backend, 1);
    write_placeholder_snapshot(&backend, 3);
    let service = SnapshotService::new(&backend);

    let snapshots = service.list_snapshots().expect("list snapshots");

    assert_snapshot_ids(&snapshots, &[1, 3, 10]);
}

#[test]
fn latest_snapshot_returns_highest_listed_snapshot_id() {
    let backend = MemoryBackend::new();
    write_placeholder_snapshot(&backend, 2);
    write_placeholder_snapshot(&backend, 9);
    write_placeholder_snapshot(&backend, 4);
    let service = SnapshotService::new(&backend);

    let latest = service
        .latest_snapshot()
        .expect("latest snapshot")
        .expect("snapshot exists");

    assert_eq!(latest.snapshot_id(), 9);
    assert_eq!(latest.object(), &snapshot_object(9));
}

#[test]
fn latest_snapshot_returns_none_when_family_is_absent() {
    let backend = MemoryBackend::new();
    let service = SnapshotService::new(&backend);

    let latest = service.latest_snapshot().expect("latest snapshot");

    assert!(latest.is_none());
}

#[test]
fn snapshot_list_rejects_malformed_snapshot_family_objects() {
    let malformed = [
        snapshot_raw("000000000000000A"),
        snapshot_raw("0001"),
        snapshot_raw("00000000000000001"),
        snapshot_raw("000000000000000g"),
        format!(
            "{}/{}/part",
            ObjectFamily::Snapshots.as_str(),
            "0000000000000001"
        ),
        snapshot_raw("0000000000000000"),
    ];

    for raw in malformed {
        let object = object_name(raw);
        let backend = ListingBackend::with_names(vec![object.clone()]);
        let service = SnapshotService::new(&backend);

        let error = service
            .list_snapshots()
            .expect_err("malformed snapshot object rejected");

        match error {
            SnapshotServiceError::InvalidListedObject {
                object: actual,
                source,
            } => {
                assert_eq!(actual, object);
                assert_eq!(source.kind(), BackendErrorKind::InvalidObjectName);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

#[test]
fn snapshot_list_ignores_objects_outside_snapshot_family() {
    let backend = ListingBackend::with_names(vec![
        object_name(format!(
            "{}2/{}",
            ObjectFamily::Snapshots.as_str(),
            "0000000000000001"
        )),
        family_object(ObjectFamily::Wal, "0000000000000001"),
        snapshot_object(2),
    ]);
    let service = SnapshotService::new(&backend);

    let snapshots = service.list_snapshots().expect("list snapshots");

    assert_snapshot_ids(&snapshots, &[2]);
}

#[test]
fn snapshot_list_failure_returns_typed_error_without_reads() {
    let backend = ListingBackend::with_list_error(BackendError::new(
        BackendErrorKind::Unavailable,
        "list failed",
    ));
    let service = SnapshotService::new(&backend);

    let error = service.list_snapshots().expect_err("list failure");

    match error {
        SnapshotServiceError::List { source } => {
            assert_eq!(source.kind(), BackendErrorKind::Unavailable);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(backend.read_count(), 0);
}

#[test]
fn snapshot_list_requires_list_capability_before_parsing() {
    let malformed = object_name(snapshot_raw("000000000000000g"));
    let backend = ListingBackend::without_list_capability(vec![malformed]);
    let service = SnapshotService::new(&backend);

    let error = service
        .list_snapshots()
        .expect_err("missing list capability");

    match error {
        SnapshotServiceError::UnsupportedCapability { capability } => {
            assert_eq!(capability, BackendCapability::ListPrefix);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(backend.list_count(), 0);
    assert_eq!(backend.read_count(), 0);
}

#[test]
fn prune_snapshots_list_failure_returns_typed_error_without_deletes() {
    let backend = ListingBackend::with_list_error(BackendError::new(
        BackendErrorKind::Unavailable,
        "list failed",
    ));
    let service = SnapshotService::new(&backend);

    let error = service.prune_snapshots(None, 1).expect_err("list failure");

    match error {
        SnapshotServiceError::List { source } => {
            assert_eq!(source.kind(), BackendErrorKind::Unavailable);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(backend.deleted_objects().is_empty());
}

#[test]
fn prune_snapshots_rejects_malformed_snapshot_family_object_before_delete() {
    let malformed = object_name(snapshot_raw("000000000000000g"));
    let backend = ListingBackend::with_names(vec![snapshot_object(1), malformed.clone()]);
    let service = SnapshotService::new(&backend);

    let error = service
        .prune_snapshots(None, 1)
        .expect_err("malformed snapshot object rejected");

    match error {
        SnapshotServiceError::InvalidListedObject { object, source } => {
            assert_eq!(object, malformed);
            assert_eq!(source.kind(), BackendErrorKind::InvalidObjectName);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(backend.deleted_objects().is_empty());
}

#[test]
fn prune_snapshots_protects_live_snapshot_and_newest_retained() {
    let backend = DurableMemoryBackend::new();
    for snapshot_id in 1..=5 {
        write_placeholder_snapshot(&backend, snapshot_id);
    }
    let service = SnapshotService::new(&backend);

    let report = service
        .prune_snapshots(Some(2), 2)
        .expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[1, 3]);
    assert_snapshot_ids(report.protected(), &[2, 4, 5]);
    assert!(report.failed().is_empty());
    assert_missing(&backend, 1);
    assert_missing(&backend, 3);
    assert_present(&backend, 2);
    assert_present(&backend, 4);
    assert_present(&backend, 5);
}

#[test]
fn prune_snapshots_clamps_retain_newest_to_one() {
    let backend = DurableMemoryBackend::new();
    write_placeholder_snapshot(&backend, 1);
    write_placeholder_snapshot(&backend, 2);
    let service = SnapshotService::new(&backend);

    let report = service.prune_snapshots(None, 0).expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[1]);
    assert_snapshot_ids(report.protected(), &[2]);
    assert!(report.failed().is_empty());
}

#[test]
fn prune_snapshots_reports_non_durable_cache_delete_as_health_debt() {
    let backend = MemoryBackend::new();
    write_placeholder_snapshot(&backend, 1);
    write_placeholder_snapshot(&backend, 2);
    let service = SnapshotService::new(&backend);

    let report = service.prune_snapshots(None, 1).expect("prune snapshots");

    assert!(report.deleted().is_empty());
    assert!(report.delete_outcomes().is_empty());
    assert_snapshot_ids(report.protected(), &[2]);
    assert_eq!(report.failed().len(), 1);
    assert_eq!(report.failed()[0].snapshot().snapshot_id(), 1);
    assert_eq!(
        report.failed()[0].delete_error().kind(),
        DeleteFailureKind::RemovedDurabilityUnconfirmed
    );
    assert_missing(&backend, 1);
    assert_present(&backend, 2);
}

#[test]
fn prune_snapshots_empty_family_reports_no_work() {
    let backend = MemoryBackend::new();
    let service = SnapshotService::new(&backend);

    let report = service.prune_snapshots(None, 1).expect("prune snapshots");

    assert!(report.deleted().is_empty());
    assert!(report.protected().is_empty());
    assert!(report.failed().is_empty());
}

#[test]
fn prune_snapshots_at_or_below_retain_count_deletes_nothing() {
    let backend = MemoryBackend::new();
    write_placeholder_snapshot(&backend, 3);
    write_placeholder_snapshot(&backend, 1);
    write_placeholder_snapshot(&backend, 2);
    let service = SnapshotService::new(&backend);

    let report = service.prune_snapshots(None, 3).expect("prune snapshots");

    assert!(report.deleted().is_empty());
    assert_snapshot_ids(report.protected(), &[1, 2, 3]);
    assert!(report.failed().is_empty());
    assert_present(&backend, 1);
    assert_present(&backend, 2);
    assert_present(&backend, 3);
}

#[test]
fn prune_snapshots_reports_are_sorted_under_unsorted_backend_listing() {
    let failing_one = snapshot_object(1);
    let failing_three = snapshot_object(3);
    let backend = ListingBackend::with_delete_failures(
        vec![
            snapshot_object(4),
            failing_three.clone(),
            snapshot_object(5),
            failing_one.clone(),
            snapshot_object(2),
        ],
        vec![failing_three, failing_one],
    );
    let service = SnapshotService::new(&backend);

    let report = service
        .prune_snapshots(Some(2), 1)
        .expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[4]);
    assert_snapshot_ids(report.protected(), &[2, 5]);
    assert_failed_snapshot_ids(report.failed(), &[1, 3]);
    assert_eq!(backend.deleted_objects(), vec![snapshot_object(4)]);
}

#[test]
fn prune_snapshots_never_deletes_non_snapshot_family_objects() {
    let outside_snapshot_family = object_name(format!(
        "{}2/{}",
        ObjectFamily::Snapshots.as_str(),
        "0000000000000001"
    ));
    let wal_object = family_object(ObjectFamily::Wal, "0000000000000001");
    let backend = ListingBackend::with_names(vec![
        outside_snapshot_family,
        snapshot_object(1),
        wal_object,
        snapshot_object(2),
    ]);
    let service = SnapshotService::new(&backend);

    let report = service.prune_snapshots(None, 1).expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[1]);
    assert_snapshot_ids(report.protected(), &[2]);
    assert!(report.failed().is_empty());
    assert_eq!(backend.deleted_objects(), vec![snapshot_object(1)]);
}

#[test]
fn prune_snapshots_reports_delete_failures_without_hiding_successes() {
    let failing = snapshot_object(1);
    let backend = ListingBackend::with_delete_failures(
        vec![failing.clone(), snapshot_object(2), snapshot_object(3)],
        vec![failing],
    );
    let service = SnapshotService::new(&backend);

    let report = service.prune_snapshots(None, 1).expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[2]);
    assert_eq!(report.delete_outcomes().len(), 1);
    assert_eq!(report.delete_outcomes()[0].snapshot().snapshot_id(), 2);
    assert_eq!(
        report.delete_outcomes()[0].outcome().object(),
        &snapshot_object(2)
    );
    assert_snapshot_ids(report.protected(), &[3]);
    assert_eq!(report.failed().len(), 1);
    assert_eq!(report.failed()[0].snapshot().snapshot_id(), 1);
    assert_eq!(
        report.failed()[0].source().kind(),
        BackendErrorKind::Unavailable
    );
    assert_eq!(
        report.failed()[0].delete_error().object(),
        &snapshot_object(1)
    );
    assert_eq!(backend.deleted_objects(), vec![snapshot_object(2)]);
}

#[test]
fn prune_snapshots_rejects_zero_live_snapshot_before_listing() {
    let backend = ListingBackend::with_names(vec![snapshot_object(1)]);
    let service = SnapshotService::new(&backend);

    let error = service
        .prune_snapshots(Some(0), 1)
        .expect_err("zero live snapshot rejected");

    match error {
        SnapshotServiceError::InvalidSnapshotFact { snapshot_id, field } => {
            assert_eq!(snapshot_id, 0);
            assert_eq!(field, "snapshot_id");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(backend.list_count(), 0);
}

#[test]
fn prune_snapshots_requires_delete_capability_before_listing() {
    let backend = ListingBackend::without_delete_capability(vec![snapshot_object(1)]);
    let service = SnapshotService::new(&backend);

    let error = service
        .prune_snapshots(None, 1)
        .expect_err("missing delete capability");

    match error {
        SnapshotServiceError::UnsupportedCapability { capability } => {
            assert_eq!(capability, BackendCapability::DeleteObject);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(backend.list_count(), 0);
}

#[derive(Debug)]
struct ListingBackend {
    names: Vec<ObjectName>,
    list_error: Option<BackendError>,
    delete_failures: BTreeSet<ObjectName>,
    deleted: Mutex<Vec<ObjectName>>,
    reads: Mutex<u64>,
    lists: Mutex<u64>,
    capabilities: BackendCapabilities,
}

#[derive(Debug, Default)]
struct DurableMemoryBackend {
    inner: MemoryBackend,
}

impl DurableMemoryBackend {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
        }
    }
}

impl Backend for DurableMemoryBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.inner.read_object(name)
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.inner.read_range(name, range)
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.inner.write_object(name, bytes)
    }

    fn delete_object(&self, name: &ObjectName) -> DeleteResult {
        let outcome = self.inner.delete_object(name)?;
        match outcome.status() {
            DeleteStatus::Deleted => Ok(DeleteOutcome::deleted(
                outcome.object().clone(),
                DeleteDurability::Durable,
            )),
            DeleteStatus::AlreadyMissing => Ok(outcome),
        }
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.inner.list_prefix(prefix)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.inner.object_metadata(name)
    }
}

impl ListingBackend {
    fn with_names(names: Vec<ObjectName>) -> Self {
        Self::new(
            names,
            None,
            Vec::new(),
            BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES),
        )
    }

    fn with_list_error(source: BackendError) -> Self {
        Self::new(
            Vec::new(),
            Some(source),
            Vec::new(),
            BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES),
        )
    }

    fn with_delete_failures(names: Vec<ObjectName>, delete_failures: Vec<ObjectName>) -> Self {
        Self::new(
            names,
            None,
            delete_failures,
            BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES),
        )
    }

    fn without_delete_capability(names: Vec<ObjectName>) -> Self {
        let capabilities = BackendCapabilities::from_slice(&[
            BackendCapability::ReadObject,
            BackendCapability::ReadRange,
            BackendCapability::WriteObject,
            BackendCapability::ListPrefix,
            BackendCapability::ObjectMetadata,
        ]);
        Self::new(names, None, Vec::new(), capabilities)
    }

    fn without_list_capability(names: Vec<ObjectName>) -> Self {
        let capabilities = BackendCapabilities::from_slice(&[
            BackendCapability::ReadObject,
            BackendCapability::ReadRange,
            BackendCapability::WriteObject,
            BackendCapability::DeleteObject,
            BackendCapability::ObjectMetadata,
        ]);
        Self::new(names, None, Vec::new(), capabilities)
    }

    fn new(
        names: Vec<ObjectName>,
        list_error: Option<BackendError>,
        delete_failures: Vec<ObjectName>,
        capabilities: BackendCapabilities,
    ) -> Self {
        Self {
            names,
            list_error,
            delete_failures: delete_failures.into_iter().collect(),
            deleted: Mutex::new(Vec::new()),
            reads: Mutex::new(0),
            lists: Mutex::new(0),
            capabilities,
        }
    }

    fn deleted_objects(&self) -> Vec<ObjectName> {
        self.deleted.lock().expect("deleted lock").clone()
    }

    fn read_count(&self) -> u64 {
        *self.reads.lock().expect("reads lock")
    }

    fn list_count(&self) -> u64 {
        *self.lists.lock().expect("lists lock")
    }
}

impl Backend for ListingBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities
    }

    fn read_object(&self, _name: &ObjectName) -> BackendResult<Vec<u8>> {
        *self.reads.lock().expect("reads lock") += 1;
        Err(BackendError::new(BackendErrorKind::NotFound, "not found"))
    }

    fn read_range(&self, _name: &ObjectName, _range: BackendRange) -> BackendResult<Vec<u8>> {
        Err(BackendError::new(
            BackendErrorKind::UnsupportedOperation,
            "read range not implemented",
        ))
    }

    fn write_object(&self, _name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        if self.delete_failures.contains(name) {
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(BackendErrorKind::Unavailable, "delete failed"),
            );
        }
        self.deleted
            .lock()
            .expect("deleted lock")
            .push(name.clone());
        crate::backend::durable_delete_result(name, true)
    }

    fn list_prefix(&self, _prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        *self.lists.lock().expect("lists lock") += 1;
        match &self.list_error {
            Some(source) => Err(source.clone()),
            None => Ok(self.names.clone()),
        }
    }

    fn object_metadata(&self, _name: &ObjectName) -> BackendResult<BackendMetadata> {
        Err(BackendError::new(BackendErrorKind::NotFound, "not found"))
    }
}

fn write_placeholder_snapshot(backend: &dyn Backend, snapshot_id: u64) {
    backend
        .write_object(&snapshot_object(snapshot_id), b"snapshot")
        .expect("write snapshot placeholder");
}

fn assert_present(backend: &dyn Backend, snapshot_id: u64) {
    backend
        .read_object(&snapshot_object(snapshot_id))
        .expect("snapshot should remain");
}

fn assert_missing(backend: &dyn Backend, snapshot_id: u64) {
    let error = backend
        .read_object(&snapshot_object(snapshot_id))
        .expect_err("snapshot should be deleted");
    assert_eq!(error.kind(), BackendErrorKind::NotFound);
}

fn assert_snapshot_ids(snapshots: &[SnapshotObject], expected: &[u64]) {
    let actual: Vec<u64> = snapshots.iter().map(SnapshotObject::snapshot_id).collect();
    assert_eq!(actual, expected);
}

fn assert_failed_snapshot_ids(failures: &[super::SnapshotDeleteFailure], expected: &[u64]) {
    let actual: Vec<u64> = failures
        .iter()
        .map(|failure| failure.snapshot().snapshot_id())
        .collect();
    assert_eq!(actual, expected);
}

fn snapshot_object(snapshot_id: u64) -> ObjectName {
    ObjectLayout::snapshot(snapshot_id).expect("snapshot object")
}

fn object_name(raw: impl Into<String>) -> ObjectName {
    ObjectName::new(raw).expect("object name")
}

fn snapshot_raw(component: &str) -> String {
    format!("{}/{}", ObjectFamily::Snapshots.as_str(), component)
}

fn family_object(family: ObjectFamily, component: &str) -> ObjectName {
    object_name(format!("{}/{}", family.as_str(), component))
}

// Space-reclamation contract §3.4 (slice 5, #3592): the proof-driven modes.

#[test]
fn prune_snapshots_with_mode_superseded_deletes_below_the_live_id_only() {
    use super::SnapshotPruneMode;

    let backend = DurableMemoryBackend::new();
    for snapshot_id in [1, 2, 3, 5] {
        write_placeholder_snapshot(&backend, snapshot_id);
    }
    let service = SnapshotService::new(&backend);

    let report = service
        .prune_snapshots_with_mode(Some(3), SnapshotPruneMode::Superseded, 1)
        .expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[1, 2]);
    assert_snapshot_ids(report.protected(), &[3, 5]);
    assert!(report.failed().is_empty());
    assert_missing(&backend, 1);
    assert_missing(&backend, 2);
    assert_present(&backend, 3);
    assert_present(&backend, 5);
}

#[test]
fn prune_snapshots_with_mode_reconcile_deletes_everything_but_the_attested_id() {
    use super::SnapshotPruneMode;

    let backend = DurableMemoryBackend::new();
    for snapshot_id in [1, 2, 3, 5] {
        write_placeholder_snapshot(&backend, snapshot_id);
    }
    let service = SnapshotService::new(&backend);

    let report = service
        .prune_snapshots_with_mode(Some(3), SnapshotPruneMode::ReconcileToAttested, 1)
        .expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[1, 2, 5]);
    assert_snapshot_ids(report.protected(), &[3]);
    assert!(report.failed().is_empty());
    for snapshot_id in [1, 2, 5] {
        assert_missing(&backend, snapshot_id);
    }
    assert_present(&backend, 3);
}

#[test]
fn prune_snapshots_proof_driven_modes_protect_everything_without_a_live_id() {
    use super::SnapshotPruneMode;

    for mode in [
        SnapshotPruneMode::Superseded,
        SnapshotPruneMode::ReconcileToAttested,
    ] {
        let backend = DurableMemoryBackend::new();
        for snapshot_id in [1, 2, 3] {
            write_placeholder_snapshot(&backend, snapshot_id);
        }
        let service = SnapshotService::new(&backend);

        let report = service
            .prune_snapshots_with_mode(None, mode, 1)
            .expect("prune snapshots");

        assert!(report.deleted().is_empty(), "{mode:?}");
        assert_snapshot_ids(report.protected(), &[1, 2, 3]);
        for snapshot_id in [1, 2, 3] {
            assert_present(&backend, snapshot_id);
        }
    }
}

#[test]
fn prune_snapshots_proof_driven_modes_ignore_the_newest_window() {
    use super::SnapshotPruneMode;

    let backend = DurableMemoryBackend::new();
    for snapshot_id in [1, 2, 3] {
        write_placeholder_snapshot(&backend, snapshot_id);
    }
    let service = SnapshotService::new(&backend);

    // A window wide enough to keep everything under the newest-window verb.
    let report = service
        .prune_snapshots_with_mode(Some(3), SnapshotPruneMode::Superseded, 10)
        .expect("prune snapshots");

    assert_snapshot_ids(report.deleted(), &[1, 2]);
    assert_snapshot_ids(report.protected(), &[3]);
}

#[test]
fn list_snapshot_sizes_reports_each_objects_bytes_in_id_order() {
    let backend = DurableMemoryBackend::new();
    write_placeholder_snapshot(&backend, 3);
    write_placeholder_snapshot(&backend, 1);
    let service = SnapshotService::new(&backend);

    let sizes = service.list_snapshot_sizes().expect("snapshot sizes");

    let ids: Vec<u64> = sizes
        .iter()
        .map(|(snapshot, _)| snapshot.snapshot_id())
        .collect();
    assert_eq!(ids, [1, 3]);
    for (snapshot, bytes) in &sizes {
        let stored = backend
            .read_object(snapshot.object())
            .expect("placeholder bytes");
        assert_eq!(*bytes, stored.len() as u64);
    }
}

#[test]
fn list_snapshot_sizes_drops_a_snapshot_that_vanishes_before_its_stat() {
    // Listed, but gone by the stat (a concurrent prune): dropped, not an error.
    let backend = ListingBackend::with_names(vec![snapshot_object(1), snapshot_object(2)]);
    let service = SnapshotService::new(&backend);

    let sizes = service
        .list_snapshot_sizes()
        .expect("a vanished snapshot is tolerated");

    assert!(sizes.is_empty());
}
