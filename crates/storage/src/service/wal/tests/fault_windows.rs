use super::*;
use crate::backend::{PublishError, PublishFailureKind};
use crate::format::FormatError;
use crate::service::wal::WalOperation;
use strata_core::CommitVersion;

enum AppendFault {
    VisiblePrefixThenError {
        prefix_len: usize,
        kind: BackendErrorKind,
    },
    VisiblePrefixThenWrongMetadata {
        prefix_len: usize,
    },
}

// This backend models fault windows the generic testkit cannot express: bytes
// can become visible before append_object returns an error or a misleading
// append report.
#[derive(Default)]
struct FaultWindowBackend {
    inner: StoredWalBackend,
    publish_failure: Mutex<Option<(PublishFailureKind, BackendErrorKind)>>,
    metadata_failure: Mutex<Option<BackendErrorKind>>,
    list_failure: Mutex<Option<BackendErrorKind>>,
    read_failure: Mutex<Option<BackendErrorKind>>,
    append_fault: Mutex<Option<AppendFault>>,
    delete_failures: Mutex<Vec<ObjectName>>,
}

impl FaultWindowBackend {
    fn new() -> Self {
        Self::default()
    }

    fn fail_next_publish(&self, kind: PublishFailureKind, source: BackendErrorKind) {
        *self.publish_failure.lock().expect("publish failure lock") = Some((kind, source));
    }

    fn fail_next_metadata(&self, kind: BackendErrorKind) {
        *self.metadata_failure.lock().expect("metadata failure lock") = Some(kind);
    }

    fn fail_next_list(&self, kind: BackendErrorKind) {
        *self.list_failure.lock().expect("list failure lock") = Some(kind);
    }

    fn fail_next_read(&self, kind: BackendErrorKind) {
        *self.read_failure.lock().expect("read failure lock") = Some(kind);
    }

    fn fail_delete_for(&self, object: &ObjectName) {
        self.delete_failures
            .lock()
            .expect("delete failures lock")
            .push(object.clone());
    }

    fn append_visible_prefix_then_error(&self, prefix_len: usize, kind: BackendErrorKind) {
        *self.append_fault.lock().expect("append fault lock") =
            Some(AppendFault::VisiblePrefixThenError { prefix_len, kind });
    }

    fn append_visible_prefix_then_wrong_metadata(&self, prefix_len: usize) {
        *self.append_fault.lock().expect("append fault lock") =
            Some(AppendFault::VisiblePrefixThenWrongMetadata { prefix_len });
    }

    fn stored_bytes(&self, object: &ObjectName) -> Vec<u8> {
        self.inner.read_object(object).expect("stored bytes")
    }

    fn listed_objects(&self) -> Vec<ObjectName> {
        self.inner.listed_objects()
    }
}

impl Backend for FaultWindowBackend {
    fn capabilities(&self) -> BackendCapabilities {
        let mut capabilities = self.inner.capabilities();
        capabilities.insert(BackendCapability::DeleteObject);
        capabilities
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        if let Some(kind) = self.read_failure.lock().expect("read failure lock").take() {
            return Err(BackendError::new(kind, "injected read failure"));
        }
        self.inner.read_object(name)
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.inner.read_range(name, range)
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.inner.write_object(name, bytes)
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        let mut failures = self.delete_failures.lock().expect("delete failures lock");
        if let Some(index) = failures.iter().position(|object| object == name) {
            failures.remove(index);
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(BackendErrorKind::Unavailable, "injected delete failure"),
            );
        }
        self.inner.delete_object(name)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        if let Some(kind) = self.list_failure.lock().expect("list failure lock").take() {
            return Err(BackendError::new(kind, "injected list failure"));
        }
        self.inner.list_prefix(prefix)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        if let Some(kind) = self
            .metadata_failure
            .lock()
            .expect("metadata failure lock")
            .take()
        {
            return Err(BackendError::new(kind, "injected metadata failure"));
        }
        self.inner.object_metadata(name)
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        if let Some(fault) = self.append_fault.lock().expect("append fault lock").take() {
            let current = self.inner.read_object(name)?;
            let start_offset = current.len() as u64;
            let prefix_len = match fault {
                AppendFault::VisiblePrefixThenError { prefix_len, .. }
                | AppendFault::VisiblePrefixThenWrongMetadata { prefix_len } => {
                    prefix_len.min(bytes.len())
                }
            };
            let mut visible = current;
            visible.extend_from_slice(&bytes[..prefix_len]);
            self.inner.write_object(name, &visible)?;
            // The visible prefix simulates a crash or backend fault after a
            // partial write. Service state must not assume the full frame was
            // accepted just because object bytes changed.
            return match fault {
                AppendFault::VisiblePrefixThenError { kind, .. } => {
                    Err(BackendError::new(kind, "partial append visible"))
                }
                AppendFault::VisiblePrefixThenWrongMetadata { .. } => Ok(BackendAppend::new(
                    start_offset,
                    bytes.len() as u64,
                    BackendMetadata::new(visible.len() as u64, None),
                )),
            };
        }
        self.inner.append_object(name, bytes)
    }

    fn sync_object(&self, name: &ObjectName) -> crate::backend::BackendResult<()> {
        self.inner.sync_object(name)
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        if let Some((kind, source_kind)) = self
            .publish_failure
            .lock()
            .expect("publish failure lock")
            .take()
        {
            return Err(PublishError::new(
                name.clone(),
                kind,
                BackendError::new(source_kind, "injected publish failure"),
            ));
        }
        self.inner.publish_object(name, bytes, mode)
    }
}

fn assert_backend_error(
    error: WalServiceError,
    operation: WalOperation,
    object: &ObjectName,
    kind: BackendErrorKind,
) {
    match error {
        WalServiceError::Backend {
            operation: actual_operation,
            object: actual_object,
            source,
        } => {
            assert_eq!(actual_operation, operation);
            assert_eq!(actual_object, *object);
            assert_eq!(source.kind(), kind);
        }
        other => panic!("expected WAL backend error, got {other:?}"),
    }
}

fn assert_publish_error(
    error: WalServiceError,
    object: &ObjectName,
    kind: PublishFailureKind,
    source_kind: BackendErrorKind,
) {
    match error {
        WalServiceError::Publish { operation, source } => {
            assert_eq!(operation, WalOperation::CreateSegment);
            assert_eq!(source.object(), object);
            assert_eq!(source.kind(), kind);
            assert_eq!(source.source_error().kind(), source_kind);
        }
        other => panic!("expected WAL publish error, got {other:?}"),
    }
}

fn assert_list_error(error: WalServiceError, kind: BackendErrorKind) {
    match error {
        WalServiceError::List { source } => assert_eq!(source.kind(), kind),
        other => panic!("expected WAL list error, got {other:?}"),
    }
}

fn assert_latest_tail(
    read: &super::super::WalRead,
    expected_records: &[WalRecord],
    expected_object_size: u64,
) {
    assert_eq!(read.records(), expected_records);
    let truncation = read.truncation().expect("truncation fact");
    assert_eq!(truncation.segment_id(), 1);
    assert_eq!(
        truncation.valid_end_offset(),
        WAL_SEGMENT_HEADER_SIZE as u64
    );
    assert_eq!(truncation.object_size(), expected_object_size);
}

#[test]
fn active_segment_create_publish_failure_returns_typed_publish_error() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    backend.fail_next_publish(
        PublishFailureKind::FailedBeforeVisibility,
        BackendErrorKind::Unavailable,
    );

    let result = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    );

    assert_publish_error(
        open_error(result, "create publish should fail"),
        &segment_one,
        PublishFailureKind::FailedBeforeVisibility,
        BackendErrorKind::Unavailable,
    );
    assert_eq!(
        backend
            .object_metadata(&segment_one)
            .expect_err("segment should not be visible")
            .kind(),
        BackendErrorKind::NotFound
    );
}

#[test]
fn metadata_failure_before_append_returns_typed_error_without_writing() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    let before = backend.stored_bytes(&segment_one);
    backend.fail_next_metadata(BackendErrorKind::MetadataMismatch);

    let error = service
        .append(&record(1, b"metadata fault".to_vec()))
        .expect_err("metadata failure should reject append");

    assert_backend_error(
        error,
        WalOperation::Append,
        &segment_one,
        BackendErrorKind::MetadataMismatch,
    );
    assert_clean_append_state(&service, 1);
    assert_eq!(backend.stored_bytes(&segment_one), before);
    assert_eq!(backend.listed_objects(), vec![segment_one]);
}

#[test]
fn list_failure_during_read_returns_list_error() {
    let backend = FaultWindowBackend::new();
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    service
        .append(&record(1, b"before list fault".to_vec()))
        .expect("append record");
    backend.fail_next_list(BackendErrorKind::Unavailable);

    let error = service
        .read_all()
        .expect_err("list failure should fail read");

    assert_list_error(error, BackendErrorKind::Unavailable);
}

#[test]
fn read_failure_during_read_returns_backend_read_error() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    service
        .append(&record(1, b"before read fault".to_vec()))
        .expect("append record");
    backend.fail_next_read(BackendErrorKind::Interrupted);

    let error = service
        .read_all()
        .expect_err("read failure should fail read");

    assert_backend_error(
        error,
        WalOperation::Read,
        &segment_one,
        BackendErrorKind::Interrupted,
    );
}

#[test]
fn delete_failure_records_failed_segment_without_hiding_other_results() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let segment_two = ObjectLayout::wal_segment(2).expect("segment two");
    let segment_three = ObjectLayout::wal_segment(3).expect("segment three");
    backend
        .write_object(
            &segment_one,
            &segment_bytes(1, &[record(1, b"covered one".to_vec())]),
        )
        .expect("seed segment one");
    backend
        .write_object(
            &segment_two,
            &segment_bytes(2, &[record(2, b"covered two".to_vec())]),
        )
        .expect("seed segment two");
    backend
        .write_object(&segment_three, &segment_bytes(3, &[]))
        .expect("seed active segment");
    backend.fail_delete_for(&segment_one);
    let service = WalService::open(
        &backend,
        database_id(),
        3,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(2)))
        .expect("delete covered segments");

    assert_eq!(report.failed_segments(), &[1]);
    assert_eq!(report.deleted_segments(), &[2]);
    assert_eq!(report.protected_segments(), &[3]);
    assert!(backend.object_metadata(&segment_one).is_ok());
    assert_eq!(
        backend
            .object_metadata(&segment_two)
            .expect_err("segment two should be deleted")
            .kind(),
        BackendErrorKind::NotFound
    );
    assert!(backend.object_metadata(&segment_three).is_ok());
}

/// #3603: a covered segment listed at the start of the delete pass can be gone
/// by the time it is read — another covered-segment pass (the off-lock
/// retention clone, or a worker detached at close) deleted it. The pass must
/// report it as already missing and keep going, never fail on the read.
#[test]
fn covered_segment_vanishing_before_its_read_is_reported_already_missing() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let segment_two = ObjectLayout::wal_segment(2).expect("segment two");
    let segment_three = ObjectLayout::wal_segment(3).expect("segment three");
    backend
        .write_object(
            &segment_one,
            &segment_bytes(1, &[record(1, b"covered one".to_vec())]),
        )
        .expect("seed segment one");
    backend
        .write_object(
            &segment_two,
            &segment_bytes(2, &[record(2, b"covered two".to_vec())]),
        )
        .expect("seed segment two");
    backend
        .write_object(&segment_three, &segment_bytes(3, &[]))
        .expect("seed active segment");
    let service = WalService::open(
        &backend,
        database_id(),
        3,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    // The first read of the pass is segment one's: it "vanishes" under us.
    backend.fail_next_read(BackendErrorKind::NotFound);

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(2)))
        .expect("a vanished covered segment never fails the pass");

    assert_eq!(report.failed_segments(), &[]);
    assert_eq!(report.deleted_segments(), &[1, 2]);
    assert_eq!(report.protected_segments(), &[3]);
    // The pass skipped what it could not read rather than deleting blind: the
    // object the double merely pretended was gone is still there.
    assert!(backend.object_metadata(&segment_one).is_ok());
    assert_eq!(
        backend
            .object_metadata(&segment_two)
            .expect_err("segment two should be deleted")
            .kind(),
        BackendErrorKind::NotFound
    );
    assert!(backend.object_metadata(&segment_three).is_ok());
}

/// Only a vanished segment is tolerated: any other read failure still fails
/// the pass before it deletes anything, exactly as before #3603.
#[test]
fn covered_segment_read_failure_other_than_not_found_fails_the_pass() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let segment_two = ObjectLayout::wal_segment(2).expect("segment two");
    let segment_three = ObjectLayout::wal_segment(3).expect("segment three");
    backend
        .write_object(
            &segment_one,
            &segment_bytes(1, &[record(1, b"covered one".to_vec())]),
        )
        .expect("seed segment one");
    backend
        .write_object(
            &segment_two,
            &segment_bytes(2, &[record(2, b"covered two".to_vec())]),
        )
        .expect("seed segment two");
    backend
        .write_object(&segment_three, &segment_bytes(3, &[]))
        .expect("seed active segment");
    let service = WalService::open(
        &backend,
        database_id(),
        3,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    backend.fail_next_read(BackendErrorKind::Interrupted);

    let error = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(2)))
        .expect_err("a read failure other than NotFound fails the pass");

    assert_backend_error(
        error,
        WalOperation::Read,
        &segment_one,
        BackendErrorKind::Interrupted,
    );
    // The pass stopped at the failed read: nothing after it was deleted.
    assert!(backend.object_metadata(&segment_one).is_ok());
    assert!(backend.object_metadata(&segment_two).is_ok());
    assert!(backend.object_metadata(&segment_three).is_ok());
}

#[test]
fn visible_partial_append_error_reopens_as_latest_tail() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    let first = record(1, b"partial error".to_vec());
    backend.append_visible_prefix_then_error(3, BackendErrorKind::Interrupted);

    let error = service
        .append(&first)
        .expect_err("partial append error should reject append");

    assert_backend_error(
        error,
        WalOperation::Append,
        &segment_one,
        BackendErrorKind::Interrupted,
    );
    assert_clean_append_state(&service, 1);
    let object_size = WAL_SEGMENT_HEADER_SIZE as u64 + 3;
    assert_eq!(
        backend
            .object_metadata(&segment_one)
            .expect("segment metadata")
            .size_bytes(),
        object_size
    );

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen latest partial WAL");
    let read = reopened.read_all().expect("read latest partial tail");
    assert_latest_tail(&read, &[], object_size);

    let append_error = reopened
        .append(&record(2, b"after partial".to_vec()))
        .expect_err("append after unrepaired partial tail should fail");

    assert_eq!(
        append_error,
        WalServiceError::UnexpectedAppendOffset {
            object: segment_one,
            expected: WAL_SEGMENT_HEADER_SIZE as u64,
            actual: object_size,
        }
    );
}

#[test]
fn visible_partial_append_wrong_metadata_reopens_as_latest_tail() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    let first = record(1, b"partial metadata".to_vec());
    let frame_len = record_frame_len(&first);
    backend.append_visible_prefix_then_wrong_metadata(5);

    let error = service
        .append(&first)
        .expect_err("partial append metadata should reject append");

    let object_size = WAL_SEGMENT_HEADER_SIZE as u64 + 5;
    assert_eq!(
        error,
        WalServiceError::UnexpectedObjectSize {
            object: segment_one.clone(),
            expected: WAL_SEGMENT_HEADER_SIZE as u64 + frame_len,
            actual: object_size,
        }
    );
    assert_clean_append_state(&service, 1);

    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen latest partial WAL");
    let read = reopened.read_all().expect("read latest partial tail");
    assert_latest_tail(&read, &[], object_size);
}

#[test]
fn visible_partial_append_in_non_latest_segment_is_corruption() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let segment_two = ObjectLayout::wal_segment(2).expect("segment two");
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");
    backend.append_visible_prefix_then_error(4, BackendErrorKind::Interrupted);
    service
        .append(&record(1, b"partial nonlatest".to_vec()))
        .expect_err("partial append should fail");
    backend
        .write_object(&segment_two, &segment_bytes(2, &[]))
        .expect("seed newer segment");
    let reopened = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open newer segment");

    let error = reopened
        .read_all()
        .expect_err("non-latest partial tail should be corruption");

    match error {
        WalServiceError::Format {
            operation,
            object,
            source,
        } => {
            assert_eq!(operation, WalOperation::Read);
            assert_eq!(object, segment_one);
            assert!(matches!(source, FormatError::InsufficientBytes { .. }));
        }
        other => panic!("expected WAL format error, got {other:?}"),
    }
}

/// W3.3a: a clean flush failure (no bytes visible) is durability-uncertain
/// (`Backend { Flush }`), keeps the staged bytes, and a retry after the fault
/// clears drains them — the barrier then covers everything.
#[test]
fn buffered_flush_failure_is_durability_uncertain_and_retry_drains() {
    let backend = FaultWindowBackend::new();
    let segment_one = ObjectLayout::wal_segment(1).expect("segment one");
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default().with_append_buffer_bytes(u64::from(u16::MAX)),
    )
    .expect("open WAL");
    let first = record(1, b"buffered then stuck".to_vec());
    let second = record(2, b"still buffered".to_vec());
    service.append(&first).expect("buffered append");
    service.append(&second).expect("buffered append");
    assert_eq!(service.dirty_records(), 2);

    backend.append_visible_prefix_then_error(0, BackendErrorKind::Interrupted);
    let error = service
        .force_durable()
        .expect_err("flush behind the barrier should surface the fault");
    assert!(
        error.is_durability_uncertain_append_failure(),
        "flush failure must be durability-uncertain, got {error:?}"
    );
    assert_backend_error(
        error,
        WalOperation::Flush,
        &segment_one,
        BackendErrorKind::Interrupted,
    );
    // The staged bytes belong to accepted records — never dropped.
    assert_eq!(service.dirty_records(), 2);

    service
        .force_durable()
        .expect("retry drains the staged bytes");
    assert_eq!(service.dirty_records(), 0);
    let read = service.read_all().expect("read after retry");
    assert_eq!(read.records().len(), 2);
    assert!(read.truncation().is_none());
}

/// W3.3a: a PARTIAL flush write leaves the backend ahead of the physical
/// model; the retry's handle re-open reconciliation must reject with the
/// durability-uncertain offset error rather than blindly re-appending (which
/// would duplicate the visible prefix).
#[test]
fn buffered_flush_partial_write_rejects_blind_retry() {
    let backend = FaultWindowBackend::new();
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default().with_append_buffer_bytes(u64::from(u16::MAX)),
    )
    .expect("open WAL");
    let first = record(1, b"partially flushed".to_vec());
    service.append(&first).expect("buffered append");

    backend.append_visible_prefix_then_error(5, BackendErrorKind::Interrupted);
    service
        .force_durable()
        .expect_err("partial flush should surface the fault");

    let error = service
        .force_durable()
        .expect_err("retry must detect the partial prefix, not re-append blindly");
    assert!(
        matches!(error, WalServiceError::UnexpectedAppendOffset { .. }),
        "expected offset reconciliation failure, got {error:?}"
    );
}
