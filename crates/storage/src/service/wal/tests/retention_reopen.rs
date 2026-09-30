use super::*;
use crate::backend::{Backend, DeleteOutcome, DeleteStatus, PublishFailureKind};
use crate::format::FormatError;
use crate::service::wal::WalOperation;
use strata_core::CommitVersion;

fn wal_segment(segment_id: u64) -> ObjectName {
    ObjectLayout::wal_segment(segment_id).expect("WAL segment")
}

fn wal_sidecar(segment_id: u64) -> ObjectName {
    ObjectLayout::wal_segment_metadata(segment_id).expect("WAL metadata sidecar")
}

fn read_records(service: &WalService<'_>) -> Vec<WalRecord> {
    service.read_all().expect("read WAL").records().to_vec()
}

fn seed_segment(backend: &StoredWalBackend, segment_id: u64, records: &[WalRecord]) -> ObjectName {
    let object = wal_segment(segment_id);
    backend
        .write_object(&object, &segment_bytes(segment_id, records))
        .expect("seed WAL segment");
    object
}

fn seed_sidecar(backend: &StoredWalBackend, segment_id: u64) -> ObjectName {
    let object = wal_sidecar(segment_id);
    backend
        .write_object(&object, b"sidecar")
        .expect("seed WAL sidecar");
    object
}

fn assert_segment_missing(backend: &StoredWalBackend, object: &ObjectName) {
    assert_eq!(
        backend
            .object_metadata(object)
            .expect_err("segment should be deleted")
            .kind(),
        BackendErrorKind::NotFound
    );
}

fn assert_object_missing(backend: &StoredWalBackend, object: &ObjectName) {
    assert_eq!(
        backend
            .object_metadata(object)
            .expect_err("object should be missing")
            .kind(),
        BackendErrorKind::NotFound
    );
}

fn assert_segment_present(backend: &StoredWalBackend, object: &ObjectName) {
    backend.object_metadata(object).expect("segment is present");
}

#[test]
fn retention_deletes_only_covered_old_segments_and_sorts_report() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"covered".to_vec())]);
    let segment_two = seed_segment(
        &backend,
        2,
        &[
            record(2, b"partly covered".to_vec()),
            record(5, b"above watermark".to_vec()),
        ],
    );
    let segment_three = seed_segment(&backend, 3, &[record(3, b"active".to_vec())]);
    let segment_four = seed_segment(&backend, 4, &[record(4, b"newer".to_vec())]);
    let manifest = ObjectLayout::database_manifest().expect("database manifest");
    backend
        .write_object(&manifest, b"not a WAL segment")
        .expect("seed non-WAL object");
    // The backend order is deliberately hostile: retention reports must be
    // ordered by parsed segment id, and non-WAL objects must stay outside the
    // WAL retention decision.
    backend.set_list_order(vec![
        segment_four.clone(),
        segment_two.clone(),
        manifest.clone(),
        segment_three.clone(),
        segment_one.clone(),
    ]);
    let service = WalService::open(
        &backend,
        database_id(),
        3,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(3)))
        .expect("delete covered WAL segments");

    // The seed of 3 is stale (segment 4 exists): open reconciles the writer to
    // the on-disk tail (#2555), so sealed segment 3 sits below the active
    // boundary. Segment 2 stays protected by its above-watermark record, and
    // #3697: the trim is a strict oldest-first prefix, so it STOPS there —
    // deleting the fully covered segment 3 above it would leave an interior
    // hole ({2, 4}) that the next open refuses as segment loss. 4 is the
    // active tail.
    assert_eq!(service.active_segment_id(), 4);
    assert_eq!(report.deleted_segments(), &[1]);
    assert_eq!(report.protected_segments(), &[2, 3, 4]);
    assert_eq!(report.failed_segments(), &[] as &[u64]);
    assert_segment_missing(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
    assert_segment_present(&backend, &segment_three);
    assert_segment_present(&backend, &segment_four);
    crate::service::wal::verify_wal_segment_inventory(&backend)
        .expect("the surviving log is a contiguous suffix");
    assert_eq!(
        backend.read_object(&manifest).expect("manifest bytes"),
        b"not a WAL segment"
    );
}

#[test]
fn retention_treats_delete_not_found_as_already_pruned() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"raced delete".to_vec())]);
    let segment_two = seed_segment(&backend, 2, &[record(2, b"active".to_vec())]);
    backend.fail_delete_with(&segment_one, BackendErrorKind::NotFound);
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(1)))
        .expect("delete covered WAL segments");

    assert_eq!(report.deleted_segments(), &[1]);
    assert_eq!(report.protected_segments(), &[2]);
    assert_eq!(report.failed_segments(), &[]);
    assert_segment_missing(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
}

#[test]
fn retention_deletes_header_only_old_segments() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[]);
    let segment_two = seed_segment(&backend, 2, &[record(1, b"active".to_vec())]);
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::ZERO))
        .expect("delete covered WAL segments");

    assert_eq!(report.deleted_segments(), &[1]);
    assert_eq!(report.protected_segments(), &[2]);
    assert_eq!(report.failed_segments(), &[]);
    assert_segment_missing(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
}

#[test]
fn retention_removes_sidecar_for_deleted_segment() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"covered".to_vec())]);
    let sidecar_one = seed_sidecar(&backend, 1);
    let sidecar_two = seed_sidecar(&backend, 2);
    let segment_two = seed_segment(&backend, 2, &[record(2, b"active".to_vec())]);
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(1)))
        .expect("delete covered WAL segments");

    assert_eq!(report.deleted_segments(), &[1]);
    assert_eq!(report.sidecar_deletes().len(), 1);
    assert_eq!(report.sidecar_deletes()[0].segment_id(), 1);
    assert_eq!(report.sidecar_deletes()[0].object(), &sidecar_one);
    assert!(report.sidecar_deletes()[0].outcome().is_some());
    assert!(report.sidecar_deletes()[0].failure().is_none());
    assert_segment_missing(&backend, &segment_one);
    assert_object_missing(&backend, &sidecar_one);
    assert_segment_present(&backend, &segment_two);
    assert_segment_present(&backend, &sidecar_two);
}

#[test]
fn retention_reports_missing_sidecar_delete_as_idempotent_cleanup() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"covered".to_vec())]);
    let sidecar_one = wal_sidecar(1);
    backend.fail_delete_with(&sidecar_one, BackendErrorKind::NotFound);
    let segment_two = seed_segment(&backend, 2, &[record(2, b"active".to_vec())]);
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(1)))
        .expect("delete covered WAL segments");

    assert_eq!(report.deleted_segments(), &[1]);
    assert_eq!(report.sidecar_deletes().len(), 1);
    assert_eq!(report.sidecar_deletes()[0].segment_id(), 1);
    assert_eq!(report.sidecar_deletes()[0].object(), &sidecar_one);
    assert_eq!(
        report.sidecar_deletes()[0]
            .outcome()
            .map(DeleteOutcome::status),
        Some(DeleteStatus::AlreadyMissing)
    );
    assert!(report.sidecar_deletes()[0].failure().is_none());
    assert_segment_missing(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
}

#[test]
fn retention_is_idempotent_across_consecutive_calls() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"covered".to_vec())]);
    let segment_two = seed_segment(&backend, 2, &[record(2, b"active".to_vec())]);
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let first = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(1)))
        .expect("first retention pass");
    let second = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::new(1)))
        .expect("second retention pass");

    assert_eq!(first.deleted_segments(), &[1]);
    assert_eq!(first.protected_segments(), &[2]);
    assert_eq!(first.failed_segments(), &[]);
    assert_eq!(second.deleted_segments(), &[]);
    assert_eq!(second.protected_segments(), &[2]);
    assert_eq!(second.failed_segments(), &[]);
    assert_segment_missing(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
}

#[test]
fn retention_then_reopen_reads_only_remaining_segments() {
    let backend = StoredWalBackend::new();
    let first = record(1, b"covered one".to_vec());
    let second = record(2, b"covered two".to_vec());
    let third = record(3, b"covered three".to_vec());
    let active = record(4, b"active".to_vec());
    let segment_one = seed_segment(&backend, 1, std::slice::from_ref(&first));
    let segment_two = seed_segment(&backend, 2, std::slice::from_ref(&second));
    let segment_three = seed_segment(&backend, 3, std::slice::from_ref(&third));
    let segment_four = seed_segment(&backend, 4, std::slice::from_ref(&active));
    {
        let service = WalService::open(
            &backend,
            database_id(),
            4,
            DurabilityPolicy::Standard,
            WalServiceConfig::default(),
        )
        .expect("open WAL");
        let report = service
            .delete_covered_segments(WalRetentionProof::flush_watermark(CommitVersion::new(3)))
            .expect("retention pass");
        assert_eq!(report.deleted_segments(), &[1, 2, 3]);
        assert_eq!(report.protected_segments(), &[4]);
    }

    let reopened = WalService::open(
        &backend,
        database_id(),
        4,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen retained WAL");

    assert_segment_missing(&backend, &segment_one);
    assert_segment_missing(&backend, &segment_two);
    assert_segment_missing(&backend, &segment_three);
    assert_segment_present(&backend, &segment_four);
    assert_eq!(read_records(&reopened), vec![active.clone()]);
    assert_eq!(reopened.active_segment_id(), 4);
    assert_eq!(reopened.active_metadata().record_count(), 1);
    assert_eq!(
        reopened.active_metadata().min_commit_version(),
        active.commit_version()
    );
    assert_eq!(
        reopened.active_metadata().max_commit_version(),
        active.commit_version()
    );
}

/// #3697: a failed delete stops the oldest-first trim — the failure is
/// reported, and nothing newer than the failed segment is deleted (deleting
/// segment two would leave the interior hole {1, 3}).
#[test]
fn retention_records_delete_failure_and_stops_the_trim() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"failed delete".to_vec())]);
    let segment_two = seed_segment(&backend, 2, &[record(2, b"deleted".to_vec())]);
    let segment_three = seed_segment(&backend, 3, &[record(3, b"active".to_vec())]);
    backend.fail_delete_for(&segment_one);
    backend.set_list_order(vec![
        segment_three.clone(),
        segment_two.clone(),
        segment_one.clone(),
    ]);
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
        .expect("delete covered WAL segments");

    assert_eq!(report.deleted_segments(), &[] as &[u64]);
    assert!(report.delete_outcomes().is_empty());
    assert_eq!(report.protected_segments(), &[2, 3]);
    assert_eq!(report.failed_segments(), &[1]);
    assert_eq!(report.delete_failures().len(), 1);
    assert_eq!(report.delete_failures()[0].segment_id(), 1);
    assert_eq!(report.delete_failures()[0].object(), &segment_one);
    assert_eq!(
        report.delete_failures()[0].failure().source_error().kind(),
        BackendErrorKind::Unavailable
    );
    assert_segment_present(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
    assert_segment_present(&backend, &segment_three);
    crate::service::wal::verify_wal_segment_inventory(&backend)
        .expect("a stopped trim leaves a contiguous log");
}

#[test]
fn retention_requires_delete_capability_before_listing_or_deleting() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(1, b"covered".to_vec())]);
    // The backend can mechanically delete objects; only the advertised
    // capability is hidden. This proves retention checks the backend contract
    // before list/delete work, not just that delete later fails.
    backend.hide_delete_capability();
    let service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL without delete capability");

    let error = service
        .delete_covered_segments(WalRetentionProof::snapshot_watermark(CommitVersion::MAX))
        .expect_err("retention should require delete capability");

    assert_unsupported_capability(&error, BackendCapability::DeleteObject);
    assert_segment_present(&backend, &segment_one);
}

#[test]
fn retention_reports_no_covered_segments_without_deleting() {
    let backend = StoredWalBackend::new();
    let segment_one = seed_segment(&backend, 1, &[record(10, b"too new".to_vec())]);
    let segment_two = seed_segment(&backend, 2, &[record(11, b"active".to_vec())]);
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL");

    let report = service
        .delete_covered_segments(WalRetentionProof::flush_watermark(CommitVersion::new(1)))
        .expect("delete covered WAL segments");

    assert_eq!(report.deleted_segments(), &[]);
    assert_eq!(report.protected_segments(), &[1, 2]);
    assert_eq!(report.failed_segments(), &[]);
    assert_segment_present(&backend, &segment_one);
    assert_segment_present(&backend, &segment_two);
}

#[test]
fn reopen_after_standard_close_rebuilds_records_and_active_metadata() {
    let backend = StoredWalBackend::new();
    let first = record(4, b"before close".to_vec());
    let second = record(7, b"after close".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::default(),
        )
        .expect("open WAL");
        service.append(&first).expect("append first");
        service.append(&second).expect("append second");
        service.close().expect("close WAL");
    }

    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen WAL");

    assert_eq!(read_records(&reopened), vec![first.clone(), second.clone()]);
    assert_eq!(reopened.dirty_bytes(), 0);
    assert_eq!(reopened.dirty_records(), 0);
    assert_eq!(reopened.active_metadata().record_count(), 2);
    assert_eq!(
        reopened.active_metadata().min_commit_version(),
        first.commit_version()
    );
    assert_eq!(
        reopened.active_metadata().max_commit_version(),
        second.commit_version()
    );
    assert_eq!(
        reopened.active_metadata().min_timestamp(),
        first.commit_timestamp()
    );
    assert_eq!(
        reopened.active_metadata().max_timestamp(),
        second.commit_timestamp()
    );
}

#[test]
fn reopen_after_dirty_standard_append_reads_visible_record() {
    let backend = StoredWalBackend::new();
    let first = record(1, b"dirty standard".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::default(),
        )
        .expect("open WAL");
        service
            .append(&first)
            .expect("append dirty standard record");
    }

    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen WAL");

    assert_eq!(read_records(&reopened), vec![first]);
    assert_eq!(reopened.dirty_bytes(), 0);
    assert_eq!(reopened.dirty_records(), 0);
    assert_eq!(reopened.active_metadata().record_count(), 1);
}

#[test]
fn reopen_after_always_append_reads_visible_clean_record() {
    let backend = StoredWalBackend::new();
    let first = record(1, b"always".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Always,
            WalServiceConfig::default(),
        )
        .expect("open WAL");
        let append = service.append(&first).expect("append always record");
        assert!(append.forced_durable());
        assert_eq!(service.dirty_bytes(), 0);
        assert_eq!(service.dirty_records(), 0);
    }

    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Always,
        WalServiceConfig::default(),
    )
    .expect("reopen WAL");

    assert_eq!(read_records(&reopened), vec![first]);
    assert_eq!(reopened.dirty_bytes(), 0);
    assert_eq!(reopened.dirty_records(), 0);
}

#[test]
fn reopen_after_rotation_reads_all_segments_and_rebuilds_active_metadata() {
    let backend = StoredWalBackend::new();
    let first = record(1, vec![0x11; 800]);
    let second = record(2, vec![0x22; 800]);
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::new(1024),
        )
        .expect("open WAL");
        service.append(&first).expect("append first");
        let append = service.append(&second).expect("append second");
        assert_eq!(append.segment_id(), 2);
        assert_eq!(service.active_segment_id(), 2);
    }

    let reopened = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen rotated WAL");

    assert_eq!(read_records(&reopened), vec![first, second.clone()]);
    assert_eq!(reopened.active_segment_id(), 2);
    assert_eq!(reopened.active_metadata().record_count(), 1);
    assert_eq!(
        reopened.active_metadata().min_commit_version(),
        second.commit_version()
    );
    assert_eq!(
        reopened.active_metadata().max_commit_version(),
        second.commit_version()
    );
}

#[test]
fn reopen_after_latest_partial_tail_reports_truncation_and_refuses_append() {
    let backend = StoredWalBackend::new();
    let segment_one = wal_segment(1);
    let first = record(1, b"partial latest".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::new(1024),
        )
        .expect("open WAL");
        service.append(&first).expect("append first");
    }
    let valid_end = backend
        .object_metadata(&segment_one)
        .expect("segment metadata before partial")
        .size_bytes();
    backend
        .append_object(&segment_one, &[0xff])
        .expect("append partial tail");
    let object_size = valid_end + 1;

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen latest partial WAL");
    let read = reopened.read_all().expect("read recoverable latest tail");
    assert_eq!(read.records(), std::slice::from_ref(&first));
    let truncation = read.truncation().expect("truncation fact");
    assert_eq!(truncation.segment_id(), 1);
    assert_eq!(truncation.valid_end_offset(), valid_end);
    assert_eq!(truncation.object_size(), object_size);

    // A latest partial tail is recoverable for reads, but appending before the
    // lifecycle layer truncates or repairs it would write after uncertain bytes.
    let same_segment_error = reopened
        .append(&record(2, b"same segment".to_vec()))
        .expect_err("same-segment append should be refused before repair");
    assert_eq!(
        same_segment_error,
        WalServiceError::UnexpectedAppendOffset {
            object: segment_one.clone(),
            expected: valid_end,
            actual: object_size,
        }
    );

    // This frame fits a fresh segment but not the current valid prefix, so the
    // append would rotate if the partial tail were not blocking progress.
    let rotating_record = record_with_frame_len(3, 900, 0x33);
    // This assertion keeps the rotation branch honest. Without it, the test can
    // silently collapse into a second same-segment append-refusal case.
    assert!(
        valid_end + record_frame_len(&rotating_record) > 1024,
        "test record must force rotation after the valid prefix"
    );
    let rotation_error = reopened
        .append(&rotating_record)
        .expect_err("rotation append should be refused before repair");
    assert_eq!(
        rotation_error,
        WalServiceError::UnexpectedAppendOffset {
            object: segment_one,
            expected: valid_end,
            actual: object_size,
        }
    );
}

#[test]
fn repair_latest_partial_tail_rewrites_valid_prefix_and_allows_append() {
    let backend = StoredWalBackend::new();
    let segment_one = wal_segment(1);
    let first = record(1, b"repairable latest".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::new(1024),
        )
        .expect("open WAL");
        service.append(&first).expect("append first");
    }
    let valid_end = backend
        .object_metadata(&segment_one)
        .expect("segment metadata before partial")
        .size_bytes();
    backend
        .append_object(&segment_one, &[0xaa, 0xbb])
        .expect("append partial tail");
    let object_size = valid_end + 2;

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen latest partial WAL");
    let read = reopened.read_all().expect("read recoverable latest tail");
    let truncation = read.truncation().expect("truncation fact").clone();
    assert_eq!(truncation.valid_end_offset(), valid_end);
    assert_eq!(truncation.object_size(), object_size);
    drop(read);

    let repair = reopened
        .repair_latest_tail(&truncation)
        .expect("repair latest partial tail");
    assert_eq!(repair.segment_id(), 1);
    assert_eq!(repair.valid_end_offset(), valid_end);
    assert_eq!(repair.removed_bytes(), 2);
    assert_eq!(
        backend
            .object_metadata(&segment_one)
            .expect("repaired segment metadata")
            .size_bytes(),
        valid_end
    );

    let repaired_read = reopened.read_all().expect("read repaired WAL");
    assert_eq!(repaired_read.records(), std::slice::from_ref(&first));
    assert!(repaired_read.truncation().is_none());

    let second = record(2, b"after repair".to_vec());
    let append = reopened.append(&second).expect("append after repair");
    assert_eq!(append.start_offset(), valid_end);
    assert_eq!(read_records(&reopened), vec![first, second]);
}

#[test]
fn repair_latest_partial_tail_rejects_stale_truncation_fact() {
    let backend = StoredWalBackend::new();
    let segment_one = wal_segment(1);
    let first = record(1, b"stale repair".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::new(1024),
        )
        .expect("open WAL");
        service.append(&first).expect("append first");
    }
    let valid_end = backend
        .object_metadata(&segment_one)
        .expect("segment metadata before partial")
        .size_bytes();
    backend
        .append_object(&segment_one, &[0xaa])
        .expect("append partial tail");

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen latest partial WAL");
    let stale = reopened
        .read_all()
        .expect("read recoverable latest tail")
        .truncation()
        .expect("truncation fact")
        .clone();
    backend
        .append_object(&segment_one, &[0xbb])
        .expect("mutate after truncation fact");

    let error = reopened
        .repair_latest_tail(&stale)
        .expect_err("stale truncation facts should not repair");
    assert_eq!(
        error,
        WalServiceError::UnexpectedAppendOffset {
            object: segment_one,
            expected: valid_end + 1,
            actual: valid_end + 2,
        }
    );
}

#[test]
fn repair_latest_partial_tail_publish_uncertainty_blocks_append_until_reopen() {
    let backend = StoredWalBackend::new();
    let segment_one = wal_segment(1);
    let first = record(1, b"uncertain repair".to_vec());
    {
        let mut service = WalService::open(
            &backend,
            database_id(),
            1,
            DurabilityPolicy::Standard,
            WalServiceConfig::new(1024),
        )
        .expect("open WAL");
        service.append(&first).expect("append first");
    }
    let valid_end = backend
        .object_metadata(&segment_one)
        .expect("segment metadata before partial")
        .size_bytes();
    backend
        .append_object(&segment_one, &[0xaa, 0xbb])
        .expect("append partial tail");

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen latest partial WAL");
    let truncation = reopened
        .read_all()
        .expect("read recoverable latest tail")
        .truncation()
        .expect("truncation fact")
        .clone();
    backend.fail_publish_with(
        &segment_one,
        PublishFailureKind::VisibleDurabilityUnconfirmed,
        true,
    );

    let error = reopened
        .repair_latest_tail(&truncation)
        .expect_err("uncertain repair publish should be reported");
    match error {
        WalServiceError::Publish { operation, source } => {
            assert_eq!(operation, WalOperation::Repair);
            assert_eq!(
                source.kind(),
                PublishFailureKind::VisibleDurabilityUnconfirmed
            );
        }
        other => panic!("expected WAL repair publish error, got {other:?}"),
    }
    assert_eq!(
        backend
            .object_metadata(&segment_one)
            .expect("visible repaired prefix metadata")
            .size_bytes(),
        valid_end
    );

    let append_error = reopened
        .append(&record(2, b"blocked after uncertainty".to_vec()))
        .expect_err("append should wait for recovery after uncertain repair");
    assert_eq!(
        append_error,
        WalServiceError::RepairUncertain { segment_id: 1 }
    );

    let mut recovered = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen after visible uncertain repair");
    let append = recovered
        .append(&record(2, b"after reopen".to_vec()))
        .expect("append after reopen");
    assert_eq!(append.start_offset(), valid_end);
}

#[test]
fn reopen_after_non_latest_partial_tail_fails_strict_read() {
    let backend = StoredWalBackend::new();
    let segment_one = wal_segment(1);
    let segment_two = seed_segment(&backend, 2, &[]);
    let first = record(1, b"non-latest partial".to_vec());
    backend
        .write_object(
            &segment_one,
            &segment_bytes(1, std::slice::from_ref(&first)),
        )
        .expect("seed first segment");
    backend
        .append_object(&segment_one, &[0xff])
        .expect("append partial tail");
    assert_segment_present(&backend, &segment_two);
    // Only the latest segment may expose a recoverable tail. A partial tail in
    // any older segment means the WAL no longer has a trustworthy prefix.
    let service = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open newer segment");

    let error = service
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

#[test]
fn reopen_after_corrupt_header_fails_strict_open() {
    let segment_one = wal_segment(1);
    let mut bytes = segment_bytes(1, &[]);
    bytes[0] ^= 0xff;
    let backend = StoredWalBackend::with_object(&segment_one, &bytes);

    let result = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    );

    match open_error(result, "corrupt header should fail open") {
        WalServiceError::Format {
            operation,
            object,
            source: _,
        } => {
            assert_eq!(operation, WalOperation::Read);
            assert_eq!(object, segment_one);
        }
        other => panic!("expected WAL format error, got {other:?}"),
    }
}

/// W3.3a contract: Standard-mode records staged in the coalescing buffer are
/// lost on ABRUPT process termination (simulated by leaking the service so no
/// drop-flush runs) — recovery sees only flushed records, as a clean prefix.
/// Orderly drops flush (see below); power-loss exposure is unchanged either
/// way: the last forced barrier.
#[test]
fn buffered_abrupt_termination_recovers_only_flushed_records() {
    let backend = StoredWalBackend::default();
    let buffered_config = WalServiceConfig::default().with_append_buffer_bytes(u64::from(u16::MAX));
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        buffered_config,
    )
    .expect("open WAL");
    let flushed = record(1, b"flushed before the crash".to_vec());
    let staged = record(2, b"staged and lost".to_vec());
    service.append(&flushed).expect("append flushed");
    service.force_durable().expect("barrier");
    service.append(&staged).expect("append staged");
    // Abrupt termination: no drop runs, the staged buffer vanishes with the
    // process. (An orderly drop would flush — covered by the test below.)
    // forget_registered instead of mem::forget so the leak checker sees
    // the never-dropped service as an intentional, registered leak.
    crate::testkit::forget_registered(service);

    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        buffered_config,
    )
    .expect("reopen WAL");
    let read = reopened.read_all().expect("read after reopen");
    assert_eq!(read.records().len(), 1);
    assert_eq!(
        read.records()[0].commit_version(),
        CommitVersion::new(1),
        "only the flushed record survives a reopen without a barrier"
    );
    assert!(
        read.truncation().is_none(),
        "staged bytes never reach the backend — the tail stays well-formed"
    );
}

/// W3.3a: an ORDERLY drop (no explicit close) flushes staged bytes best-effort
/// — page-cache parity with the pre-coalescing behavior for every
/// process-alive abandon path. Only an abrupt process kill loses the buffer.
#[test]
fn buffered_drop_flushes_staged_records_for_reopen() {
    let backend = StoredWalBackend::default();
    let buffered_config = WalServiceConfig::default().with_append_buffer_bytes(u64::from(u16::MAX));
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        buffered_config,
    )
    .expect("open WAL");
    service
        .append(&record(1, b"staged then dropped".to_vec()))
        .expect("append staged");
    drop(service);

    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        buffered_config,
    )
    .expect("reopen WAL");
    let read = reopened.read_all().expect("read after drop");
    assert_eq!(
        read.records().len(),
        1,
        "drop must flush staged bytes to the backend (no fsync — close owns the barrier)"
    );
}

// --- #2555: open-time reconciliation of the writer's resume segment ---------
//
// The manifest `active_wal_segment` pointer advances only when a checkpoint
// publishes, so after post-checkpoint rotations it lags the on-disk tail.
// `WalService::open` must resume at the directory max, never inside a sealed
// older segment (appending there collides on the next roll and disorders the
// package).

#[test]
fn reopen_with_stale_seed_resumes_at_the_on_disk_tail_and_rolls_fresh() {
    let backend = StoredWalBackend::new();
    // Segments 2..4 survive retention; 2 is the stale manifest pointer. Both
    // 2 and 4 are over the segment budget so the first append must rotate.
    let floor = record_with_frame_len(2, 1100, 0x42);
    let middle = record(3, b"middle".to_vec());
    let tail = record_with_frame_len(4, 1100, 0x43);
    seed_segment(&backend, 2, std::slice::from_ref(&floor));
    seed_segment(&backend, 3, std::slice::from_ref(&middle));
    seed_segment(&backend, 4, std::slice::from_ref(&tail));

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen with stale manifest seed");
    assert_eq!(reopened.active_segment_id(), 4);

    // Pre-fix this append resumed in segment 2 and its rotation CREATEd the
    // already-existing segment 3 (the #2555 collision). Post-fix it rotates
    // off the true tail into a fresh segment 5.
    let appended = record(5, b"after reopen".to_vec());
    let append = reopened.append(&appended).expect("append after reopen");
    assert_eq!(append.segment_id(), 5);
    assert_eq!(reopened.active_segment_id(), 5);
    assert_segment_present(&backend, &wal_segment(5));
    let read = read_records(&reopened);
    assert_eq!(read, vec![floor, middle, tail, appended]);
}

#[test]
fn reopen_with_deleted_stale_seed_does_not_resurrect_the_truncated_segment() {
    let backend = StoredWalBackend::new();
    // Retention already deleted segments 1..2; the manifest still points at 2.
    seed_segment(&backend, 3, &[record(3, b"survivor".to_vec())]);
    seed_segment(&backend, 4, &[record(4, b"tail".to_vec())]);

    let reopened = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen with deleted stale seed");

    assert_eq!(reopened.active_segment_id(), 4);
    assert_object_missing(&backend, &wal_segment(2));
}

#[test]
fn reopen_with_seed_above_the_on_disk_tail_creates_the_seed_segment() {
    let backend = StoredWalBackend::new();
    seed_segment(&backend, 1, &[record(1, b"one".to_vec())]);
    seed_segment(&backend, 2, &[record(2, b"two".to_vec())]);

    // A seed above the directory max cannot arise from the engine's own state
    // machine (rotation durably creates before the pointer advances); it pins
    // external restore/tamper behavior: honor the seed, matching fresh-store
    // semantics.
    let reopened = WalService::open(
        &backend,
        database_id(),
        7,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen with seed above disk max");

    assert_eq!(reopened.active_segment_id(), 7);
    assert_segment_present(&backend, &wal_segment(7));
}

#[test]
fn reopen_with_stale_seed_routes_a_torn_tail_through_the_repair_contract() {
    let backend = StoredWalBackend::new();
    let survivor = record(2, b"survivor".to_vec());
    let tail = record(3, b"tail".to_vec());
    seed_segment(&backend, 2, std::slice::from_ref(&survivor));
    let segment_three = seed_segment(&backend, 3, std::slice::from_ref(&tail));
    let valid_end = backend
        .object_metadata(&segment_three)
        .expect("tail metadata")
        .size_bytes();
    backend
        .append_object(&segment_three, &[0xff])
        .expect("append partial tail");

    let mut reopened = WalService::open(
        &backend,
        database_id(),
        2,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(1024),
    )
    .expect("reopen stale seed onto torn tail");

    // Reconciliation must land on the torn MAX segment so the existing
    // recoverable-tail contract applies: reads surface the truncation fact and
    // appends refuse until the lifecycle repair step truncates the tail.
    assert_eq!(reopened.active_segment_id(), 3);
    let read = reopened.read_all().expect("read torn tail");
    assert_eq!(read.records(), &[survivor, tail]);
    let truncation = read.truncation().expect("truncation fact");
    assert_eq!(truncation.segment_id(), 3);
    assert_eq!(truncation.valid_end_offset(), valid_end);
    let error = reopened
        .append(&record(4, b"blocked".to_vec()))
        .expect_err("append onto torn tail must refuse before repair");
    assert_eq!(
        error,
        WalServiceError::UnexpectedAppendOffset {
            object: segment_three,
            expected: valid_end,
            actual: valid_end + 1,
        }
    );
}

#[test]
fn reopen_with_stale_seed_keeps_retention_boundary_at_the_true_tail() {
    let backend = StoredWalBackend::new();
    let one = seed_segment(&backend, 1, &[record(1, b"covered one".to_vec())]);
    let two = seed_segment(&backend, 2, &[record(2, b"covered two".to_vec())]);
    let three = seed_segment(&backend, 3, &[record(3, b"covered three".to_vec())]);
    let four = seed_segment(&backend, 4, &[record(4, b"active".to_vec())]);

    // Pre-fix a stale seed of 1 protected EVERY segment (`>= active`), so
    // retention silently stopped deleting covered garbage.
    let reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen with stale seed");
    assert_eq!(reopened.active_segment_id(), 4);
    let report = reopened
        .delete_covered_segments(WalRetentionProof::flush_watermark(CommitVersion::new(3)))
        .expect("retention pass");

    assert_eq!(report.deleted_segments(), &[1, 2, 3]);
    assert_eq!(report.protected_segments(), &[4]);
    assert_segment_missing(&backend, &one);
    assert_segment_missing(&backend, &two);
    assert_segment_missing(&backend, &three);
    assert_segment_present(&backend, &four);
}

#[test]
fn segment_inventory_check_rejects_interior_gaps_only() {
    use super::super::verify_wal_segment_inventory;

    // Empty directory: nothing to verify at this layer. Whether emptiness is
    // loss is judged by the durable commit watermark at recovery (and by the
    // manifest attestation guard, #2765) — not by the id inventory.
    let fresh = StoredWalBackend::new();
    verify_wal_segment_inventory(&fresh).expect("an empty inventory has no interior gaps");

    // A contiguous run is valid wherever it starts: retention trims a
    // contiguous prefix, so a higher-based run is a legitimate shape.
    let healthy = StoredWalBackend::new();
    seed_segment(&healthy, 1, &[record(1, b"a".to_vec())]);
    seed_segment(&healthy, 2, &[record(2, b"b".to_vec())]);
    verify_wal_segment_inventory(&healthy).expect("contiguous inventory is valid");

    let retained = StoredWalBackend::new();
    seed_segment(&retained, 2, &[record(2, b"b".to_vec())]);
    seed_segment(&retained, 3, &[record(3, b"c".to_vec())]);
    verify_wal_segment_inventory(&retained)
        .expect("a contiguous run above a retention floor is valid");

    // An interior hole can never be produced by retention or rotation: a
    // segment — and every committed record it held — was removed out of band.
    let gapped = StoredWalBackend::new();
    seed_segment(&gapped, 1, &[record(1, b"a".to_vec())]);
    seed_segment(&gapped, 3, &[record(3, b"c".to_vec())]);
    assert!(matches!(
        verify_wal_segment_inventory(&gapped),
        Err(WalServiceError::SegmentInventoryGap { missing_segment: 2 })
    ));
}

#[test]
fn commit_watermark_publishes_at_seal_points_and_is_monotonic() {
    use crate::format::decode_wal_watermark;

    let backend = StoredWalBackend::new();
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open");
    assert_eq!(service.durable_commit_watermark(), None);

    // Appends alone never publish the watermark — only a seal point does,
    // strictly after the sync that made the records durable.
    service
        .append(&record(1, b"one".to_vec()))
        .expect("append one");
    service
        .append(&record(2, b"two".to_vec()))
        .expect("append two");
    let watermark_object = ObjectLayout::wal_watermark().expect("watermark object");
    assert!(
        backend.read_object(&watermark_object).is_err(),
        "no watermark before the first seal point"
    );

    // Close seals the active segment durable and attests its highest commit.
    service.close().expect("close");
    let attested = decode_wal_watermark(
        &backend
            .read_object(&watermark_object)
            .expect("watermark published at close"),
    )
    .expect("watermark decodes");
    assert_eq!(attested, 2, "close attests the synced maximum");

    // Reopen reads the marker back; an empty re-close never regresses it.
    let mut reopened = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("reopen");
    assert_eq!(reopened.durable_commit_watermark(), Some(2));
    let publishes_before_reclose = backend.publish_count();
    reopened.close().expect("idempotent close");
    let attested = decode_wal_watermark(
        &backend
            .read_object(&watermark_object)
            .expect("watermark still present"),
    )
    .expect("watermark decodes");
    assert_eq!(
        attested, 2,
        "an empty close never moves the marker backward"
    );
    assert_eq!(
        backend.publish_count(),
        publishes_before_reclose,
        "a marker at or above the sealed maximum must not republish"
    );
}

/// Background truncation deletes covered segments on an off-lock retention
/// clone while the commit path's growth sampling scans under the lock — a
/// segment listed at the start of the scan can be gone by the time its size
/// is read. A vanished segment's bytes are already reclaimed; the sample
/// must skip it, never fail the commit-path caller.
#[test]
fn growth_sampling_skips_a_segment_deleted_between_listing_and_stat() {
    let backend = StoredWalBackend::new();
    let vanished = seed_segment(&backend, 1, &[record(1, "one")]);
    let survivor = seed_segment(&backend, 2, &[record(2, "two")]);
    seed_segment(&backend, 3, &[record(3, "three")]);
    let survivor_bytes = backend
        .object_metadata(&survivor)
        .expect("survivor size")
        .size_bytes();

    let service = WalService::open(
        &backend,
        database_id(),
        3,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(64 * 1024),
    )
    .expect("open WAL service");
    let active_size = backend
        .object_metadata(&wal_segment(3))
        .expect("active size")
        .size_bytes();

    // Freeze the race window: the listing still names segment 1, but its
    // object is gone (the off-lock truncation clone deleted it mid-scan).
    backend.set_list_order(vec![vanished.clone(), survivor.clone(), wal_segment(3)]);
    backend
        .delete_object(&vanished)
        .expect("simulate the concurrent truncation delete");

    let facts = service
        .growth_facts()
        .expect("a concurrently-deleted segment must not fail growth sampling");
    assert_eq!(
        facts.retained_segments(),
        2,
        "the vanished segment is excluded; survivor + active remain"
    );
    assert_eq!(
        facts.retained_bytes(),
        survivor_bytes + active_size,
        "retained bytes count only the survivor and the active segment"
    );
}

/// Direction control: only absence is tolerated — a stat failure that is
/// not `NotFound` still fails the sample loudly.
#[test]
fn growth_sampling_still_fails_when_segment_stat_errors_are_not_absence() {
    let backend = StoredWalBackend::new();
    seed_segment(&backend, 1, &[record(1, "one")]);
    let broken = seed_segment(&backend, 2, &[record(2, "two")]);
    seed_segment(&backend, 3, &[record(3, "three")]);

    let service = WalService::open(
        &backend,
        database_id(),
        3,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(64 * 1024),
    )
    .expect("open WAL service");
    backend.fail_metadata_for(&broken);

    let error = service
        .growth_facts()
        .expect_err("non-absence stat failures must propagate");
    assert!(
        matches!(
            &error,
            WalServiceError::Backend { operation: WalOperation::List, source, .. }
                if source.kind() == BackendErrorKind::Unavailable
        ),
        "unexpected error shape: {error:?}"
    );
}
