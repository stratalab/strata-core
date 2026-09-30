//! #3721: the link stage — the quarantine object is a durable no-clobber link
//! to the source instead of a byte copy. Every fault window must land in a
//! state the copy stage already produces (quarantine absent, or quarantine
//! present holding exactly the source's bytes), with the source deleted only
//! after the quarantine name is durable.

use super::super::super::mutation::{
    link_failure_action, quarantine_stage_mode, LinkFailureAction, QuarantineStageMode,
};
use super::*;

fn purge_request(service: &QuarantineService<'_>, branch_id: BranchId) -> QuarantinePurgeRequest {
    let token = service
        .load_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect("inventory token")
        .token();
    QuarantinePurgeRequest::new(
        branch_id,
        DATABASE_ID,
        CODEC_ID,
        QuarantineGate::Safe,
        Some(token),
    )
}

fn is_copy_publish(operation: &Operation, object: &ObjectName) -> bool {
    matches!(operation, Operation::Publish(name, PublishMode::Create) if name == object)
}

#[test]
fn quarantine_stage_mode_truth_table() {
    let without = BackendCapabilities::from_slice(&[
        BackendCapability::ReadObject,
        BackendCapability::ObjectMetadata,
        BackendCapability::DurablePublish,
        BackendCapability::DurableSync,
        BackendCapability::DeleteObject,
    ]);
    let mut with = without;
    with.insert(BackendCapability::DurableLink);
    let mut link_only = BackendCapabilities::empty();
    link_only.insert(BackendCapability::DurableLink);

    for (capabilities, expected) in [
        (without, QuarantineStageMode::Copy),
        (BackendCapabilities::empty(), QuarantineStageMode::Copy),
        (with, QuarantineStageMode::Link),
        (link_only, QuarantineStageMode::Link),
    ] {
        assert_eq!(
            quarantine_stage_mode(capabilities),
            expected,
            "{capabilities:?}"
        );
    }
}

#[test]
fn link_failure_action_truth_table() {
    const ALL_BACKEND_ERROR_KINDS: [BackendErrorKind; 14] = [
        BackendErrorKind::NotFound,
        BackendErrorKind::AlreadyExists,
        BackendErrorKind::PreconditionFailed,
        BackendErrorKind::PermissionDenied,
        BackendErrorKind::InvalidObjectName,
        BackendErrorKind::InvalidRange,
        BackendErrorKind::UnsupportedOperation,
        BackendErrorKind::CapabilityMismatch,
        BackendErrorKind::Unavailable,
        BackendErrorKind::Interrupted,
        BackendErrorKind::MetadataMismatch,
        BackendErrorKind::Corruption,
        BackendErrorKind::NoSpace,
        BackendErrorKind::Unknown,
    ];
    for kind in ALL_PUBLISH_FAILURE_KINDS {
        for source in ALL_BACKEND_ERROR_KINDS {
            // The trait default (the backend never tried).
            let never_tried = kind == PublishFailureKind::Unsupported;
            // No hard links here (EXDEV/ENOTSUP, EPERM), nothing visible.
            let cannot_link_here = kind == PublishFailureKind::FailedBeforeVisibility
                && (source == BackendErrorKind::UnsupportedOperation
                    || source == BackendErrorKind::PermissionDenied);
            // Anything that may be visible is never published over, and a
            // real pre-visibility failure is reported like a copy's.
            let expected = if never_tried || cannot_link_here {
                LinkFailureAction::FallBackToCopy
            } else {
                LinkFailureAction::Report
            };
            assert_eq!(
                link_failure_action(kind, source),
                expected,
                "{kind:?} / {source:?}"
            );
        }
    }
}

#[test]
fn link_stage_publishes_inventory_then_links_then_deletes_source_without_reading_it() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let inventory_object = inventory_object(branch_id);
    let backend = MutationBackend::durable()
        .with_link()
        .with_object(source_object.clone(), b"table-bytes");
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("quarantine object");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
    assert_eq!(report.byte_count(), 11);
    assert_eq!(backend.read_count(&source_object), 0, "no byte is read");
    assert_eq!(backend.bytes(&quarantine_object), b"table-bytes");
    assert!(!backend.contains(&source_object));
    let operations = backend.operations();
    let inventory_at = operations
        .iter()
        .position(|op| *op == Operation::Publish(inventory_object.clone(), PublishMode::Replace))
        .expect("inventory publish");
    let link_at = operations
        .iter()
        .position(|op| *op == Operation::Link(source_object.clone(), quarantine_object.clone()))
        .expect("link");
    let delete_at = operations
        .iter()
        .position(|op| *op == Operation::Delete(source_object.clone()))
        .expect("source delete");
    assert!(
        inventory_at < link_at && link_at < delete_at,
        "inventory, then link, then source delete: {operations:?}"
    );
    assert!(
        !operations
            .iter()
            .any(|op| is_copy_publish(op, &quarantine_object)),
        "the link stage writes no copy: {operations:?}"
    );
    let entries = service
        .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect("inventory")
        .inventory()
        .entries()
        .to_vec();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].byte_count(), 11);
    assert_eq!(entries[0].source_object(), &source_object);
}

#[test]
fn a_backend_without_a_durable_link_still_copies() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let backend = MutationBackend::durable().with_object(source_object.clone(), b"table-bytes");
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("quarantine object");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
    let operations = backend.operations();
    assert!(
        operations
            .iter()
            .any(|op| is_copy_publish(op, &quarantine_object)),
        "{operations:?}"
    );
    assert!(
        !operations
            .iter()
            .any(|op| matches!(op, Operation::Link(..))),
        "no link without the capability: {operations:?}"
    );
    assert_eq!(backend.read_count(&source_object), 1);
}

#[test]
fn an_unsupported_link_falls_back_to_the_copy_after_the_inventory() {
    for (kind, source) in [
        (
            PublishFailureKind::Unsupported,
            BackendErrorKind::UnsupportedOperation,
        ),
        (
            PublishFailureKind::FailedBeforeVisibility,
            BackendErrorKind::UnsupportedOperation,
        ),
        (
            PublishFailureKind::FailedBeforeVisibility,
            BackendErrorKind::PermissionDenied,
        ),
    ] {
        let branch_id = branch_id();
        let source_object = source_object();
        let quarantine_object = quarantine_object(branch_id, "table0002");
        let backend = MutationBackend::durable()
            .with_link()
            .with_object(source_object.clone(), b"table-bytes");
        backend.fail_link(quarantine_object.clone(), kind, source, false);
        let service = QuarantineService::new(&backend);

        let report = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("quarantine object");

        assert_eq!(
            report.status(),
            QuarantineObjectStatus::QuarantinedSourceDeleted,
            "{kind:?}/{source:?}"
        );
        assert_eq!(backend.bytes(&quarantine_object), b"table-bytes");
        assert!(!backend.contains(&source_object));
        let operations = backend.operations();
        let link_at = operations
            .iter()
            .position(|op| matches!(op, Operation::Link(..)))
            .expect("link attempted");
        let copy_at = operations
            .iter()
            .position(|op| is_copy_publish(op, &quarantine_object))
            .expect("copy fallback");
        assert!(link_at < copy_at, "{operations:?}");
        assert_eq!(backend.read_count(&source_object), 1);
    }
}

/// Crash window: the inventory lists the entry, the quarantine object was
/// never created. The source is intact; the purge clears the entry and a
/// fresh stage links it — the copy stage's same window and same recovery.
#[test]
fn a_link_failing_before_visibility_keeps_the_source_and_the_purge_then_restage_recovers() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let backend = MutationBackend::durable()
        .with_link()
        .with_object(source_object.clone(), b"table-bytes");
    backend.fail_link(
        quarantine_object.clone(),
        PublishFailureKind::FailedBeforeVisibility,
        BackendErrorKind::Interrupted,
        false,
    );
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("quarantine object");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinePublishFailed
    );
    assert!(backend.contains(&source_object), "the source is kept");
    assert!(!backend.contains(&quarantine_object));
    assert!(
        !backend
            .operations()
            .iter()
            .any(|op| matches!(op, Operation::Delete(_)) || is_copy_publish(op, &quarantine_object)),
        "a real link failure neither deletes nor copies: {:?}",
        backend.operations()
    );
    assert_eq!(
        service
            .reconcile_branch_quarantine(branch_id, DATABASE_ID, CODEC_ID)
            .expect("reconcile")
            .kind(),
        QuarantineReconciliationKind::MissingQuarantineObject
    );

    let purge = service
        .purge_quarantine(purge_request(&service, branch_id))
        .expect("purge");
    assert_eq!(purge.already_missing().len(), 1);
    assert!(backend.contains(&source_object));

    backend.link_failures.lock().expect("lock").clear();
    let restaged = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("restage");
    assert_eq!(
        restaged.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
    assert_eq!(backend.bytes(&quarantine_object), b"table-bytes");
    assert!(!backend.contains(&source_object));
}

/// Crash window: the link is visible but its durability is unconfirmed. Both
/// names hold the same bytes; the retry's byte comparison passes and deletes
/// the source — never a second link or copy over the visible object.
#[test]
fn a_visible_unconfirmed_link_is_uncertain_and_the_retry_deletes_the_source() {
    for kind in [
        PublishFailureKind::VisibleDurabilityUnconfirmed,
        PublishFailureKind::VisibilityUnknown,
    ] {
        let branch_id = branch_id();
        let source_object = source_object();
        let quarantine_object = quarantine_object(branch_id, "table0002");
        let backend = MutationBackend::durable()
            .with_link()
            .with_object(source_object.clone(), b"table-bytes");
        // Even an "unsupported-looking" source kind must not fall back once
        // the link may be visible.
        backend.fail_link(
            quarantine_object.clone(),
            kind,
            BackendErrorKind::UnsupportedOperation,
            true,
        );
        let service = QuarantineService::new(&backend);

        let report = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("quarantine object");

        assert_eq!(
            report.status(),
            QuarantineObjectStatus::QuarantinePublishUncertain,
            "{kind:?}"
        );
        assert!(backend.contains(&source_object));
        assert!(backend.contains(&quarantine_object));
        assert!(
            !backend
                .operations()
                .iter()
                .any(|op| is_copy_publish(op, &quarantine_object)),
            "{kind:?}: {:?}",
            backend.operations()
        );

        backend.link_failures.lock().expect("lock").clear();
        let retry = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("retry");
        assert_eq!(
            retry.status(),
            QuarantineObjectStatus::SourceDeleteRetried,
            "{kind:?}"
        );
        assert!(!backend.contains(&source_object));
        assert_eq!(backend.bytes(&quarantine_object), b"table-bytes");
    }
}

/// Crash window: linked durably, the source delete failed. The retry deletes
/// the source; the purge then reclaims the quarantine object.
#[test]
fn a_failed_source_delete_after_the_link_is_retried_then_purged() {
    let branch_id = branch_id();
    let source_object = source_object();
    let quarantine_object = quarantine_object(branch_id, "table0002");
    let backend = MutationBackend::durable()
        .with_link()
        .with_object(source_object.clone(), b"table-bytes");
    backend.fail_delete(source_object.clone());
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("quarantine object");
    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleteFailed
    );
    assert!(backend.contains(&source_object));
    assert!(backend.contains(&quarantine_object));

    backend.delete_failures.lock().expect("lock").clear();
    let retry = service
        .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
        .expect("retry");
    assert_eq!(retry.status(), QuarantineObjectStatus::SourceDeleteRetried);
    assert!(!backend.contains(&source_object));

    let purge = service
        .purge_quarantine(purge_request(&service, branch_id))
        .expect("purge");
    assert_eq!(purge.deleted().len(), 1);
    assert_eq!(purge.reclaimed_bytes(), 11);
    assert!(!backend.contains(&quarantine_object));
}

#[test]
fn a_link_stage_of_a_missing_source_fails_before_any_mutation() {
    let branch_id = branch_id();
    let source_object = source_object();
    let backend = MutationBackend::durable().with_link();
    let service = QuarantineService::new(&backend);

    assert_eq!(
        service.quarantine_object(&request(branch_id, "table0002", source_object.clone())),
        Err(QuarantineServiceError::Missing {
            object: source_object,
        })
    );
    assert!(
        !backend.operations().iter().any(|op| matches!(
            op,
            Operation::Publish(..) | Operation::Link(..) | Operation::Delete(_)
        )),
        "{:?}",
        backend.operations()
    );
}

/// #3721: a link stage sizes the source from its metadata. A metadata failure
/// other than absence is a `Metadata` error carrying the backend's own kind —
/// never folded into `Missing` — and it fails before any mutation.
#[test]
fn a_link_stage_propagates_a_non_absence_metadata_failure_before_any_mutation() {
    for kind in [
        BackendErrorKind::Interrupted,
        BackendErrorKind::PermissionDenied,
        BackendErrorKind::Corruption,
    ] {
        let branch_id = branch_id();
        let source_object = source_object();
        let backend = MutationBackend::durable()
            .with_link()
            .with_object(source_object.clone(), b"table-bytes");
        backend.fail_metadata(source_object.clone(), kind);
        let service = QuarantineService::new(&backend);

        let error = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect_err("metadata failure");

        match error {
            QuarantineServiceError::Metadata { object, source } => {
                assert_eq!(object, source_object, "{kind:?}");
                assert_eq!(source.kind(), kind);
            }
            other => panic!("{kind:?}: expected a metadata error, got {other:?}"),
        }
        assert!(backend.contains(&source_object));
        assert!(
            !backend.operations().iter().any(|op| matches!(
                op,
                Operation::Publish(..) | Operation::Link(..) | Operation::Delete(_)
            )),
            "{kind:?}: {:?}",
            backend.operations()
        );
    }
}

/// #3721: the inventory entry records the source's REAL size, read from its
/// metadata, and the link outcome is validated against that size. A stage
/// that recorded any other size would publish an entry that disagrees with
/// the quarantine object it names.
#[test]
fn a_link_stage_records_the_metadata_size_of_the_source() {
    for bytes in [&b"t"[..], &b"table-bytes"[..], &[0x5a; 4096][..]] {
        let branch_id = branch_id();
        let source_object = source_object();
        let backend = MutationBackend::durable()
            .with_link()
            .with_object(source_object.clone(), bytes);
        let service = QuarantineService::new(&backend);

        let report = service
            .quarantine_object(&request(branch_id, "table0002", source_object.clone()))
            .expect("stage");

        assert_eq!(report.byte_count(), bytes.len() as u64);
        let entries = service
            .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
            .expect("inventory")
            .inventory()
            .entries()
            .to_vec();
        assert_eq!(entries[0].byte_count(), bytes.len() as u64);
        assert_eq!(
            report
                .quarantine_publish_outcome()
                .expect("link outcome")
                .metadata()
                .size_bytes(),
            bytes.len() as u64
        );
    }
}
