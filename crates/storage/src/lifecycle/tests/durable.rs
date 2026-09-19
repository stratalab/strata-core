use super::*;
use crate::backend::memory::MemoryBackend;
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata,
    BackendRange, BackendResult, BackendWriterGuard, PublishDurability, PublishError,
    PublishFailureKind, PublishMode, PublishOutcome, PublishResult,
    DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::state::BranchLocalState;
use crate::commit::{
    CommitBatch, CommitBatchOptions, CommitBranchGeneration, CommitBranchGenerationGuard,
    CommitConflictValidationMode, CommitDuplicateKeyPolicy, CommitDurabilityClass,
    CommitDurabilityMode, CommitExpiry, CommitManualTimestampSource, CommitMutation, CommitOrigin,
    CommitRetentionHint, CommitRuntimeConfig, CommitRuntimeError, CommitStamp,
    CommitTimestampPolicy, CommitUnresolvedDurable, CommitValidationFacts,
};
use crate::config::mode::DurabilityPolicy;
use crate::format::{
    encode_manifest, encode_wal_segment_header, DatabaseManifest, WalSegmentHeader,
};
use crate::layout::ObjectLayout;
use crate::lifecycle::admission_ramp::LifecycleAdmissionMode;
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::{TableManifestService, WalServiceConfig};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use strata_core::{BranchId, CommitVersion, Timestamp};

const DATABASE_ID: [u8; 16] = [0x8e; 16];
const OTHER_DATABASE_ID: [u8; 16] = [0x8f; 16];

#[test]
fn durable_assembly_creates_manifest_opens_wal_and_remains_recovering() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x10);
    let shell =
        assemble_shell(StorageMode::DurableLocalStandard, branch, backend).expect("durable shell");

    assert_eq!(shell.state(), LifecycleState::Recovering);
    assert_eq!(
        shell.open_plan().storage_mode(),
        StorageMode::DurableLocalStandard
    );
    assert_eq!(
        shell.assembly_facts().mode(),
        StorageMode::DurableLocalStandard
    );
    assert_eq!(
        shell.assembly_facts().disposition(),
        StorageOpenDisposition::Created
    );
    assert_eq!(shell.assembly_facts().database_id(), &DATABASE_ID);
    assert_eq!(shell.assembly_facts().codec_id(), "identity");
    assert_eq!(
        shell.assembly_facts().durability_policy(),
        DurabilityPolicy::Standard
    );
    assert_eq!(shell.assembly_facts().active_wal_segment(), 1);
    assert_eq!(
        shell.assembly_facts().writer_lock_object(),
        &ObjectLayout::writer_lock().expect("writer lock")
    );
    assert_eq!(shell.assembly_facts().manifest_snapshot_watermark(), None);
    assert_eq!(shell.assembly_facts().manifest_snapshot_id(), None);
    assert_eq!(shell.assembly_facts().manifest_flush_watermark(), None);
    assert_eq!(shell.services().wal().active_segment_id(), 1);
    assert_eq!(
        shell.services().wal().durability_policy(),
        DurabilityPolicy::Standard
    );
    assert_eq!(
        shell.services().capability_outcome().storage_mode(),
        StorageMode::DurableLocalStandard
    );
    assert!(shell.branch_state().is_empty());
    assert_eq!(shell.branch_state().branch_id(), branch);
    assert_eq!(shell.visible_version(), CommitVersion::ZERO);
    assert_eq!(shell.unresolved_durable().expect("gate"), None);
    assert!(shell.admit_recovery_step().is_ok());
    assert!(shell.admit_ordinary_read().is_err());
    assert!(shell.admit_commit().is_err());
    assert!(shell.admit_ordinary_maintenance().is_err());
    assert!(shell.admit_health_query().is_ok());
    touch_shell_parts(&shell);

    let operations = backend.operations();
    assert_call_order(
        &operations,
        OperationKind::Capabilities,
        OperationKind::AcquireWriterLock,
    );
    assert_call_order(
        &operations,
        OperationKind::AcquireWriterLock,
        OperationKind::ReadObject,
    );
    assert!(operations.iter().any(|operation| {
        matches!(operation, Operation::AcquireWriterLock(object) if object == &ObjectLayout::writer_lock().expect("writer lock"))
    }));
    assert!(operations
        .iter()
        .any(|operation| matches!(operation, Operation::Publish(object, PublishMode::Create) if object == &ObjectLayout::database_manifest().expect("database object"))));
    assert!(operations
        .iter()
        .any(|operation| matches!(operation, Operation::ObjectMetadata(object) if object == &ObjectLayout::wal_segment(1).expect("segment object"))));
    // Assembly lists the five post-manifest families first — the #3015
    // missing-manifest residue check that distinguishes a new store from a
    // damaged one — then the WAL prefix twice: the #2690 segment-inventory
    // contiguity check, then the resume-segment reconciliation (#2555).
    let listed: Vec<_> = operations
        .iter()
        .filter_map(|operation| match operation {
            Operation::ListPrefix(prefix) => Some(prefix.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        listed,
        vec![
            ObjectLayout::wal_prefix().expect("wal prefix"),
            ObjectLayout::meta_prefix().expect("meta prefix"),
            ObjectLayout::table_prefix().expect("table prefix"),
            ObjectLayout::snapshot_prefix().expect("snapshot prefix"),
            ObjectLayout::quarantine_prefix().expect("quarantine prefix"),
            ObjectLayout::wal_prefix().expect("wal prefix"),
            ObjectLayout::wal_prefix().expect("wal prefix"),
        ]
    );
    assert!(backend.lock_is_held());
    drop(shell);
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_assembly_loads_existing_manifest_and_preserves_recovery_facts() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let manifest = DatabaseManifest::new(DATABASE_ID, "identity")
        .expect("database object")
        .with_recovery_facts(7, Some(44), Some(3), Some(CommitVersion::new(43)))
        .expect("recovery facts");
    backend.write_raw(
        ObjectLayout::database_manifest().expect("manifest object"),
        encode_manifest(&manifest).expect("manifest bytes"),
    );
    // #2765: the attested manifest requires its active segment on disk.
    backend.write_raw(
        ObjectLayout::wal_segment(7).expect("segment object"),
        encode_wal_segment_header(&WalSegmentHeader::new(7, DATABASE_ID)),
    );

    let shell = assemble_shell(StorageMode::DurableLocalAlways, branch_id(0x11), backend)
        .expect("durable shell");

    assert_eq!(
        shell.assembly_facts().disposition(),
        StorageOpenDisposition::OpenedExisting
    );
    assert_eq!(
        shell.assembly_facts().durability_policy(),
        DurabilityPolicy::Always
    );
    assert_eq!(shell.assembly_facts().active_wal_segment(), 7);
    assert_eq!(
        shell.assembly_facts().manifest_snapshot_watermark(),
        Some(44)
    );
    assert_eq!(shell.assembly_facts().manifest_snapshot_id(), Some(3));
    assert_eq!(
        shell.assembly_facts().manifest_flush_watermark(),
        Some(CommitVersion::new(43))
    );
    assert_eq!(shell.services().wal().active_segment_id(), 7);
    assert_eq!(
        shell.services().wal().durability_policy(),
        DurabilityPolicy::Always
    );

    let manifest_object = ObjectLayout::database_manifest().expect("manifest object");
    assert!(!backend.operations().iter().any(|operation| {
        matches!(operation, Operation::Publish(object, _) if object == &manifest_object)
    }));
}

#[test]
fn durable_request_rejects_non_durable_modes_without_backend_calls() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    assert_eq!(
        request(StorageMode::Cache, branch_id(0x12)),
        Err(LifecycleError::InvalidOpenPlan {
            reason: "durable local assembly requires durable local storage mode",
        })
    );
    assert!(backend.operations().is_empty());
}

#[test]
fn durable_request_rejects_object_durable_candidate_until_fencing_exists() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    assert_eq!(
        request(StorageMode::ObjectDurableCandidate, branch_id(0x12)),
        Err(LifecycleError::InvalidOpenPlan {
            reason:
                "object-durable mode requires fenced object publication before runtime assembly",
        })
    );
    assert!(backend.operations().is_empty());
}

#[test]
fn durable_request_rejects_codec_mismatch_before_backend_calls() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let plan = StorageOpenPlan::new(
        StorageMode::DurableLocalStandard,
        LifecycleCodecId::new("zstd").expect("codec"),
        RecoveryStrictness::Strict,
        LifecycleConfig::default(),
    )
    .expect("plan");
    let error = LifecycleDurableLocalOpenRequest::new(
        plan,
        DATABASE_ID,
        branch_id(0x13),
        CommitBranchGeneration::new(1).expect("generation"),
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        WalServiceConfig::default(),
    )
    .expect_err("WAL codec mismatch rejects");

    assert_eq!(
        error,
        LifecycleError::InvalidOpenPlan {
            reason: "durable open plan codec must match WAL codec",
        }
    );
    assert!(backend.operations().is_empty());
}

#[test]
fn durable_request_rejects_invalid_wal_config_before_backend_calls() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let error = LifecycleDurableLocalOpenRequest::new(
        open_plan(StorageMode::DurableLocalStandard),
        DATABASE_ID,
        branch_id(0x13),
        CommitBranchGeneration::new(1).expect("generation"),
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        WalServiceConfig::new(1),
    )
    .expect_err("invalid WAL config rejects");

    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "WAL service failed",
            ..
        }
    ));
    assert!(error.source().is_some());
    assert!(backend.operations().is_empty());
}

#[test]
fn durable_capability_rejection_happens_before_writer_lock() {
    let backend: &'static DurableTestBackend = crate::testkit::leak_static(
        DurableTestBackend::with_capabilities(BackendCapabilities::empty()),
    );
    let error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x14)).expect("request"),
        backend,
        timestamp_source(),
    )
    .expect_err("capability mismatch");

    assert!(matches!(error, LifecycleError::CapabilityMismatch { .. }));
    assert_eq!(backend.operation_kinds(), vec![OperationKind::Capabilities]);
}

#[test]
fn durable_writer_lock_failure_happens_before_manifest_access() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_lock_failure());
    let error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x15)).expect("request"),
        backend,
        timestamp_source(),
    )
    .expect_err("writer lock failure");

    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Backend,
            ..
        }
    ));
    assert_eq!(
        backend.operation_kinds(),
        vec![
            OperationKind::Capabilities,
            OperationKind::AcquireWriterLock
        ]
    );
}

#[test]
fn durable_manifest_identity_mismatch_rejects_before_wal_open() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let manifest = DatabaseManifest::new(OTHER_DATABASE_ID, "identity").expect("database object");
    backend.write_raw(
        ObjectLayout::database_manifest().expect("manifest object"),
        encode_manifest(&manifest).expect("manifest bytes"),
    );

    let error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x16)).expect("request"),
        backend,
        timestamp_source(),
    )
    .expect_err("manifest identity mismatch");

    assert_eq!(
        error,
        LifecycleError::InvalidOpenPlan {
            reason: "database manifest id does not match durable open request",
        }
    );
    assert!(!backend.lock_is_held());
    assert!(!backend
        .operations()
        .iter()
        .any(|operation| matches!(operation, Operation::ObjectMetadata(_))));
}

#[test]
fn durable_manifest_codec_mismatch_rejects_before_wal_open() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let manifest = DatabaseManifest::new(DATABASE_ID, "zstd").expect("database object");
    backend.write_raw(
        ObjectLayout::database_manifest().expect("database object"),
        encode_manifest(&manifest).expect("encoded object"),
    );

    let error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x16)).expect("request"),
        backend,
        timestamp_source(),
    )
    .expect_err("manifest codec mismatch");

    assert_eq!(
        error,
        LifecycleError::InvalidOpenPlan {
            reason: "database manifest codec does not match durable open request",
        }
    );
    assert!(!backend.lock_is_held());
    assert!(!backend
        .operations()
        .iter()
        .any(|operation| matches!(operation, Operation::ObjectMetadata(_))));
}

#[test]
fn durable_manifest_publish_uncertainty_preserves_source_chain() {
    for (kind, expected_reason) in [
        (
            PublishFailureKind::VisibleDurabilityUnconfirmed,
            "database manifest publish durability unconfirmed",
        ),
        (
            PublishFailureKind::VisibilityUnknown,
            "database manifest publish visibility unknown",
        ),
        (
            PublishFailureKind::FailedBeforeVisibility,
            "database manifest publish failed before visibility",
        ),
        (
            PublishFailureKind::Unsupported,
            "database manifest publish unsupported",
        ),
    ] {
        let backend: &'static DurableTestBackend =
            crate::testkit::leak_static(DurableTestBackend::with_publish_failure(kind));
        let error = LifecycleDurableLocalShell::assemble(
            request(StorageMode::DurableLocalStandard, branch_id(0x17)).expect("request"),
            backend,
            timestamp_source(),
        )
        .expect_err("publish fault should reject");

        assert!(matches!(
            error,
            LifecycleError::LowerLayer {
                layer: LifecycleLowerLayer::Service,
                reason,
                ..
            } if reason == expected_reason
        ));
        assert!(
            error.source().is_some(),
            "publish failure should retain lower-layer source"
        );
        assert!(!backend.lock_is_held());
    }
}

#[test]
fn durable_manifest_create_precondition_race_reloads_existing_manifest() {
    // #3015 renegotiation: the race can only be staged on an EMPTY store
    // now. The old staging (raced manifest attesting segment 9, with the
    // segment pre-seeded) is contract-impossible — a completed concurrent
    // creation publishes its manifest before any other durable object, so
    // WAL residue with a still-absent manifest is loss and refuses (see
    // durable_assembly_refuses_a_missing_manifest_when_durable_objects_exist).
    // The arm under test is unchanged: a create that loses the publish race
    // reloads the winner's manifest instead of erroring.
    let race_manifest = DatabaseManifest::new(DATABASE_ID, "identity").expect("database object");
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_create_race(race_manifest));

    let shell = assemble_shell(StorageMode::DurableLocalStandard, branch_id(0x18), backend)
        .expect("durable shell");

    assert_eq!(
        shell.assembly_facts().disposition(),
        StorageOpenDisposition::OpenedExisting
    );
    assert_eq!(shell.assembly_facts().manifest_flush_watermark(), None);

    let operations = backend.operations();
    assert!(operations
        .iter()
        .any(|operation| matches!(operation, Operation::Publish(_, PublishMode::Create))));
}

#[test]
fn durable_manifest_create_precondition_race_reloads_and_revalidates_identity() {
    let race_manifest =
        DatabaseManifest::new(OTHER_DATABASE_ID, "identity").expect("database object");
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_create_race(race_manifest));

    let error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x19)).expect("request"),
        backend,
        timestamp_source(),
    )
    .expect_err("race mismatch should reject");

    assert_eq!(
        error,
        LifecycleError::InvalidOpenPlan {
            reason: "database manifest id does not match durable open request",
        }
    );
    assert!(!backend.lock_is_held());
    assert!(!backend
        .operations()
        .iter()
        .any(|operation| matches!(operation, Operation::ObjectMetadata(_))));
}

#[test]
fn durable_existing_manifest_decode_failures_reject_before_wal_open() {
    let valid_manifest = DatabaseManifest::new(DATABASE_ID, "identity").expect("database object");
    let mut bad_checksum = encode_manifest(&valid_manifest).expect("encoded object");
    let checksum_byte = bad_checksum.last_mut().expect("checksum byte");
    *checksum_byte = checksum_byte.wrapping_add(1);

    let mut future_version = encode_manifest(&valid_manifest).expect("encoded object");
    future_version[4..8].copy_from_slice(&u32::MAX.to_le_bytes());

    let mut pre_v1_version = encode_manifest(&valid_manifest).expect("encoded object");
    pre_v1_version[4..8].copy_from_slice(&0_u32.to_le_bytes());

    let mut zero_active_segment = encode_manifest(&valid_manifest).expect("encoded object");
    let active_segment_offset = 4 + 4 + 16 + 4 + valid_manifest.codec_id().len();
    zero_active_segment[active_segment_offset..active_segment_offset + 8]
        .copy_from_slice(&0_u64.to_le_bytes());
    refresh_manifest_crc(&mut zero_active_segment);

    for bytes in [
        vec![0; 8],
        bad_checksum,
        future_version,
        pre_v1_version,
        zero_active_segment,
    ] {
        let backend: &'static DurableTestBackend =
            crate::testkit::leak_static(DurableTestBackend::new());
        backend.write_raw(
            ObjectLayout::database_manifest().expect("database object"),
            bytes,
        );

        let error = LifecycleDurableLocalShell::assemble(
            request(StorageMode::DurableLocalStandard, branch_id(0x1a)).expect("request"),
            backend,
            timestamp_source(),
        )
        .expect_err("invalid database object should reject");

        assert!(matches!(
            error,
            LifecycleError::LowerLayer {
                layer: LifecycleLowerLayer::Service,
                reason: "database manifest decode failed",
                ..
            }
        ));
        assert!(!backend.lock_is_held());
        assert!(!backend
            .operations()
            .iter()
            .any(|operation| matches!(operation, Operation::ObjectMetadata(_))));
    }
}

#[test]
fn durable_wal_open_failures_are_typed_and_do_not_mark_open() {
    let metadata_failure: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_metadata_failure());
    write_existing_manifest(metadata_failure, &manifest_with_active_segment(4));
    let metadata_error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x1b)).expect("request"),
        metadata_failure,
        timestamp_source(),
    )
    .expect_err("WAL metadata failure should reject");
    assert!(matches!(
        metadata_error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "WAL service failed",
            ..
        }
    ));
    assert!(metadata_error.source().is_some());
    assert!(!metadata_failure.lock_is_held());

    let publish_failure: &'static DurableTestBackend = crate::testkit::leak_static(
        DurableTestBackend::with_publish_failure(PublishFailureKind::FailedBeforeVisibility),
    );
    write_existing_manifest(publish_failure, &manifest_with_active_segment(5));
    let publish_error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x1c)).expect("request"),
        publish_failure,
        timestamp_source(),
    )
    .expect_err("WAL create failure should reject");
    assert!(matches!(
        publish_error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "WAL service failed",
            ..
        }
    ));
    assert!(publish_error.source().is_some());
    assert!(!publish_failure.lock_is_held());
}

#[test]
fn durable_wal_header_database_mismatch_rejects_existing_segment() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    write_existing_manifest(backend, &manifest_with_active_segment(6));
    let wrong_header = WalSegmentHeader::new(6, OTHER_DATABASE_ID);
    backend.write_raw(
        ObjectLayout::wal_segment(6).expect("segment object"),
        encode_wal_segment_header(&wrong_header),
    );

    let error = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x1d)).expect("request"),
        backend,
        timestamp_source(),
    )
    .expect_err("wrong segment header database should reject");

    // A WAL segment header from a different database is tamper/corruption, not a
    // transient outage; assembly refuses with a non-retryable corruption error
    // that still preserves the underlying WAL service source.
    assert!(matches!(error, LifecycleError::RecoveryCorruption { .. }));
    assert!(error.source().is_some());
    assert!(!backend.lock_is_held());
}

fn refresh_manifest_crc(bytes: &mut [u8]) {
    let checksum_offset = bytes.len() - 4;
    let crc = crc32fast::hash(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&crc.to_le_bytes());
}

#[cfg(all(feature = "localfs", unix))]
#[test]
fn durable_localfs_writer_lock_excludes_second_shell_until_drop() {
    use crate::backend::local_fs::LocalFsBackend;

    let dir = tempfile::tempdir().expect("temp dir");
    let first_backend: &'static LocalFsBackend =
        crate::testkit::leak_static(LocalFsBackend::new(dir.path()));
    let second_backend: &'static LocalFsBackend =
        crate::testkit::leak_static(LocalFsBackend::new(dir.path()));
    let first = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x1e)).expect("request"),
        first_backend,
        timestamp_source(),
    )
    .expect("first durable shell");

    let blocked = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x1e)).expect("request"),
        second_backend,
        timestamp_source(),
    )
    .expect_err("second durable shell should be blocked by writer guard");
    // Its own variant AND its own code, not an unmapped lower-layer failure:
    // contention is a precondition the caller can satisfy, never a backend
    // outage, and consumers key on the code to recognise it (#3005, #3167).
    assert_eq!(blocked.code(), "failed_precondition.lifecycle.writer_lock");
    assert!(matches!(blocked, LifecycleError::WriterLockHeld));

    drop(first);

    let second = LifecycleDurableLocalShell::assemble(
        request(StorageMode::DurableLocalStandard, branch_id(0x1e)).expect("request"),
        second_backend,
        timestamp_source(),
    )
    .expect("second durable shell after guard release");
    assert_eq!(
        second.assembly_facts().disposition(),
        StorageOpenDisposition::OpenedExisting
    );
}

#[test]
fn durable_close_syncs_log_releases_writer_guard_and_is_idempotent() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x20);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"close-sync", b"value"),
            generation_guard(),
        )
        .expect("durable commit before close");

    assert!(backend.lock_is_held());
    assert!(runtime.services().writer_guard().is_some());
    let close = runtime.close().expect("durable close");

    assert_eq!(runtime.state(), LifecycleState::Closed);
    assert_eq!(close.phase(), ClosePhase::Closed);
    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.close_fact(), Some(LifecycleCloseFact::Complete));
    assert!(close.commits_quiesced());
    assert!(close.maintenance_drained());
    assert!(close.durable_synced());
    assert!(close.guards_released());
    assert_eq!(close.stats().close_attempts(), 1);
    assert_eq!(close.stats().maintenance_tasks(), 0);
    assert!(!backend.lock_is_held());
    assert!(runtime.services().writer_guard().is_none());
    assert!(backend
        .operations()
        .iter()
        .any(|operation| matches!(operation, Operation::SyncObject(_))));

    let operations_after_first_close = backend.operations().len();
    let second = runtime.close().expect("idempotent close");
    assert_eq!(second.status(), CloseOutcomeStatus::Idempotent);
    assert_eq!(second.close_fact(), Some(LifecycleCloseFact::AlreadyClosed));
    assert!(second.prior_final());
    assert_eq!(backend.operations().len(), operations_after_first_close);
    // Idempotent close must surface the same stats as the first close —
    // a retry that fabricates `(0, 0, 0, 0, 1)` would drift from the
    // observable record. The cached prior-close outcome guarantees this.
    assert_eq!(second.stats(), close.stats());
}

#[test]
fn durable_close_calls_wal_close_in_always_mode() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x24);
    let mut runtime = open_runtime(StorageMode::DurableLocalAlways, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch_with_mode(
                branch,
                b"always-close",
                b"value",
                CommitDurabilityMode::Always,
            ),
            generation_guard(),
        )
        .expect("durable commit before close");
    let close = runtime.close().expect("durable close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert!(close.durable_synced());
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_close_does_not_report_complete_with_unresolved_durable_gate() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x25);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let unresolved = CommitUnresolvedDurable::durable_not_applied_with_facts(
        CommitStamp::new(branch, CommitVersion::new(4), Timestamp::from_micros(8_004))
            .expect("stamp"),
        CommitDurabilityClass::Standard,
        "seed unresolved durable fact",
    )
    .expect("unresolved fact");
    runtime
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("record unresolved");
    let operations_before_close = backend.operations().len();

    let error = runtime
        .close()
        .expect_err("unresolved durable blocks close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    assert_eq!(error.code(), "failed_precondition.lifecycle.close");
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert!(backend.lock_is_held());
    assert!(!close_operations
        .iter()
        .any(|operation| matches!(operation, Operation::SyncObject(_))));
}

/// BS2.2 deliberate behavior change (durable mirror of the cache test): a row applied above
/// `visible` — the `applied_not_visible` publish-failure state — is hidden from the bounded
/// runtime Latest read while the gate blocks follow-on commits.
#[test]
fn durable_bounded_latest_read_hides_applied_not_visible_row_while_gate_blocks_commits() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x27);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // A normally committed row: `visible` covers it.
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"vis-acked", b"value"),
            generation_guard(),
        )
        .expect("acked durable commit");
    assert_eq!(runtime.visible_version(), CommitVersion::new(1));

    // Recreate the applied-not-visible shape: trip the gate, then apply a row above `visible`.
    let hidden_version = CommitVersion::new(2);
    let unresolved = CommitUnresolvedDurable::applied_not_visible(
        CommitStamp::new(branch, hidden_version, Timestamp::from_micros(2_000)).expect("stamp"),
        CommitDurabilityClass::Standard,
        "test: visible publication failed after apply",
    )
    .expect("unresolved fact");
    runtime
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("trip the unresolved gate");
    let generation = runtime
        .branch_catalog()
        .registry()
        .lookup(branch)
        .expect("branch lookup")
        .generation();
    let hidden_key = physical_key(branch, b"vis-hidden");
    runtime
        .branch_catalog_mut_for_test()
        .branch_state_mut(branch, CommitBranchGenerationGuard::exact(generation))
        .expect("branch state")
        .append_committed_row(StorageRow::put(
            hidden_key.clone(),
            hidden_version,
            Timestamp::from_micros(2_000),
            Timestamp::EPOCH,
            b"hidden-value".to_vec(),
        ))
        .expect("apply row above visible");
    // BS2.3: this test mutates branch state directly (bypassing the commit publish); resync.
    runtime.publish_branch_snapshot_for_test(branch);

    // The bounded Latest point read hides the unacknowledged row; acked rows stay served.
    assert!(runtime
        .read_latest_point_or_tombstone_for_branch(branch, &hidden_key)
        .expect("bounded point read")
        .is_none());
    let acked = runtime
        .read_latest_point_or_tombstone_for_branch(branch, &physical_key(branch, b"vis-acked"))
        .expect("bounded point read")
        .expect("acked row stays visible");
    assert_eq!(acked.row().commit_version(), CommitVersion::new(1));
    let bounds = crate::branch::read::BranchScanBounds::prefix(&physical_key(branch, b"vis-"));
    let scanned = runtime
        .scan_latest_including_tombstones_for_branch(branch, &bounds, None)
        .expect("bounded scan");
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].row().physical_key().user_key(), b"vis-acked");

    // The gate blocks the next mutating commit at the runtime level.
    let error = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"vis-blocked", b"blocked"),
            generation_guard(),
        )
        .expect_err("unresolved gate blocks the follow-on commit");
    let LifecycleError::LowerLayer {
        layer: LifecycleLowerLayer::CommitRuntime,
        source: Some(source),
        ..
    } = error
    else {
        panic!("expected commit-runtime lower-layer error, got {error:?}");
    };
    let commit_error = source
        .downcast_ref::<CommitRuntimeError>()
        .expect("commit runtime source");
    assert!(matches!(
        commit_error,
        CommitRuntimeError::UnresolvedDurableCommit { .. }
    ));
}

#[test]
fn durable_close_does_not_truncate_wal_prune_snapshots_or_purge_quarantine_implicitly() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x26);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"close-no-retention", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let operations_before_close = backend.operations().len();

    runtime.close().expect("durable close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    assert!(close_operations
        .iter()
        .any(|operation| matches!(operation, Operation::SyncObject(_))));
    assert!(!close_operations
        .iter()
        .any(|operation| matches!(operation, Operation::DeleteObject(_))));
    assert!(!close_operations
        .iter()
        .any(|operation| matches!(operation, Operation::ListPrefix(_))));
    // The only close-time publish is the #2690 durable commit watermark
    // attesting the synced log; close never rewrites checkpoints, manifests,
    // snapshots, or quarantine state.
    let watermark = ObjectLayout::wal_watermark().expect("watermark object");
    assert!(!close_operations.iter().any(|operation| {
        matches!(operation, Operation::Publish(object, _) if object != &watermark)
    }));
}

#[test]
fn durable_reopen_can_acquire_writer_guard_after_close() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x27);
    let mut first = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    assert!(backend.lock_is_held());

    first.close().expect("first close");
    assert!(!backend.lock_is_held());

    let second = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    assert_eq!(second.state(), LifecycleState::Open);
    assert!(backend.lock_is_held());
}

#[test]
fn second_durable_runtime_can_open_after_first_clean_close() {
    durable_reopen_can_acquire_writer_guard_after_close();
}

#[test]
fn durable_close_calls_wal_close_in_standard_mode() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x29);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"standard-close", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    let close = runtime.close().expect("durable close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert!(close.durable_synced());
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_close_wal_close_failure_returns_typed_source_chain() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_sync_failure());
    let branch = branch_id(0x2a);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"wal-close-failure", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    let error = runtime.close().expect_err("WAL close failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    assert_eq!(runtime.state(), LifecycleState::Closing);
}

#[test]
fn durable_close_wal_sync_uncertain_returns_retry_pending() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_sync_failure());
    let branch = branch_id(0x2b);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"wal-sync-uncertain", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    let error = runtime.close().expect_err("sync failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert_eq!(runtime.state(), LifecycleState::Closing);
}

#[test]
fn durable_close_does_not_release_writer_guard_before_sync_failure() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_sync_failure());
    let branch = branch_id(0x2c);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"sync-before-release", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    let error = runtime.close().expect_err("sync failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(backend.lock_is_held());
    assert_eq!(backend.release_count(), 0);
}

#[test]
fn durable_close_releases_writer_guard_after_sync() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x2d);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"release-after-sync", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    let close = runtime.close().expect("durable close");

    assert!(close.durable_synced());
    assert!(close.guards_released());
    assert_eq!(backend.release_count(), 1);
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_double_close_does_not_double_release_writer_guard() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x2e);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.close().expect("first close");
    let releases_after_first = backend.release_count();

    let second = runtime.close().expect("second close");

    assert_eq!(second.status(), CloseOutcomeStatus::Idempotent);
    assert_eq!(backend.release_count(), releases_after_first);
}

#[test]
fn durable_close_reports_typed_error_when_writer_guard_is_missing_at_release() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x52);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    assert!(runtime.release_writer_guard_for_test());

    let error = runtime.close().expect_err("missing guard rejects close");

    assert_eq!(error.code(), "failed_precondition.lifecycle.close");
    assert!(matches!(error, LifecycleError::CloseFailed { reason }
            if reason == "writer guard was already released before close completed"));
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_failed_close_keeps_guard_when_retry_requires_it() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_sync_failure());
    let branch = branch_id(0x2f);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"failed-close-keeps-guard", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    assert!(runtime.close().is_err());

    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert!(backend.lock_is_held());
    assert!(runtime.services().writer_guard().is_some());
}

#[test]
fn durable_retry_after_release_does_not_use_released_guard() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x30);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.close().expect("first close");
    let operations_after_first = backend.operations().len();
    backend.set_sync_failure(true);

    let second = runtime.close().expect("second close");

    assert_eq!(second.status(), CloseOutcomeStatus::Idempotent);
    assert_eq!(backend.operations().len(), operations_after_first);
    assert_eq!(backend.release_count(), 1);
}

#[test]
fn double_close_after_success_does_not_touch_backend() {
    durable_retry_after_release_does_not_use_released_guard();
}

#[test]
fn durable_close_skips_manifest_write_when_no_final_fact_dirty() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x32);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let operations_before_close = backend.operations().len();

    runtime.close().expect("durable close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    assert!(!close_operations
        .iter()
        .any(|operation| matches!(operation, Operation::Publish(_, _))));
}

#[test]
fn durable_close_force_syncs_manifest_when_health_changed() {
    // V1 lifecycle health is *not* persisted in the manifest payload (see
    // `force_final_manifest_fsync_on_health_change` doc). What we assert
    // here is the durability tighten the hook is responsible for: when
    // recovery health degraded during the session, close re-publishes the
    // existing manifest at PublishMode::Replace to force one final
    // backend fsync before the writer guard releases. The republished
    // bytes are byte-identical to the bytes that were loaded — the value
    // of the operation is the durable barrier, not a new payload.
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x33);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let health = close_health_debt();
    runtime.record_recovery_health_for_test(&health);

    let manifest_before = backend
        .last_manifest_bytes()
        .expect("manifest present after open");
    let operations_before_close = backend.operations().len();
    let close = runtime.close().expect("close with manifest fsync");
    let close_operations = backend.operations()[operations_before_close..].to_vec();
    let manifest_after = backend
        .last_manifest_bytes()
        .expect("manifest still present after close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.stats().maintenance_tasks(), 0);
    let publish_count = close_operations
        .iter()
        .filter(|op| matches!(op, Operation::Publish(_, PublishMode::Replace)))
        .count();
    assert_eq!(
        publish_count, 1,
        "force-fsync should issue exactly one manifest republish"
    );
    assert_eq!(
        manifest_before, manifest_after,
        "V1 manifest payload must not change across a force-fsync — health is not a manifest field"
    );
}

#[test]
fn durable_close_manifest_publish_failure_returns_typed_source_chain() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x34);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let health = close_health_debt();
    runtime.record_recovery_health_for_test(&health);
    backend.set_publish_failure(Some(PublishFailureKind::FailedBeforeVisibility));

    let error = runtime.close().expect_err("final fact publish failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn durable_close_after_checkpoint_does_not_rewrite_checkpoint_without_dirty_fact() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x35);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"checkpoint-before-close", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let checkpoint = runtime
        .checkpoint(&checkpoint_request(branch, 1))
        .expect("checkpoint");
    assert_eq!(checkpoint.status(), LifecycleCheckpointStatus::Completed);
    let operations_before_close = backend.operations().len();

    runtime.close().expect("durable close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    // The #2690 commit watermark is the only close-time publish; the
    // checkpoint itself is never rewritten without a dirty fact.
    let watermark = ObjectLayout::wal_watermark().expect("watermark object");
    assert!(!close_operations.iter().any(|operation| {
        matches!(operation, Operation::Publish(object, _) if object != &watermark)
    }));
}

#[test]
fn durable_close_after_flush_does_not_advance_flush_watermark_unless_checkpointed() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x36);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"flush-before-close", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    let flush = runtime
        .flush_frozen(&flush_request(branch, "close-flush"))
        .expect("flush frozen");
    assert!(flush.completed());
    let operations_before_close = backend.operations().len();

    runtime.close().expect("durable close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    // The #2690 commit watermark replace is close's only publish; the flush
    // watermark itself must not advance without a checkpoint.
    let watermark = ObjectLayout::wal_watermark().expect("watermark object");
    assert!(!close_operations.iter().any(|operation| {
        matches!(
            operation,
            Operation::Publish(object, PublishMode::Replace) if object != &watermark
        )
    }));
}

#[test]
fn durable_commit_schedules_flush_when_post_commit_pressure_suggests_it() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x6c);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"scheduled-flush-a", b"value-a"),
            generation_guard(),
        )
        .expect("first durable commit");
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);

    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    assert!(runtime.storage_pressure().suggested_task().is_some());

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"scheduled-flush-b", b"value-b"),
            generation_guard(),
        )
        .expect("second durable commit");

    let status = runtime.maintenance_status();
    assert_eq!(status.pending_tasks(), 1);
    assert_eq!(status.stats().enqueued(), 1);
}

#[test]
fn durable_post_commit_schedules_compaction_even_while_a_flush_is_suggested() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x6d);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // Build an L0 backlog at the compaction trigger (four flushed L0 tables)...
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 4);
    // ...then leave a frozen memtable so the single flush-first suggested task is a flush.
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"post-compact-frozen", b"value"),
            generation_guard(),
        )
        .expect("frozen-seed commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate to freeze");

    // Precondition: the single suggested task is the flush (frozen wins the cascade), so
    // before Lever A nothing would have scheduled the L0 compaction on the source branch.
    let pressure = runtime.storage_pressure();
    assert_eq!(pressure.frozen_tables(), 1);
    assert_eq!(pressure.level_zero_tables(), 4);
    assert_eq!(
        pressure.suggested_task().map(MaintenanceTaskRequest::kind),
        Some(MaintenanceTaskKind::Flush)
    );

    // Post-commit scheduling now enqueues the compaction independently of the flush;
    // the rewrite dispatcher finds it (it returns `None` when nothing is queued).
    let _ = runtime.schedule_post_commit_maintenance_for_test(branch);
    let outcome = runtime
        .run_next_table_rewrite_maintenance()
        .expect("run table rewrite")
        .expect("a compaction task was queued");
    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Compaction);
}

#[test]
fn durable_compaction_runs_with_a_frozen_table_below_the_blocking_threshold() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x6e);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // L0 backlog at the trigger, plus a single frozen memtable (below the blocking
    // threshold of 4).
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 4);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"a3-frozen", b"value"),
            generation_guard(),
        )
        .expect("frozen-seed commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate to freeze");
    assert_eq!(runtime.storage_pressure().frozen_tables(), 1);
    assert_eq!(runtime.storage_pressure().level_zero_tables(), 4);

    // Post-commit enqueues the compaction (A.1); with one frozen table it is below the
    // blocking threshold, so the compaction RUNS instead of flush-preempting (A.3).
    let _ = runtime.schedule_post_commit_maintenance_for_test(branch);
    let outcome = runtime
        .run_next_table_rewrite_maintenance()
        .expect("run table rewrite")
        .expect("a compaction task was queued");
    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Compaction);
    assert!(
        !crate::lifecycle::compaction::table_rewrite_outcome_was_flush_preempted(&outcome),
        "compaction must run (not flush-preempt) with frozen below the blocking threshold"
    );
    // The L0 backlog was compacted down (L0->L1), so fewer L0 tables remain.
    assert!(runtime.storage_pressure().level_zero_tables() < 4);
}

#[test]
fn durable_post_commit_coverage_discovers_quiet_branch_flush_backlog() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let active = branch_id(0x8a);
    let quiet = branch_id(0x8b);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, active, backend);
    runtime
        .create_branch(
            quiet,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create quiet branch");

    runtime
        .execute_durable_commit(
            durable_put_batch(quiet, b"durable-quiet-flush-seed", b"value"),
            generation_guard(),
        )
        .expect("quiet durable commit");
    runtime
        .rotate_active_for_branch_for_maintenance(quiet)
        .expect("rotate quiet branch");
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);

    runtime
        .execute_durable_commit(
            durable_put_batch(active, b"durable-coverage-trigger", b"value"),
            generation_guard(),
        )
        .expect("active durable commit");

    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
    let flush = runtime
        .run_next_flush_maintenance()
        .expect("run coverage flush")
        .expect("flush outcome");
    assert_eq!(flush.task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(
        flush.task_scope(),
        Some(MaintenanceTaskScope::Branch(quiet))
    );
    assert_eq!(flush.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        runtime
            .branch_catalog()
            .branch_state(quiet)
            .expect("quiet branch")
            .frozen_table_count(),
        0
    );
}

#[test]
fn durable_post_commit_coverage_runs_quiet_branch_flushes_in_deterministic_order() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let active = branch_id(0x92);
    let quiet_high = branch_id(0x94);
    let quiet_low = branch_id(0x93);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, active, backend);
    for (branch, seed) in [
        (quiet_high, b"durable-coverage-order-high".as_slice()),
        (quiet_low, b"durable-coverage-order-low".as_slice()),
    ] {
        runtime
            .create_branch(
                branch,
                CommitBranchGeneration::new(1).expect("generation"),
                None,
            )
            .expect("create quiet branch");
        let quiet_state = runtime
            .branch_catalog_mut_for_test()
            .branch_state_mut(branch, generation_guard())
            .expect("quiet branch state");
        quiet_state
            .append_committed_rows_atomically(vec![active_pressure_put_row(
                branch, seed, 1, 1_000, 32, 0x92,
            )])
            .expect("append quiet row");
        quiet_state.rotate_active();
    }
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);

    runtime
        .execute_durable_commit(
            durable_put_batch(active, b"durable-coverage-order-trigger", b"value"),
            generation_guard(),
        )
        .expect("active durable commit");

    assert_eq!(runtime.maintenance_status().pending_tasks(), 2);
    let first = runtime
        .run_next_flush_maintenance()
        .expect("run first coverage flush")
        .expect("first flush outcome");
    let second = runtime
        .run_next_flush_maintenance()
        .expect("run second coverage flush")
        .expect("second flush outcome");

    assert_eq!(
        first.task_scope(),
        Some(MaintenanceTaskScope::Branch(quiet_low))
    );
    assert_eq!(
        second.task_scope(),
        Some(MaintenanceTaskScope::Branch(quiet_high))
    );
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_maintenance_coverage_perf_trace_records_scan_enqueue_and_idle_stops() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let active = branch_id(0x8c);
    let quiet = branch_id(0x8d);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, active, backend);
    runtime
        .create_branch(
            quiet,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create quiet branch");
    runtime
        .execute_durable_commit(
            durable_put_batch(quiet, b"durable-coverage-counter-seed", b"value"),
            generation_guard(),
        )
        .expect("quiet durable commit");
    runtime
        .rotate_active_for_branch_for_maintenance(quiet)
        .expect("rotate quiet branch");

    crate::observability::perf_trace::reset();
    runtime
        .execute_durable_commit(
            durable_put_batch(active, b"durable-coverage-counter-trigger", b"value"),
            generation_guard(),
        )
        .expect("active durable commit");
    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.lifecycle_maintenance_coverage_scans(), 1);
    assert_eq!(perf.lifecycle_maintenance_coverage_branches_scanned(), 2);
    assert_eq!(
        perf.lifecycle_maintenance_coverage_quiet_branches_with_pressure(),
        1
    );
    assert_eq!(perf.lifecycle_maintenance_coverage_tasks_enqueued(), 1);
    assert_eq!(perf.lifecycle_maintenance_coverage_tasks_coalesced(), 0);
    assert_eq!(
        perf.lifecycle_maintenance_coverage_idle_rounds_consumed(),
        0
    );

    runtime
        .run_next_flush_maintenance()
        .expect("run coverage flush")
        .expect("flush outcome");
    crate::observability::perf_trace::reset();
    for index in 0..6 {
        let key: &'static [u8] = match index {
            0 => b"durable-coverage-idle-0",
            1 => b"durable-coverage-idle-1",
            2 => b"durable-coverage-idle-2",
            3 => b"durable-coverage-idle-3",
            4 => b"durable-coverage-idle-4",
            _ => b"durable-coverage-idle-5",
        };
        runtime
            .execute_durable_commit(durable_put_batch(active, key, b"value"), generation_guard())
            .expect("idle durable commit");
    }
    let idle = crate::observability::perf_trace::snapshot();
    assert_eq!(idle.lifecycle_maintenance_coverage_scans(), 6);
    assert_eq!(idle.lifecycle_maintenance_coverage_branches_scanned(), 12);
    assert_eq!(
        idle.lifecycle_maintenance_coverage_idle_rounds_consumed(),
        5
    );
    assert_eq!(idle.lifecycle_maintenance_coverage_stop_no_pressure(), 4);
    assert_eq!(idle.lifecycle_maintenance_coverage_stop_idle_limit(), 1);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_maintenance_coverage_queue_full_records_stop_without_failing_commit() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let active = branch_id(0x8e);
    let quiet = branch_id(0x8f);
    let config = LifecycleConfig::new(
        1,
        64,
        LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
        LifecycleLossyRecoveryPolicy::Disabled,
    )
    .expect("single-slot maintenance queue config");
    let mut runtime =
        open_runtime_with_config(StorageMode::DurableLocalStandard, active, backend, config);
    runtime
        .create_branch(
            quiet,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create quiet branch");
    runtime
        .execute_durable_commit(
            durable_put_batch(quiet, b"durable-coverage-queue-full-seed", b"value"),
            generation_guard(),
        )
        .expect("quiet durable commit");
    runtime
        .rotate_active_for_branch_for_maintenance(quiet)
        .expect("rotate quiet branch");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::health_collection())
        .expect("fill maintenance queue");

    crate::observability::perf_trace::reset();
    runtime
        .execute_durable_commit(
            durable_put_batch(active, b"durable-coverage-queue-full-trigger", b"value"),
            generation_guard(),
        )
        .expect("active durable commit");
    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(perf.lifecycle_maintenance_coverage_scans(), 1);
    assert_eq!(perf.lifecycle_maintenance_coverage_branches_scanned(), 2);
    assert_eq!(
        perf.lifecycle_maintenance_coverage_quiet_branches_with_pressure(),
        1
    );
    assert_eq!(perf.lifecycle_maintenance_coverage_tasks_enqueued(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_tasks_coalesced(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_stop_queue_full(), 1);
    assert_eq!(perf.lifecycle_maintenance_coverage_stop_failure(), 0);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);

    let key = physical_key(active, b"durable-coverage-queue-full-trigger");
    assert!(
        runtime
            .read_view_for_branch(active)
            .expect("read view")
            .latest(&key)
            .expect("latest read")
            .is_some(),
        "coverage queue pressure must not roll back the successful commit"
    );
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_maintenance_coverage_closing_state_records_failure_without_enqueue() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let active = branch_id(0x90);
    let quiet = branch_id(0x91);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, active, backend);
    runtime
        .create_branch(
            quiet,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create quiet branch");
    runtime
        .execute_durable_commit(
            durable_put_batch(quiet, b"durable-coverage-closing-seed", b"value"),
            generation_guard(),
        )
        .expect("quiet durable commit");
    runtime
        .rotate_active_for_branch_for_maintenance(quiet)
        .expect("rotate quiet branch");
    runtime
        .force_close_requested_for_test()
        .expect("force closing state");

    crate::observability::perf_trace::reset();
    let _ = runtime.schedule_post_commit_maintenance_for_test(active);
    let perf = crate::observability::perf_trace::snapshot();

    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_scans(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_tasks_enqueued(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_tasks_coalesced(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_stop_queue_full(), 0);
    assert_eq!(perf.lifecycle_maintenance_coverage_stop_failure(), 1);
}

#[test]
fn durable_coalesced_flush_task_drains_all_currently_frozen_tables() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x6f);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"durable-drain-a", b"value-a"),
            generation_guard(),
        )
        .expect("first durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate first frozen table");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"durable-drain-b", b"value-b"),
            generation_guard(),
        )
        .expect("second durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate second frozen table");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"durable-drain-c", b"value-c"),
            generation_guard(),
        )
        .expect("third durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate third frozen table");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"durable-drain-d", b"value-d"),
            generation_guard(),
        )
        .expect("fourth durable commit");

    let status = runtime.maintenance_status();
    assert_eq!(status.pending_tasks(), 1);
    assert_eq!(status.stats().enqueued(), 1);
    assert_eq!(status.stats().coalesced(), 2);
    assert_eq!(runtime.branch_state().frozen_table_count(), 3);

    let flush = runtime
        .run_next_flush_maintenance()
        .expect("run flush maintenance")
        .expect("flush task");

    assert_eq!(flush.task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(flush.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(flush.stats().maintenance_tasks(), 4);
    assert_eq!(flush.affected_objects(), 4);
    assert!(flush.source_error().is_none());
    assert!(flush.recovery_health().is_none());
    assert_eq!(runtime.branch_state().frozen_table_count(), 0);
    assert_eq!(runtime.branch_state().owned_levels()[0].len(), 4);
    assert!(
        backend
            .operations()
            .iter()
            .filter(|operation| matches!(operation, Operation::Publish(_, PublishMode::Create)))
            .count()
            >= 3
    );
}

#[test]
fn durable_close_drain_flush_publishes_table_manifest() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x70);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"close-drain-flush", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate frozen table");
    runtime
        .enqueue_maintenance(drain_task(
            MaintenanceTaskKind::Flush,
            MaintenanceTaskScope::Branch(branch),
            MaintenanceTaskPriority::Normal,
        ))
        .expect("enqueue close-drain flush");

    let close = runtime.close().expect("close drains flush");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.stats().maintenance_tasks(), 1);
    assert_eq!(runtime.branch_state().frozen_table_count(), 0);
    assert_eq!(runtime.branch_state().owned_levels()[0].len(), 1);
    let manifest = TableManifestService::new(backend)
        .load_required(branch)
        .expect("table manifest");
    assert_eq!(manifest.levels().len(), 1);
    assert_eq!(manifest.levels()[0].tables().len(), 1);
}

#[test]
fn durable_commit_respects_disabled_post_commit_maintenance_scheduling() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x6d);
    let config = LifecycleConfig::default()
        .with_maintenance_scheduling_policy(LifecycleMaintenanceSchedulingPolicy::Disabled)
        .expect("disable maintenance scheduling");
    let mut runtime =
        open_runtime_with_config(StorageMode::DurableLocalStandard, branch, backend, config);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"disabled-scheduled-flush-a", b"value-a"),
            generation_guard(),
        )
        .expect("first durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    assert!(runtime.storage_pressure().suggested_task().is_some());

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"disabled-scheduled-flush-b", b"value-b"),
            generation_guard(),
        )
        .expect("second durable commit");

    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(runtime.maintenance_status().stats().enqueued(), 0);
}

#[test]
fn durable_commit_blocks_when_recovery_health_is_unsafe() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x7e);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.record_recovery_health_for_test(&data_loss_health_debt());
    let before_visible = runtime.visible_version();

    let error = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"unsafe-recovery-health", b"value"),
            generation_guard(),
        )
        .expect_err("unsafe recovery health blocks mutating commit");

    assert!(matches!(
        error,
        LifecycleError::StoragePressureRejected {
            branch_id: rejected_branch,
            severity: LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            pressure_reason: LifecycleStoragePressureReason::None,
            retryable: false,
            ..
        } if rejected_branch == branch
    ));
    assert_eq!(runtime.last_write_admission(), None);
    assert_eq!(runtime.visible_version(), before_visible);
    assert!(
        runtime
            .read_view()
            .expect("read view")
            .latest(&physical_key(branch, b"unsafe-recovery-health"))
            .expect("latest read")
            .is_none(),
        "commit rejected by recovery health must not append rows"
    );
}

#[test]
fn durable_commit_allows_telemetry_degraded_recovery_health() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x7f);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.record_recovery_health_for_test(&close_health_debt());

    let outcome = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"telemetry-recovery-health", b"value"),
            generation_guard(),
        )
        .expect("telemetry health debt does not block mutating commit admission");

    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(1)));
    assert!(runtime.last_write_admission().is_some());
    let row = runtime
        .read_view()
        .expect("read view")
        .latest(&physical_key(branch, b"telemetry-recovery-health"))
        .expect("latest read")
        .expect("visible row");
    assert_eq!(row.row().value(), b"value");
}

#[test]
fn durable_unresolved_gate_rejection_takes_precedence_over_blocking_pressure() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x82);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 36);
    assert_eq!(
        runtime.storage_pressure().severity(),
        LifecycleStoragePressureSeverity::BlockMutatingAdmission
    );
    let unresolved = CommitUnresolvedDurable::durable_not_applied_with_facts(
        CommitStamp::new(branch, CommitVersion::new(4), Timestamp::from_micros(8_004))
            .expect("stamp"),
        CommitDurabilityClass::Standard,
        "seed unresolved durable fact",
    )
    .expect("unresolved fact");
    runtime
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("record unresolved");

    let error = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"unresolved-before-pressure", b"value"),
            generation_guard(),
        )
        .expect_err("unresolved gate rejects before pressure policy");

    let LifecycleError::LowerLayer {
        layer: LifecycleLowerLayer::CommitRuntime,
        source: Some(source),
        ..
    } = error
    else {
        panic!("expected unresolved durable commit runtime error, got {error:?}");
    };
    let commit_error = source
        .downcast_ref::<CommitRuntimeError>()
        .expect("commit runtime source");
    assert!(matches!(
        commit_error,
        CommitRuntimeError::UnresolvedDurableCommit {
            branch_id: rejected_branch,
            commit_version,
            ..
        } if *rejected_branch == branch && *commit_version == CommitVersion::new(4)
    ));
    assert_eq!(runtime.last_write_admission(), None);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_unresolved_rejection_under_pressure_keeps_pressure_counters_separate() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x86);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 36);
    let unresolved = CommitUnresolvedDurable::durable_not_applied_with_facts(
        CommitStamp::new(branch, CommitVersion::new(4), Timestamp::from_micros(8_004))
            .expect("stamp"),
        CommitDurabilityClass::Standard,
        "seed unresolved durable fact",
    )
    .expect("unresolved fact");
    runtime
        .durable_gate()
        .record_unresolved(unresolved)
        .expect("record unresolved");
    crate::observability::perf_trace::reset();

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"unresolved-pressure-counters", b"value"),
            generation_guard(),
        )
        .expect_err("unresolved gate rejects before pressure policy");

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 1);
    assert_eq!(perf.commit_unresolved_gate_admission_acquired(), 0);
    assert_eq!(perf.commit_unresolved_gate_rejected_unresolved(), 1);
    assert_eq!(perf.commit_branch_guard_attempts(), 0);
    assert_eq!(perf.lifecycle_write_admission_evaluations(), 0);
    assert_eq!(perf.lifecycle_write_admission_pressure_rejects(), 0);
}

#[test]
fn durable_branch_guard_rejection_takes_precedence_over_blocking_pressure() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x87);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 36);
    assert_eq!(
        runtime.storage_pressure().severity(),
        LifecycleStoragePressureSeverity::BlockMutatingAdmission
    );
    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active branch guard");

    let error = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"guard-before-pressure", b"value"),
            generation_guard(),
        )
        .expect_err("active guard rejects before pressure policy");

    assert!(matches!(
        commit_runtime_source(&error),
        CommitRuntimeError::BranchGuardUnavailable {
            branch_id: rejected_branch,
            ..
        } if *rejected_branch == branch
    ));
    assert_eq!(runtime.last_write_admission(), None);
    drop(guard);
}

#[cfg(feature = "perf-trace")]
#[test]
fn durable_branch_guard_rejection_under_pressure_keeps_pressure_counters_separate() {
    let _capture = crate::observability::perf_trace::begin_test_capture();
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x88);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 36);
    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active branch guard");
    crate::observability::perf_trace::reset();

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"guard-pressure-counters", b"value"),
            generation_guard(),
        )
        .expect_err("active guard rejects before pressure policy");

    let perf = crate::observability::perf_trace::snapshot();
    assert_eq!(perf.commit_unresolved_gate_admission_attempts(), 0);
    assert_eq!(perf.commit_branch_guard_attempts(), 1);
    assert_eq!(perf.commit_branch_guard_acquired(), 0);
    assert_eq!(perf.commit_branch_guard_rejected(), 1);
    assert_eq!(perf.lifecycle_write_admission_evaluations(), 0);
    assert_eq!(perf.lifecycle_write_admission_pressure_rejects(), 0);
    drop(guard);
}

#[test]
fn cache_and_durable_l0_pressure_facts_diverge_for_equivalent_source_shapes() {
    for (index, table_count, expected_durable_severity) in [
        (0_u8, 4_usize, LifecycleStoragePressureSeverity::Background),
        (1, 20, LifecycleStoragePressureSeverity::Urgent),
        (
            2,
            36,
            LifecycleStoragePressureSeverity::BlockMutatingAdmission,
        ),
    ] {
        let branch = branch_id(0x83 + index);
        let cache_backend: &'static MemoryBackend =
            crate::testkit::leak_static(MemoryBackend::new());
        let mut cache = open_cache_runtime(branch, cache_backend);
        build_cache_l0_tables_with_scheduled_flushes(&mut cache, branch, table_count);

        let durable_backend: &'static DurableTestBackend =
            crate::testkit::leak_static(DurableTestBackend::new());
        let mut durable = open_runtime(StorageMode::DurableLocalStandard, branch, durable_backend);
        build_durable_l0_tables_with_scheduled_flushes(&mut durable, branch, table_count);

        let cache_pressure = cache.storage_pressure();
        let durable_pressure = durable.storage_pressure();

        // Durable mode is unchanged: equivalent L0 source shapes still raise
        // level-zero backlog pressure with escalating severity.
        assert_eq!(durable_pressure.severity(), expected_durable_severity);
        assert_eq!(
            durable_pressure.reason(),
            LifecycleStoragePressureReason::LevelZeroTableBacklog
        );

        // Cache is volatile in-memory storage: it neutralizes source-shape
        // write-admission pressure regardless of the same L0 backlog.
        assert_eq!(
            cache_pressure.severity(),
            LifecycleStoragePressureSeverity::None
        );
        assert_eq!(
            cache_pressure.reason(),
            LifecycleStoragePressureReason::None
        );
        assert!(cache_pressure.suggested_task().is_none());
        assert_ne!(cache_pressure.severity(), durable_pressure.severity());
        assert_ne!(cache_pressure.reason(), durable_pressure.reason());
    }
}

#[test]
fn durable_commit_deterministic_inline_runs_suggested_flush() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x6e);
    let config = LifecycleConfig::default()
        .with_maintenance_scheduling_policy(
            LifecycleMaintenanceSchedulingPolicy::DeterministicInline,
        )
        .expect("inline maintenance scheduling");
    let mut runtime =
        open_runtime_with_config(StorageMode::DurableLocalStandard, branch, backend, config);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"inline-scheduled-flush-a", b"value-a"),
            generation_guard(),
        )
        .expect("first durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"inline-scheduled-flush-b", b"value-b"),
            generation_guard(),
        )
        .expect("second durable commit");

    let status = runtime.maintenance_status();
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.stats().enqueued(), 1);
    assert_eq!(status.stats().started(), 1);
    assert_eq!(status.stats().completed(), 1);
    assert!(runtime.storage_pressure().suggested_task().is_none());
}

#[test]
fn durable_commit_urgent_active_bytes_records_accept_without_inline_attempt() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x89);
    let storage_budget = storage_budget_with_active_limit(1024 * 1024, 4);
    let config = LifecycleConfig::default()
        .with_storage_budget(storage_budget)
        .expect("storage budget")
        .with_maintenance_scheduling_policy(
            LifecycleMaintenanceSchedulingPolicy::DeterministicInline,
        )
        .expect("inline maintenance scheduling");
    let mut runtime =
        open_runtime_with_config(StorageMode::DurableLocalStandard, branch, backend, config);

    runtime
        .execute_durable_commit(
            durable_put_batch_owned(
                branch,
                b"durable-active-byte-urgent-seed",
                vec![0x61; 850 * 1024],
            ),
            generation_guard(),
        )
        .expect("seed urgent active-byte pressure");
    assert_eq!(
        runtime.storage_pressure().reason(),
        LifecycleStoragePressureReason::ActiveMutableBytes
    );
    assert_eq!(
        runtime.storage_pressure().severity(),
        LifecycleStoragePressureSeverity::Urgent
    );

    let outcome = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"durable-active-byte-urgent-admission", b"value"),
            generation_guard(),
        )
        .expect("urgent active-byte pressure accepts durable commit");

    assert_eq!(outcome.commit_version(), Some(CommitVersion::new(2)));
    let admission = runtime
        .last_write_admission()
        .expect("active-byte admission facts");
    assert_eq!(
        admission.status(),
        LifecycleWriteAdmissionStatus::AcceptedUnderPressure
    );
    assert_eq!(
        admission.pressure().reason(),
        LifecycleStoragePressureReason::ActiveMutableBytes
    );
    assert_eq!(
        admission.pressure().severity(),
        LifecycleStoragePressureSeverity::Urgent
    );
    assert!(admission.pressure().suggested_task().is_none());
    assert!(!admission.inline_maintenance_driven());
}

#[test]
fn durable_commit_rejects_blocking_active_bytes_before_allocating_version() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x8a);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    *runtime
        .branch_catalog_mut_for_test()
        .branch_state_mut(branch, generation_guard())
        .expect("branch state") = blocked_active_byte_pressure_state(branch, 512 * 1024);
    // BS2.3: synthetic state was written directly (bypassing the commit publish); resync.
    runtime.publish_branch_snapshot_for_test(branch);
    assert_eq!(runtime.visible_version(), CommitVersion::ZERO);
    assert_eq!(
        runtime.allocator().version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    let pressure = runtime.storage_pressure();
    assert_eq!(
        pressure.reason(),
        LifecycleStoragePressureReason::ActiveMutableBytes
    );
    assert_eq!(
        pressure.severity(),
        LifecycleStoragePressureSeverity::BlockMutatingAdmission
    );
    assert!(pressure.suggested_task().is_some());

    let error = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"durable-active-byte-blocked-rejected", b"value"),
            generation_guard(),
        )
        .expect_err("blocking active bytes reject durable commit");

    assert!(matches!(
        error,
        LifecycleError::StoragePressureRejected {
            branch_id: rejected_branch,
            severity: LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            pressure_reason: LifecycleStoragePressureReason::ActiveMutableBytes,
            retryable: true,
            ..
        } if rejected_branch == branch
    ));
    assert_eq!(runtime.visible_version(), CommitVersion::ZERO);
    assert_eq!(
        runtime.allocator().version_allocator().last_allocated(),
        CommitVersion::ZERO
    );
    assert!(
        runtime
            .read_view()
            .expect("read view")
            .latest(&physical_key(
                branch,
                b"durable-active-byte-blocked-rejected"
            ))
            .expect("latest read")
            .is_none(),
        "rejected active-byte admission must not append durable rows"
    );
}

#[test]
fn durable_close_does_not_truncate_wal_unless_drain_task_did_so() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x37);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"no-truncate-on-close", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    let operations_before_close = backend.operations().len();

    runtime.close().expect("durable close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    assert!(!close_operations
        .iter()
        .any(|operation| matches!(operation, Operation::DeleteObject(_))));
}

#[test]
fn durable_close_does_not_prune_snapshots_or_purge_quarantine_implicitly() {
    durable_close_does_not_truncate_wal_prune_snapshots_or_purge_quarantine_implicitly();
}

#[test]
fn durable_close_with_pending_retention_drain_runs_required_task() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x38);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .enqueue_maintenance(drain_task(
            MaintenanceTaskKind::Retention,
            MaintenanceTaskScope::Retention,
            MaintenanceTaskPriority::Low,
        ))
        .expect("enqueue retention");

    let close = runtime.close().expect("close drains retention");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.stats().maintenance_tasks(), 1);
}

#[test]
fn durable_close_with_pending_quarantine_drain_preserves_reclaim_facts() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x39);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .enqueue_maintenance(drain_task(
            MaintenanceTaskKind::Quarantine,
            MaintenanceTaskScope::Quarantine,
            MaintenanceTaskPriority::Low,
        ))
        .expect("enqueue quarantine");

    let close = runtime.close().expect("close drains quarantine");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.stats().maintenance_tasks(), 1);
}

#[test]
fn durable_close_with_ordinary_compaction_task_does_not_start_compaction() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x3a);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue ordinary compaction");

    let close = runtime.close().expect("durable close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.stats().maintenance_tasks(), 1);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn durable_close_after_failed_maintenance_reports_health_debt() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x3b);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"failed-maintenance-close", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .enqueue_maintenance(drain_checkpoint_task())
        .expect("enqueue checkpoint");
    backend.set_publish_failure(Some(PublishFailureKind::FailedBeforeVisibility));

    let error = runtime.close().expect_err("checkpoint failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
}

#[test]
fn durable_open_commit_close_reopen_recovers_committed_rows() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x3c);
    let key = physical_key(branch, b"reopen-row");
    let mut first = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    first
        .execute_durable_commit(
            durable_put_batch(branch, b"reopen-row", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    first.close().expect("close first");

    let second = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let row = second
        .read_view()
        .expect("read view")
        .latest(&key)
        .expect("read")
        .expect("row");

    assert_eq!(row.row().value(), b"value");
    assert_eq!(second.visible_version(), CommitVersion::new(1));
}

#[test]
fn active_commit_guard_causes_typed_close_timeout() {
    durable_close_timeout_while_commit_guard_active_is_retryable();
}

#[test]
fn close_retry_after_timeout_completes_when_blocker_clears() {
    durable_close_timeout_while_commit_guard_active_is_retryable();
}

#[test]
fn close_retry_after_wal_failure_retries_sync_phase() {
    durable_close_log_sync_failure_preserves_writer_guard_for_retry();
}

#[test]
fn close_retry_after_manifest_failure_retries_final_fact_phase() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x3d);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let health = close_health_debt();
    runtime.record_recovery_health_for_test(&health);
    backend.set_publish_failure(Some(PublishFailureKind::FailedBeforeVisibility));
    assert!(runtime.close().is_err());
    backend.set_publish_failure(None);

    let close = runtime.close().expect("retry close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(runtime.state(), LifecycleState::Closed);
}

#[test]
fn close_retry_after_missing_writer_guard_failure_retries_release_phase() {
    durable_close_reports_typed_error_when_writer_guard_is_missing_at_release();
}

#[test]
fn close_failure_during_quiesce_preserves_drain_facts() {
    durable_close_preserves_drain_required_checkpoint_when_quiesce_is_unavailable();
}

#[test]
fn close_failure_during_wal_sync_preserves_quiesce_fact() {
    durable_close_log_sync_failure_preserves_writer_guard_for_retry();
}

#[test]
fn close_failure_during_manifest_sync_preserves_wal_fact() {
    close_retry_after_manifest_failure_retries_final_fact_phase();
}

#[test]
fn close_failure_during_guard_release_preserves_sync_fact() {
    durable_double_close_does_not_double_release_writer_guard();
}

#[test]
fn close_acquires_commit_quiesce_after_maintenance_drain() {
    // Enqueue a drain-required checkpoint task. The close path must
    // execute drain (which produces a snapshot Publish operation against
    // the backend) BEFORE issuing the WAL sync that close performs.
    // We assert temporal ordering on the recorded backend operation log:
    // every checkpoint-driven Publish must appear before the WAL SyncObject
    // that close itself issues at the end of the sequence. If a refactor
    // ever inverts these phases (quiesce/sync before drain), the
    // assertion below catches it.
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x3e);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"ordering-precommit", b"value"),
            generation_guard(),
        )
        .expect("commit before close");
    runtime
        .enqueue_maintenance(drain_checkpoint_task())
        .expect("enqueue drain-required checkpoint");

    let operations_before_close = backend.operations().len();
    let close = runtime.close().expect("close");
    let close_operations = backend.operations()[operations_before_close..].to_vec();

    assert_eq!(close.stats().maintenance_tasks(), 1);
    assert!(close.commits_quiesced());
    assert_eq!(runtime.state(), LifecycleState::Closed);

    // The drained checkpoint task issued at least one snapshot Publish
    // before close itself ran the WAL sync. Pick the first Publish index
    // (drain output) and the first SyncObject index (WAL close) — drain
    // must strictly precede sync.
    let first_publish = close_operations
        .iter()
        .position(|operation| matches!(operation, Operation::Publish(_, _)))
        .expect("drained checkpoint produced no Publish");
    let first_sync = close_operations
        .iter()
        .position(|operation| matches!(operation, Operation::SyncObject(_)))
        .expect("close produced no SyncObject");
    assert!(
        first_publish < first_sync,
        "drain checkpoint Publish at index {first_publish} must precede close SyncObject at index {first_sync}; close ordering inverted",
    );
}

#[test]
fn quiesce_blocks_new_branch_guards_until_close_completes() {
    let guard_set = crate::commit::CommitBranchGuardSet::new();
    let quiesce = guard_set.try_begin_quiesce().expect("quiesce");
    let branch = branch_id(0x3f);

    assert!(guard_set.try_acquire_branch_guard(branch).is_err());
    drop(quiesce);
    assert!(guard_set.try_acquire_branch_guard(branch).is_ok());
}

#[test]
fn quiesce_guard_released_on_retryable_failure_when_contract_allows_retry() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_sync_failure());
    let branch = branch_id(0x40);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"quiesce-release", b"value"),
            generation_guard(),
        )
        .expect("durable commit");

    assert!(runtime.close().is_err());

    assert!(!runtime.guard_set().is_quiescing().expect("quiesce state"));
}

#[test]
fn quiesce_guard_not_reacquired_on_idempotent_second_close() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x41);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.close().expect("first close");

    let second = runtime.close().expect("second close");

    assert_eq!(second.status(), CloseOutcomeStatus::Idempotent);
    assert!(!runtime.guard_set().is_quiescing().expect("quiesce state"));
}

#[test]
fn cross_branch_commit_after_quiesce_rejects() {
    let guard_set = crate::commit::CommitBranchGuardSet::new();
    let quiesce = guard_set.try_begin_quiesce().expect("quiesce");

    let error = guard_set
        .try_acquire_branch_guard(branch_id(0x42))
        .expect_err("branch guard rejected");

    assert!(matches!(
        error,
        crate::commit::CommitRuntimeError::CommitQuiesceUnavailable { .. }
    ));
    drop(quiesce);
}

#[test]
fn durable_clear_branch_requires_quiesce_and_rejects_when_branch_guard_active() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x50);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let pre_branches = runtime.list_branches(false).len();

    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime
        .clear_branch(branch, generation_guard())
        .expect_err("clear_branch must reject while branch guard is active");
    assert_quiesce_unavailable(&error);
    assert_eq!(runtime.list_branches(false).len(), pre_branches);
    drop(guard);
}

#[test]
fn durable_delete_branch_requires_quiesce_and_rejects_when_branch_guard_active() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x51);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let pre_branches = runtime.list_branches(false).len();

    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime
        .delete_branch(branch, generation_guard(), None)
        .expect_err("delete_branch must reject while branch guard is active");
    assert_quiesce_unavailable(&error);
    assert_eq!(runtime.list_branches(false).len(), pre_branches);
    drop(guard);
}

#[test]
fn durable_fork_current_requires_quiesce_and_rejects_when_branch_guard_active() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x52);
    let other = branch_id(0x53);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let pre_branches = runtime.list_branches(false).len();

    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime
        .fork_current(
            branch,
            other,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect_err("fork_current must reject while branch guard is active");
    assert_quiesce_unavailable(&error);
    assert_eq!(runtime.list_branches(false).len(), pre_branches);
    drop(guard);
}

#[test]
fn durable_fork_at_retained_version_requires_quiesce_and_rejects_when_branch_guard_active() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x54);
    let other = branch_id(0x55);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"fork-source", b"value"),
            generation_guard(),
        )
        .expect("seed commit so fork target version is retained");
    let pre_branches = runtime.list_branches(false).len();

    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime
        .fork_at_retained_version(
            branch,
            other,
            CommitBranchGeneration::new(1).expect("generation"),
            CommitVersion::new(1),
            CommitVersion::ZERO,
            None,
        )
        .expect_err("fork_at_retained_version must reject while branch guard is active");
    assert_quiesce_unavailable(&error);
    assert_eq!(runtime.list_branches(false).len(), pre_branches);
    drop(guard);
}

#[test]
fn durable_fork_at_retained_timestamp_requires_quiesce_and_rejects_when_branch_guard_active() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x56);
    let other = branch_id(0x57);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"fork-source-ts", b"value"),
            generation_guard(),
        )
        .expect("seed commit so fork target timestamp is retained");
    let pre_branches = runtime.list_branches(false).len();

    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime
        .fork_at_retained_timestamp(
            branch,
            other,
            CommitBranchGeneration::new(1).expect("generation"),
            Timestamp::from_micros(1_000_000),
            CommitVersion::ZERO,
            None,
        )
        .expect_err("fork_at_retained_timestamp must reject while branch guard is active");
    assert_quiesce_unavailable(&error);
    assert_eq!(runtime.list_branches(false).len(), pre_branches);
    drop(guard);
}

#[test]
fn branch_lifecycle_quiesce_guard_releases_on_failure_so_followup_acquire_succeeds() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x58);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // Hold a branch guard so the next clear_branch call fails on quiesce
    // acquisition. The wrapper returns an error; if RAII Drop did not run,
    // the runtime's guard set would remain quiesced and subsequent attempts
    // would keep failing even after the guard is released.
    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");
    let first_attempt = runtime.clear_branch(branch, generation_guard());
    assert!(
        first_attempt.is_err(),
        "first clear_branch attempt must fail while guard held"
    );
    drop(guard);

    // The wrapper's failure must have released the quiesce token via the
    // `_quiesce` RAII binding. Confirm by acquiring a fresh quiesce token
    // directly on the guard set.
    let post_failure_quiesce = runtime
        .guard_set()
        .try_begin_quiesce()
        .expect("quiesce token after wrapper failure proves Drop ran");
    drop(post_failure_quiesce);

    // And the wrapper should now succeed because no guard is held.
    runtime
        .clear_branch(branch, generation_guard())
        .expect("clear_branch succeeds once guard is released");
}

fn assert_quiesce_unavailable(error: &LifecycleError) {
    use crate::commit::CommitRuntimeError;
    let LifecycleError::LowerLayer { layer, source, .. } = error else {
        panic!("expected LifecycleError::LowerLayer, got {error:?}");
    };
    assert_eq!(*layer, super::super::LifecycleLowerLayer::CommitRuntime);
    let source = source
        .as_ref()
        .expect("lower-layer error must carry a source");
    let commit_error = source
        .downcast_ref::<CommitRuntimeError>()
        .expect("source must downcast to CommitRuntimeError");
    assert!(
        matches!(
            commit_error,
            CommitRuntimeError::CommitQuiesceUnavailable { .. }
        ),
        "expected CommitQuiesceUnavailable, got {commit_error:?}"
    );
}

#[test]
fn commit_after_close_requested_rejects_before_version_allocation() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x28);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime.close().expect_err("active guard blocks close");
    assert_eq!(error.code(), "failed_precondition.lifecycle.close_timeout");
    assert_eq!(runtime.state(), LifecycleState::Closing);
    let operations_after_close_failure = backend.operations().len();
    let allocation_before_commit = runtime.allocator().version_allocator().last_allocated();

    let commit_error = runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"after-close-request", b"value"),
            generation_guard(),
        )
        .expect_err("commit after close request rejects");

    assert_eq!(commit_error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(
        runtime.allocator().version_allocator().last_allocated(),
        allocation_before_commit
    );
    assert_eq!(backend.operations().len(), operations_after_close_failure);
    drop(guard);
    runtime.close().expect("retry close");
}

#[test]
fn durable_close_timeout_while_commit_guard_active_is_retryable() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x21);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime.close().expect_err("active guard blocks close");
    assert_eq!(error.code(), "failed_precondition.lifecycle.close_timeout");
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert_eq!(
        runtime
            .guard_set()
            .active_guard_count()
            .expect("guard count"),
        1
    );
    assert!(backend.lock_is_held());

    drop(guard);
    let close = runtime.close().expect("retry after active guard drops");
    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(runtime.state(), LifecycleState::Closed);
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_close_drains_stale_active_maintenance_before_closing() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x24);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let active = MaintenanceTask::new_for_test(
        77,
        MaintenanceTaskRequest::new(
            MaintenanceTaskKind::HealthCollection,
            MaintenanceTaskPriority::High,
            MaintenanceTaskScope::Global,
            MaintenanceTaskPolicy::drain_before_close(),
        )
        .expect("active close-drain task"),
    )
    .expect("active task");
    runtime.set_active_maintenance_for_test(active);

    let close = runtime.close().expect("close drains active task");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(runtime.state(), LifecycleState::Closed);
    assert_eq!(runtime.maintenance_status().active_task(), None);
    assert_eq!(runtime.maintenance_status().stats().drained(), 1);
    assert_eq!(close.stats().maintenance_tasks(), 1);
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_close_preserves_drain_required_checkpoint_when_quiesce_is_unavailable() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x23);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .enqueue_maintenance(
            MaintenanceTaskRequest::new(
                MaintenanceTaskKind::Checkpoint,
                MaintenanceTaskPriority::High,
                MaintenanceTaskScope::Checkpoint,
                MaintenanceTaskPolicy::drain_before_close(),
            )
            .expect("drain-required checkpoint"),
        )
        .expect("enqueue checkpoint");
    let guard = runtime
        .guard_set()
        .try_acquire_branch_guard(branch)
        .expect("active commit guard");

    let error = runtime
        .close()
        .expect_err("checkpoint quiesce blocks close");

    assert_eq!(error.code(), "failed_precondition.lifecycle.close_timeout");
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
    assert!(backend.lock_is_held());

    drop(guard);
    let close = runtime.close().expect("retry drains checkpoint and closes");
    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.stats().maintenance_tasks(), 1);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(runtime.state(), LifecycleState::Closed);
    assert!(!backend.lock_is_held());
}

#[test]
fn durable_close_log_sync_failure_preserves_writer_guard_for_retry() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::with_sync_failure());
    let branch = branch_id(0x22);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"close-fail", b"value"),
            generation_guard(),
        )
        .expect("durable commit before close");

    let error = runtime.close().expect_err("sync failure blocks close");
    assert!(matches!(
        error,
        LifecycleError::LowerLayer {
            layer: LifecycleLowerLayer::Service,
            reason: "WAL service failed",
            ..
        }
    ));
    assert_eq!(runtime.state(), LifecycleState::Closing);
    assert!(backend.lock_is_held());
    assert!(runtime.services().writer_guard().is_some());

    backend.set_sync_failure(false);
    let close = runtime.close().expect("retry close after sync recovers");
    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert!(close.durable_synced());
    assert!(close.guards_released());
    assert!(!backend.lock_is_held());
}

fn assemble_shell(
    mode: StorageMode,
    branch: BranchId,
    backend: &'static DurableTestBackend,
) -> LifecycleResult<LifecycleDurableLocalShell<'static>> {
    LifecycleDurableLocalShell::assemble(request(mode, branch)?, backend, timestamp_source())
}

/// #2524: the table-object mark and sweep run DURING an active off-lock
/// build — the wholesale `has_active_build_task` defer starved reclaim
/// under sustained load (zero retention passes across a whole seed). The
/// build's published-not-yet-installed outputs are pinned by name in the
/// in-flight registry: unreachable bait is reclaimed mid-build while the
/// build's own outputs survive, and the pin hands off to manifest
/// reachability at install.
#[test]
fn table_object_mark_and_sweep_run_during_active_build() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x77);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // Unreachable bait the mid-build sweep should reclaim.
    let orphan =
        ObjectLayout::table_object(&branch.to_string(), 0, "mid-build-orphan").expect("orphan");
    backend
        .write_object(&orphan, b"orphan-table")
        .expect("orphan write");

    // A real off-lock flush build, started and HELD (the task stays active).
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"mid-build-row", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let step = runtime
        .start_next_background_flush_maintenance()
        .expect("start background flush")
        .expect("background flush step");
    let pending_build = match step {
        DurableBackgroundMaintenanceStep::Build(pending) => *pending,
        _ => panic!("expected a build step"),
    };
    // Off-lock build: publishes the flush output and reserves its name.
    let built = pending_build.build().expect("off-lock flush build");
    let pinned = runtime.inflight_table_outputs().snapshot();
    assert!(
        !pinned.is_empty(),
        "the build must reserve its published output names before install",
    );

    // Mark + sweep DURING the active build: both complete (pre-#2524 both
    // deferred wholesale on the in-flight build).
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue retention");
    let retention = runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention outcome");
    assert_eq!(retention.status(), MaintenanceOutcomeStatus::Completed);
    let sweep = runtime
        .run_next_quarantine_maintenance()
        .expect("run sweep")
        .expect("sweep outcome");
    assert_eq!(sweep.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        sweep.state_changes(),
        1,
        "exactly the unreachable bait is staged; the pinned build output is not",
    );
    assert!(
        backend.object_metadata(&orphan).is_err(),
        "the bait's source is deleted by quarantine staging mid-build",
    );
    for name in &pinned {
        assert!(
            backend.object_metadata(name).is_ok(),
            "pinned in-flight output {name} must survive the mid-build sweep",
        );
    }

    // Install: the surviving outputs publish normally, and the pin hands
    // off to manifest reachability (registry drains).
    let publish = runtime
        .begin_publish_phase(built)
        .expect("begin publish phase");
    let outcome = match publish {
        PreparedPublishStep::Done(result) => result.expect("publish done"),
        PreparedPublishStep::OffLock(prepared) => {
            let (prepared, write_result) = prepared.persist_off_lock();
            runtime
                .finish_publish_phase(prepared, write_result)
                .expect("finish publish phase")
        }
    };
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(
        runtime.inflight_table_outputs().snapshot().is_empty(),
        "reservations drain once the build is installed",
    );
    for name in &pinned {
        assert!(
            backend.object_metadata(name).is_ok(),
            "installed output {name} is manifest-reachable and survives",
        );
    }
}

/// #3382: a background build whose adopted output a concurrent table-object
/// sweep deleted mid-read reports the typed race, and the dispatcher DEFERS
/// the task — the build-phase analog of the #2553 install-time arm. A
/// deferral is not a failure: nothing lands in the failure ring, so the
/// nightly stress oracle (`recent_failures` must stay empty) holds.
#[test]
fn background_build_sweep_race_defers_instead_of_recording_failure() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x78);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"sweep-race-row", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let step = runtime
        .start_next_background_flush_maintenance()
        .expect("start background flush")
        .expect("background flush step");
    let pending_build = match step {
        DurableBackgroundMaintenanceStep::Build(pending) => *pending,
        _ => panic!("expected a build step"),
    };
    let task = pending_build.task();
    let object =
        ObjectLayout::table_object(&branch.to_string(), 0, "swept-mid-build").expect("object");

    let outcome = runtime
        .finish_background_build_error(task, LifecycleError::RewriteOutputRacedSweep { object })
        .expect("build error outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(runtime.maintenance_status().stats().failed(), 0);
    assert_eq!(runtime.maintenance_status().stats().deferred(), 1);
}

/// #2524: reservations release when a build is abandoned — the published
/// outputs become ordinary orphans and the next mark/sweep cycle reclaims
/// them (the crash-window analog: the registry is in-memory by design).
#[test]
fn abandoned_build_outputs_release_their_pins_and_get_reclaimed() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x78);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"abandoned-row", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let step = runtime
        .start_next_background_flush_maintenance()
        .expect("start background flush")
        .expect("background flush step");
    let pending_build = match step {
        DurableBackgroundMaintenanceStep::Build(pending) => *pending,
        _ => panic!("expected a build step"),
    };
    let built = pending_build.build().expect("off-lock flush build");
    let published = runtime.inflight_table_outputs().snapshot();
    assert!(!published.is_empty(), "build published + reserved outputs");

    // Abandon the build (publish never runs): the guard drops with it.
    drop(built);
    assert!(
        runtime.inflight_table_outputs().snapshot().is_empty(),
        "abandoning the build releases its reservations",
    );

    // The next mark/sweep cycle reclaims the now-orphaned outputs.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue retention");
    let retention = runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention outcome");
    assert_eq!(retention.status(), MaintenanceOutcomeStatus::Completed);
    let sweep = runtime
        .run_next_quarantine_maintenance()
        .expect("run sweep")
        .expect("sweep outcome");
    assert_eq!(sweep.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        sweep.state_changes(),
        published.len(),
        "every abandoned output is staged for reclaim",
    );
    for name in &published {
        assert!(
            backend.object_metadata(name).is_err(),
            "abandoned output {name} must be reclaimed once its pin is gone",
        );
    }
}

fn assemble_shell_with_config(
    mode: StorageMode,
    branch: BranchId,
    backend: &'static DurableTestBackend,
    config: LifecycleConfig,
) -> LifecycleResult<LifecycleDurableLocalShell<'static>> {
    LifecycleDurableLocalShell::assemble(
        request_with_config(mode, branch, config)?,
        backend,
        timestamp_source(),
    )
}

fn open_runtime(
    mode: StorageMode,
    branch: BranchId,
    backend: &'static DurableTestBackend,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut shell = assemble_shell(mode, branch, backend).expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    shell.complete_recovery(&recovery).expect("open runtime")
}

fn open_runtime_with_config(
    mode: StorageMode,
    branch: BranchId,
    backend: &'static DurableTestBackend,
    config: LifecycleConfig,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut shell =
        assemble_shell_with_config(mode, branch, backend, config).expect("durable shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    shell.complete_recovery(&recovery).expect("open runtime")
}

fn checkpoint_request(branch: BranchId, snapshot_id: u64) -> LifecycleCheckpointRequest {
    LifecycleCheckpointRequest::new(
        branch,
        snapshot_id,
        Timestamp::from_micros(9_000 + snapshot_id),
    )
    .expect("checkpoint request")
}

fn flush_request(branch: BranchId, id: &'static str) -> FlushFrozenRequest {
    FlushFrozenRequest::new(
        branch,
        None,
        FlushTableIdentitySeed::new(id).expect("identity seed"),
        FlushTableObjectId::new(id).expect("object id"),
    )
    .expect("flush request")
}

fn drain_checkpoint_task() -> MaintenanceTaskRequest {
    drain_task(
        MaintenanceTaskKind::Checkpoint,
        MaintenanceTaskScope::Checkpoint,
        MaintenanceTaskPriority::High,
    )
}

fn drain_task(
    kind: MaintenanceTaskKind,
    scope: MaintenanceTaskScope,
    priority: MaintenanceTaskPriority,
) -> MaintenanceTaskRequest {
    MaintenanceTaskRequest::new(
        kind,
        priority,
        scope,
        MaintenanceTaskPolicy::drain_before_close(),
    )
    .expect("drain task")
}

fn generation_guard() -> CommitBranchGenerationGuard {
    CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation"))
}

fn commit_runtime_source(error: &LifecycleError) -> &CommitRuntimeError {
    let LifecycleError::LowerLayer {
        layer: LifecycleLowerLayer::CommitRuntime,
        source: Some(source),
        ..
    } = error
    else {
        panic!("expected commit runtime error, got {error:?}");
    };
    source
        .downcast_ref::<CommitRuntimeError>()
        .expect("commit runtime source")
}

fn open_cache_runtime(
    branch: BranchId,
    backend: &'static dyn Backend,
) -> LifecycleCacheRuntime<CommitManualTimestampSource> {
    let request = LifecycleCacheOpenRequest::new(
        open_plan_with_config(StorageMode::Cache, LifecycleConfig::default()),
        branch,
        CommitBranchGeneration::new(1).expect("generation"),
    )
    .expect("cache request");
    LifecycleCacheRuntime::open(
        request,
        backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(1_000)),
    )
    .expect("cache runtime")
}

fn durable_put_batch(
    branch: BranchId,
    user_key: &'static [u8],
    value: &'static [u8],
) -> CommitBatch {
    durable_put_batch_with_mode(branch, user_key, value, CommitDurabilityMode::Standard)
}

fn durable_put_batch_owned(
    branch: BranchId,
    user_key: &'static [u8],
    value: Vec<u8>,
) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            physical_key(branch, user_key),
            value,
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::new(
            CommitDurabilityMode::Standard,
            CommitConflictValidationMode::Skip,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
    )
}

fn durable_put_batch_with_mode(
    branch: BranchId,
    user_key: &'static [u8],
    value: &'static [u8],
    durability: CommitDurabilityMode,
) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            physical_key(branch, user_key),
            value.to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::new(
            durability,
            CommitConflictValidationMode::Skip,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
    )
}

fn blocked_active_byte_pressure_state(branch: BranchId, rotation_bytes: usize) -> BranchLocalState {
    let branch_config = BranchRuntimeConfig::new(8, 64, 1)
        .expect("branch config")
        .with_active_rotation_bytes(rotation_bytes)
        .expect("custom active rotation threshold");
    let mut state = BranchLocalState::new(branch, branch_config).expect("branch state");
    let value_len = rotation_bytes.saturating_add(64 * 1024);
    state
        .append_committed_rows_atomically(vec![active_pressure_put_row(
            branch,
            b"durable-active-byte-blocking-frozen",
            1,
            1_000,
            value_len,
            0x62,
        )])
        .expect("append frozen pressure row");
    assert_eq!(state.frozen_table_count(), 1);
    state
        .append_committed_rows_atomically(vec![active_pressure_put_row(
            branch,
            b"durable-active-byte-blocking-active",
            2,
            2_000,
            value_len,
            0x63,
        )])
        .expect("append active pressure row");
    assert_eq!(state.frozen_table_count(), 1);
    assert!(state.active_byte_count() >= rotation_bytes as u64);
    state
}

fn active_pressure_put_row(
    branch: BranchId,
    user_key: &'static [u8],
    version: u64,
    timestamp: u64,
    value_len: usize,
    byte: u8,
) -> StorageRow {
    StorageRow::put(
        physical_key(branch, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
        Timestamp::EPOCH,
        vec![byte; value_len],
    )
}

fn active_pressure_put_row_owned(
    branch: BranchId,
    user_key: Vec<u8>,
    version: u64,
    timestamp: u64,
    value_len: usize,
    byte: u8,
) -> StorageRow {
    StorageRow::put(
        PhysicalKey::new(
            branch,
            "lifecycle",
            StorageSpaceId::engine(0x24).expect("space"),
            user_key,
        )
        .expect("physical key"),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
        Timestamp::EPOCH,
        vec![byte; value_len],
    )
}

fn storage_budget_with_active_limit(
    active_bytes: u64,
    max_frozen_tables: u32,
) -> StorageRuntimeBudget {
    let mut parts = StorageRuntimeBudgetParts {
        block_cache_bytes: 0,
        table_reader_bytes: 8 * 1024,
        active_mutable_bytes: active_bytes,
        frozen_mutable_bytes: active_bytes
            .saturating_mul(u64::from(max_frozen_tables))
            .max(active_bytes),
        maintenance_queue_bytes: 1024,
        generated_artifact_bytes: 8 * 1024,
        manifest_catalog_bytes: 1024,
        max_open_readers: 4,
        max_frozen_tables,
        max_pending_maintenance_tasks: 4,
        ..StorageRuntimeBudgetParts::default()
    };
    parts.total_bytes = storage_budget_pool_sum(parts);
    StorageRuntimeBudget::from_parts(parts).expect("storage budget")
}

fn storage_budget_pool_sum(parts: StorageRuntimeBudgetParts) -> u64 {
    parts.block_cache_bytes
        + parts.table_reader_bytes
        + parts.active_mutable_bytes
        + parts.frozen_mutable_bytes
        + parts.maintenance_queue_bytes
        + parts.generated_artifact_bytes
        + parts.manifest_catalog_bytes
}

fn build_cache_l0_tables_with_scheduled_flushes(
    runtime: &mut LifecycleCacheRuntime<CommitManualTimestampSource>,
    branch: BranchId,
    table_count: usize,
) {
    assert!(table_count > 0);
    for index in 0..table_count {
        {
            let state = runtime
                .branch_catalog_mut_for_test()
                .branch_state_mut(branch, generation_guard())
                .expect("cache branch state");
            state
                .append_committed_rows_atomically(vec![active_pressure_put_row_owned(
                    branch,
                    format!("cache-l0-trigger-{index}").into_bytes(),
                    1 + u64::try_from(index).expect("index fits"),
                    10_000 + u64::try_from(index).expect("index fits"),
                    128,
                    0x41,
                )])
                .expect("append cache L0 fixture row");
            state.rotate_active();
        }
        runtime
            .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
            .expect("enqueue cache fixture flush");
        let outcome = runtime
            .run_next_flush_maintenance()
            .expect("run cache flush maintenance")
            .expect("cache flush task");
        assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Flush);
        assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
        assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    }
    runtime.catch_up_commit_frontier_for_test(
        CommitVersion::new(u64::try_from(table_count).expect("table count fits")),
        Timestamp::from_micros(10_000 + u64::try_from(table_count - 1).expect("table count fits")),
    );
}

// BS3.4b — graded write-admission (debt-adaptive rate ramp). The rate recomputes inside
// `republish_all_branch_snapshots` (the point the inline flush path also hits), so
// `build_durable_l0_tables_with_scheduled_flushes` drives real event cadence. All use a manual clock
// so the token bucket is deterministic.

#[test]
fn graded_admission_rate_stays_at_max_below_the_l0_delay_band() {
    let branch = branch_id(0x91);
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.with_admission_mode_for_test(LifecycleAdmissionMode::Graded);
    runtime.with_admission_clock_for_test(Arc::new(ManualMaintenanceClock::default()));

    let max_rate = runtime.admission_current_rate_for_test();
    // 10 L0 tables is below the slowdown grade (20): the ramp must not engage.
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 10);
    assert_eq!(
        runtime.admission_current_rate_for_test(),
        max_rate,
        "graded rate must stay at the ceiling below the L0 slowdown grade"
    );
}

#[test]
fn graded_admission_rate_drops_inside_the_l0_delay_band() {
    let branch = branch_id(0x92);
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.with_admission_mode_for_test(LifecycleAdmissionMode::Graded);
    runtime.with_admission_clock_for_test(Arc::new(ManualMaintenanceClock::default()));

    let max_rate = runtime.admission_current_rate_for_test();
    // 25 L0 tables sits inside the delay band (slowdown 20 .. stop 36): each flush install recomputes
    // the rate via the ramp, so it drops below the ceiling.
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 25);
    let throttled = runtime.admission_current_rate_for_test();
    assert!(
        throttled < max_rate,
        "graded rate must ramp down inside the L0 delay band (ceiling {max_rate}, got {throttled})"
    );
}

#[test]
fn legacy_admission_leaves_the_graded_rate_untouched() {
    // The legacy escape hatch (STRATA_ADMISSION=legacy) never touches the
    // graded rate — the ramp is inert. Explicitly selected since BS3.4c made
    // graded the default.
    let branch = branch_id(0x93);
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.with_admission_mode_for_test(LifecycleAdmissionMode::Legacy);
    let max_rate = runtime.admission_current_rate_for_test();
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 25);
    assert_eq!(
        runtime.admission_current_rate_for_test(),
        max_rate,
        "legacy admission must not ramp the graded rate"
    );
}

#[test]
fn graded_admission_paces_a_commit_only_when_the_rate_is_throttled() {
    let branch = branch_id(0x94);
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.with_admission_mode_for_test(LifecycleAdmissionMode::Graded);
    runtime.with_admission_clock_for_test(Arc::new(ManualMaintenanceClock::default()));

    // At the un-throttled ceiling, even a large commit is not paced.
    assert_eq!(
        runtime.graded_write_throttle_delay_millis(10 * 1024 * 1024),
        0,
        "no pacing at the full write rate"
    );
    // Drop the rate into the delay band; then a large commit (frozen manual clock -> no accrued
    // credit) is paced with a nonzero delay.
    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 25);
    assert!(runtime.admission_current_rate_for_test() < 16 * 1024 * 1024);
    assert!(
        runtime.graded_write_throttle_delay_millis(10 * 1024 * 1024) > 0,
        "a throttled rate must pace a large commit"
    );
}

#[test]
fn graded_admission_caps_the_per_commit_delay() {
    let branch = branch_id(0x95);
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime.with_admission_mode_for_test(LifecycleAdmissionMode::Graded);
    runtime.with_admission_clock_for_test(Arc::new(ManualMaintenanceClock::default()));

    build_durable_l0_tables_with_scheduled_flushes(&mut runtime, branch, 25);
    // A 1 GiB batch at the throttled near-stop rate would pace for many seconds uncapped; the cap
    // bounds a single commit to the policy's max_graded_delay_millis (250 ms default).
    let cap = runtime
        .open_plan()
        .lifecycle_config()
        .write_throttle_policy()
        .max_graded_delay_millis();
    let delay = runtime.graded_write_throttle_delay_millis(1024 * 1024 * 1024);
    assert!(
        delay > 0,
        "a large commit at a throttled rate must still be paced"
    );
    assert!(
        delay <= cap,
        "the per-commit graded delay must be capped at {cap} ms, got {delay} ms"
    );
}

fn build_durable_l0_tables_with_scheduled_flushes(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    table_count: usize,
) {
    assert!(table_count > 0);
    for index in 0..table_count {
        {
            let state = runtime
                .branch_catalog_mut_for_test()
                .branch_state_mut(branch, generation_guard())
                .expect("durable branch state");
            state
                .append_committed_rows_atomically(vec![active_pressure_put_row_owned(
                    branch,
                    format!("durable-l0-trigger-{index}").into_bytes(),
                    1 + u64::try_from(index).expect("index fits"),
                    20_000 + u64::try_from(index).expect("index fits"),
                    128,
                    0x42,
                )])
                .expect("append durable L0 fixture row");
            state.rotate_active();
        }
        runtime
            .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
            .expect("enqueue durable fixture flush");
        let outcome = runtime
            .run_next_flush_maintenance()
            .expect("run durable flush maintenance")
            .expect("durable flush task");
        assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Flush);
        assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
        assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    }
    runtime.catch_up_commit_frontier_for_test(
        CommitVersion::new(u64::try_from(table_count).expect("table count fits")),
        Timestamp::from_micros(20_000 + u64::try_from(table_count - 1).expect("table count fits")),
    );
}

fn physical_key(branch: BranchId, user_key: &'static [u8]) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "lifecycle",
        StorageSpaceId::engine(0x24).expect("space"),
        user_key.to_vec(),
    )
    .expect("physical key")
}

fn request(
    mode: StorageMode,
    branch: BranchId,
) -> LifecycleResult<LifecycleDurableLocalOpenRequest> {
    request_with_config(mode, branch, LifecycleConfig::default())
}

fn request_with_config(
    mode: StorageMode,
    branch: BranchId,
    config: LifecycleConfig,
) -> LifecycleResult<LifecycleDurableLocalOpenRequest> {
    LifecycleDurableLocalOpenRequest::new(
        open_plan_with_config(mode, config),
        DATABASE_ID,
        branch,
        CommitBranchGeneration::new(1).expect("generation"),
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        WalServiceConfig::default(),
    )
}

fn write_existing_manifest(backend: &'static DurableTestBackend, manifest: &DatabaseManifest) {
    backend.write_raw(
        ObjectLayout::database_manifest().expect("database object"),
        encode_manifest(manifest).expect("encoded object"),
    );
}

fn manifest_with_active_segment(segment_id: u64) -> DatabaseManifest {
    DatabaseManifest::new(DATABASE_ID, "identity")
        .expect("database object")
        .with_recovery_facts(segment_id, None, None, None)
        .expect("recovery facts")
}

fn close_health_debt() -> RecoveryHealth {
    RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![
            RecoveryFault::new(RecoveryFaultKind::WalTailRepairFailed, "close health debt")
                .expect("fault"),
        ],
    )
    .expect("health")
}

fn data_loss_health_debt() -> RecoveryHealth {
    RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![RecoveryFault::new(
            RecoveryFaultKind::MissingTableObject,
            "unsafe recovery health",
        )
        .expect("fault")],
    )
    .expect("health")
}

fn open_plan(mode: StorageMode) -> StorageOpenPlan {
    open_plan_with_config(mode, LifecycleConfig::default())
}

fn open_plan_with_config(mode: StorageMode, config: LifecycleConfig) -> StorageOpenPlan {
    StorageOpenPlan::new(
        mode,
        LifecycleCodecId::identity(),
        RecoveryStrictness::Strict,
        config,
    )
    .expect("open plan")
}

fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; 16])
}

fn timestamp_source() -> CommitManualTimestampSource {
    CommitManualTimestampSource::new(Timestamp::from_micros(8_000))
}

fn touch_shell_parts(shell: &LifecycleDurableLocalShell<'static>) {
    let services = shell.services();
    let _ = services.manifest();
    let _ = services.table_manifest();
    let _ = services.wal_sidecar();
    let _ = services.snapshot();
    let _ = services.table_object();
    let _ = services.table_reader();
    let _ = services.checkpoint();
    let _ = services.quarantine();
    let _ = services.writer_guard();
    let _ = shell.registry();
    let _ = shell.guard_set();
    let _ = shell.allocator();
    let _ = shell.durable_gate();
    let _ = shell.commit_config();
}

fn assert_call_order(operations: &[Operation], first: OperationKind, second: OperationKind) {
    let first_index = operations
        .iter()
        .position(|operation| operation.kind() == first)
        .expect("first operation");
    let second_index = operations
        .iter()
        .position(|operation| operation.kind() == second)
        .expect("second operation");
    assert!(
        first_index < second_index,
        "{first:?} should happen before {second:?}: {operations:?}"
    );
}

#[derive(Debug)]
struct DurableTestBackend {
    capabilities: BackendCapabilities,
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    operations: Mutex<Vec<Operation>>,
    lock_held: Arc<AtomicBool>,
    release_count: Arc<AtomicUsize>,
    fail_lock: bool,
    publish_failure: Mutex<Option<PublishFailureKind>>,
    // #2553 (T2): one-shot apply-then-fail for a TARGETED object — the bytes land, then the
    // publish reports the failure kind. Models a visible-but-durability-unconfirmed manifest
    // replace, which `set_publish_failure` (fails BEFORE writing) cannot.
    publish_apply_then_fail: Mutex<Option<(ObjectName, PublishFailureKind)>>,
    create_race_manifest: Mutex<Option<DatabaseManifest>>,
    fail_metadata: bool,
    fail_sync: AtomicBool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Operation {
    Capabilities,
    ReadObject(ObjectName),
    ReadRange(ObjectName),
    WriteObject(ObjectName),
    DeleteObject(ObjectName),
    ListPrefix(ObjectPrefix),
    ObjectMetadata(ObjectName),
    AcquireWriterLock(ObjectName),
    AppendObject(ObjectName),
    SyncObject(ObjectName),
    Publish(ObjectName, PublishMode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationKind {
    Capabilities,
    ReadObject,
    ReadRange,
    WriteObject,
    DeleteObject,
    ListPrefix,
    ObjectMetadata,
    AcquireWriterLock,
    AppendObject,
    SyncObject,
    Publish,
}

impl Operation {
    const fn kind(&self) -> OperationKind {
        match self {
            Self::Capabilities => OperationKind::Capabilities,
            Self::ReadObject(_) => OperationKind::ReadObject,
            Self::ReadRange(_) => OperationKind::ReadRange,
            Self::WriteObject(_) => OperationKind::WriteObject,
            Self::DeleteObject(_) => OperationKind::DeleteObject,
            Self::ListPrefix(_) => OperationKind::ListPrefix,
            Self::ObjectMetadata(_) => OperationKind::ObjectMetadata,
            Self::AcquireWriterLock(_) => OperationKind::AcquireWriterLock,
            Self::AppendObject(_) => OperationKind::AppendObject,
            Self::SyncObject(_) => OperationKind::SyncObject,
            Self::Publish(_, _) => OperationKind::Publish,
        }
    }
}

impl DurableTestBackend {
    fn new() -> Self {
        Self::with_capabilities(BackendCapabilities::from_slice(
            DURABLE_LOCAL_MODE_REQUIREMENTS,
        ))
    }

    fn with_capabilities(capabilities: BackendCapabilities) -> Self {
        Self {
            capabilities,
            objects: Mutex::new(BTreeMap::new()),
            operations: Mutex::new(Vec::new()),
            lock_held: Arc::new(AtomicBool::new(false)),
            release_count: Arc::new(AtomicUsize::new(0)),
            fail_lock: false,
            publish_failure: Mutex::new(None),
            publish_apply_then_fail: Mutex::new(None),
            create_race_manifest: Mutex::new(None),
            fail_metadata: false,
            fail_sync: AtomicBool::new(false),
        }
    }

    fn with_lock_failure() -> Self {
        Self {
            fail_lock: true,
            ..Self::new()
        }
    }

    fn with_publish_failure(kind: PublishFailureKind) -> Self {
        Self {
            publish_failure: Mutex::new(Some(kind)),
            publish_apply_then_fail: Mutex::new(None),
            ..Self::new()
        }
    }

    fn with_create_race(manifest: DatabaseManifest) -> Self {
        Self {
            create_race_manifest: Mutex::new(Some(manifest)),
            ..Self::new()
        }
    }

    fn with_metadata_failure() -> Self {
        Self {
            fail_metadata: true,
            ..Self::new()
        }
    }

    fn with_sync_failure() -> Self {
        let backend = Self::new();
        backend.set_sync_failure(true);
        backend
    }

    fn set_sync_failure(&self, fail: bool) {
        self.fail_sync.store(fail, Ordering::SeqCst);
    }

    fn set_publish_failure(&self, failure: Option<PublishFailureKind>) {
        *self.publish_failure.lock().expect("publish failure") = failure;
    }

    fn set_publish_apply_then_fail(&self, target: Option<(ObjectName, PublishFailureKind)>) {
        *self
            .publish_apply_then_fail
            .lock()
            .expect("apply then fail") = target;
    }

    fn write_raw(&self, object: ObjectName, bytes: Vec<u8>) {
        self.objects.lock().expect("objects").insert(object, bytes);
    }

    fn operations(&self) -> Vec<Operation> {
        self.operations.lock().expect("operations").clone()
    }

    fn last_manifest_bytes(&self) -> Option<Vec<u8>> {
        let manifest_object = ObjectLayout::database_manifest().expect("database manifest layout");
        self.objects
            .lock()
            .expect("objects")
            .get(&manifest_object)
            .cloned()
    }

    fn operation_kinds(&self) -> Vec<OperationKind> {
        self.operations().iter().map(Operation::kind).collect()
    }

    fn lock_is_held(&self) -> bool {
        self.lock_held.load(Ordering::SeqCst)
    }

    fn release_count(&self) -> usize {
        self.release_count.load(Ordering::SeqCst)
    }

    fn record(&self, operation: Operation) {
        self.operations.lock().expect("operations").push(operation);
    }
}

impl Backend for DurableTestBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.record(Operation::Capabilities);
        self.capabilities
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.record(Operation::ReadObject(name.clone()));
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.record(Operation::ReadRange(name.clone()));
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset()).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end_offset().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
        Ok(bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec())
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.record(Operation::WriteObject(name.clone()));
        self.objects
            .lock()
            .expect("objects")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        self.record(Operation::DeleteObject(name.clone()));
        let removed = self.objects.lock().expect("objects").remove(name).is_some();
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.record(Operation::ListPrefix(prefix.clone()));
        let mut names: Vec<_> = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.record(Operation::ObjectMetadata(name.clone()));
        if self.fail_metadata {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "metadata unavailable",
            ));
        }
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        self.record(Operation::AcquireWriterLock(name.clone()));
        if self.fail_lock {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "writer lock unavailable",
            ));
        }
        if self.lock_held.swap(true, Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "writer lock already held",
            ));
        }
        Ok(BackendWriterGuard::new(
            name.clone(),
            HeldWriterLock {
                locked: Arc::clone(&self.lock_held),
                release_count: Arc::clone(&self.release_count),
            },
        ))
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        self.record(Operation::AppendObject(name.clone()));
        let mut objects = self.objects.lock().expect("objects");
        let object = objects.entry(name.clone()).or_default();
        let start_offset = object.len() as u64;
        object.extend_from_slice(bytes);
        Ok(BackendAppend::new(
            start_offset,
            bytes.len() as u64,
            BackendMetadata::new(object.len() as u64, None),
        ))
    }

    fn sync_object(&self, name: &ObjectName) -> crate::backend::BackendResult<()> {
        self.record(Operation::SyncObject(name.clone()));
        if self.fail_sync.load(Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "sync unavailable",
            ));
        }
        Ok(())
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        self.record(Operation::Publish(name.clone(), mode));
        if mode == PublishMode::Create
            && name == &ObjectLayout::database_manifest().expect("database object")
        {
            if let Some(manifest) = self.create_race_manifest.lock().expect("race").take() {
                self.objects.lock().expect("objects").insert(
                    name.clone(),
                    encode_manifest(&manifest).expect("encoded race object"),
                );
                return Err(PublishError::precondition_failed(
                    name,
                    "object already exists",
                ));
            }
        }
        if let Some(kind) = *self.publish_failure.lock().expect("publish failure") {
            return Err(PublishError::new(
                name.clone(),
                kind,
                BackendError::new(BackendErrorKind::Unavailable, "injected publish failure"),
            ));
        }
        let apply_then_fail = {
            let mut armed = self
                .publish_apply_then_fail
                .lock()
                .expect("apply then fail");
            match armed.as_ref() {
                Some((target, _)) if target == name => armed.take(),
                _ => None,
            }
        };
        if let Some((_, kind)) = apply_then_fail {
            self.objects
                .lock()
                .expect("objects")
                .insert(name.clone(), bytes.to_vec());
            return Err(PublishError::new(
                name.clone(),
                kind,
                BackendError::new(
                    BackendErrorKind::Unavailable,
                    "injected apply-then-fail publish",
                ),
            ));
        }
        let mut objects = self.objects.lock().expect("objects");
        if mode == PublishMode::Create && objects.contains_key(name) {
            return Err(PublishError::precondition_failed(
                name,
                "object already exists",
            ));
        }
        objects.insert(name.clone(), bytes.to_vec());
        Ok(PublishOutcome::new(
            name.clone(),
            BackendMetadata::new(bytes.len() as u64, None),
            PublishDurability::Durable,
        ))
    }
}

struct HeldWriterLock {
    locked: Arc<AtomicBool>,
    release_count: Arc<AtomicUsize>,
}

impl Drop for HeldWriterLock {
    fn drop(&mut self) {
        self.release_count.fetch_add(1, Ordering::SeqCst);
        self.locked.store(false, Ordering::SeqCst);
    }
}

/// #2527: a fork with unflushed rows takes the HYBRID COW path — the
/// outcome carries an inherited layer over the parent's sealed tables (the
/// eager path reported 0/0 and materialized the whole store).
#[test]
fn fork_with_unsealed_rows_builds_a_cow_child() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x91);
    let child = branch_id(0x92);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"sealed-row", b"sealed"),
            generation_guard(),
        )
        .expect("sealed commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    let flush = runtime
        .flush_frozen(&flush_request(branch, "fork-cow-seal"))
        .expect("flush sealed rows");
    assert!(flush.completed());
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"unsealed-row", b"unsealed"),
            generation_guard(),
        )
        .expect("unsealed commit");

    let fork_version = runtime.visible_version();
    let outcome = runtime
        .fork_at_retained_version(
            branch,
            child,
            CommitBranchGeneration::new(1).expect("generation"),
            fork_version,
            CommitVersion::ZERO,
            None,
        )
        .expect("hybrid fork");
    assert_eq!(
        outcome.inherited_layer_count(),
        1,
        "unsealed rows no longer force the eager whole-store materialization",
    );
    assert!(outcome.inherited_table_count() >= 1);
}

/// #2527: a source whose rows are ALL unsealed (no owned table to
/// reference) keeps the eager path — there is nothing to COW.
#[test]
fn fork_of_an_all_unsealed_source_stays_eager() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x93);
    let child = branch_id(0x94);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"only-unsealed", b"value"),
            generation_guard(),
        )
        .expect("unsealed commit");

    let fork_version = runtime.visible_version();
    let outcome = runtime
        .fork_at_retained_version(
            branch,
            child,
            CommitBranchGeneration::new(1).expect("generation"),
            fork_version,
            CommitVersion::ZERO,
            None,
        )
        .expect("eager fork");
    assert_eq!(outcome.inherited_layer_count(), 0, "no table to reference");
    assert_eq!(outcome.inherited_table_count(), 0);
}

/// #2553 test scaffolding: commit one row, rotate, and drive a background
/// flush through its off-lock build. Returns the built step (publish NOT yet
/// begun) and the flush's published output names, still pinned in-flight.
fn build_flush_for_frontier_tests(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    key: &'static [u8],
    value: &'static [u8],
) -> (DurableBackgroundMaintenanceBuilt, Vec<ObjectName>) {
    runtime
        .execute_durable_commit(durable_put_batch(branch, key, value), generation_guard())
        .expect("frontier commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("frontier rotate");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("frontier enqueue flush");
    let step = runtime
        .start_next_background_flush_maintenance()
        .expect("frontier start flush")
        .expect("frontier flush step");
    let DurableBackgroundMaintenanceStep::Build(pending) = step else {
        panic!("expected a flush build step");
    };
    let built = pending.build().expect("frontier build flush");
    let outputs = runtime.inflight_table_outputs().snapshot();
    assert!(!outputs.is_empty(), "the flush published an output");
    (built, outputs)
}

/// #2553 test scaffolding: run a built maintenance step through publish to a
/// CONFIRMED durable manifest (both publish arms).
fn publish_to_confirmation(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    built: DurableBackgroundMaintenanceBuilt,
) {
    match runtime.begin_publish_phase(built).expect("begin publish") {
        PreparedPublishStep::OffLock(prepared) => {
            let (prepared, write_result) = prepared.persist_off_lock();
            runtime
                .finish_publish_phase(prepared, write_result)
                .expect("finish publish");
        }
        PreparedPublishStep::Done(result) => {
            result.expect("publish done");
        }
    }
}

/// #2553: an object consumed OUT of branch state while the manifest that still
/// lists it is mid-persist (per-branch publish slot held, global lock released)
/// must not be swept — the landing manifest is what recovery loads.
///
/// Shape: flush A's publish is HELD at the off-lock step (its manifest `M_A`
/// lists t1,t2); a compaction consumes t1,t2 (installs, then its own publish
/// DEFERS on the busy slot); the mark+sweep runs; `M_A` then persists and
/// confirms. Pre-fix the sweep deleted t2 (absent from the visible manifest
/// and from branch state, unpinned) and recovery failed with
/// `corruption.lifecycle.table_manifest`. The pending-publication frontier
/// keeps every M_A-listed object protected until the persist resolves.
#[test]
fn sweep_spares_objects_listed_by_a_mid_persist_manifest() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x79);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // Flush 1 → t1, fully published + confirmed (the durable manifest lists t1).
    let (built, flush_one_outputs) =
        build_flush_for_frontier_tests(&mut runtime, branch, b"frontier-row-1", b"value-1");
    publish_to_confirmation(&mut runtime, built);

    // Flush 2 → t2; its publish is HELD at the off-lock step: `M_A` (listing
    // t1,t2) is built and the branch publish slot is taken, but nothing has
    // been persisted yet.
    let (built, flush_two_outputs) =
        build_flush_for_frontier_tests(&mut runtime, branch, b"frontier-row-2", b"value-2");
    let held_publish = match runtime.begin_publish_phase(built).expect("publish flush 2") {
        PreparedPublishStep::OffLock(prepared) => prepared,
        PreparedPublishStep::Done(_) => panic!("flush 2 publish must reach the off-lock step"),
    };

    // Compaction consumes t1,t2 into an L1 output; its install lands but its
    // own manifest publish DEFERS on the slot flush 2 holds.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue compaction");
    let step = runtime
        .start_next_background_table_rewrite_maintenance()
        .expect("start compaction")
        .expect("compaction step");
    let DurableBackgroundMaintenanceStep::Build(pending) = step else {
        panic!("expected compaction build step");
    };
    let built = pending.build().expect("build compaction");
    let outcome = match runtime
        .begin_publish_phase(built)
        .expect("publish compaction")
    {
        PreparedPublishStep::Done(result) => result.expect("compaction publish resolves locked"),
        PreparedPublishStep::OffLock(_) => {
            panic!("compaction publish must defer on the held slot")
        }
    };
    assert_eq!(
        outcome.status(),
        MaintenanceOutcomeStatus::Deferred,
        "the compaction's manifest publish defers while flush 2 holds the slot",
    );

    // Mark + sweep while M_A is mid-persist. Pre-fix this deleted the
    // consumed inputs; the pending-publication frontier must pin them.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue retention");
    runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention outcome");
    if let Some(sweep) = runtime
        .run_next_quarantine_maintenance()
        .expect("run sweep")
    {
        assert_eq!(sweep.status(), MaintenanceOutcomeStatus::Completed);
    }
    for name in flush_one_outputs.iter().chain(flush_two_outputs.iter()) {
        assert!(
            backend.object_metadata(name).is_ok(),
            "mid-persist-manifest-listed object {name} must survive the sweep",
        );
    }

    // `M_A` lands and confirms: the durable manifest now lists t1,t2.
    let (held_publish, write_result) = held_publish.persist_off_lock();
    runtime
        .finish_publish_phase(held_publish, write_result)
        .expect("finish flush 2 publish");
    drop(runtime);

    // Recovery loads `M_A`; every listed object must exist.
    let mut shell =
        assemble_shell(StorageMode::DurableLocalStandard, branch, backend).expect("reopen shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery after mid-persist manifest landed");
    shell.complete_recovery(&recovery).expect("reopen runtime");
}

/// #2553 (hole 2): a manifest replace that lands VISIBLE but reports its
/// durability unconfirmed must not expose the PRIOR manifest's objects to the
/// sweep — a crash reverts recovery to the last confirmed manifest, which
/// still lists them. The confirmed frontier (advanced only on confirmed
/// publishes) keeps them pinned.
#[test]
fn sweep_spares_objects_listed_by_the_last_confirmed_manifest() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x7a);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    let manifest_object =
        ObjectLayout::branch_table_manifest(&branch.to_string()).expect("manifest object");

    // Two flushes, fully published + confirmed: the durable manifest lists t1,t2.
    let mut flush_outputs = Vec::new();
    for (key, value) in [
        (&b"confirmed-row-1"[..], &b"value-1"[..]),
        (&b"confirmed-row-2"[..], &b"value-2"[..]),
    ] {
        let (built, outputs) = build_flush_for_frontier_tests(&mut runtime, branch, key, value);
        flush_outputs.extend(outputs);
        publish_to_confirmation(&mut runtime, built);
    }
    assert_eq!(flush_outputs.len(), 2, "two confirmed L0 tables");
    let confirmed_manifest_bytes = backend
        .read_object(&manifest_object)
        .expect("confirmed manifest bytes");

    // Inline compaction consumes t1,t2; its manifest replace LANDS (visible)
    // but reports durability unconfirmed → manifest debt, no confirm.
    backend.set_publish_apply_then_fail(Some((
        manifest_object.clone(),
        PublishFailureKind::VisibleDurabilityUnconfirmed,
    )));
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue compaction");
    let outcome = runtime
        .run_next_table_rewrite_maintenance()
        .expect("run compaction")
        .expect("compaction outcome");
    assert_ne!(
        outcome.status(),
        MaintenanceOutcomeStatus::Deferred,
        "the inline compaction ran (its manifest persist ended uncertain)",
    );

    // Sweep while the visible manifest is unconfirmed. Pre-fix the mark read
    // the visible manifest (which no longer lists t1,t2) and swept them; the
    // confirmed frontier must keep the crash-revert targets alive.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue retention");
    runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention outcome");
    if let Some(sweep) = runtime
        .run_next_quarantine_maintenance()
        .expect("run sweep")
    {
        assert_eq!(sweep.status(), MaintenanceOutcomeStatus::Completed);
    }
    for name in &flush_outputs {
        assert!(
            backend.object_metadata(name).is_ok(),
            "confirmed-manifest-listed object {name} must survive the sweep",
        );
    }

    // Simulated crash-revert: the unconfirmed replace never became durable —
    // restore the confirmed manifest bytes and recover. Every object it lists
    // must exist.
    drop(runtime);
    backend.write_raw(manifest_object, confirmed_manifest_bytes);
    let mut shell =
        assemble_shell(StorageMode::DurableLocalStandard, branch, backend).expect("reopen shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery from the crash-reverted confirmed manifest");
    shell.complete_recovery(&recovery).expect("reopen runtime");
}

/// #2553 anti-starvation guard: frontier protection RELEASES once a confirmed
/// publish stops listing the objects — superseded tables still get reclaimed
/// (the #2524 reclaim-liveness regression this fix must not reintroduce).
#[test]
fn frontier_protection_releases_after_the_next_confirmed_publish() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x7b);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // Two confirmed L0 tables, then an inline compaction that consumes them
    // and CONFIRMS its manifest (no fault): t1,t2 are superseded and no
    // recovery-relevant manifest lists them.
    let mut flush_outputs = Vec::new();
    for (key, value) in [
        (&b"released-row-1"[..], &b"value-1"[..]),
        (&b"released-row-2"[..], &b"value-2"[..]),
    ] {
        let (built, outputs) = build_flush_for_frontier_tests(&mut runtime, branch, key, value);
        flush_outputs.extend(outputs);
        publish_to_confirmation(&mut runtime, built);
    }
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue compaction");
    let outcome = runtime
        .run_next_table_rewrite_maintenance()
        .expect("run compaction")
        .expect("compaction outcome");
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);

    // The superseded inputs must now be sweepable: mark + sweep reclaims them.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue retention");
    runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention outcome");
    let sweep = runtime
        .run_next_quarantine_maintenance()
        .expect("run sweep")
        .expect("sweep outcome");
    assert_eq!(sweep.status(), MaintenanceOutcomeStatus::Completed);
    for name in &flush_outputs {
        assert!(
            backend.object_metadata(name).is_err(),
            "superseded object {name} must be reclaimed once no manifest lists it",
        );
    }

    // And the store still recovers cleanly.
    drop(runtime);
    let mut shell =
        assemble_shell(StorageMode::DurableLocalStandard, branch, backend).expect("reopen shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery after frontier release");
    shell.complete_recovery(&recovery).expect("reopen runtime");
}

/// #2553 test scaffolding: enqueue a level-0 compaction and drive it through
/// its off-lock build; the publish phase is the caller's.
fn build_compaction_for_adoption_test(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
) -> DurableBackgroundMaintenanceBuilt {
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(branch, 0))
        .expect("enqueue compaction");
    let step = runtime
        .start_next_background_table_rewrite_maintenance()
        .expect("start compaction")
        .expect("compaction step");
    let DurableBackgroundMaintenanceStep::Build(pending) = step else {
        panic!("expected a compaction build step");
    };
    pending.build().expect("build compaction")
}

/// #2553 (adoption race): content-derived rewrite identities are
/// deterministic across retries, so a re-planned compaction can find its
/// output already on disk — an orphan of an abandoned attempt — and ADOPT it
/// without republishing. If a sweep stage has already frozen that name for
/// deletion, the adopted install would let the next manifest reference an
/// object the stage is deleting off-lock. The install must defer instead.
#[test]
fn adopted_rewrite_output_defers_while_its_object_is_sweep_staged() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x7c);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);

    // Two confirmed L0 tables — the compaction inputs.
    for (key, value) in [
        (&b"adopt-row-1"[..], &b"value-1"[..]),
        (&b"adopt-row-2"[..], &b"value-2"[..]),
    ] {
        let (built, _outputs) = build_flush_for_frontier_tests(&mut runtime, branch, key, value);
        publish_to_confirmation(&mut runtime, built);
    }

    // Attempt A: build the compaction off-lock (outputs published under
    // content-derived names), then ABANDON it — the orphans stay on disk.
    let built_a = build_compaction_for_adoption_test(&mut runtime, branch);
    let orphaned = runtime.inflight_table_outputs().snapshot();
    assert!(!orphaned.is_empty(), "attempt A published outputs");
    drop(built_a);
    assert!(
        runtime.inflight_table_outputs().snapshot().is_empty(),
        "abandoning attempt A releases its pins",
    );
    // Reopen: the crash-analog of the abandonment (the in-flight registry is
    // in-memory by design). The orphans persist on disk under their
    // content-derived names; the task queue starts fresh.
    drop(runtime);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    for name in &orphaned {
        assert!(
            backend.object_metadata(name).is_ok(),
            "attempt A's orphan {name} persists across the reopen",
        );
    }

    // Mark the orphans and HOLD the sweep stage un-run: the registry now
    // freezes the doomed names while the deletion is notionally in flight.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue retention");
    runtime
        .run_next_retention_maintenance()
        .expect("run retention")
        .expect("retention outcome");
    let held_stage = match runtime
        .start_next_background_quarantine_sweep()
        .expect("start sweep")
        .expect("sweep step")
    {
        DurableBackgroundMaintenanceStep::SweepStage(inputs) => *inputs,
        _ => panic!("expected a sweep stage step"),
    };

    // Attempt B: identical inputs, identical content, identical names — the
    // publish collides and ADOPTS the orphans (their bytes still exist).
    let built_b = build_compaction_for_adoption_test(&mut runtime, branch);
    let publish = runtime
        .begin_publish_phase(built_b)
        .expect("begin compaction B publish");
    let outcome = match publish {
        PreparedPublishStep::Done(result) => result.expect("compaction B resolves under lock"),
        PreparedPublishStep::OffLock(prepared) => {
            // Pre-fix path: the adopted outputs install and the manifest
            // persists, referencing objects the held stage is about to
            // delete.
            let (prepared, write_result) = prepared.persist_off_lock();
            runtime
                .finish_publish_phase(prepared, write_result)
                .expect("finish compaction B publish")
        }
    };
    assert_eq!(
        outcome.status(),
        MaintenanceOutcomeStatus::Deferred,
        "an install adopting sweep-staged objects must defer, not proceed",
    );

    // The held stage now runs and deletes the orphans; folding it back
    // releases the registry.
    let staged = held_stage.stage();
    runtime
        .finish_quarantine_sweep(staged)
        .expect("finish held sweep");
    for name in &orphaned {
        assert!(
            backend.object_metadata(name).is_err(),
            "the staged orphan {name} is deleted by the sweep",
        );
    }

    // Attempt C: with the names free again, the retried compaction publishes
    // FRESH bytes and completes; the store recovers cleanly.
    let built_c = build_compaction_for_adoption_test(&mut runtime, branch);
    let outcome = match runtime
        .begin_publish_phase(built_c)
        .expect("begin compaction C publish")
    {
        PreparedPublishStep::OffLock(prepared) => {
            let (prepared, write_result) = prepared.persist_off_lock();
            runtime
                .finish_publish_phase(prepared, write_result)
                .expect("finish compaction C publish")
        }
        PreparedPublishStep::Done(result) => result.expect("compaction C publish done"),
    };
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);

    drop(runtime);
    let mut shell =
        assemble_shell(StorageMode::DurableLocalStandard, branch, backend).expect("reopen shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery after adoption race resolves");
    shell.complete_recovery(&recovery).expect("reopen runtime");
}

/// Foreground kill for the enqueue/delete race (#2859 family F): drive the two
/// foreground maintenance lanes DIRECTLY (no background controller exists on the
/// lifecycle runtime), so the deleted-scope cancel arms — and specifically their
/// task-selection closures — are deterministically exercised. Each lane must
/// consume its stale task as Canceled; a wrong-task selection strands the stale
/// task and the lane returns nothing.
#[test]
fn foreground_lanes_cancel_tasks_whose_branch_was_deleted_after_enqueue() {
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let seeded = branch_id(0x8c);
    let victim = branch_id(0x8d);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, seeded, backend);
    runtime
        .create_branch(
            victim,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create victim branch");
    runtime
        .execute_durable_commit(
            durable_put_batch(victim, b"deleted-scope-row", b"value"),
            generation_guard(),
        )
        .expect("victim durable commit");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(victim))
        .expect("enqueue victim flush");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::compaction(victim, 0))
        .expect("enqueue victim compaction");
    runtime
        .delete_branch(victim, generation_guard(), None)
        .expect("delete victim after enqueue");

    let mut flush_lane_canceled = 0;
    while let Some(outcome) = runtime.run_next_flush_maintenance().expect("flush lane") {
        assert_ne!(
            outcome.status(),
            MaintenanceOutcomeStatus::Failed,
            "the flush lane must not fail on the enqueue/delete race",
        );
        if outcome.status() == MaintenanceOutcomeStatus::Canceled {
            assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Flush);
            flush_lane_canceled += 1;
        }
    }
    assert_eq!(
        flush_lane_canceled, 1,
        "the stale flush task must be consumed as Canceled by its own lane",
    );

    let mut rewrite_lane_canceled = 0;
    while let Some(outcome) = runtime
        .run_next_table_rewrite_maintenance()
        .expect("rewrite lane")
    {
        assert_ne!(
            outcome.status(),
            MaintenanceOutcomeStatus::Failed,
            "the rewrite lane must not fail on the enqueue/delete race",
        );
        if outcome.status() == MaintenanceOutcomeStatus::Canceled {
            assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Compaction);
            rewrite_lane_canceled += 1;
        }
    }
    assert_eq!(
        rewrite_lane_canceled, 1,
        "the stale compaction task must be consumed as Canceled by its own lane",
    );
}

/// #2863 foreground-lane kill: a checkpoint over a snapshot-recovered base —
/// whose rows live in a VOLATILE owned L0 with no durable catalog entry — must
/// publish a SELF-CONTAINED snapshot (the volatile rows captured) and must not
/// record the volatile coverage as a flushed base. Driven through the sync
/// checkpoint lane so the arm is deterministically exercised.
#[test]
fn sync_checkpoint_captures_volatile_base_rows_and_records_no_flush_floor() {
    use crate::format::SNAPSHOT_ROW_SECTION_KIND;
    use crate::service::{DatabaseManifestService, SnapshotService};

    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x8f);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"vol-a", b"one"),
            generation_guard(),
        )
        .expect("commit a");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"vol-b", b"two"),
            generation_guard(),
        )
        .expect("commit b");
    // The close writes a delta checkpoint carrying both rows.
    runtime.close().expect("first close");

    // The reopen installs the snapshot's rows as a volatile owned L0 (no table
    // manifest exists), then one more commit lands in the memtable.
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"vol-c", b"three"),
            generation_guard(),
        )
        .expect("commit c");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue checkpoint");
    let outcome = runtime
        .run_next_checkpoint_maintenance()
        .expect("checkpoint lane")
        .expect("checkpoint outcome");
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);

    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(
        manifest.flushed_through_commit_id(),
        None,
        "a volatile snapshot-install L0 is not a flushed base",
    );
    let snapshot_id = manifest.snapshot_id().expect("published snapshot id");
    let container = SnapshotService::new(backend)
        .load_required(snapshot_id)
        .expect("published snapshot");
    let mut versions: Vec<u64> = Vec::new();
    for section in container.sections() {
        if section.section_kind() == SNAPSHOT_ROW_SECTION_KIND {
            for row in crate::format::decode_snapshot_row_payload(section.payload())
                .expect("decode snapshot rows")
            {
                versions.push(row.commit_version().as_u64());
            }
        }
    }
    versions.sort_unstable();
    assert_eq!(
        versions,
        vec![1, 2, 3],
        "the snapshot must be self-contained: volatile-base rows captured alongside the delta",
    );
}

/// #2863 background-lane kill: an EMPTY delta over a DURABLE flushed base must
/// still publish through the background checkpoint pipeline (the watermark
/// advance depends on `has_durable_rows`), and the published snapshot must
/// carry no rows (durable-base content is deltaed over, never re-captured).
#[test]
fn background_checkpoint_publishes_empty_delta_over_durable_base() {
    use crate::format::SNAPSHOT_ROW_SECTION_KIND;
    use crate::service::{DatabaseManifestService, SnapshotService};

    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    let branch = branch_id(0x90);
    let mut runtime = open_runtime(StorageMode::DurableLocalStandard, branch, backend);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, b"empty-delta-base", b"value"),
            generation_guard(),
        )
        .expect("durable commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let flush = runtime
        .run_next_flush_maintenance()
        .expect("flush lane")
        .expect("flush outcome");
    assert_eq!(flush.status(), MaintenanceOutcomeStatus::Completed);

    // Memtable empty, durable base present: the background checkpoint must
    // publish the watermark advance.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue checkpoint");
    let step = runtime
        .start_next_background_checkpoint_maintenance()
        .expect("start background checkpoint")
        .expect("background checkpoint step");
    let DurableBackgroundMaintenanceStep::Build(pending) = step else {
        panic!("expected a checkpoint build step, got a completed outcome");
    };
    let built = (*pending).build().expect("build checkpoint");
    let publish = runtime
        .begin_publish_phase(built)
        .expect("begin publish phase");
    let outcome = match publish {
        PreparedPublishStep::Done(result) => result.expect("publish done"),
        PreparedPublishStep::OffLock(prepared) => {
            let (prepared, write_result) = prepared.persist_off_lock();
            runtime
                .finish_publish_phase(prepared, write_result)
                .expect("finish publish phase")
        }
    };
    assert_eq!(
        outcome.status(),
        MaintenanceOutcomeStatus::Completed,
        "an empty delta over a durable base advances the snapshot watermark",
    );

    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    let snapshot_id = manifest.snapshot_id().expect("published snapshot id");
    let container = SnapshotService::new(backend)
        .load_required(snapshot_id)
        .expect("published snapshot");
    let mut row_count = 0usize;
    for section in container.sections() {
        if section.section_kind() == SNAPSHOT_ROW_SECTION_KIND {
            row_count += crate::format::decode_snapshot_row_payload(section.payload())
                .expect("decode snapshot rows")
                .len();
        }
    }
    assert_eq!(
        row_count, 0,
        "durable-base rows are deltaed over, not re-captured",
    );
}

#[test]
fn durable_assembly_refuses_a_missing_manifest_when_durable_objects_exist() {
    // #3015 promoted contract: an existing store whose database manifest is
    // gone is structurally damaged — silently creating a fresh manifest
    // erased the snapshot-id floor, flush watermark, and checkpoint lineage
    // (checkpoints then collided with orphaned snapshot objects forever,
    // behind a Healthy claim). Creation cannot legally race this shape: the
    // manifest is the first durable publish of a new store and its replace
    // is atomic, so residue without a manifest always means loss.
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    backend.write_raw(
        ObjectLayout::wal_segment(1).expect("segment object"),
        encode_wal_segment_header(&WalSegmentHeader::new(1, DATABASE_ID)),
    );

    let error = assemble_shell(StorageMode::DurableLocalStandard, branch_id(0x2a), backend)
        .expect_err("durable residue without a manifest must refuse the open");
    assert!(
        matches!(error, LifecycleError::RecoveryCorruption { .. }),
        "{error:?}"
    );
    assert_eq!(error.code(), "corruption.lifecycle.recovery_corruption");
}

#[test]
fn durable_assembly_refuses_a_missing_manifest_when_snapshots_exist() {
    // The exact #3015 shape: an orphaned snapshot object with no manifest.
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    backend.write_raw(
        ObjectLayout::snapshot(1).expect("snapshot object"),
        vec![0xA5; 16],
    );

    let error = assemble_shell(StorageMode::DurableLocalStandard, branch_id(0x2b), backend)
        .expect_err("snapshot residue without a manifest must refuse the open");
    assert!(
        matches!(error, LifecycleError::RecoveryCorruption { .. }),
        "{error:?}"
    );
    assert_eq!(error.code(), "corruption.lifecycle.recovery_corruption");
}

#[test]
fn durable_assembly_still_creates_over_pre_manifest_residue() {
    // Direction control: a crash during FIRST creation can leave scratch
    // (temporary/) and lock objects before the manifest publishes — that
    // store is legitimately new and must still create, not refuse.
    let backend: &'static DurableTestBackend =
        crate::testkit::leak_static(DurableTestBackend::new());
    backend.write_raw(
        ObjectLayout::temporary_object("create", "manifest-attempt").expect("temporary object"),
        vec![0xA5; 8],
    );

    let shell = assemble_shell(StorageMode::DurableLocalStandard, branch_id(0x2c), backend)
        .expect("pre-manifest scratch must not block creation");
    assert_eq!(
        shell.assembly_facts().disposition(),
        StorageOpenDisposition::Created
    );
}

#[test]
fn durable_assembly_refuses_a_missing_manifest_for_every_post_manifest_family() {
    // One residue object per remaining post-manifest family (WAL and
    // snapshots have dedicated tests above) — a dropped family in the
    // residue check would silently re-legalize fabricating a manifest over
    // that family's survivors.
    let residue_objects = [
        ObjectLayout::wal_watermark().expect("meta watermark"),
        ObjectLayout::branch_table_manifest("0101010101010101").expect("table manifest"),
        ObjectLayout::quarantine_manifest("0101010101010101").expect("quarantine manifest"),
    ];
    for (index, object) in residue_objects.into_iter().enumerate() {
        let backend: &'static DurableTestBackend =
            crate::testkit::leak_static(DurableTestBackend::new());
        backend.write_raw(object.clone(), vec![0xA5; 8]);
        let branch = branch_id(0x30 + u8::try_from(index).expect("small index"));
        let Err(error) = assemble_shell(StorageMode::DurableLocalStandard, branch, backend) else {
            panic!("{object:?} residue without a manifest must refuse the open");
        };
        assert!(
            matches!(error, LifecycleError::RecoveryCorruption { .. }),
            "{object:?}: {error:?}"
        );
    }
}
