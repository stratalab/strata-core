use super::*;

#[test]
#[cfg(feature = "localfs")]
fn open_local_returns_durable_standard_runtime() {
    let outcome = StorageRuntime::open_local(temp_dir_for_api_test("open-local"))
        .expect("local durable open should succeed");
    let summary = outcome.summary();
    let mut runtime = outcome.into_runtime();

    assert_eq!(
        summary.mode(),
        StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Standard,
        }
    );
    assert_eq!(summary.disposition(), StorageOpenDisposition::Created);
    assert_eq!(summary.recovery_health(), RecoveryHealthSummary::Healthy);
    assert!(summary.recovered_visible_version().is_some());
    assert!(summary.has_durable_recovery_facts());
    assert!(summary.backend_capabilities_used());
    assert_eq!(
        summary.maintenance_scheduling_policy(),
        StorageMaintenanceSchedulingPolicy::Background
    );
    assert_eq!(runtime.state(), StorageRuntimeState::Open);
    assert_eq!(
        runtime.maintenance_scheduling_policy_for_test(),
        crate::lifecycle::LifecycleMaintenanceSchedulingPolicy::Background
    );
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .background_worker_count(),
        default_background_worker_count()
    );

    let close = runtime.close().expect("local durable close");
    assert!(close.durable_synced());
}

#[test]
#[cfg(feature = "localfs")]
fn open_local_reopens_persisted_commits_from_same_root() {
    let root = temp_dir_for_api_test("open-local-reopen");
    let branch = StorageRuntime::default_branch_id_for_test();
    let storage_space = StorageSpaceId::new(vec![0x20]).expect("valid engine storage space");
    let storage_key = key(b"persisted");
    let storage_value = StorageValue::new(b"value".to_vec());
    let batch = CommitBatch::new(
        branch,
        vec![CommitMutation::Put {
            storage_space: storage_space.clone(),
            key: storage_key.clone(),
            value: storage_value.clone(),
            ttl: None,
        }],
        CommitOptions::default(),
    )
    .expect("valid put batch");

    let mut first = StorageRuntime::open_local(root.clone())
        .expect("first local durable open")
        .into_runtime();
    first.commit(&batch).expect("persisted commit");
    first.close().expect("first local durable close");
    drop(first);

    let second = StorageRuntime::open_local(root).expect("second local durable open");
    let second_summary = second.summary();
    let second = second.into_runtime();
    let read = second
        .read_point(&PointReadRequest::new(
            branch,
            storage_space,
            storage_key,
            ReadBound::Latest,
        ))
        .expect("read persisted value");
    let row = read.row().expect("persisted row");

    assert_eq!(
        second_summary.disposition(),
        StorageOpenDisposition::OpenedExisting
    );
    assert_eq!(row.value(), Some(&storage_value));
}

#[test]
#[cfg(feature = "localfs")]
fn open_durable_local_returns_requested_policy() {
    let outcome = StorageRuntime::open_durable_local(
        temp_dir_for_api_test("open-durable-local-always"),
        StorageDurabilityPolicy::Always,
    )
    .expect("local durable open should succeed");
    let summary = outcome.summary();
    let mut runtime = outcome.into_runtime();

    assert_eq!(
        summary.mode(),
        StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Always,
        }
    );
    assert!(summary.has_durable_recovery_facts());

    let close = runtime.close().expect("local durable close");
    assert!(close.durable_synced());
}

#[test]
#[cfg(not(feature = "localfs"))]
fn open_local_without_localfs_rejects_without_cache_fallback() {
    let outcome = StorageRuntime::open_local(std::path::PathBuf::from("no-localfs"));

    match outcome {
        Ok(open) => {
            let summary = open.summary();
            panic!(
                "open_local unexpectedly succeeded in mode {:?}",
                summary.mode()
            );
        }
        Err(error) => {
            assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
            assert_eq!(error.code(), "unsupported.storage_api.capability");
        }
    }
}

#[test]
fn open_cache_returns_open_runtime() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let runtime = outcome.into_runtime();

    assert_eq!(runtime.state(), StorageRuntimeState::Open);
    assert!(runtime.is_open());
}

#[test]
fn open_cache_reports_cache_mode() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");

    assert_eq!(outcome.summary().mode(), StorageMode::Cache);
}

#[test]
fn open_cache_reports_no_durable_recovery_facts() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");

    assert!(!outcome.summary().has_durable_recovery_facts());
}

#[test]
fn open_cache_does_not_construct_wal_or_manifest_services() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let mut runtime = outcome.into_runtime();
    let close = runtime.close().expect("cache close");

    assert!(!close.durable_synced());
}

#[test]
fn open_cache_close_is_idempotent() {
    let outcome =
        StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open should succeed");
    let mut runtime = outcome.into_runtime();

    let first = runtime.close().expect("first close");
    assert_eq!(first.state(), StorageRuntimeState::Closed);
    assert!(!first.idempotent());
    assert!(first.commits_quiesced());
    assert!(first.maintenance_drained());
    assert!(!first.durable_synced());
    assert!(first.guards_released());

    let second = runtime.close().expect("second close");
    assert_eq!(second.state(), StorageRuntimeState::Closed);
    assert!(second.idempotent());
}

#[test]
fn close_open_cache_returns_final_facts() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let mut runtime = outcome.into_runtime();
    let close = runtime.close().expect("cache close");

    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert!(close.commits_quiesced());
    assert!(close.maintenance_drained());
    assert!(!close.durable_synced());
    assert!(close.guards_released());
}

#[test]
fn close_twice_returns_idempotent_outcome() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let mut runtime = outcome.into_runtime();

    let first = runtime.close().expect("first close");
    let second = runtime.close().expect("second close");

    assert!(!first.idempotent());
    assert!(second.idempotent());
    assert_eq!(second.state(), StorageRuntimeState::Closed);
}

#[test]
#[cfg(feature = "localfs")]
fn close_failure_preserves_source_chain() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-close-failure"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open should succeed");
    let mut runtime = outcome.into_runtime();
    assert!(runtime.release_writer_guard_for_test());

    let error = runtime
        .close()
        .expect_err("missing writer guard fails close");

    assert_eq!(error.code(), "internal.storage_api.lifecycle");
    assert_eq!(error.class(), StorageApiErrorClass::Internal);
    let source = error.source().expect("lifecycle source is preserved");
    assert!(source.is::<crate::lifecycle::LifecycleError>());
}

#[test]
fn close_then_read_rejects_closed_runtime() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let mut runtime = outcome.into_runtime();
    runtime.close().expect("close");

    let error = runtime
        .require_open("read requires an open storage runtime")
        .expect_err("closed runtime rejects read");

    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn close_then_commit_rejects_closed_runtime() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let mut runtime = outcome.into_runtime();
    runtime.close().expect("close");

    let error = runtime
        .require_open("commit requires an open storage runtime")
        .expect_err("closed runtime rejects commit");

    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn close_then_maintenance_rejects_closed_runtime() {
    let outcome = StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open");
    let mut runtime = outcome.into_runtime();
    runtime.close().expect("close");

    let error = runtime
        .require_open("maintenance requires an open storage runtime")
        .expect_err("closed runtime rejects maintenance");

    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
fn open_cache_operation_after_close_rejects() {
    let outcome =
        StorageRuntime::open(StorageOpenOptions::cache()).expect("cache open should succeed");
    let mut runtime = outcome.into_runtime();

    runtime.close().expect("close");
    let error = runtime
        .require_open("read requires an open storage runtime")
        .expect_err("closed runtime rejects operation");

    assert_eq!(error.code(), "failed_precondition.storage_api.state");
}

#[test]
#[cfg(feature = "localfs")]
fn open_durable_modes_return_open_runtime() {
    for policy in [
        StorageDurabilityPolicy::Standard,
        StorageDurabilityPolicy::Always,
    ] {
        let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-mode"));
        let outcome =
            StorageRuntime::open_with_backend(StorageOpenOptions::durable_local(policy), &backend)
                .expect("durable open should succeed");
        let summary = outcome.summary();
        let mut runtime = outcome.into_runtime();

        assert_eq!(summary.mode(), StorageMode::DurableLocal { policy });
        assert_eq!(summary.disposition(), StorageOpenDisposition::Created);
        assert_eq!(summary.recovery_health(), RecoveryHealthSummary::Healthy);
        assert!(summary.recovered_visible_version().is_some());
        assert!(summary.has_durable_recovery_facts());
        assert!(summary.backend_capabilities_used());
        assert_eq!(runtime.state(), StorageRuntimeState::Open);

        let close = runtime.close().expect("durable close");
        assert_eq!(close.state(), StorageRuntimeState::Closed);
        assert!(close.durable_synced());
    }
}

#[test]
#[cfg(feature = "localfs")]
fn open_durable_local_with_backend_returns_open_runtime() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-helper"));
    let outcome = StorageRuntime::open_durable_local_with_backend(
        StorageDurabilityPolicy::Standard,
        &backend,
    )
    .expect("durable helper open should succeed");
    let summary = outcome.summary();
    let runtime = outcome.into_runtime();

    assert_eq!(
        summary.mode(),
        StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Standard
        }
    );
    assert!(summary.has_durable_recovery_facts());
    assert!(summary.backend_capabilities_used());
    assert_eq!(
        summary.maintenance_scheduling_policy(),
        StorageMaintenanceSchedulingPolicy::Background
    );
    assert_eq!(runtime.state(), StorageRuntimeState::Open);
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("maintenance status")
            .background_worker_count(),
        default_background_worker_count()
    );
}

#[test]
#[cfg(feature = "localfs")]
fn open_durable_with_backend_deterministic_inline_uses_inline_background_driver() {
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(temp_dir_for_api_test(
        "durable-inline-background",
    )));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::DeterministicInline,
            ),
        backend,
    )
    .expect("durable deterministic-inline open should use owned inline background driver");
    let summary = outcome.summary();
    let runtime = outcome.into_runtime();

    assert_eq!(
        summary.maintenance_scheduling_policy(),
        StorageMaintenanceSchedulingPolicy::DeterministicInline
    );
    assert_eq!(
        runtime.maintenance_scheduling_policy_for_test(),
        crate::lifecycle::LifecycleMaintenanceSchedulingPolicy::Background
    );
    assert_eq!(
        runtime
            .maintenance_status()
            .expect("initial maintenance status")
            .background_worker_count(),
        0
    );

    runtime
        .commit(&background_put_batch(
            b"durable-inline-background",
            b"value".to_vec(),
        ))
        .expect("seed durable row");
    runtime
        .enqueue_maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("enqueue checkpoint through inline background driver");
    runtime.wait_background_idle_for_test();

    let status = runtime
        .maintenance_status()
        .expect("final maintenance status");
    assert_eq!(status.pending_tasks(), 0);
    assert_eq!(status.background_worker_count(), 0);
    assert!(status.background_tasks_completed() >= 1);
}

#[test]
#[cfg(feature = "localfs")]
fn open_durable_standard_returns_open_runtime() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-standard"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("standard durable open should succeed");
    let runtime = outcome.into_runtime();

    assert_eq!(runtime.state(), StorageRuntimeState::Open);
    assert!(runtime.is_open());
}

#[test]
#[cfg(feature = "localfs")]
fn open_durable_always_returns_open_runtime() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-always"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("always durable open should succeed");
    let runtime = outcome.into_runtime();

    assert_eq!(runtime.state(), StorageRuntimeState::Open);
    assert!(runtime.is_open());
}

#[test]
#[cfg(feature = "localfs")]
fn create_durable_local_returns_created_disposition() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-created"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable create should succeed");

    assert_eq!(
        outcome.summary().disposition(),
        StorageOpenDisposition::Created
    );
}

#[test]
#[cfg(feature = "localfs")]
fn durable_open_reports_backend_capabilities_used() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-capabilities"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open should succeed");

    assert!(outcome.summary().backend_capabilities_used());
}

#[test]
#[cfg(feature = "localfs")]
fn durable_open_reports_recovery_health() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-health"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open should succeed");

    assert_eq!(
        outcome.summary().recovery_health(),
        RecoveryHealthSummary::Healthy
    );
}

#[test]
fn durable_open_degraded_health_survives_boundary_mapping() {
    let summary = StorageOpenSummary::with_open_facts(
        StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Standard,
        },
        StorageOpenDisposition::OpenedExisting,
        RecoveryHealthSummary::Degraded,
        Some(CommitVersion::new(3)),
        true,
        StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
        true,
        true,
        crate::api::StorageBudgetSource::FixedDefault { total_bytes: 0 },
    );

    assert_eq!(summary.recovery_health(), RecoveryHealthSummary::Degraded);
    assert!(summary.has_durable_recovery_facts());
}

#[test]
fn borrowed_memory_durable_background_open_rejects_with_policy_error() {
    let backend = StorageBackend::memory();
    let error = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect_err("background durable borrowed memory backend cannot be promoted");

    // Assert on the structured field + stable class/code/remediation, not on
    // display prose (error-contract testing rule).
    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
    assert!(!error.remediation().trim().is_empty());
    match error {
        StorageApiError::InvalidArgument { field, .. } => {
            assert_eq!(field, "maintenance_scheduling_policy");
        }
        _ => panic!("expected invalid maintenance scheduling policy argument"),
    }
}

#[test]
fn durable_open_failure_returns_storage_api_error() {
    let backend = StorageBackend::memory();
    let error = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
        &backend,
    )
    .expect_err("memory backend cannot satisfy durable local mode");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
}

#[test]
#[cfg(feature = "localfs")]
fn close_open_durable_returns_final_facts() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("durable-close"));
    let outcome = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("durable open should succeed");
    let mut runtime = outcome.into_runtime();
    let close = runtime.close().expect("durable close");

    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert!(close.commits_quiesced());
    assert!(close.maintenance_drained());
    assert!(close.durable_synced());
    assert!(close.guards_released());
}

#[test]
#[cfg(feature = "localfs")]
fn open_existing_durable_local_returns_opened_disposition() {
    let root = temp_dir_for_api_test("durable-reopen");
    let first_backend = StorageBackend::local_fs(root.clone());
    let first = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &first_backend,
    )
    .expect("first durable open");
    let mut first_runtime = first.into_runtime();
    first_runtime.close().expect("first close");
    drop(first_runtime);
    drop(first_backend);

    let second_backend = StorageBackend::local_fs(root);
    let second = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &second_backend,
    )
    .expect("second durable open");

    assert_eq!(
        second.summary().disposition(),
        StorageOpenDisposition::OpenedExisting
    );
}

#[test]
fn durable_open_with_memory_backend_returns_storage_api_error() {
    let backend = StorageBackend::memory();
    let error = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_maintenance_scheduling_policy(
                StorageMaintenanceSchedulingPolicy::EvaluateAndEnqueue,
            ),
        &backend,
    )
    .expect_err("memory backend cannot satisfy durable local mode");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
    assert!(error.source().is_none());
}

#[test]
fn commit_batch_rejects_empty_and_duplicate_mutations() {
    let branch = branch_id(1);
    let empty = CommitBatch::new(branch, Vec::new(), CommitOptions::default())
        .expect_err("empty batch should fail");
    assert_eq!(empty.code(), "invalid_argument.storage_api.argument");

    let mutation = CommitMutation::Delete {
        storage_space: space(),
        key: key(b"k"),
    };
    let duplicate = CommitBatch::new(
        branch,
        vec![mutation.clone(), mutation],
        CommitOptions::default(),
    )
    .expect_err("duplicate batch should fail");
    assert_eq!(duplicate.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn request_shells_are_constructible() {
    let branch = branch_id(2);
    let point = PointReadRequest::new(branch, space(), key(b"k"), ReadBound::Latest);
    assert_eq!(point.bound(), ReadBound::Latest);
    assert_eq!(point.branch_id(), branch);
    assert_eq!(point.storage_space(), &space());
    assert_eq!(point.key(), &key(b"k"));

    let scan = ScanReadRequest::new(
        branch,
        space(),
        ScanRange::new(None, None).expect("unbounded range"),
        ReadBound::AtVersion(CommitVersion::new(5)),
        Some(ReadLimit::new(10).expect("valid limit")),
    );
    assert_eq!(scan.branch_id(), branch);
    assert_eq!(scan.storage_space(), &space());
    assert_eq!(
        scan.range(),
        &ScanRange::new(None, None).expect("unbounded range")
    );
    assert_eq!(scan.bound(), ReadBound::AtVersion(CommitVersion::new(5)));
    assert_eq!(scan.limit(), Some(ReadLimit::new(10).expect("valid limit")));

    let branch_request = BranchRequest::new(
        branch,
        BranchAction::ForkAtTimestamp {
            source: branch_id(3),
            timestamp: Timestamp::from_micros(10),
        },
        Some(BranchGeneration::new(1)),
    );
    assert!(matches!(
        branch_request.action(),
        BranchAction::ForkAtTimestamp { .. }
    ));
    assert_eq!(branch_request.branch_id(), branch);
    assert_eq!(
        branch_request.expected_generation(),
        Some(BranchGeneration::new(1))
    );

    assert_eq!(
        MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global).task(),
        MaintenanceTask::Checkpoint
    );
    assert_eq!(
        MaintenanceRequest::new(MaintenanceTask::Checkpoint, MaintenanceScope::Global).scope(),
        MaintenanceScope::Global
    );
    assert_eq!(
        DiagnosticsRequest::new(DiagnosticsScope::Branch(branch)).scope(),
        DiagnosticsScope::Branch(branch)
    );
}

#[test]
fn outcome_summaries_expose_stored_fields() {
    let open = StorageOpenSummary::new(
        StorageOpenDisposition::OpenedExisting,
        RecoveryHealthSummary::Degraded,
        Some(CommitVersion::new(42)),
    );
    assert_eq!(open.disposition(), StorageOpenDisposition::OpenedExisting);
    assert_eq!(open.recovery_health(), RecoveryHealthSummary::Degraded);
    assert_eq!(
        open.recovered_visible_version(),
        Some(CommitVersion::new(42))
    );
    assert_eq!(
        open.maintenance_scheduling_policy(),
        StorageMaintenanceSchedulingPolicy::Background
    );

    let close = StorageCloseSummary::new(StorageRuntimeState::Closed, true);
    assert_eq!(close.state(), StorageRuntimeState::Closed);
    assert!(close.idempotent());

    let commit = CommitSummary::new(
        branch_id(6),
        CommitVersion::new(7),
        Timestamp::from_micros(8),
    );
    assert_eq!(commit.branch_id(), branch_id(6));
    assert_eq!(commit.commit_version(), CommitVersion::new(7));
    assert_eq!(commit.commit_timestamp(), Timestamp::from_micros(8));
}

/// #2555 end-to-end: the WAL rolls past the last published checkpoint, the
/// store closes cleanly, and the reopened writer must resume at the on-disk
/// tail. Pre-fix the reopened writer resumed in the manifest's stale segment,
/// its first rotation collided with an existing segment (commit failed
/// `unavailable`), and the disordered package bricked the NEXT recovery with
/// `RecoveryFailed`.
#[test]
#[cfg(feature = "localfs")]
fn reopen_after_wal_rolls_past_last_checkpoint_accepts_writes_and_recovers() {
    let root = temp_dir_for_api_test("reopen-stale-wal-pointer");
    let options = || {
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_segment_size_for_test(1024)
    };
    let value = vec![0x5A; 256];

    // Session 1: roll the WAL well past segment 1 (the manifest pointer for a
    // fresh store) without crossing any checkpoint threshold, then close.
    let mut first = StorageRuntime::open_with_backend(
        options(),
        crate::testkit::leak_static(StorageBackend::local_fs(root.clone())),
    )
    .expect("first open")
    .into_runtime();
    for index in 0..24_u32 {
        let key = format!("stale-pointer-{index:04}");
        first
            .commit(&background_put_batch(key.as_bytes(), value.clone()))
            .expect("first-session commit");
    }
    assert!(
        wal_segment_file_count(&root) >= 3,
        "test shape must roll the WAL across several segments"
    );
    first.close().expect("first close");
    drop(first);

    // Session 2: reopen and write again. The first commit forces appends (and
    // rotation off the tail); pre-fix this failed with the CreateSegment
    // AlreadyExists publish error surfaced as an unavailable commit.
    let mut second = StorageRuntime::open_with_backend(
        options(),
        crate::testkit::leak_static(StorageBackend::local_fs(root.clone())),
    )
    .expect("reopen after clean close")
    .into_runtime();
    for index in 0..8_u32 {
        let key = format!("post-reopen-{index:04}");
        second
            .commit(&background_put_batch(key.as_bytes(), value.clone()))
            .expect("post-reopen commit must not collide with sealed segments");
    }
    second.close().expect("second close");
    drop(second);

    // Session 3: the package must still recover (pre-fix the disordered WAL
    // failed closed here) and reads must see both sessions' rows.
    let third = StorageRuntime::open_with_backend(
        options(),
        crate::testkit::leak_static(StorageBackend::local_fs(root.clone())),
    )
    .expect("recovery after post-reopen writes must stay ordered")
    .into_runtime();
    let branch = StorageRuntime::default_branch_id_for_test();
    for probe in ["stale-pointer-0000", "post-reopen-0007"] {
        let read = third
            .read_point(&PointReadRequest::new(
                branch,
                background_space(),
                key(probe.as_bytes()),
                ReadBound::Latest,
            ))
            .expect("read after second recovery");
        assert!(
            read.row().is_some(),
            "row `{probe}` must survive both reopens"
        );
    }
}

/// V1 cutover (hard rule 42): a directory holding a pre-V1 database layout
/// is rejected with a structured layout error — a fresh V1 layout is never
/// silently created inside one. The pre-V1 signature is a root `strata.toml`
/// or the uppercase `MANIFEST` file; V1 uses a `manifest/` directory and
/// creates neither name.
#[test]
#[cfg(feature = "localfs")]
fn open_durable_local_rejects_pre_v1_layout() {
    for marker in ["strata.toml", "MANIFEST"] {
        let dir = temp_dir_for_api_test(&format!("pre-v1-{marker}"));
        std::fs::create_dir_all(&dir).expect("create dir");
        std::fs::write(dir.join(marker), b"pre-v1").expect("write marker");
        let error = StorageRuntime::open_durable_local(&dir, StorageDurabilityPolicy::Standard)
            .expect_err("pre-V1 layout must refuse");
        assert_eq!(
            error.code(),
            "failed_precondition.storage_api.incompatible_layout",
            "marker {marker}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // A clean directory still opens and creates a fresh V1 database.
    let dir = temp_dir_for_api_test("pre-v1-clean");
    std::fs::create_dir_all(&dir).expect("create dir");
    let outcome = StorageRuntime::open_durable_local(&dir, StorageDurabilityPolicy::Standard)
        .expect("clean dir opens");
    assert_eq!(
        outcome.summary().disposition(),
        StorageOpenDisposition::Created
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- #3658: the manifest format version gates a downgrade -----------------

/// The on-disk format version of the database manifest under `root`.
#[cfg(feature = "localfs")]
fn manifest_format_version(root: &std::path::Path) -> u32 {
    let bytes = std::fs::read(root.join("manifest/current.object@")).expect("read manifest");
    u32::from_le_bytes(bytes[4..8].try_into().expect("version field"))
}

/// Rewrite the manifest's version field (checksum recomputed), as the build
/// that wrote version `version` would have left it.
#[cfg(feature = "localfs")]
fn set_manifest_format_version(root: &std::path::Path, version: u32) {
    let path = root.join("manifest/current.object@");
    let mut bytes = std::fs::read(&path).expect("read manifest");
    bytes[4..8].copy_from_slice(&version.to_le_bytes());
    let footer = bytes.len() - 4;
    let crc = crc32fast::hash(&bytes[..footer]);
    bytes[footer..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes).expect("write manifest");
}

#[cfg(feature = "localfs")]
fn put_persisted(runtime: &mut StorageRuntime<'static>, value: &[u8]) {
    let batch = CommitBatch::new(
        StorageRuntime::default_branch_id_for_test(),
        vec![CommitMutation::Put {
            storage_space: StorageSpaceId::new(vec![0x20]).expect("space"),
            key: key(b"persisted"),
            value: StorageValue::new(value.to_vec()),
            ttl: None,
        }],
        CommitOptions::default(),
    )
    .expect("batch");
    runtime.commit(&batch).expect("commit");
}

#[cfg(feature = "localfs")]
fn read_persisted(runtime: &StorageRuntime<'static>) -> Option<Vec<u8>> {
    runtime
        .read_point(&PointReadRequest::new(
            StorageRuntime::default_branch_id_for_test(),
            StorageSpaceId::new(vec![0x20]).expect("space"),
            key(b"persisted"),
            ReadBound::Latest,
        ))
        .expect("read")
        .row()
        .and_then(|row| row.value().map(|value| value.as_bytes().to_vec()))
}

#[cfg(feature = "localfs")]
fn file_digests(root: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.insert(
                    path.display().to_string(),
                    std::fs::read(&path).expect("read file"),
                );
            }
        }
    }
    files
}

/// #3658: a new database is written at manifest format version 3, which a
/// 1.2.5 binary refuses at open.
#[cfg(feature = "localfs")]
#[test]
fn a_new_database_writes_manifest_format_version_3() {
    let root = temp_dir_for_api_test("manifest-v3-new");
    let mut runtime = StorageRuntime::open_local(root.clone())
        .expect("open")
        .into_runtime();
    assert_eq!(manifest_format_version(&root), 3);
    put_persisted(&mut runtime, b"value");
    runtime.close().expect("close");
    assert_eq!(manifest_format_version(&root), 3);
}

/// #3658: a database a 1.2.5 binary wrote (a V1 manifest) opens, and the open
/// upgrades its manifest to version 3 before admitting a single write — every
/// fact preserved, the data intact.
#[cfg(feature = "localfs")]
#[test]
fn an_open_upgrades_a_v1_manifest_before_any_write() {
    let root = temp_dir_for_api_test("manifest-v1-upgrade");
    let mut runtime = StorageRuntime::open_local(root.clone())
        .expect("open")
        .into_runtime();
    put_persisted(&mut runtime, b"written-before");
    runtime.close().expect("close");
    set_manifest_format_version(&root, 1);

    let reopened = StorageRuntime::open_local(root.clone())
        .expect("a V1 manifest still opens")
        .into_runtime();
    assert_eq!(
        manifest_format_version(&root),
        3,
        "upgraded by the open itself, before any commit"
    );
    assert_eq!(
        read_persisted(&reopened).as_deref(),
        Some(&b"written-before"[..])
    );
}

/// #3658: an open that fails leaves a V1 manifest at version 1, so the
/// database stays openable by the build that wrote it.
#[cfg(feature = "localfs")]
#[test]
fn a_failed_open_leaves_a_v1_manifest_at_version_1() {
    let root = temp_dir_for_api_test("manifest-v1-failed-open");
    let mut runtime = StorageRuntime::open_local(root.clone())
        .expect("open")
        .into_runtime();
    put_persisted(&mut runtime, b"value");
    runtime.close().expect("close checkpoints");
    set_manifest_format_version(&root, 1);
    // Lose the attested snapshot: strict recovery refuses the open.
    for entry in std::fs::read_dir(root.join("snapshots"))
        .expect("snapshots")
        .flatten()
    {
        std::fs::remove_file(entry.path()).expect("remove snapshot");
    }

    let error = StorageRuntime::open_local(root.clone()).expect_err("strict recovery refuses");
    assert_eq!(
        error.class(),
        StorageApiErrorClass::FailedPrecondition,
        "{error:?}"
    );
    assert_eq!(
        manifest_format_version(&root),
        1,
        "the failed open upgraded nothing"
    );
}

/// #3659: a manifest from a newer build is refused with its own typed,
/// non-retryable error naming the format and both versions, never the
/// generic lower-layer (retryable, "unavailable") failure.
#[cfg(feature = "localfs")]
#[test]
fn a_newer_manifest_is_refused_as_an_incompatible_format() {
    let root = temp_dir_for_api_test("manifest-newer-typed");
    let mut runtime = StorageRuntime::open_local(root.clone())
        .expect("open")
        .into_runtime();
    put_persisted(&mut runtime, b"value");
    runtime.close().expect("close");
    set_manifest_format_version(&root, 9);

    let error = StorageRuntime::open_local(root).expect_err("a newer manifest is refused");
    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.incompatible_format"
    );
    assert!(
        matches!(
            error,
            StorageApiError::IncompatibleFormat {
                format: "database_manifest",
                version: 9,
                max_supported,
            } if max_supported == crate::format::DATABASE_MANIFEST_FORMAT_VERSION
        ),
        "{error:?}"
    );
}

/// #3659: a manifest carrying a reserved pre-V1 version is the pre-V1 layout
/// refusal, not a generic failure.
#[cfg(feature = "localfs")]
#[test]
fn a_pre_v1_manifest_version_is_refused_as_an_incompatible_layout() {
    let root = temp_dir_for_api_test("manifest-pre-v1-typed");
    let mut runtime = StorageRuntime::open_local(root.clone())
        .expect("open")
        .into_runtime();
    runtime.close().expect("close");
    set_manifest_format_version(&root, 2);

    let error = StorageRuntime::open_local(root).expect_err("a pre-V1 manifest is refused");
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.incompatible_layout"
    );
}

/// #3658: a manifest from a newer build is refused, and the refusal writes
/// nothing — a downgrade can never damage the newer database.
#[cfg(feature = "localfs")]
#[test]
fn a_newer_manifest_is_refused_without_touching_the_database() {
    let root = temp_dir_for_api_test("manifest-newer");
    let mut runtime = StorageRuntime::open_local(root.clone())
        .expect("open")
        .into_runtime();
    put_persisted(&mut runtime, b"value");
    runtime.close().expect("close");
    set_manifest_format_version(&root, 4);
    let before = file_digests(&root);

    assert!(StorageRuntime::open_local(root.clone()).is_err());
    // The writer lock is the open's own scratch; every durable object must be
    // byte-identical and none may appear or vanish.
    let durable = |files: std::collections::BTreeMap<String, Vec<u8>>| {
        files
            .into_iter()
            .filter(|(path, _)| !path.ends_with("writer.object@"))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(durable(file_digests(&root)), durable(before));
}
