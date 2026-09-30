use super::*;

use std::time::Duration;

fn open_runtime() -> StorageRuntime<'static> {
    StorageRuntime::open(StorageOpenOptions::cache())
        .expect("open cache runtime")
        .into_runtime()
}

fn branch() -> BranchId {
    StorageRuntime::default_branch_id_for_test()
}

fn other_branch() -> BranchId {
    branch_id(0x44)
}

fn engine_space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20]).expect("engine storage space")
}

fn multi_byte_space() -> StorageSpaceId {
    StorageSpaceId::new(vec![0x20, 0x21]).expect("valid opaque storage space")
}

fn api_key(bytes: &[u8]) -> StorageKey {
    StorageKey::new(bytes.to_vec()).expect("valid key")
}

fn put_mutation(key: &[u8], value: &[u8]) -> CommitMutation {
    CommitMutation::Put {
        storage_space: engine_space(),
        key: api_key(key),
        value: StorageValue::new(value.to_vec()),
        ttl: None,
    }
}

fn put_mutation_with_ttl(key: &[u8], value: &[u8], ttl: Duration) -> CommitMutation {
    CommitMutation::Put {
        storage_space: engine_space(),
        key: api_key(key),
        value: StorageValue::new(value.to_vec()),
        ttl: Some(ttl),
    }
}

fn delete_mutation(key: &[u8]) -> CommitMutation {
    CommitMutation::Delete {
        storage_space: engine_space(),
        key: api_key(key),
    }
}

fn put_batch(key: &[u8], value: &[u8]) -> CommitBatch {
    CommitBatch::new(
        branch(),
        vec![put_mutation(key, value)],
        CommitOptions::default(),
    )
    .expect("valid put batch")
}

fn delete_batch(key: &[u8]) -> CommitBatch {
    CommitBatch::new(
        branch(),
        vec![delete_mutation(key)],
        CommitOptions::default(),
    )
    .expect("valid delete batch")
}

fn read_latest(runtime: &StorageRuntime<'_>, key: &[u8]) -> PointReadOutcome {
    runtime
        .read_point(&PointReadRequest::new(
            branch(),
            engine_space(),
            api_key(key),
            ReadBound::Latest,
        ))
        .expect("read latest")
}

#[test]
fn commit_rejects_empty_batch() {
    let error = CommitBatch::new(branch(), Vec::new(), CommitOptions::default())
        .expect_err("empty batch rejected");

    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_rejects_duplicate_keys() {
    let mutation = put_mutation(b"dup", b"value");
    let error = CommitBatch::new(
        branch(),
        vec![mutation.clone(), mutation],
        CommitOptions::default(),
    )
    .expect_err("duplicate key rejected");

    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_rejects_malformed_key() {
    let error = StorageKey::new(Vec::new()).expect_err("empty key rejected");

    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_rejects_zero_ttl() {
    let error = CommitBatch::new(
        branch(),
        vec![put_mutation_with_ttl(b"zero-ttl", b"value", Duration::ZERO)],
        CommitOptions::default(),
    )
    .expect_err("zero TTL rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_rejects_unknown_branch() {
    let batch = CommitBatch::new(
        other_branch(),
        vec![put_mutation(b"unknown", b"value")],
        CommitOptions::default(),
    )
    .expect("valid shape");
    let runtime = open_runtime();

    let error = runtime.commit(&batch).expect_err("unknown branch rejected");

    assert_eq!(error.class(), StorageApiErrorClass::NotFound);
}

#[test]
fn commit_rejects_generation_mismatch() {
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"generation", b"value")],
        CommitOptions::default().with_expected_generation(BranchGeneration::new(99)),
    )
    .expect("valid shape");
    let runtime = open_runtime();

    let error = runtime
        .commit(&batch)
        .expect_err("stale branch generation rejected");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.branch_generation"
    );
}

#[test]
fn commit_rejects_zero_expected_generation() {
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"zero-generation", b"value")],
        CommitOptions::default().with_expected_generation(BranchGeneration::ZERO),
    )
    .expect("valid shape");
    let runtime = open_runtime();

    let error = runtime
        .commit(&batch)
        .expect_err("zero branch generation rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_rejects_cross_branch_mutation() {
    let source = include_str!("../commit.rs");
    let mutation_section = source
        .split("pub enum CommitMutation")
        .nth(1)
        .expect("mutation enum present")
        .split("impl CommitMutation")
        .next()
        .expect("mutation impl follows enum");

    assert!(!mutation_section.contains("branch_id"));
}

#[test]
fn commit_rejects_unsupported_durability_for_cache() {
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"durability", b"value")],
        CommitOptions::default().with_durability(CommitDurability::Standard),
    )
    .expect("valid shape");
    let runtime = open_runtime();

    let error = runtime
        .commit(&batch)
        .expect_err("cache cannot satisfy durable commit request");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
}

#[test]
#[cfg(feature = "localfs")]
fn commit_rejects_always_request_on_standard_runtime() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("commit-always-on-standard"));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"always-on-standard", b"value")],
        CommitOptions::default().with_durability(CommitDurability::Always),
    )
    .expect("valid shape");

    let error = runtime
        .commit(&batch)
        .expect_err("always request rejected by standard runtime");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
    assert_eq!(error.code(), "unsupported.storage_api.capability");
}

#[test]
#[cfg(feature = "localfs")]
fn commit_rejects_standard_request_on_always_runtime() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("commit-standard-on-always"));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"standard-on-always", b"value")],
        CommitOptions::default().with_durability(CommitDurability::Standard),
    )
    .expect("valid shape");

    let error = runtime
        .commit(&batch)
        .expect_err("standard request rejected by always runtime");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
    assert_eq!(error.code(), "unsupported.storage_api.capability");
}

#[test]
#[cfg(feature = "localfs")]
fn commit_rejects_not_durable_request_on_durable_runtime() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("commit-not-durable-on-durable"));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"not-durable-on-durable", b"value")],
        CommitOptions::default().with_durability(CommitDurability::NotDurable),
    )
    .expect("valid shape");

    let error = runtime
        .commit(&batch)
        .expect_err("not-durable request rejected by durable runtime");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
    assert_eq!(error.code(), "unsupported.storage_api.capability");
}

#[test]
fn commit_rejects_transaction_id_field_absence_by_type() {
    let source = include_str!("../commit.rs").to_ascii_lowercase();

    assert!(!source.contains("transaction_id"));
    assert!(!source.contains("transactionid"));
}

#[test]
fn cache_commit_returns_not_durable_outcome() {
    let runtime = open_runtime();

    let summary = runtime
        .commit(&put_batch(b"cache", b"value"))
        .expect("commit");

    assert_eq!(summary.durability(), CommitDurabilitySummary::NotDurable);
    assert_eq!(
        summary.admission().status(),
        CommitAdmissionStatus::AcceptedClean
    );
    assert_eq!(
        summary.admission().pressure_severity(),
        CommitAdmissionPressureSeverity::None
    );
    assert_eq!(
        summary.admission().pressure_reason(),
        CommitAdmissionPressureReason::None
    );
    assert!(!summary.admission().inline_maintenance_driven());
    assert!(!summary.admission().cleared_prior_pressure_rejection());
    assert!(summary.visible());
}

#[test]
#[cfg(feature = "localfs")]
fn standard_commit_returns_standard_outcome() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("commit-standard"));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &backend,
    )
    .expect("durable open")
    .into_runtime();

    let summary = runtime
        .commit(&put_batch(b"standard", b"value"))
        .expect("commit");

    assert_eq!(summary.durability(), CommitDurabilitySummary::Standard);
}

#[test]
#[cfg(feature = "localfs")]
fn always_commit_returns_always_outcome() {
    let backend = StorageBackend::local_fs(temp_dir_for_api_test("commit-always"));
    let runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &backend,
    )
    .expect("durable open")
    .into_runtime();

    let summary = runtime
        .commit(&put_batch(b"always", b"value"))
        .expect("commit");

    assert_eq!(summary.durability(), CommitDurabilitySummary::Always);
}

#[test]
#[cfg(feature = "localfs")]
fn durable_runtime_default_uses_configured_policy() {
    let standard_backend =
        StorageBackend::local_fs(temp_dir_for_api_test("commit-runtime-default-standard"));
    let standard_runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        &standard_backend,
    )
    .expect("standard durable open")
    .into_runtime();
    let standard_summary = standard_runtime
        .commit(
            &CommitBatch::new(
                branch(),
                vec![put_mutation(b"runtime-default-standard", b"value")],
                CommitOptions::default().with_durability(CommitDurability::RuntimeDefault),
            )
            .expect("valid batch"),
        )
        .expect("standard commit");
    assert_eq!(
        standard_summary.durability(),
        CommitDurabilitySummary::Standard
    );

    let always_backend =
        StorageBackend::local_fs(temp_dir_for_api_test("commit-runtime-default-always"));
    let always_runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Always),
        &always_backend,
    )
    .expect("always durable open")
    .into_runtime();
    let always_summary = always_runtime
        .commit(
            &CommitBatch::new(
                branch(),
                vec![put_mutation(b"runtime-default-always", b"value")],
                CommitOptions::default().with_durability(CommitDurability::RuntimeDefault),
            )
            .expect("valid batch"),
        )
        .expect("always commit");
    assert_eq!(always_summary.durability(), CommitDurabilitySummary::Always);
}

#[test]
fn commit_put_then_read_latest_observes_value() {
    let runtime = open_runtime();
    let summary = runtime
        .commit(&put_batch(b"alpha", b"value"))
        .expect("commit");

    let outcome = read_latest(&runtime, b"alpha");
    let row = outcome.row().expect("row");

    assert_eq!(row.value().expect("value").as_bytes(), b"value");
    assert_eq!(row.commit_version(), summary.commit_version());
}

#[test]
fn commit_delete_then_read_latest_observes_tombstone() {
    let runtime = open_runtime();
    runtime.commit(&put_batch(b"alpha", b"value")).expect("put");
    let summary = runtime.commit(&delete_batch(b"alpha")).expect("delete");

    let outcome = read_latest(&runtime, b"alpha");
    let row = outcome.row().expect("tombstone");

    assert!(row.is_tombstone());
    assert!(row.value().is_none());
    assert_eq!(row.commit_version(), summary.commit_version());
}

#[test]
fn commit_ttl_metadata_roundtrips_to_read_facts() {
    let runtime = open_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation_with_ttl(
            b"ttl",
            b"value",
            Duration::from_micros(10),
        )],
        CommitOptions::default(),
    )
    .expect("valid batch");
    let summary = runtime.commit(&batch).expect("commit");

    let outcome = read_latest(&runtime, b"ttl");
    let row = outcome.row().expect("row");

    assert_eq!(
        row.expires_at(),
        Some(
            summary
                .commit_timestamp()
                .saturating_add(Duration::from_micros(10))
        )
    );
}

#[test]
fn commit_outcome_reports_mutation_counts() {
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"a", b"value"), delete_mutation(b"b")],
        CommitOptions::default(),
    )
    .expect("valid batch");
    let runtime = open_runtime();

    let summary = runtime.commit(&batch).expect("commit");

    assert_eq!(summary.put_count(), 1);
    assert_eq!(summary.delete_count(), 1);
    assert_eq!(summary.mutation_count(), 2);
    assert_eq!(summary.timeline_row_count(), 0);
}

#[test]
fn commit_outcome_reports_timestamp_and_version() {
    let runtime = open_runtime();

    let summary = runtime
        .commit(&put_batch(b"facts", b"value"))
        .expect("commit");

    assert!(summary.commit_version() > CommitVersion::ZERO);
    assert!(summary.commit_timestamp() >= Timestamp::EPOCH);
}

#[test]
fn commit_rejected_request_does_not_allocate_version() {
    let runtime = open_runtime();
    let first = runtime
        .commit(&put_batch(b"before-reject", b"value"))
        .expect("first commit");
    let rejected = CommitBatch::new(
        branch(),
        vec![put_mutation(b"rejected", b"value")],
        CommitOptions::default().with_durability(CommitDurability::Standard),
    )
    .expect("valid shape");

    let error = runtime
        .commit(&rejected)
        .expect_err("unsupported durability rejected");
    let second = runtime
        .commit(&put_batch(b"after-reject", b"value"))
        .expect("second commit");

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
    assert_eq!(
        second.commit_version(),
        first.commit_version().checked_next().expect("next version")
    );
}

#[test]
fn commit_rejects_ttl_duration_too_large() {
    let runtime = open_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation_with_ttl(
            b"ttl-too-large",
            b"value",
            Duration::MAX,
        )],
        CommitOptions::default(),
    )
    .expect("valid shape");

    let error = runtime.commit(&batch).expect_err("TTL overflow rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_rejects_ttl_expiration_overflow() {
    let runtime = open_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation_with_ttl(
            b"ttl-expiration-overflow",
            b"value",
            Duration::from_micros(1),
        )],
        CommitOptions::default(),
    )
    .expect("valid shape");

    let error = runtime
        .commit_for_test(&batch, Timestamp::from_micros(u64::MAX))
        .expect_err("expiration overflow rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_outcome_timestamps_advance() {
    let runtime = open_runtime();

    let first = runtime
        .commit(&put_batch(b"first-time", b"value"))
        .expect("first commit");
    let second = runtime
        .commit(&put_batch(b"second-time", b"value"))
        .expect("second commit");

    assert!(second.commit_timestamp() > first.commit_timestamp());
}

#[test]
fn commit_ttl_uses_actual_commit_timestamp_after_prior_commit() {
    let runtime = open_runtime();
    runtime
        .commit(&put_batch(b"prior", b"value"))
        .expect("prior commit");
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation_with_ttl(
            b"ttl-after-prior",
            b"value",
            Duration::from_micros(7),
        )],
        CommitOptions::default(),
    )
    .expect("valid batch");

    let summary = runtime.commit(&batch).expect("commit");
    let row = read_latest(&runtime, b"ttl-after-prior")
        .row()
        .cloned()
        .expect("row");

    assert_eq!(
        row.expires_at(),
        Some(
            summary
                .commit_timestamp()
                .saturating_add(Duration::from_micros(7))
        )
    );
}

#[test]
fn commit_blind_write_succeeds_without_read_set() {
    let runtime = open_runtime();
    runtime
        .commit(&put_batch(b"blind", b"value"))
        .expect("blind write");
}

#[test]
fn commit_expected_version_match_succeeds() {
    let runtime = open_runtime();
    let first = runtime.commit(&put_batch(b"cas", b"old")).expect("first");
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"cas", b"new")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_present(
        engine_space(),
        api_key(b"cas"),
        first.commit_version(),
    )])
    .expect("valid condition");

    runtime.commit(&batch).expect("matching condition");
}

#[test]
fn commit_expected_version_mismatch_conflicts() {
    let runtime = open_runtime();
    runtime.commit(&put_batch(b"cas", b"old")).expect("first");
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"cas", b"new")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_present(
        engine_space(),
        api_key(b"cas"),
        CommitVersion::new(99),
    )])
    .expect("valid condition");

    let error = runtime.commit(&batch).expect_err("condition conflicts");

    assert_eq!(error.class(), StorageApiErrorClass::Conflict);
}

#[test]
fn commit_expected_absent_match_succeeds() {
    let runtime = open_runtime();
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"absent", b"value")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_absent(
        engine_space(),
        api_key(b"absent"),
    )])
    .expect("valid condition");

    runtime.commit(&batch).expect("absent condition");
}

#[test]
fn commit_expected_absent_mismatch_conflicts() {
    let runtime = open_runtime();
    runtime
        .commit(&put_batch(b"absent", b"old"))
        .expect("first");
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"absent", b"new")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_absent(
        engine_space(),
        api_key(b"absent"),
    )])
    .expect("valid condition");

    let error = runtime.commit(&batch).expect_err("condition conflicts");

    assert_eq!(error.class(), StorageApiErrorClass::Conflict);
}

#[test]
fn commit_expected_absent_succeeds_after_visible_delete() {
    let runtime = open_runtime();
    runtime
        .commit(&put_batch(b"deleted-cas", b"old"))
        .expect("first");
    runtime
        .commit(&delete_batch(b"deleted-cas"))
        .expect("delete");
    assert!(read_latest(&runtime, b"deleted-cas")
        .row()
        .expect("tombstone")
        .is_tombstone());
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"deleted-cas", b"new")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_absent(
        engine_space(),
        api_key(b"deleted-cas"),
    )])
    .expect("valid condition");

    runtime
        .commit(&batch)
        .expect("visible tombstone counts as absent for CAS");

    let row = read_latest(&runtime, b"deleted-cas")
        .row()
        .cloned()
        .expect("new row");
    assert!(!row.is_tombstone());
    assert_eq!(row.value().expect("value").as_bytes(), b"new");
}

#[test]
fn commit_expected_present_rejects_after_visible_delete() {
    let runtime = open_runtime();
    let first = runtime
        .commit(&put_batch(b"deleted-present-cas", b"old"))
        .expect("first");
    runtime
        .commit(&delete_batch(b"deleted-present-cas"))
        .expect("delete");
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"deleted-present-cas", b"new")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_present(
        engine_space(),
        api_key(b"deleted-present-cas"),
        first.commit_version(),
    )])
    .expect("valid condition");

    let error = runtime
        .commit(&batch)
        .expect_err("visible tombstone is not present for CAS");

    assert_eq!(error.class(), StorageApiErrorClass::Conflict);
    assert!(read_latest(&runtime, b"deleted-present-cas")
        .row()
        .expect("tombstone remains after failed condition")
        .is_tombstone());
}

#[test]
fn commit_conditions_are_explicit_cas_not_captured_read_sets() {
    let runtime = open_runtime();
    let guarded = runtime
        .commit(&put_batch(b"guarded", b"v1"))
        .expect("initial guarded row");
    runtime
        .commit(&put_batch(b"unrelated", b"v1"))
        .expect("initial unrelated row");
    assert!(read_latest(&runtime, b"unrelated").row().is_some());
    runtime
        .commit(&put_batch(b"unrelated", b"v2"))
        .expect("unrelated row can change before conditional commit");
    let conditioned = CommitBatch::new(
        branch(),
        vec![put_mutation(b"guarded", b"v2")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_present(
        engine_space(),
        api_key(b"guarded"),
        guarded.commit_version(),
    )])
    .expect("valid condition");

    runtime
        .commit(&conditioned)
        .expect("only the explicit guarded condition is checked");
}

#[test]
fn commit_condition_rejects_zero_expected_present_version() {
    let error = CommitBatch::new(
        branch(),
        vec![put_mutation(b"zero-condition", b"value")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_present(
        engine_space(),
        api_key(b"zero-condition"),
        CommitVersion::ZERO,
    )])
    .expect_err("zero expected-present version is malformed");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
    match error {
        StorageApiError::InvalidArgument { field, reason } => {
            assert_eq!(field, "conditions");
            assert_eq!(reason, "expected present version must be nonzero");
        }
        other => panic!("expected invalid condition argument, got {other:?}"),
    }
}

#[test]
fn commit_conflict_error_has_structured_branch_and_key() {
    let runtime = open_runtime();
    runtime
        .commit(&put_batch(b"structured", b"old"))
        .expect("first");
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"structured", b"new")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_absent(
        engine_space(),
        api_key(b"structured"),
    )])
    .expect("valid condition");

    let error = runtime.commit(&batch).expect_err("condition conflicts");

    match error {
        StorageApiError::Conflict {
            branch_id,
            storage_space,
            key_fingerprint,
            user_key_len,
            ..
        } => {
            assert_eq!(branch_id, branch());
            assert_eq!(storage_space, Some(0x20));
            assert!(key_fingerprint.is_some());
            assert_eq!(user_key_len, Some(b"structured".len()));
        }
        other => panic!("expected structured conflict, got {other:?}"),
    }
}

#[test]
fn commit_wal_append_failure_maps_to_durable_not_acquired() {
    let error = crate::api::map_commit_error_for_test(
        crate::commit::CommitRuntimeError::durability_uncertain_with(
            branch(),
            CommitVersion::new(2),
            "durable WAL append did not complete",
            SourceError,
        ),
    );

    assert_eq!(error.class(), StorageApiErrorClass::AmbiguousCommit);
    assert_eq!(
        error.code(),
        "ambiguous_commit.storage_api.durable_uncertain"
    );
    assert!(error.source().is_some());
}

#[test]
fn commit_durability_uncertain_survives_boundary() {
    let error = crate::api::map_commit_error_for_test(
        crate::commit::CommitRuntimeError::DurabilityUncertain {
            branch_id: branch(),
            commit_version: CommitVersion::new(3),
            reason: "durability is uncertain",
            source: None,
        },
    );

    assert_eq!(error.class(), StorageApiErrorClass::AmbiguousCommit);
}

#[test]
fn commit_applied_not_visible_survives_boundary() {
    let error = crate::api::map_commit_error_for_test(
        crate::commit::CommitRuntimeError::AppliedButNotVisible {
            branch_id: branch(),
            commit_version: CommitVersion::new(4),
            reason: "commit was applied but not visible",
        },
    );

    assert_eq!(
        error.code(),
        "ambiguous_commit.storage_api.durable_uncertain"
    );
}

#[test]
fn commit_disabled_read_only_diagnostics_maps_to_api_capability_error() {
    let error = crate::api::map_commit_error_for_test(
        crate::commit::CommitRuntimeError::InvalidCommitPhase {
            reason: "read-only diagnostics are disabled",
        },
    );

    assert_eq!(error.class(), StorageApiErrorClass::Unsupported);
    assert_eq!(error.code(), "unsupported.storage_api.capability");
    assert!(error.source().is_none());
    match error {
        StorageApiError::UnsupportedCapability { capability, reason } => {
            assert_eq!(capability, "read_only_diagnostics");
            assert_eq!(reason, "read-only diagnostics are disabled");
        }
        other => panic!("expected unsupported diagnostics capability, got {other:?}"),
    }
}

#[test]
fn commit_visibility_publish_failure_preserves_source_chain() {
    let error = crate::api::map_commit_error_for_test(
        crate::commit::CommitRuntimeError::durable_but_not_visible_with(
            branch(),
            CommitVersion::new(5),
            "visibility publication failed",
            SourceError,
        ),
    );

    assert!(error.source().is_some());
}

#[test]
fn commit_rejects_condition_with_multi_byte_storage_space() {
    let batch = CommitBatch::new(
        branch(),
        vec![put_mutation(b"condition-space", b"value")],
        CommitOptions::default(),
    )
    .expect("valid batch")
    .with_conditions(vec![CommitCondition::expected_absent(
        multi_byte_space(),
        api_key(b"condition-space"),
    )])
    .expect("condition shape accepted");
    let runtime = open_runtime();

    let error = runtime
        .commit(&batch)
        .expect_err("condition storage space rejected");

    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn commit_after_close_rejects_closed_runtime() {
    let mut runtime = open_runtime();
    runtime.close().expect("close");

    let error = runtime
        .commit(&put_batch(b"closed", b"value"))
        .expect_err("closed runtime rejected");

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
}

#[test]
fn commit_unresolved_durable_gate_rejects_followup() {
    let error = crate::api::map_commit_error_for_test(
        crate::commit::CommitRuntimeError::UnresolvedDurableCommit {
            branch_id: branch(),
            commit_version: CommitVersion::new(6),
            reason: "unresolved durable commit must recover before follow-up commits",
        },
    );

    assert_eq!(error.class(), StorageApiErrorClass::AmbiguousCommit);
}

#[test]
fn commit_storage_pressure_rejection_maps_to_retryable_api_error() {
    let error = crate::api::map_lifecycle_error_for_test(
        crate::lifecycle::LifecycleError::StoragePressureRejected {
            branch_id: branch(),
            severity: crate::lifecycle::LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            pressure_reason:
                crate::lifecycle::LifecycleStoragePressureReason::LevelZeroTableBacklog,
            retryable: true,
            reason: "mutating commit admission requires maintenance progress",
        },
    );

    assert_eq!(error.class(), StorageApiErrorClass::FailedPrecondition);
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.storage_pressure"
    );
    assert!(matches!(
        error,
        StorageApiError::StoragePressure {
            branch_id,
            severity: crate::api::CommitAdmissionPressureSeverity::Blocking,
            pressure_reason:
                crate::api::CommitAdmissionPressureReason::LevelZeroTableBacklog,
            retryable: true,
            ..
        } if branch_id == branch()
    ));
}

#[test]
fn public_open_uses_background_maintenance_policy() {
    let runtime = open_runtime();

    assert_eq!(
        runtime.maintenance_scheduling_policy_for_test(),
        crate::lifecycle::LifecycleMaintenanceSchedulingPolicy::Background
    );
}

#[test]
fn commit_api_has_no_public_transaction_session_type() {
    let source = include_str!("../commit.rs").to_ascii_lowercase();

    assert!(!source.contains("transactionsession"));
    assert!(!source.contains("begin_transaction"));
}

#[test]
fn commit_api_has_no_durable_transaction_id_type() {
    let source = include_str!("../commit.rs").to_ascii_lowercase();

    assert!(!source.contains("durabletransactionid"));
    assert!(!source.contains("transaction_id"));
}

#[test]
fn commit_api_does_not_claim_serializable_isolation() {
    let source = include_str!("../commit.rs").to_ascii_lowercase();

    assert!(!source.contains("serializable"));
}

#[test]
fn commit_api_rejects_cross_branch_atomic_request() {
    let source = include_str!("../commit.rs").to_ascii_lowercase();

    assert!(!source.contains("atomic"));
    assert!(!source.contains("branches:"));
}

#[test]
fn commit_at_stamps_the_supplied_timestamp() {
    let runtime = open_runtime();
    let timestamp = Timestamp::from_micros(50_000);
    let summary = runtime
        .commit_at(&put_batch(b"restore-a", b"one"), timestamp)
        .expect("explicit commit succeeds");
    assert_eq!(summary.commit_timestamp(), timestamp);
    let read = read_latest(&runtime, b"restore-a");
    assert_eq!(
        read.row().expect("row present").commit_timestamp(),
        timestamp
    );
}

#[test]
fn commit_at_accepts_non_decreasing_and_rejects_regression() {
    let runtime = open_runtime();
    runtime
        .commit_at(&put_batch(b"restore-b", b"one"), Timestamp::from_micros(70))
        .expect("first explicit commit");
    // Equal to the floor is allowed.
    runtime
        .commit_at(&put_batch(b"restore-c", b"two"), Timestamp::from_micros(70))
        .expect("equal-timestamp commit");
    // Earlier than the floor is rejected with a caller-actionable code.
    let error = runtime
        .commit_at(
            &put_batch(b"restore-d", b"three"),
            Timestamp::from_micros(69),
        )
        .expect_err("regressing timestamp refuses");
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

#[test]
fn ordinary_commits_stay_monotonic_after_commit_at() {
    let runtime = open_runtime();
    let explicit = Timestamp::from_micros(90_000);
    runtime
        .commit_at(&put_batch(b"restore-e", b"one"), explicit)
        .expect("explicit commit");
    let summary = runtime
        .commit(&put_batch(b"restore-f", b"two"))
        .expect("ordinary commit after explicit");
    assert!(
        summary.commit_timestamp() >= explicit,
        "clock never regresses below an explicit stamp"
    );
}

// ---- #3698: an oversized commit RECORD is a caller error that burns no version ----

/// A batch of `rows` puts, each carrying `value_len` bytes, as one commit.
fn multi_put_batch(prefix: &str, rows: usize, value_len: usize) -> CommitBatch {
    let mutations = (0..rows)
        .map(|index| {
            put_mutation(
                format!("{prefix}-{index}").as_bytes(),
                &vec![b'x'; value_len],
            )
        })
        .collect();
    CommitBatch::new(branch(), mutations, CommitOptions::default()).expect("valid multi-put batch")
}

/// The typed record-size refusal: `invalid_argument`, field `batch`, the
/// measured frame and the limit. Returns the measured frame length.
fn assert_record_size_refusal(error: &StorageApiError, expected_limit: u64) -> u64 {
    assert_eq!(
        error.class(),
        StorageApiErrorClass::InvalidArgument,
        "{error:?}"
    );
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
    match error {
        StorageApiError::SizeLimitExceeded {
            field,
            actual_bytes,
            limit_bytes,
            ..
        } => {
            assert_eq!(*field, "batch");
            assert_eq!(*limit_bytes, expected_limit);
            assert!(
                actual_bytes > limit_bytes,
                "the refused frame {actual_bytes} must be over the limit {limit_bytes}"
            );
            *actual_bytes
        }
        other => panic!("expected a typed record-size refusal, got {other:?}"),
    }
}

/// Five rows of 15 MiB: every row fits the 16 MiB row cap, the whole record
/// does not fit the 64 MiB production segment.
const OVERSIZED_RECORD_ROWS: usize = 5;
const OVERSIZED_RECORD_ROW_VALUE: usize = 15 * 1024 * 1024;

/// The same refusal, with the same limit, in cache mode and in default
/// durable mode — and in both the refused commit consumes no version: the
/// next commit gets the immediately following one (#3698). Cache mode never
/// reaches a WAL (hard rule 14); it refuses because a default durable
/// database could not append the record (#3391).
fn assert_oversized_record_refused_without_a_version_gap(runtime: &StorageRuntime<'_>) {
    let limit = crate::format::default_wal_record_frame_limit();
    let before = runtime
        .commit(&put_batch(b"before", b"v"))
        .expect("small commit before the refusal")
        .commit_version();

    let error = runtime
        .commit(&multi_put_batch(
            "oversized",
            OVERSIZED_RECORD_ROWS,
            OVERSIZED_RECORD_ROW_VALUE,
        ))
        .expect_err("a record over the WAL frame limit is refused");
    assert_record_size_refusal(&error, limit);

    let after = runtime
        .commit(&put_batch(b"after", b"v"))
        .expect("small commit after the refusal")
        .commit_version();
    assert_eq!(
        after,
        CommitVersion::new(before.as_u64() + 1),
        "the refused commit must not consume a commit version"
    );
    assert!(read_latest(runtime, b"oversized-0").row().is_none());
}

#[test]
fn cache_oversized_commit_record_is_a_typed_caller_error_without_a_version_gap() {
    let runtime = open_runtime();
    assert_oversized_record_refused_without_a_version_gap(&runtime);
}

#[test]
#[cfg(feature = "localfs")]
fn durable_oversized_commit_record_is_a_typed_caller_error_without_a_version_gap() {
    let backend = crate::testkit::leak_static(StorageBackend::local_fs(temp_dir_for_api_test(
        "oversized-record-default-segment",
    )));
    let mut runtime = StorageRuntime::open_with_backend(
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard),
        backend,
    )
    .expect("durable open")
    .into_runtime();
    assert_oversized_record_refused_without_a_version_gap(&runtime);
    runtime.close().expect("durable close");
}

/// The #3698 repro, with the test-seam segment small enough that ordinary
/// values reach the record cap: a single 4000-byte value (well under the row
/// cap) and a multi-row batch are both refused before allocation, typed, with
/// the configured segment's limit — and neither leaves a version gap, before
/// or after a reopen. Then the boundary at limit-1 / limit / limit+1: the
/// frames at and under the limit are really appended (and survive reopen), so
/// the pre-allocation size and the WAL append agree on where the limit is.
#[test]
#[cfg(feature = "localfs")]
fn durable_small_segment_refuses_oversized_records_before_allocation_at_the_exact_limit() {
    const SEGMENT: u64 = 1024;
    let root = temp_dir_for_api_test("oversized-record-small-segment");
    let options = || {
        StorageOpenOptions::durable_local(StorageDurabilityPolicy::Standard)
            .with_wal_segment_size_for_test(SEGMENT)
    };
    let open = || {
        StorageRuntime::open_with_backend(
            options(),
            crate::testkit::leak_static(StorageBackend::local_fs(root.clone())),
        )
        .expect("durable open")
    };
    let limit = crate::format::wal_record_frame_limit(SEGMENT);

    let mut runtime = open().into_runtime();
    let mut last = CommitVersion::ZERO;
    for index in 0..4_u8 {
        last = runtime
            .commit(&put_batch(&[b'k', index], &[0x61; 96]))
            .expect("small commit")
            .commit_version();
    }
    assert_eq!(last, CommitVersion::new(4));

    // One row, under the 16 MiB row cap, over this segment's record cap.
    let single = runtime
        .commit(&put_batch(b"single", &[0x62; 4000]))
        .expect_err("a record larger than the segment is refused");
    let single_frame = assert_record_size_refusal(&single, limit);

    // Several rows, each small, together over the record cap.
    let multi = runtime
        .commit(&multi_put_batch("multi", 4, 300))
        .expect_err("a multi-row record larger than the segment is refused");
    assert_record_size_refusal(&multi, limit);

    // Neither refusal consumed a version.
    last = runtime
        .commit(&put_batch(b"after-refusals", b"v"))
        .expect("commit after the refusals")
        .commit_version();
    assert_eq!(last, CommitVersion::new(5));

    // Frame length grows one-for-one with the value, so the refused single
    // row's measured frame locates the exact boundary.
    let excess = usize::try_from(single_frame - limit).expect("excess fits usize");
    let at_limit = 4000 - excess;
    let over = runtime
        .commit(&put_batch(b"single", &vec![0x63; at_limit + 1]))
        .expect_err("one byte over the limit is refused");
    assert_eq!(assert_record_size_refusal(&over, limit), limit + 1);
    for (value_len, expected) in [(at_limit - 1, 6_u64), (at_limit, 7)] {
        last = runtime
            .commit(&put_batch(b"single", &vec![0x63; value_len]))
            .unwrap_or_else(|error| {
                panic!("a {value_len}-byte value at or under the limit is appended: {error:?}")
            })
            .commit_version();
        assert_eq!(last, CommitVersion::new(expected), "no version gap");
    }
    runtime.close().expect("close");
    drop(runtime);

    let reopened = open();
    assert_eq!(
        reopened.summary().recovery_health(),
        RecoveryHealthSummary::Healthy
    );
    let reopened = reopened.into_runtime();
    let row = read_latest(&reopened, b"single");
    let row = row.row().expect("the at-limit record survives reopen");
    assert_eq!(row.commit_version(), CommitVersion::new(7));
    let next = reopened
        .commit(&put_batch(b"after-reopen", b"v"))
        .expect("commit after reopen")
        .commit_version();
    assert_eq!(next, CommitVersion::new(8), "no version gap across reopen");
}
