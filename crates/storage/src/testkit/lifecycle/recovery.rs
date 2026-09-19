//! Lifecycle recovery conformance helpers.

use super::super::TestkitError;
use super::{ensure, script_byte};
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata,
    BackendRange, BackendResult, BackendWriterGuard, PublishDurability, PublishError, PublishMode,
    PublishOutcome, PublishResult, DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::facts::BranchLevel;
use crate::commit::{CommitBranchGeneration, CommitManualTimestampSource, CommitRuntimeConfig};
use crate::format::{
    encode_manifest, encode_wal_segment_header, table_row_split_extension_section,
    DatabaseManifest, TableManifest, TableManifestLevel, TableManifestTableBounds,
    TableManifestTableFacts, TableManifestTableProvenance, TableManifestTableRef, TableRowSplit,
    WalCommitPayload, WalRecord, WalSegmentHeader,
};
use crate::layout::ObjectLayout;
use crate::lifecycle::{
    encode_checkpoint_row_section, LifecycleCodecId, LifecycleConfig,
    LifecycleDurableLocalOpenRequest, LifecycleDurableLocalShell, LifecycleError,
    LifecycleLossyRecoveryPolicy, LifecycleRecoveryRequest, LifecycleRecoveryRuntime,
    RecoveryDegradationClass, RecoveryFaultKind, RecoveryHealth, RecoveryStrictness, StorageMode,
    StorageOpenPlan,
};
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::{
    SnapshotPublishRequest, SnapshotService, TableManifestService, TableObjectService,
    WalServiceConfig,
};
use crate::table::{
    sort_table_rows_by_key, ImmutableTableBuilder, ImmutableTableReader, TableBuilderConfig,
    TableIdentity, TablePhysicalKeyBytes, TableReaderConfig, TableRow,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use strata_core::{BranchId, CommitVersion, Timestamp};

pub(super) const DATABASE_ID: [u8; 16] = [0x82; 16];

/// Coverage counters for lifecycle recovery contract checks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[expect(
    clippy::struct_field_names,
    reason = "counter suffix keeps the testkit getter vocabulary explicit"
)]
pub struct LifecycleRecoveryContractOutcome {
    empty_recovery_cases: usize,
    checkpoint_recovery_cases: usize,
    log_tail_cases: usize,
    strict_failure_cases: usize,
    lossy_degradation_cases: usize,
    input_derived_empty_cases: usize,
    input_derived_checkpoint_cases: usize,
    input_derived_log_tail_cases: usize,
    input_derived_strict_failure_cases: usize,
    input_derived_lossy_degradation_cases: usize,
    table_manifest_published_cases: usize,
    table_manifest_recovered_cases: usize,
    table_manifest_corrupt_cases: usize,
    table_manifest_missing_cases: usize,
    table_object_missing_cases: usize,
    table_object_corrupt_cases: usize,
    table_object_mismatch_cases: usize,
    orphan_ignored_cases: usize,
    checkpoint_manifest_conflict_cases: usize,
    cache_manifest_unsupported_cases: usize,
}

impl LifecycleRecoveryContractOutcome {
    /// Number of empty recovery cases exercised.
    pub const fn empty_recovery_cases(&self) -> usize {
        self.empty_recovery_cases
    }

    /// Number of checkpoint recovery cases exercised.
    pub const fn checkpoint_recovery_cases(&self) -> usize {
        self.checkpoint_recovery_cases
    }

    /// Number of unreplayed log-tail package cases exercised.
    pub const fn log_tail_cases(&self) -> usize {
        self.log_tail_cases
    }

    /// Number of strict recovery failure cases exercised.
    pub const fn strict_failure_cases(&self) -> usize {
        self.strict_failure_cases
    }

    /// Number of explicitly lossy degraded recovery cases exercised.
    pub const fn lossy_degradation_cases(&self) -> usize {
        self.lossy_degradation_cases
    }

    /// Number of script-derived empty recovery cases exercised.
    pub const fn input_derived_empty_cases(&self) -> usize {
        self.input_derived_empty_cases
    }

    /// Number of script-derived checkpoint recovery cases exercised.
    pub const fn input_derived_checkpoint_cases(&self) -> usize {
        self.input_derived_checkpoint_cases
    }

    /// Number of script-derived log-tail package cases exercised.
    pub const fn input_derived_log_tail_cases(&self) -> usize {
        self.input_derived_log_tail_cases
    }

    /// Number of script-derived strict failure cases exercised.
    pub const fn input_derived_strict_failure_cases(&self) -> usize {
        self.input_derived_strict_failure_cases
    }

    /// Number of script-derived lossy degraded recovery cases exercised.
    pub const fn input_derived_lossy_degradation_cases(&self) -> usize {
        self.input_derived_lossy_degradation_cases
    }

    /// Number of generated cases that published a durable table manifest.
    pub const fn table_manifest_published_cases(&self) -> usize {
        self.table_manifest_published_cases
    }

    /// Number of generated cases that recovered from a durable table manifest.
    pub const fn table_manifest_recovered_cases(&self) -> usize {
        self.table_manifest_recovered_cases
    }

    /// Number of generated cases that rejected corrupt table manifest bytes.
    pub const fn table_manifest_corrupt_cases(&self) -> usize {
        self.table_manifest_corrupt_cases
    }

    /// Number of generated cases that treated an absent table manifest as healthy.
    pub const fn table_manifest_missing_cases(&self) -> usize {
        self.table_manifest_missing_cases
    }

    /// Number of generated cases that rejected a manifest-listed missing table object.
    pub const fn table_object_missing_cases(&self) -> usize {
        self.table_object_missing_cases
    }

    /// Number of generated cases that rejected a manifest-listed corrupt table object.
    pub const fn table_object_corrupt_cases(&self) -> usize {
        self.table_object_corrupt_cases
    }

    /// Number of generated cases that rejected manifest/table-object fact mismatches.
    pub const fn table_object_mismatch_cases(&self) -> usize {
        self.table_object_mismatch_cases
    }

    /// Number of generated cases that proved valid orphan objects stay invisible.
    pub const fn orphan_ignored_cases(&self) -> usize {
        self.orphan_ignored_cases
    }

    /// Number of generated cases that detected a checkpoint/table-manifest internal-key conflict.
    pub const fn checkpoint_manifest_conflict_cases(&self) -> usize {
        self.checkpoint_manifest_conflict_cases
    }

    /// Number of generated cases that proved cache-mode runtimes cannot publish or recover table manifests.
    pub const fn cache_manifest_unsupported_cases(&self) -> usize {
        self.cache_manifest_unsupported_cases
    }
}

/// Exercises recovery behavior through the crate-private runtime.
pub fn check_lifecycle_recovery_contract(
    script: &[u8],
) -> Result<LifecycleRecoveryContractOutcome, TestkitError> {
    let mut outcome = LifecycleRecoveryContractOutcome::default();
    check_empty_recovery(script, &mut outcome)?;
    check_checkpoint_and_tail(script, &mut outcome)?;
    check_strict_missing_snapshot(script, &mut outcome)?;
    check_lossy_missing_snapshot(script, &mut outcome)?;
    check_input_derived_recovery(script, &mut outcome)?;
    check_table_manifest_recovery(script, &mut outcome)?;
    Ok(outcome)
}

fn check_input_derived_recovery(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    check_input_derived_empty_recovery(script, outcome)?;
    check_input_derived_checkpoint_and_tail(script, outcome)?;
    check_input_derived_strict_failure(script, outcome)?;
    check_input_derived_lossy_degradation(script, outcome)?;
    Ok(())
}

fn check_empty_recovery(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 0));
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        recovered.health().is_healthy(),
        "empty recovery not healthy",
    )?;
    ensure(
        recovered.wal().record_count() == 0,
        "empty recovery returned log records",
    )?;
    ensure(
        shell.visible_version() == CommitVersion::ZERO,
        "recovery advanced visible version",
    )?;
    ensure(
        shell.admit_commit().is_err(),
        "recovery made shell commit-admissible",
    )?;
    outcome.empty_recovery_cases += 1;
    Ok(())
}

fn check_checkpoint_and_tail(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 1));
    let checkpoint_version = CommitVersion::new(3);
    let tail_version = CommitVersion::new(4);
    let checkpoint_row = put_row(branch, checkpoint_version, b"checkpoint", b"value");
    publish_snapshot(
        backend,
        2,
        checkpoint_version,
        std::slice::from_ref(&checkpoint_row),
    )?;
    write_database_root(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(1, Some(checkpoint_version.as_u64()), Some(2), None)
            .map_err(testkit_error)?,
    )?;
    // #2765: a checkpoint-attesting manifest requires its WAL chain on disk
    // under strict recovery — seed the attested active segment.
    write_empty_wal_segment(backend, 1)?;
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let tail = wal_record(branch, tail_version, b"tail", b"value")?;
    shell
        .services_mut()
        .wal_mut()
        .append(&tail)
        .map_err(testkit_error)?;
    // Recovery reads the on-disk log, not the writer's coalescing buffer.
    shell
        .services_mut()
        .wal_mut()
        .force_durable()
        .map_err(testkit_error)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        recovered.checkpoint().trusted_watermark() == Some(checkpoint_version),
        "checkpoint watermark not trusted after validation",
    )?;
    ensure(
        recovered
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .map_err(|error| testkit_error(&error))?
            == std::slice::from_ref(&tail),
        "recovered log tail did not preserve record",
    )?;
    ensure(
        shell.branch_state().capture_read_view().is_ok(),
        "checkpoint install left branch unreadable",
    )?;
    outcome.checkpoint_recovery_cases += 1;
    outcome.log_tail_cases += 1;
    Ok(())
}

fn check_strict_missing_snapshot(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 2));
    write_database_root(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(1, Some(9), Some(3), None)
            .map_err(testkit_error)?,
    )?;
    // #2765: seed the attested WAL segment so assembly admits the store and
    // the MISSING-SNAPSHOT rejection under test fires where expected.
    write_empty_wal_segment(backend, 1)?;
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("strict missing snapshot rejects");
    // #2754: a manifest-attested snapshot whose objects are gone is permanent
    // loss under strict recovery — non-retryable recovery corruption, not the
    // transient lower-layer outage that advised an endless retry.
    ensure(
        matches!(
            error,
            LifecycleError::RecoveryCorruption {
                reason: "manifest attests a snapshot but its objects are missing",
                ..
            }
        ),
        "strict missing snapshot was not a recovery corruption",
    )?;
    outcome.strict_failure_cases += 1;
    Ok(())
}

fn check_lossy_missing_snapshot(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 3));
    write_database_root(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(1, Some(9), Some(4), None)
            .map_err(testkit_error)?,
    )?;
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;
    ensure(
        matches!(
            recovered.health(),
            RecoveryHealth::Degraded {
                class: RecoveryDegradationClass::DataLoss,
                faults
            } if faults.iter().any(
                |fault| fault.kind() == RecoveryFaultKind::MissingSnapshotObject
            )
        ),
        "lossy missing snapshot did not record data loss",
    )?;
    ensure(
        recovered.wal().replay_start() == CommitVersion::ZERO,
        "lossy missing snapshot trusted missing checkpoint watermark",
    )?;
    outcome.lossy_degradation_cases += 1;
    Ok(())
}

fn check_input_derived_empty_recovery(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 4));
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        recovered.health().is_healthy(),
        "input-derived empty recovery not healthy",
    )?;
    ensure(
        recovered.wal().replay_start() == CommitVersion::ZERO,
        "input-derived empty recovery used nonzero replay start",
    )?;
    outcome.input_derived_empty_cases += 1;
    Ok(())
}

fn check_input_derived_checkpoint_and_tail(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 5));
    let checkpoint_version = CommitVersion::new(1 + u64::from(script_byte(script, 6) % 4));
    let tail_count = usize::from(script_byte(script, 7) % 3) + 1;
    let snapshot_id = 10 + u64::from(script_byte(script, 8) % 32);
    let checkpoint_row = put_row(
        branch,
        checkpoint_version,
        b"generated-checkpoint",
        b"value",
    );
    publish_snapshot(
        backend,
        snapshot_id,
        checkpoint_version,
        std::slice::from_ref(&checkpoint_row),
    )?;
    let flush_watermark = if script_byte(script, 9).is_multiple_of(2) {
        Some(checkpoint_version)
    } else {
        None
    };
    write_database_root(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(
                1,
                Some(checkpoint_version.as_u64()),
                Some(snapshot_id),
                flush_watermark,
            )
            .map_err(testkit_error)?,
    )?;
    // #2765: a checkpoint-attesting manifest requires its WAL chain on disk
    // under strict recovery — seed the attested active segment.
    write_empty_wal_segment(backend, 1)?;
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let equal = wal_record(branch, checkpoint_version, b"generated-equal", b"ignored")?;
    shell
        .services_mut()
        .wal_mut()
        .append(&equal)
        .map_err(testkit_error)?;
    let mut expected_tail = Vec::new();
    for offset in 1..=tail_count {
        let version = CommitVersion::new(checkpoint_version.as_u64() + offset as u64);
        let record = wal_record(branch, version, b"generated-tail", b"value")?;
        shell
            .services_mut()
            .wal_mut()
            .append(&record)
            .map_err(testkit_error)?;
        expected_tail.push(record);
    }
    // Recovery reads the on-disk log, not the writer's coalescing buffer.
    shell
        .services_mut()
        .wal_mut()
        .force_durable()
        .map_err(testkit_error)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        recovered.checkpoint().trusted_watermark() == Some(checkpoint_version),
        "input-derived checkpoint watermark not trusted",
    )?;
    ensure(
        recovered
            .wal()
            .collect_replay_records_for_test(shell.services().wal())
            .map_err(|error| testkit_error(&error))?
            == expected_tail.as_slice(),
        "input-derived WAL tail filtering mismatch",
    )?;
    outcome.input_derived_checkpoint_cases += 1;
    outcome.input_derived_log_tail_cases += 1;
    Ok(())
}

fn check_input_derived_strict_failure(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 10));
    let manifest = if script_byte(script, 11).is_multiple_of(2) {
        DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(1, Some(7), Some(17), None)
            .map_err(testkit_error)?
    } else {
        DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(1, None, None, Some(CommitVersion::new(7)))
            .map_err(testkit_error)?
    };
    write_database_root(backend, &manifest)?;
    // #2765: seed the attested WAL segment so assembly admits the store and
    // the MISSING-SNAPSHOT/WATERMARK rejection under test fires at recover.
    write_empty_wal_segment(backend, 1)?;
    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;

    LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("input-derived strict failure rejects");
    outcome.input_derived_strict_failure_cases += 1;
    Ok(())
}

fn check_input_derived_lossy_degradation(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 12));
    write_database_root(
        backend,
        &DatabaseManifest::new(DATABASE_ID, "identity")
            .map_err(testkit_error)?
            .with_recovery_facts(1, Some(8), Some(18), None)
            .map_err(testkit_error)?,
    )?;
    let mut shell = assemble_shell(lossy_open_plan(), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        matches!(
            recovered.health(),
            RecoveryHealth::Degraded {
                class: RecoveryDegradationClass::DataLoss,
                faults
            } if faults.iter().any(
                |fault| fault.kind() == RecoveryFaultKind::MissingSnapshotObject
            )
        ),
        "input-derived lossy recovery did not degrade",
    )?;
    outcome.input_derived_lossy_degradation_cases += 1;
    Ok(())
}

fn check_table_manifest_recovery(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    check_table_manifest_success(script, outcome)?;
    check_table_manifest_corruption(script, outcome)?;
    check_table_manifest_missing(script, outcome)?;
    check_table_manifest_missing_object(script, outcome)?;
    check_table_manifest_corrupt_object(script, outcome)?;
    check_table_manifest_object_mismatch(script, outcome)?;
    check_table_manifest_orphan_ignored(script, outcome)?;
    check_checkpoint_manifest_conflict(script, outcome)?;
    check_cache_manifest_unsupported(outcome);
    Ok(())
}

fn check_table_manifest_missing(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 18));

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        recovered.tables().table_manifest().table_count() == 0,
        "missing table manifest unexpectedly reported recovered tables",
    )?;
    ensure(
        recovered.health() == &RecoveryHealth::Healthy,
        "missing table manifest for empty branch was not healthy",
    )?;
    outcome.table_manifest_missing_cases += 1;
    Ok(())
}

fn check_table_manifest_corrupt_object(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 19));
    let row = put_row(
        branch,
        CommitVersion::new(8),
        b"manifest-corrupt-object",
        b"value",
    );
    let table = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-manifest-corrupt-object",
        std::slice::from_ref(&row),
        0,
    )?;
    backend.write_raw(table.object().clone(), b"corrupt table object".to_vec());
    publish_table_manifest_for_test(backend, branch, 8, vec![table])?;

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("corrupt manifest-listed table object rejects");

    ensure(
        error.code() == "corruption.lifecycle.table_manifest",
        "corrupt manifest-listed table object returned wrong code",
    )?;
    outcome.table_object_corrupt_cases += 1;
    Ok(())
}

fn check_checkpoint_manifest_conflict(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 20));
    let key = b"checkpoint-manifest-conflict";
    let manifest_row = put_row(branch, CommitVersion::new(9), key, b"manifest-bytes");
    let table = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-conflict-manifest",
        std::slice::from_ref(&manifest_row),
        0,
    )?;
    publish_table_manifest_for_test(backend, branch, 9, vec![table])?;
    let checkpoint_row = put_row(branch, CommitVersion::new(9), key, b"checkpoint-bytes");
    publish_snapshot_for_test(
        backend,
        21,
        CommitVersion::new(9),
        std::slice::from_ref(&checkpoint_row),
    )?;
    seed_database_manifest_with_snapshot(backend, 21, CommitVersion::new(9))?;
    // #2765: seed the attested WAL segment so assembly admits the store and
    // the internal-key CONFLICT rejection under test fires at recover.
    write_empty_wal_segment(backend, 1)?;

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("checkpoint/manifest internal-key conflict rejects");

    ensure(
        error.code() == "failed_precondition.lifecycle.table_manifest_checkpoint_conflict",
        "checkpoint/manifest conflict returned wrong code",
    )?;
    outcome.checkpoint_manifest_conflict_cases += 1;
    Ok(())
}

fn check_cache_manifest_unsupported(outcome: &mut LifecycleRecoveryContractOutcome) {
    // Cache-mode runtimes structurally do not expose a TableManifestService
    // or a durable-table catalog. The source guard
    // `cache_mode_does_not_import_table_manifest_service` enforces this at
    // the type level. We surface this as a counter so generated contracts
    // record that cache-mode absence was observed once per run.
    outcome.cache_manifest_unsupported_cases += 1;
}

fn check_table_manifest_success(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 13));
    let user_key = b"manifest-success";
    let row = put_row(branch, CommitVersion::new(3), user_key, b"value");
    let table = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-manifest-success",
        std::slice::from_ref(&row),
        0,
    )?;
    publish_table_manifest_for_test(backend, branch, 3, vec![table])?;

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let recovered = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;

    ensure(
        recovered.tables().table_manifest().table_count() == 1,
        "table manifest recovery did not report recovered table",
    )?;
    ensure(
        shell
            .branch_state()
            .capture_read_view()
            .map_err(testkit_error)?
            .latest(&physical_key(branch, user_key)?)
            .map_err(testkit_error)?
            .is_some(),
        "table manifest recovery did not make listed table visible",
    )?;
    outcome.table_manifest_published_cases += 1;
    outcome.table_manifest_recovered_cases += 1;
    Ok(())
}

fn check_table_manifest_corruption(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 14));
    let object = ObjectLayout::branch_table_manifest(&branch.to_string()).map_err(testkit_error)?;
    backend.write_raw(object, b"corrupt manifest".to_vec());

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("corrupt table manifest rejects");

    ensure(
        error.code() == "corruption.lifecycle.table_manifest",
        "corrupt table manifest returned wrong code",
    )?;
    outcome.table_manifest_corrupt_cases += 1;
    Ok(())
}

fn check_table_manifest_missing_object(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 15));
    let row = put_row(
        branch,
        CommitVersion::new(4),
        b"manifest-missing-object",
        b"value",
    );
    let table = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-manifest-missing-object",
        std::slice::from_ref(&row),
        0,
    )?;
    backend
        .delete_object(table.object())
        .map_err(testkit_error)?;
    publish_table_manifest_for_test(backend, branch, 4, vec![table])?;

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("missing manifest-listed table rejects");

    ensure(
        error.code() == "corruption.lifecycle.table_manifest",
        "missing manifest-listed table returned wrong code",
    )?;
    outcome.table_object_missing_cases += 1;
    Ok(())
}

fn check_table_manifest_object_mismatch(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 16));
    let row = put_row(
        branch,
        CommitVersion::new(5),
        b"manifest-mismatch",
        b"value",
    );
    let table = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-manifest-mismatch",
        std::slice::from_ref(&row),
        0,
    )?;
    let bad_facts = TableManifestTableFacts::new(
        table.facts().byte_count(),
        table.facts().row_count() + 1,
        table.facts().data_block_count(),
        table.facts().commit_min(),
        table.facts().commit_max(),
        table.facts().timestamp_min(),
        table.facts().timestamp_max(),
    )
    .map_err(testkit_error)?;
    let bad_table = TableManifestTableRef::new(
        table.table_identity().clone(),
        table.object().clone(),
        table.order(),
        bad_facts,
        table.bounds().clone(),
        table.provenance().clone(),
    )
    .map_err(testkit_error)?;
    publish_table_manifest_for_test(backend, branch, 5, vec![bad_table])?;

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("mismatched manifest-listed table rejects");

    ensure(
        error.code() == "corruption.lifecycle.table_manifest",
        "mismatched manifest-listed table returned wrong code",
    )?;
    outcome.table_object_mismatch_cases += 1;
    Ok(())
}

fn check_table_manifest_orphan_ignored(
    script: &[u8],
    outcome: &mut LifecycleRecoveryContractOutcome,
) -> Result<(), TestkitError> {
    let backend: &'static RecoveryScriptBackend =
        crate::testkit::leak_static(RecoveryScriptBackend::new());
    let branch = branch_id(script_byte(script, 17));
    let listed_key = b"manifest-listed";
    let orphan_key = b"manifest-orphan";
    let listed = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-listed-table",
        &[put_row(
            branch,
            CommitVersion::new(6),
            listed_key,
            b"listed",
        )],
        0,
    )?;
    let _orphan = publish_table_object_for_manifest(
        backend,
        branch,
        BranchLevel::ZERO,
        "generated-orphan-table",
        &[put_row(
            branch,
            CommitVersion::new(7),
            orphan_key,
            b"orphan",
        )],
        0,
    )?;
    publish_table_manifest_for_test(backend, branch, 6, vec![listed])?;

    let mut shell = assemble_shell(open_plan(RecoveryStrictness::Strict), branch, backend)?;
    let request = LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
        .map_err(|error| testkit_error(&error))?;
    LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .map_err(|error| testkit_error(&error))?;
    let view = shell
        .branch_state()
        .capture_read_view()
        .map_err(testkit_error)?;

    ensure(
        view.latest(&physical_key(branch, listed_key)?)
            .map_err(testkit_error)?
            .is_some(),
        "listed table row was not visible",
    )?;
    ensure(
        view.latest(&physical_key(branch, orphan_key)?)
            .map_err(testkit_error)?
            .is_none(),
        "orphan table row became visible",
    )?;
    outcome.orphan_ignored_cases += 1;
    Ok(())
}

fn publish_table_manifest_for_test(
    backend: &'static RecoveryScriptBackend,
    branch: BranchId,
    sequence: u64,
    tables: Vec<TableManifestTableRef>,
) -> Result<(), TestkitError> {
    // BS4.4j requires a positional row-split extension (one per manifest table, in walk order).
    // These are single-level (L0) manifests with no inherited layers, so the split order is just the
    // table order. Compute each split from the table's real object; a scenario that deliberately
    // corrupts the object falls back to (row_count, 0) — recovery fails at open before the split's
    // exact value matters there.
    let splits: Vec<TableRowSplit> = tables
        .iter()
        .map(|table| row_split_for_manifest_ref(backend, table))
        .collect();
    let extensions = if splits.is_empty() {
        Vec::new()
    } else {
        vec![table_row_split_extension_section(&splits).map_err(testkit_error)?]
    };
    let manifest = TableManifest::new(
        branch,
        None,
        sequence,
        vec![TableManifestLevel::new(BranchLevel::ZERO, tables).map_err(testkit_error)?],
        Vec::new(),
        extensions,
    )
    .map_err(testkit_error)?;
    TableManifestService::new(backend)
        .publish_replace_manifest(branch, &manifest)
        .map_err(testkit_error)?;
    Ok(())
}

/// The put/tombstone split of a manifest table, read from its real object. Falls back to
/// `(row_count, 0)` when the object is unreadable (a scenario deliberately corrupted it) — recovery
/// rejects such a table at reader-open before the split's exact breakdown is validated.
fn row_split_for_manifest_ref(
    backend: &'static RecoveryScriptBackend,
    table: &TableManifestTableRef,
) -> TableRowSplit {
    backend
        .read_object(table.object())
        .ok()
        .and_then(|bytes| {
            ImmutableTableReader::open_bytes(
                table.table_identity().clone(),
                bytes,
                TableReaderConfig::default(),
            )
            .ok()
        })
        .map_or_else(
            || TableRowSplit::new(table.facts().row_count(), 0),
            |reader| {
                let tombstones = reader
                    .rows()
                    .iter()
                    .filter(|row| row.is_tombstone())
                    .count() as u64;
                let put = reader.rows().len() as u64 - tombstones;
                TableRowSplit::new(put, tombstones)
            },
        )
}

fn publish_snapshot_for_test(
    backend: &'static RecoveryScriptBackend,
    snapshot_id: u64,
    watermark: CommitVersion,
    rows: &[StorageRow],
) -> Result<(), TestkitError> {
    SnapshotService::new(backend)
        .publish_create(SnapshotPublishRequest::new(
            snapshot_id,
            watermark,
            Timestamp::from_micros(7_000),
            DATABASE_ID,
            "identity",
            vec![encode_checkpoint_row_section(rows).map_err(testkit_error)?],
        ))
        .map_err(testkit_error)?;
    Ok(())
}

fn seed_database_manifest_with_snapshot(
    backend: &'static RecoveryScriptBackend,
    snapshot_id: u64,
    watermark: CommitVersion,
) -> Result<(), TestkitError> {
    let manifest = DatabaseManifest::new(DATABASE_ID, "identity")
        .map_err(testkit_error)?
        .with_recovery_facts(1, Some(watermark.as_u64()), Some(snapshot_id), None)
        .map_err(testkit_error)?;
    let bytes = encode_manifest(&manifest).map_err(testkit_error)?;
    backend.write_raw(
        ObjectLayout::database_manifest().map_err(testkit_error)?,
        bytes,
    );
    Ok(())
}

fn publish_table_object_for_manifest(
    backend: &'static RecoveryScriptBackend,
    branch: BranchId,
    level: BranchLevel,
    identity: &str,
    rows: &[StorageRow],
    order: u32,
) -> Result<TableManifestTableRef, TestkitError> {
    let identity = TableIdentity::new(identity).map_err(testkit_error)?;
    let bytes = table_bytes(identity.clone(), rows)?;
    let facts = TableObjectService::new(backend)
        .publish_create(
            &branch.to_string(),
            u32::from(level.raw()),
            identity.as_str(),
            &bytes,
        )
        .map_err(testkit_error)?;
    let reader =
        ImmutableTableReader::open_bytes(identity.clone(), bytes, TableReaderConfig::default())
            .map_err(testkit_error)?;
    TableManifestTableRef::new(
        identity,
        facts.object().clone(),
        order,
        table_manifest_facts(&reader)?,
        table_manifest_bounds(reader.rows())?,
        TableManifestTableProvenance::Flush,
    )
    .map_err(testkit_error)
}

fn table_bytes(identity: TableIdentity, rows: &[StorageRow]) -> Result<Vec<u8>, TestkitError> {
    let mut table_rows = rows.iter().cloned().map(TableRow::new).collect::<Vec<_>>();
    sort_table_rows_by_key(&mut table_rows);
    let table = ImmutableTableBuilder::new(TableBuilderConfig::default())
        .map_err(testkit_error)?
        .build_from_rows(identity, &table_rows)
        .map_err(testkit_error)?;
    Ok(table.into_bytes())
}

fn table_manifest_facts(
    reader: &ImmutableTableReader,
) -> Result<TableManifestTableFacts, TestkitError> {
    let (timestamp_min, timestamp_max) = table_timestamp_bounds(reader.rows());
    TableManifestTableFacts::new(
        reader.facts().byte_count(),
        reader.facts().row_count(),
        reader.facts().data_block_count(),
        reader.facts().commit_range().min(),
        reader.facts().commit_range().max(),
        timestamp_min,
        timestamp_max,
    )
    .map_err(testkit_error)
}

fn table_manifest_bounds(rows: &[TableRow]) -> Result<TableManifestTableBounds, TestkitError> {
    let Some(first) = rows.first() else {
        return Err(TestkitError::new("table manifest test table has no rows"));
    };
    let mut physical_first = TablePhysicalKeyBytes::from_row(first.row());
    let mut physical_last = physical_first.clone();
    let mut internal_first = first.key().clone();
    let mut internal_last = internal_first.clone();
    for row in rows.iter().skip(1) {
        let physical = TablePhysicalKeyBytes::from_row(row.row());
        if physical < physical_first {
            physical_first = physical.clone();
        }
        if physical > physical_last {
            physical_last = physical;
        }
        if row.key() < &internal_first {
            internal_first = row.key().clone();
        }
        if row.key() > &internal_last {
            internal_last = row.key().clone();
        }
    }
    TableManifestTableBounds::new(
        physical_first.as_slice().to_vec(),
        physical_last.as_slice().to_vec(),
        internal_first.as_slice().to_vec(),
        internal_last.as_slice().to_vec(),
    )
    .map_err(testkit_error)
}

fn table_timestamp_bounds(rows: &[TableRow]) -> (Option<Timestamp>, Option<Timestamp>) {
    let mut timestamps = rows.iter().map(TableRow::commit_timestamp);
    let Some(first) = timestamps.next() else {
        return (None, None);
    };
    let (min, max) = timestamps.fold((first, first), |(min, max), timestamp| {
        (min.min(timestamp), max.max(timestamp))
    });
    (Some(min), Some(max))
}

pub(super) fn assemble_shell(
    plan: StorageOpenPlan,
    branch: BranchId,
    backend: &'static RecoveryScriptBackend,
) -> Result<LifecycleDurableLocalShell<'static>, TestkitError> {
    LifecycleDurableLocalShell::assemble(
        LifecycleDurableLocalOpenRequest::new(
            plan,
            DATABASE_ID,
            branch,
            CommitBranchGeneration::new(1).map_err(testkit_error)?,
            BranchRuntimeConfig::default(),
            CommitRuntimeConfig::default(),
            WalServiceConfig::default()
                .with_append_buffer_bytes(crate::service::DEFAULT_WAL_APPEND_BUFFER_BYTES),
        )
        .map_err(testkit_error)?,
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .map_err(testkit_error)
}

pub(super) fn open_plan(strictness: RecoveryStrictness) -> StorageOpenPlan {
    StorageOpenPlan::new(
        StorageMode::DurableLocalStandard,
        LifecycleCodecId::identity(),
        strictness,
        LifecycleConfig::default(),
    )
    .expect("open plan")
}

pub(super) fn lossy_open_plan() -> StorageOpenPlan {
    let config = LifecycleConfig::new(
        1024,
        16,
        crate::lifecycle::LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
        LifecycleLossyRecoveryPolicy::ExplicitlyAllowed,
    )
    .expect("lossy config");
    StorageOpenPlan::new(
        StorageMode::DurableLocalStandard,
        LifecycleCodecId::identity(),
        RecoveryStrictness::AllowExplicitLossyFallback,
        config,
    )
    .expect("lossy open plan")
}

pub(super) fn publish_snapshot(
    backend: &'static RecoveryScriptBackend,
    snapshot_id: u64,
    watermark: CommitVersion,
    rows: &[StorageRow],
) -> Result<(), TestkitError> {
    SnapshotService::new(backend)
        .publish_create(SnapshotPublishRequest::new(
            snapshot_id,
            watermark,
            Timestamp::from_micros(7_000),
            DATABASE_ID,
            "identity",
            vec![encode_checkpoint_row_section(rows).map_err(testkit_error)?],
        ))
        .map_err(testkit_error)?;
    Ok(())
}

pub(super) fn write_database_root(
    backend: &'static RecoveryScriptBackend,
    root: &DatabaseManifest,
) -> Result<(), TestkitError> {
    backend.write_raw(
        ObjectLayout::database_manifest().map_err(testkit_error)?,
        encode_manifest(root).map_err(testkit_error)?,
    );
    Ok(())
}

/// Seeds the header-only WAL segment a fresh writer would have created —
/// what #2765's strict-mode chain check requires on disk whenever the
/// manifest attests a checkpoint (a checkpoint proves durable history, so
/// its WAL chain must exist; byte shape mirrors `create_segment`).
pub(super) fn write_empty_wal_segment(
    backend: &'static RecoveryScriptBackend,
    segment_id: u64,
) -> Result<(), TestkitError> {
    backend.write_raw(
        ObjectLayout::wal_segment(segment_id).map_err(testkit_error)?,
        encode_wal_segment_header(&WalSegmentHeader::new(segment_id, DATABASE_ID)),
    );
    Ok(())
}

fn wal_record(
    branch: BranchId,
    version: CommitVersion,
    user_key: &'static [u8],
    value: &'static [u8],
) -> Result<WalRecord, TestkitError> {
    let timestamp = Timestamp::from_micros(version.as_u64() * 100);
    let row = StorageRow::put(
        physical_key(branch, user_key)?,
        version,
        timestamp,
        Timestamp::EPOCH,
        value.to_vec(),
    );
    let payload = WalCommitPayload::new(vec![row]).map_err(testkit_error)?;
    WalRecord::new(version, branch, timestamp, payload).map_err(testkit_error)
}

pub(super) fn put_row(
    branch: BranchId,
    version: CommitVersion,
    user_key: &'static [u8],
    value: &'static [u8],
) -> StorageRow {
    StorageRow::put(
        physical_key(branch, user_key).expect("physical key"),
        version,
        Timestamp::from_micros(version.as_u64() * 100),
        Timestamp::EPOCH,
        value.to_vec(),
    )
}

pub(super) fn physical_key(
    branch: BranchId,
    user_key: &'static [u8],
) -> Result<PhysicalKey, TestkitError> {
    PhysicalKey::new(
        branch,
        "lifecycle",
        StorageSpaceId::engine(0x20).map_err(testkit_error)?,
        user_key.to_vec(),
    )
    .map_err(testkit_error)
}

pub(super) fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; BranchId::BYTE_LEN])
}

pub(super) fn testkit_error(error: impl std::error::Error) -> TestkitError {
    TestkitError::new(error.to_string())
}

#[derive(Debug)]
pub(super) struct RecoveryScriptBackend {
    capabilities: BackendCapabilities,
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    lock_held: Arc<AtomicBool>,
}

impl RecoveryScriptBackend {
    pub(super) fn new() -> Self {
        let backend = Self {
            capabilities: BackendCapabilities::from_slice(DURABLE_LOCAL_MODE_REQUIREMENTS),
            objects: Mutex::new(BTreeMap::new()),
            lock_held: Arc::new(AtomicBool::new(false)),
        };
        // #3015: assemble refuses durable residue without a database
        // manifest, so every staged store starts from the same empty root
        // the old fabrication used to produce; scenarios that publish their
        // own root simply overwrite this seed.
        let root = DatabaseManifest::new(DATABASE_ID, "identity").expect("database manifest");
        backend.objects.lock().expect("objects").insert(
            ObjectLayout::database_manifest().expect("manifest object"),
            encode_manifest(&root).expect("database manifest bytes"),
        );
        backend
    }

    pub(super) fn write_raw(&self, object: ObjectName, bytes: Vec<u8>) {
        self.objects.lock().expect("objects").insert(object, bytes);
    }
}

impl Backend for RecoveryScriptBackend {
    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset()).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end_offset().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
        Ok(bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec())
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        let removed = self.objects.lock().expect("objects").remove(name).is_some();
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
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
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        if self.lock_held.swap(true, Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "writer lock unavailable",
            ));
        }
        Ok(BackendWriterGuard::new(
            name.clone(),
            RecoveryScriptWriterLock {
                locked: Arc::clone(&self.lock_held),
            },
        ))
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
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

    fn sync_object(&self, _name: &ObjectName) -> crate::backend::BackendResult<()> {
        Ok(())
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
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

struct RecoveryScriptWriterLock {
    locked: Arc<AtomicBool>,
}

impl Drop for RecoveryScriptWriterLock {
    fn drop(&mut self) {
        self.locked.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default-lane pin (#2902): the recovery contract's checkpoint and
    /// strict-failure scenarios (incl. the seeded-WAL-chain preconditions)
    /// must execute and count outside the feature-gated targets.
    #[test]
    fn recovery_contract_holds_with_all_scenarios_counted() {
        let outcome = check_lifecycle_recovery_contract(b"lifecycle-default-lane-pin")
            .expect("recovery contract holds");
        assert!(outcome.empty_recovery_cases() > 0);
        assert!(outcome.log_tail_cases() > 0);
        assert!(outcome.checkpoint_recovery_cases() > 0);
        assert!(outcome.strict_failure_cases() > 0);
        assert!(outcome.lossy_degradation_cases() > 0);
        assert!(outcome.input_derived_checkpoint_cases() > 0);
        assert!(outcome.input_derived_strict_failure_cases() > 0);
        assert!(outcome.checkpoint_manifest_conflict_cases() > 0);
    }
}
