use super::*;
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata,
    BackendRange, BackendResult, BackendWriterGuard, PublishDurability, PublishError,
    PublishFailureKind, PublishMode, PublishOutcome, PublishResult,
    DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::state::materialization::BranchMaterializationRequest;
use crate::branch::state::BranchLocalState;
use crate::commit::{
    CommitBatch, CommitBatchOptions, CommitBranchGeneration, CommitBranchGenerationGuard,
    CommitConflictValidationMode, CommitDuplicateKeyPolicy, CommitDurabilityMode, CommitExpiry,
    CommitManualTimestampSource, CommitMutation, CommitOrigin, CommitRetentionHint,
    CommitRuntimeConfig, CommitTimelineEntry, CommitTimelineRows, CommitTimestampPolicy,
    CommitValidationFacts,
};
use crate::format::decode_snapshot_container;
use crate::layout::ObjectLayout;
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::{DatabaseManifestService, WalRetentionProof, WalRetentionProofSource};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use strata_core::{BranchId, CommitVersion, Timestamp};

mod remaining;
pub(super) mod shared;

use shared::*;

const DATABASE_ID: [u8; 16] = [0x7d; 16];

#[test]
fn checkpoint_request_rejects_zero_snapshot_id() {
    assert_eq!(
        LifecycleCheckpointRequest::new(branch_id(0x10), 0, Timestamp::from_micros(1)),
        Err(LifecycleError::InvalidConfig {
            field: "checkpoint_snapshot_id",
            reason: "checkpoint snapshot id must be nonzero",
        })
    );
}

#[test]
fn checkpoint_task_rejects_wrong_maintenance_scope() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x0f);
    let shell = assemble_shell(branch, backend).expect("shell");
    // Assembly itself performs the WAL resume-segment listing (#2555); the
    // rejection below must add no backend work on top of that baseline.
    let baseline_events = backend.event_count();
    let task = maintenance_task_for_test(31, MaintenanceTaskRequest::wal_truncation());

    let error = checkpoint_request_from_maintenance_task(
        &task,
        branch,
        shell.services().manifest(),
        Timestamp::from_micros(1),
    )
    .expect_err("wrong task rejects");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
    assert_eq!(backend.event_count(), baseline_events);
}

#[test]
fn checkpoint_rows_emit_active_frozen_delta_excluding_owned_and_newer() {
    let branch = branch_id(0x11);
    let mut state = BranchLocalState::empty(branch);
    let owned = put_row(branch, 1, b"owned", b"owned-value");
    let frozen = put_row(branch, 2, b"frozen", b"frozen-value");
    let active = put_row(branch, 3, b"active", b"active-value");
    let hidden = put_row(branch, 4, b"hidden", b"hidden-value");

    state
        .append_committed_row(owned.clone())
        .expect("append owned candidate");
    state.rotate_active();
    flush_cache_branch(&mut state, &flush_request(branch)).expect("flush owned");
    state
        .append_committed_row(frozen.clone())
        .expect("append frozen candidate");
    state.rotate_active();
    state
        .append_committed_row(active.clone())
        .expect("append active candidate");
    state
        .append_committed_row(hidden)
        .expect("append hidden candidate");

    let rows = state
        .checkpoint_rows(CommitVersion::new(3))
        .expect("checkpoint rows");

    // The checkpoint is a bounded delta: the flushed `owned` row now lives in an
    // owned level and is excluded (recovery restores it from the table manifest);
    // `hidden` is above the watermark. Only the not-yet-durable active + frozen
    // rows are emitted.
    assert_eq!(rows.len(), 2);
    assert!(!rows.contains(&owned));
    assert!(rows.contains(&frozen));
    assert!(rows.contains(&active));
    assert!(rows
        .windows(2)
        .all(
            |window| crate::table::TableInternalKeyBytes::from_row(&window[0])
                < crate::table::TableInternalKeyBytes::from_row(&window[1])
        ));
}

#[test]
fn checkpoint_rows_delta_size_is_independent_of_owned_levels() {
    // The checkpoint is a bounded delta: its size tracks the not-yet-durable
    // active/frozen backlog, never the owned-level row count. Several rows flushed
    // into an owned level plus a single active row produce a one-row delta — recovery
    // restores the owned rows from the table manifest, so they never inflate the
    // snapshot (this is what removes the snapshot size-ceiling crash at scale).
    let branch = branch_id(0x40);
    let mut state = BranchLocalState::empty(branch);
    let owned = [
        put_row(branch, 1, b"owned-a", b"value"),
        put_row(branch, 2, b"owned-b", b"value"),
        put_row(branch, 3, b"owned-c", b"value"),
    ];
    for row in &owned {
        state
            .append_committed_row(row.clone())
            .expect("append owned candidate");
    }
    state.rotate_active();
    flush_cache_branch(&mut state, &flush_request(branch)).expect("flush owned rows");
    let active = put_row(branch, 4, b"active-tail", b"value");
    state
        .append_committed_row(active.clone())
        .expect("append active candidate");

    let rows = state
        .checkpoint_rows(CommitVersion::new(4))
        .expect("checkpoint rows");

    assert_eq!(rows.len(), 1, "delta must be just the active tail");
    assert!(rows.contains(&active));
    for owned_row in &owned {
        assert!(
            !rows.contains(owned_row),
            "owned-level rows must be excluded from the delta",
        );
    }
}

#[test]
fn checkpoint_rows_include_tombstones_and_timeline_rows() {
    let branch = branch_id(0x1b);
    let mut state = BranchLocalState::empty(branch);
    let deleted = StorageRow::tombstone(
        physical_key(branch, b"deleted"),
        CommitVersion::new(1),
        Timestamp::from_micros(100),
    );
    let timeline = CommitTimelineRows::from_entry(
        CommitTimelineEntry::new(branch, CommitVersion::new(2), Timestamp::from_micros(200))
            .expect("timeline entry"),
    )
    .expect("timeline rows")
    .into_rows();

    state
        .append_committed_row(deleted.clone())
        .expect("append deleted row");
    state
        .append_committed_row(timeline[0].clone())
        .expect("append timeline row");
    state
        .append_committed_row(timeline[1].clone())
        .expect("append reverse timeline row");

    let rows = state
        .checkpoint_rows(CommitVersion::new(2))
        .expect("checkpoint rows");

    assert!(rows.iter().any(StorageRow::is_tombstone));
    assert!(rows.contains(&timeline[0]));
    assert!(rows.contains(&timeline[1]));
    assert!(rows
        .iter()
        .all(|row| row.physical_key().branch_id() == branch));
    assert!(rows
        .iter()
        .map(StorageRow::commit_timestamp)
        .any(|timestamp| timestamp == Timestamp::from_micros(200)));
}

#[test]
fn checkpoint_watermark_uses_visible_version_not_allocated_version() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x1c);
    let shell = assemble_shell(branch, backend).expect("shell");
    let mut state = BranchLocalState::empty(branch);
    let visible = put_row(branch, 1, b"visible-bound", b"value");
    let hidden = put_row(branch, 2, b"hidden-bound", b"value");
    state
        .append_committed_row(visible)
        .expect("append visible row");
    state
        .append_committed_row(hidden)
        .expect("append hidden row");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(20)).expect("request");

    let outcome = checkpoint_durable_branch(
        &state,
        shell.services(),
        shell.guard_set(),
        || CommitVersion::new(1),
        &request,
    )
    .expect("checkpoint");

    assert_eq!(outcome.checkpoint_watermark(), Some(CommitVersion::new(1)));
    assert_eq!(outcome.row_count(), 1);
}

#[test]
fn checkpoint_reads_visible_version_after_commit_quiesce() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x12);
    let shell = assemble_shell(branch, backend).expect("shell");
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, 1, b"visible", b"value"))
        .expect("append row");
    let observed_quiesce = Cell::new(false);
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(10)).expect("request");

    let outcome = checkpoint_durable_branch(
        &state,
        shell.services(),
        shell.guard_set(),
        || {
            observed_quiesce.set(shell.guard_set().is_quiescing().expect("quiesce state"));
            CommitVersion::new(1)
        },
        &request,
    )
    .expect("checkpoint");

    assert!(observed_quiesce.get());
    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(outcome.checkpoint_watermark(), Some(CommitVersion::new(1)));
    assert_eq!(outcome.row_count(), 1);
}

#[test]
fn checkpoint_snapshot_publish_failure_releases_quiesce_and_keeps_recovery_facts() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x1d);
    let shell = assemble_shell(branch, backend).expect("shell");
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, 1, b"snapshot-fail", b"value"))
        .expect("append row");
    backend.fail_snapshot_publish();
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(21)).expect("request");

    let error = checkpoint_durable_branch(
        &state,
        shell.services(),
        shell.guard_set(),
        || CommitVersion::new(1),
        &request,
    )
    .expect_err("snapshot publish failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    assert!(!shell.guard_set().is_quiescing().expect("quiesce state"));
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("current database record");
    assert_eq!(manifest.snapshot_id(), None);
    assert_eq!(manifest.snapshot_watermark(), None);
}

#[test]
fn checkpoint_publishes_empty_delta_and_advances_watermark_when_all_rows_flushed() {
    // After everything is flushed to owned levels the checkpoint delta is empty, but
    // durable owned rows exist under the watermark. The checkpoint must still PUBLISH
    // (an empty-delta snapshot) to advance the snapshot watermark — otherwise WAL
    // truncation stalls and recovery replays past the durable point. Contrast with
    // `checkpoint_defers_when_branch_has_no_rows_under_visible_watermark` below, where a
    // genuinely empty branch still defers. Recovery restores the flushed row from the
    // table manifest.
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x41);
    let mut runtime = open_runtime(branch, backend);
    let key = physical_key(branch, b"flushed-then-checkpointed");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"flushed-then-checkpointed", b"value"),
            generation_guard(),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .flush_frozen(&flush_request(branch))
        .expect("flush frozen");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(31)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("checkpoint");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(
        outcome.row_count(),
        0,
        "delta is empty; every row is durable in an owned level",
    );
    assert!(outcome.snapshot_id().is_some());
    assert_eq!(outcome.checkpoint_watermark(), Some(CommitVersion::new(1)));
    drop(runtime);

    let reopened = open_runtime(branch, backend);
    assert_eq!(
        reopened
            .read_view()
            .expect("read view")
            .latest(&key)
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"value"
    );
}

#[test]
fn checkpoint_defers_when_branch_has_no_rows_under_visible_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x13);
    let shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(10)).expect("request");

    let outcome = checkpoint_durable_branch(
        shell.branch_state(),
        shell.services(),
        shell.guard_set(),
        || CommitVersion::new(1),
        &request,
    )
    .expect("checkpoint");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::DeferredNoVisibleRows
    );
    assert!(outcome.snapshot_id().is_none());
    assert!(backend.snapshot_objects().is_empty());
}

#[test]
fn checkpoint_publishes_snapshot_between_database_record_updates() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x1e);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"ordering-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(22)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("checkpoint");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(outcome.active_wal_segment(), Some(1));
    assert_eq!(
        backend.checkpoint_events(),
        vec![
            CheckpointBackendEvent::DatabaseRecordReplace,
            CheckpointBackendEvent::SnapshotCreate,
            CheckpointBackendEvent::DatabaseRecordReplace,
        ]
    );
}

#[test]
fn checkpoint_publishes_snapshot_and_flush_watermark_after_commit() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x14);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"checkpoint-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(11))
        .expect("request")
        .with_flush_watermark_after_checkpoint(true);

    let outcome = runtime.checkpoint(&request).expect("checkpoint");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(outcome.checkpoint_watermark(), Some(CommitVersion::new(1)));
    assert_eq!(outcome.snapshot_id(), Some(1));
    assert_eq!(outcome.row_count(), 1);
    assert!(outcome.snapshot_object().is_some());
    assert!(outcome
        .flush_watermark()
        .expect("flush outcome")
        .was_persisted());
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("current database record");
    assert_eq!(manifest.snapshot_id(), Some(1));
    assert_eq!(manifest.snapshot_watermark(), Some(1));
    assert_eq!(
        manifest.flushed_through_commit_id(),
        Some(CommitVersion::new(1))
    );
}

#[test]
fn checkpoint_manifest_publish_failure_reports_partial_snapshot() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x1f);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"partial-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(2);
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(23)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("partial outcome");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::SnapshotPublishedManifestNotUpdated
    );
    assert_eq!(outcome.snapshot_id(), Some(1));
    assert!(outcome.snapshot_object().is_some());
    assert_eq!(
        outcome.failure().expect("orphan fact").code(),
        "ambiguous_commit.lifecycle.checkpoint_snapshot"
    );
    assert_eq!(
        outcome
            .maintenance_outcome()
            .source_error()
            .expect("source error")
            .code(),
        "ambiguous_commit.lifecycle.checkpoint_snapshot"
    );
    assert!(outcome.recovery_health().is_some());
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("current database record");
    assert_eq!(manifest.snapshot_id(), None);
    assert_eq!(manifest.snapshot_watermark(), None);
}

#[test]
fn recovery_ignores_unreferenced_snapshot_after_manifest_failure() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x3d);
    let key = physical_key(branch, b"orphan-key");
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"orphan-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(2);
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(29)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("partial outcome");
    let orphan = outcome.snapshot_object().expect("snapshot object").clone();
    drop(runtime);
    let reopened = open_runtime(branch, backend);

    assert!(backend.read_object(&orphan).is_ok());
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .snapshot_id(),
        None
    );
    assert_eq!(
        reopened
            .read_view()
            .expect("view")
            .latest(&key)
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"value"
    );
}

#[test]
fn checkpoint_manifest_uncertainty_reports_uncertain_status() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x20);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"uncertain-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.uncertain_manifest_replacement_on_call(2);
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(24)).expect("request");

    let outcome = runtime.checkpoint(&request).expect("uncertain outcome");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::SnapshotVisibilityUncertain
    );
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Failed
    );
}

#[test]
fn checkpoint_existing_snapshot_id_collision_fails_closed() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x21);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"collision-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(25)).expect("request");
    runtime.checkpoint(&request).expect("first checkpoint");

    let error = runtime
        .checkpoint(&request)
        .expect_err("second checkpoint rejects");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.checkpoint_publication"
    );
}

#[test]
fn checkpoint_reports_flush_watermark_failure_without_losing_snapshot_facts() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x15);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"flush-failure-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(3);
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(12))
        .expect("request")
        .with_flush_watermark_after_checkpoint(true);

    let outcome = runtime.checkpoint(&request).expect("checkpoint outcome");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::FlushWatermarkFailed
    );
    assert_eq!(outcome.snapshot_id(), Some(1));
    assert!(outcome.snapshot_object().is_some());
    assert!(outcome.flush_watermark().is_none());
    assert!(outcome.recovery_health().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Failed
    );
    assert!(outcome.maintenance_outcome().retryable());
}

#[test]
fn checkpoint_with_truncation_skips_delete_when_deferred() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x22);
    let shell = assemble_shell(branch, backend).expect("shell");
    // Assembly performs the WAL resume-segment listing (#2555); the deferred
    // checkpoint must not add a retention listing on top of that baseline.
    let baseline_lists = backend.list_calls();
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(26))
        .expect("request")
        .with_wal_truncation_after_checkpoint(true);

    let outcome = checkpoint_durable_branch(
        shell.branch_state(),
        shell.services(),
        shell.guard_set(),
        || CommitVersion::new(1),
        &request,
    )
    .expect("checkpoint");

    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::DeferredNoVisibleRows
    );
    assert_eq!(outcome.wal_truncation(), None);
    assert_eq!(backend.list_calls(), baseline_lists);
}

#[test]
fn checkpoint_reports_wal_truncation_failure_without_losing_snapshot_facts() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x16);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"truncation-failure-key", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_wal_listing();
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(13))
        .expect("request")
        .with_wal_truncation_after_checkpoint(true);

    let outcome = runtime.checkpoint(&request).expect("checkpoint outcome");

    assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(outcome.snapshot_id(), Some(1));
    assert!(outcome.snapshot_object().is_some());
    assert!(outcome.wal_truncation().is_none());
    assert!(outcome.recovery_health().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
    assert!(!outcome.maintenance_outcome().retryable());
}

#[test]
fn checkpoint_recovery_restores_rows_without_covered_log_records() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x23);
    let mut runtime = open_runtime(branch, backend);
    let key = physical_key(branch, b"recover-from-checkpoint");
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"recover-from-checkpoint", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request = LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(27))
        .expect("request")
        .with_wal_truncation_after_checkpoint(true);
    runtime.checkpoint(&request).expect("checkpoint");
    drop(runtime);

    let reopened = open_runtime(branch, backend);
    let visible = reopened
        .read_view()
        .expect("read view")
        .latest(&key)
        .expect("read latest")
        .expect("visible row");

    assert_eq!(visible.row().value(), b"value");
    assert_eq!(reopened.visible_version(), CommitVersion::new(1));
}

#[test]
fn checkpoint_recovery_restores_tombstone_and_timeline_rows() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x24);
    let mut runtime = open_runtime(branch, backend);
    let key = physical_key(branch, b"recover-deleted");
    let batch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(key.clone())],
        CommitValidationFacts::empty(),
        CommitBatchOptions::new(
            CommitDurabilityMode::Standard,
            CommitConflictValidationMode::Validate,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
    );
    runtime
        .execute_durable_commit(batch, generation_guard())
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(28)).expect("request");
    runtime.checkpoint(&request).expect("checkpoint");
    drop(runtime);

    let reopened = open_runtime(branch, backend);
    let history = reopened
        .read_view()
        .expect("read view")
        .history(&key, crate::branch::read::BranchHistoryOptions::all())
        .expect("history");

    assert!(history.first().is_some_and(|row| row.row().is_tombstone()));
    assert_eq!(reopened.visible_version(), CommitVersion::new(1));
}

#[test]
fn flush_watermark_proofs_are_conservative_and_monotonic() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x17);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(3, CommitVersion::new(7))
        .expect("snapshot facts");

    let table_only = LifecycleFlushWatermarkProof::TableObjectsOnly {
        flushed_through: CommitVersion::new(5),
    };
    assert!(matches!(
        persist_flush_watermark(
            shell.services().manifest(),
            CommitVersion::new(7),
            CommitVersion::new(5),
            &table_only,
        ),
        Err(LifecycleError::WalRetentionProofIncomplete { .. })
    ));

    let persisted = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(7),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::CheckpointCovered {
            snapshot_watermark: CommitVersion::new(7),
        },
    )
    .expect("persisted");
    assert!(persisted.was_persisted());
    assert_eq!(persisted.persisted_watermark(), Some(CommitVersion::new(5)));

    let already = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(7),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::AlreadyPersisted,
    )
    .expect("already persisted");
    assert!(already.was_already_persisted());
    assert_eq!(already.candidate(), CommitVersion::new(5));
}

#[test]
fn flush_watermark_rejects_bounds_and_preserves_branch_state() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x25);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(4, CommitVersion::new(6))
        .expect("snapshot facts");
    let before = shell.branch_state().facts();

    let above_checkpoint = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(8),
        CommitVersion::new(7),
        &LifecycleFlushWatermarkProof::CheckpointCovered {
            snapshot_watermark: CommitVersion::new(6),
        },
    )
    .expect_err("above checkpoint rejects");
    let above_visible = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(7),
        &LifecycleFlushWatermarkProof::CheckpointCovered {
            snapshot_watermark: CommitVersion::new(7),
        },
    )
    .expect_err("above visible rejects");
    let already_not_persisted = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(5),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::AlreadyPersisted,
    )
    .expect_err("not already persisted");

    assert_eq!(
        above_checkpoint.code(),
        "failed_precondition.lifecycle.wal_retention"
    );
    assert_eq!(
        above_visible.code(),
        "failed_precondition.lifecycle.wal_retention"
    );
    assert_eq!(
        already_not_persisted.code(),
        "failed_precondition.lifecycle.wal_retention"
    );
    assert_eq!(shell.branch_state().facts(), before);
}

#[test]
fn flush_watermark_persist_failure_preserves_source_chain() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x26);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(5, CommitVersion::new(7))
        .expect("snapshot facts");
    backend.fail_manifest_replacement_on_call(2);

    let error = persist_flush_watermark(
        shell.services().manifest(),
        CommitVersion::new(7),
        CommitVersion::new(5),
        &LifecycleFlushWatermarkProof::CheckpointCovered {
            snapshot_watermark: CommitVersion::new(7),
        },
    )
    .expect_err("persist failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("current database record");
    assert_eq!(manifest.flushed_through_commit_id(), None);
}

#[test]
fn wal_truncation_task_uses_strongest_manifest_retention_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x18);
    let shell = assemble_shell(branch, backend).expect("shell");
    shell
        .services()
        .manifest()
        .persist_snapshot_facts(2, CommitVersion::new(4))
        .expect("snapshot facts");
    shell
        .services()
        .manifest()
        .persist_flush_watermark(CommitVersion::new(6))
        .expect("flush facts");
    let task = maintenance_task_for_test(1, MaintenanceTaskRequest::wal_truncation());

    let confirmed = shell
        .services()
        .manifest_gate()
        .confirmed_manifest()
        .expect("confirm manifest");
    let request = wal_truncation_request_from_maintenance_task(&task, &confirmed)
        .expect("request")
        .expect("proof");

    assert_eq!(request.covered_through(), CommitVersion::new(6));
    assert_eq!(request.source(), WalRetentionProofSource::FlushWatermark);
}

#[test]
fn wal_truncation_from_checkpoint_and_flush_proofs_are_typed() {
    let snapshot = WalRetentionProof::snapshot_watermark(CommitVersion::new(3));
    let flush = WalRetentionProof::flush_watermark(CommitVersion::new(4));

    assert_eq!(snapshot.covered_through(), CommitVersion::new(3));
    assert_eq!(
        snapshot.source(),
        WalRetentionProofSource::SnapshotWatermark
    );
    assert_eq!(flush.covered_through(), CommitVersion::new(4));
    assert_eq!(flush.source(), WalRetentionProofSource::FlushWatermark);
}

#[test]
fn queued_checkpoint_task_runs_through_maintenance_executor() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x19);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"queued-checkpoint", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue");

    let maintenance = runtime
        .run_next_checkpoint_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::Checkpoint);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(runtime.maintenance_status().stats().completed(), 1);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("current database record")
            .snapshot_id(),
        Some(1)
    );
    assert_eq!(
        backend.snapshot_created_at(),
        vec![Timestamp::from_micros(9_000)]
    );
}

#[test]
fn duplicate_checkpoint_tasks_coalesce_by_checkpoint_scope() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x27);
    let mut runtime = open_runtime(branch, backend);

    let first = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("first enqueue");
    let second = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("second enqueue");

    assert!(first.was_enqueued());
    assert!(second.was_coalesced());
    assert_eq!(runtime.maintenance_status().stats().coalesced(), 1);
}

#[test]
fn queued_checkpoint_task_failure_adds_health_debt() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x28);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"queued-failure", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(2);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue");

    let maintenance = runtime
        .run_next_checkpoint_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert!(maintenance.recovery_health().is_some());
    assert!(maintenance.retryable());
    assert_eq!(runtime.maintenance_status().stats().failed(), 1);
}

#[test]
fn queued_checkpoint_retry_advances_after_orphaned_snapshot() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x29);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"queued-orphan", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_replacement_on_call(2);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue first");

    let first = runtime
        .run_next_checkpoint_maintenance()
        .expect("run first")
        .expect("first maintenance");
    assert_eq!(first.status(), MaintenanceOutcomeStatus::Failed);

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::checkpoint())
        .expect("enqueue second");
    let second = runtime
        .run_next_checkpoint_maintenance()
        .expect("run second")
        .expect("second maintenance");

    assert_eq!(second.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("database record")
            .snapshot_id(),
        Some(2)
    );
}

#[test]
fn queued_wal_truncation_task_defers_without_retention_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x1a);
    let mut runtime = open_runtime(branch, backend);
    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue");

    let maintenance = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run")
        .expect("maintenance");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::WalTruncation);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(runtime.maintenance_status().stats().deferred(), 1);
}

#[test]
fn wal_truncation_no_longer_waits_for_queued_flush_watermark_work() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x2a);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            CommitVersion::new(2),
        ))
        .expect("enqueue watermark");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue truncation");

    // Truncation is no longer gated by a queued flush-watermark task (that gate stalled WAL
    // reclaim under sustained pressure). It runs against the current persisted retention
    // watermark; with none set yet it defers, leaving the flush-watermark task for its turn.
    let maintenance = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run truncation")
        .expect("truncation maintenance");

    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::WalTruncation);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
    assert_eq!(runtime.maintenance_status().stats().deferred(), 1);
}

#[test]
fn duplicate_wal_truncation_tasks_coalesce_by_retention_scope() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x29);
    let mut runtime = open_runtime(branch, backend);

    let first = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("first enqueue");
    let second = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("second enqueue");

    assert!(first.was_enqueued());
    assert!(second.was_coalesced());
    assert_eq!(runtime.maintenance_status().stats().coalesced(), 1);
}

#[test]
fn wal_truncation_request_rejects_zero_proof() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let shell = assemble_shell(branch_id(0x29), backend).expect("shell");
    assert_eq!(
        truncate_wal(
            shell.services().wal(),
            WalRetentionProof::snapshot_watermark(CommitVersion::ZERO),
        ),
        Err(LifecycleError::WalRetentionProofIncomplete {
            reason: "WAL retention proof must be nonzero",
        })
    );
    assert_eq!(
        truncate_wal(
            shell.services().wal(),
            WalRetentionProof::flush_watermark(CommitVersion::ZERO),
        ),
        Err(LifecycleError::WalRetentionProofIncomplete {
            reason: "WAL retention proof must be nonzero",
        })
    );
}

/// #3643: the listed timeline segments sealed by snapshot `sealing`.
fn segments_sealed_by(listed: &[ObjectName], sealing: u64) -> Vec<ObjectName> {
    listed
        .iter()
        .filter(|name| {
            ObjectLayout::classify_timeline_segment_object(name)
                .expect("classify")
                .is_some_and(|id| id.sealing_snapshot_id == sealing)
        })
        .cloned()
        .collect()
}

/// #3643: a completed checkpoint records its references as the live set, so
/// the superseded prune that follows needs no read of the new snapshot to know
/// what it may delete. With that snapshot unreadable, the prune still reclaims
/// the tail the previous snapshot sealed.
#[test]
fn a_completed_checkpoint_records_its_live_segments_for_the_prune() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x73);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, value) in [(1, b"one"), (2, b"two")] {
        runtime
            .execute_durable_commit(durable_batch(branch, b"live", value), generation_guard())
            .expect("commit");
        let request = LifecycleCheckpointRequest::new(
            branch,
            snapshot_id,
            Timestamp::from_micros(20 + snapshot_id),
        )
        .expect("request");
        assert_eq!(
            runtime.checkpoint(&request).expect("checkpoint").status(),
            LifecycleCheckpointStatus::Completed
        );
    }
    let prefix = ObjectLayout::timeline_prefix().expect("prefix");
    let before = backend.list_prefix(&prefix).expect("list");
    assert!(
        !segments_sealed_by(&before, 1).is_empty(),
        "the first checkpoint sealed a tail"
    );
    backend.fail_object_on_next_read(ObjectLayout::snapshot(2).expect("snapshot object"));
    let mut pruned = false;
    for _ in 0..8 {
        match runtime
            .run_next_retention_maintenance()
            .expect("retention maintenance")
        {
            Some(outcome) if outcome.task_kind() == MaintenanceTaskKind::SnapshotPruning => {
                pruned = true;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(
        pruned,
        "the completed checkpoint chained a superseded prune"
    );
    let after = backend.list_prefix(&prefix).expect("list");
    assert!(
        segments_sealed_by(&after, 1).is_empty(),
        "the prune knew the live set without reading the snapshot: {after:?}"
    );
    assert!(!segments_sealed_by(&after, 2).is_empty());
}

/// #3643: under strict recovery a snapshot that references a missing timeline
/// segment is durable corruption: refused at the segment, not recorded as a
/// fault and refused later as merely degraded health.
#[test]
fn strict_recovery_refuses_a_missing_timeline_segment_as_corruption() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x72);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"strict", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(23)).expect("request");
    assert_eq!(
        runtime.checkpoint(&request).expect("checkpoint").status(),
        LifecycleCheckpointStatus::Completed
    );
    drop(runtime);
    let prefix = ObjectLayout::timeline_prefix().expect("prefix");
    let segments = backend.list_prefix(&prefix).expect("list");
    assert_eq!(segments.len(), 1, "{segments:?}");
    backend.delete_object(&segments[0]).expect("delete segment");

    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    assert_eq!(request.strictness(), RecoveryStrictness::Strict);
    let error = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect_err("strict recovery refuses the missing segment");
    assert_eq!(error.code(), "corruption.lifecycle.recovery_corruption");
}

/// #3671: checkpoint 1 completes durably; the store is reopened (its open
/// reconcile queued); a row lands only in the WAL; checkpoint 2's final
/// manifest replacement becomes visible, but its durability is unconfirmed for
/// the next `unconfirmed_publishes` manifest publishes.
fn second_checkpoint_with_unconfirmed_manifest(
    backend: &'static CheckpointTestBackend,
    branch: BranchId,
    unconfirmed_publishes: usize,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut first = open_runtime(branch, backend);
    first
        .execute_durable_commit(
            durable_batch(branch, b"durable-old", b"v"),
            generation_guard(),
        )
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(23)).expect("request");
    assert_eq!(
        first.checkpoint(&request).expect("checkpoint 1").status(),
        LifecycleCheckpointStatus::Completed
    );
    drop(first);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"durable-new", b"v"),
            generation_guard(),
        )
        .expect("commit");
    backend.fail_manifest_durability(2, unconfirmed_publishes);
    let request =
        LifecycleCheckpointRequest::new(branch, 2, Timestamp::from_micros(24)).expect("request");
    assert_eq!(
        runtime.checkpoint(&request).expect("checkpoint 2").status(),
        LifecycleCheckpointStatus::SnapshotVisibilityUncertain
    );
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("visible manifest")
            .snapshot_id(),
        Some(2),
        "the replacement is visible"
    );
    runtime
}

/// Drains the queued retention work (the open's reconcile) and reports
/// whether a snapshot prune ran.
fn drain_snapshot_pruning(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
) -> bool {
    let mut pruned = false;
    for _ in 0..8 {
        match runtime
            .run_next_retention_maintenance()
            .expect("retention maintenance")
        {
            Some(outcome) if outcome.task_kind() == MaintenanceTaskKind::SnapshotPruning => {
                pruned = true;
            }
            Some(_) => {}
            None => break,
        }
    }
    pruned
}

/// Strict recovery must succeed and every acknowledged row must read back.
fn assert_strict_recovery_keeps_both_rows(
    backend: &'static CheckpointTestBackend,
    branch: BranchId,
) {
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    assert_eq!(request.strictness(), RecoveryStrictness::Strict);
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("strict recovery after the crash");
    let runtime = shell.complete_recovery(&outcome).expect("open runtime");
    for key in [&b"durable-old"[..], &b"durable-new"[..]] {
        assert!(
            runtime
                .read_view_for_branch(branch)
                .expect("read view")
                .latest(&physical_key(branch, key))
                .expect("read")
                .is_some(),
            "{} survived",
            String::from_utf8_lossy(key)
        );
    }
}

/// #3671 (third review P1): a manifest replacement that is visible but whose
/// durability stays unconfirmed may be undone by power loss. Until a publish
/// confirms it, reclaim must keep both possible recovery states: the open's
/// reconcile deletes neither the previous snapshot nor its timeline segments,
/// and after the crash restores the previous manifest, strict recovery
/// succeeds with every row.
#[test]
fn an_unconfirmed_manifest_keeps_the_previous_checkpoint_through_reclaim_and_a_crash() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x74);
    let mut runtime = second_checkpoint_with_unconfirmed_manifest(backend, branch, usize::MAX);
    let prefix = ObjectLayout::timeline_prefix().expect("prefix");
    let first_snapshot = ObjectLayout::snapshot(1).expect("snapshot 1");
    assert!(backend.read_object(&first_snapshot).is_ok());
    assert!(!segments_sealed_by(&backend.list_prefix(&prefix).expect("list"), 1).is_empty());

    assert!(drain_snapshot_pruning(&mut runtime), "the reconcile ran");
    assert!(
        backend.read_object(&first_snapshot).is_ok(),
        "the previous checkpoint is a possible recovery state"
    );
    assert!(
        !segments_sealed_by(&backend.list_prefix(&prefix).expect("list"), 1).is_empty(),
        "its timeline segments too"
    );

    drop(runtime);
    backend.crash_to_durable_manifest();
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .snapshot_id(),
        Some(1),
        "power loss undid the unconfirmed replacement"
    );
    assert_strict_recovery_keeps_both_rows(backend, branch);
}

/// #3671: WAL truncation reads the same watermark. Behind an unconfirmed
/// manifest it defers: the rows checkpoint 2 covered are still the previous
/// manifest's WAL tail, and a crash that restores that manifest replays them.
#[test]
fn wal_truncation_waits_for_the_manifest_to_be_confirmed_durable() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x75);
    let mut runtime = second_checkpoint_with_unconfirmed_manifest(backend, branch, usize::MAX);
    let wal = ObjectLayout::wal_prefix().expect("wal prefix");
    let before = backend.list_prefix(&wal).expect("list");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue");
    let outcome = runtime
        .run_next_wal_truncation_maintenance()
        .expect("truncation")
        .expect("a truncation task ran");
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert!(
        outcome.source_error().is_some(),
        "the deferral reports the backend fault that left the manifest unconfirmed"
    );
    assert_eq!(backend.list_prefix(&wal).expect("list"), before);

    drop(runtime);
    backend.crash_to_durable_manifest();
    assert_strict_recovery_keeps_both_rows(backend, branch);
}

/// #3671: the protection lasts only until durability is confirmed. Once a
/// publish of the visible manifest succeeds (the confirming re-publish here),
/// the previous checkpoint is no longer a recovery state: the reconcile
/// reclaims it, and a crash keeps the confirmed manifest.
#[test]
fn a_confirmed_manifest_releases_the_previous_checkpoint_to_reclaim() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x76);
    let mut runtime = second_checkpoint_with_unconfirmed_manifest(backend, branch, 1);
    let prefix = ObjectLayout::timeline_prefix().expect("prefix");

    assert!(drain_snapshot_pruning(&mut runtime), "the reconcile ran");
    assert!(
        backend
            .read_object(&ObjectLayout::snapshot(1).expect("snapshot 1"))
            .is_err(),
        "the confirmed manifest supersedes checkpoint 1"
    );
    assert!(segments_sealed_by(&backend.list_prefix(&prefix).expect("list"), 1).is_empty());

    drop(runtime);
    backend.crash_to_durable_manifest();
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("manifest")
            .snapshot_id(),
        Some(2),
        "the confirmed replacement survives power loss"
    );
    assert_strict_recovery_keeps_both_rows(backend, branch);
}

/// #3671: the gate confirms a manifest durable once per session — the first
/// reclaim of an open re-publishes it (a previous process's last rename may
/// never have reached disk) — and afterwards trusts its record until the
/// manifest changes, so steady-state reclaim costs no extra publish.
#[test]
fn the_manifest_gate_confirms_once_until_the_manifest_changes() {
    use crate::lifecycle::retention::ConfirmedManifest;
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x77);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(durable_batch(branch, b"gate", b"v"), generation_guard())
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(23)).expect("request");
    assert_eq!(
        runtime.checkpoint(&request).expect("checkpoint").status(),
        LifecycleCheckpointStatus::Completed
    );
    drop(runtime);

    let shell = assemble_shell(branch, backend).expect("shell");
    let gate = shell.services().manifest_gate();
    let before = backend.manifest_replace_calls();
    let first = gate.confirmed_manifest().expect("confirm");
    assert!(
        matches!(&first, ConfirmedManifest::Confirmed(Some(manifest)) if manifest.snapshot_id() == Some(1)),
        "{first:?}"
    );
    assert_eq!(
        backend.manifest_replace_calls(),
        before + 1,
        "a new session confirms by re-publishing"
    );
    assert_eq!(gate.confirmed_manifest().expect("again"), first);
    assert_eq!(
        backend.manifest_replace_calls(),
        before + 1,
        "an unchanged confirmed manifest is not re-published"
    );

    drop(shell);

    // A completed checkpoint's final publish is itself the confirmation.
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(durable_batch(branch, b"gate-2", b"v"), generation_guard())
        .expect("commit");
    let request =
        LifecycleCheckpointRequest::new(branch, 2, Timestamp::from_micros(24)).expect("request");
    assert_eq!(
        runtime.checkpoint(&request).expect("checkpoint 2").status(),
        LifecycleCheckpointStatus::Completed
    );
    let after_checkpoint = backend.manifest_replace_calls();
    assert!(
        drain_snapshot_pruning(&mut runtime),
        "the checkpoint's chained prune ran"
    );
    assert_eq!(
        backend.manifest_replace_calls(),
        after_checkpoint,
        "the checkpoint's own publish confirmed the manifest"
    );
    drop(runtime);

    backend.fail_manifest_durability(2, usize::MAX);
    let fresh = assemble_shell(branch, backend).expect("second shell");
    let unconfirmed = fresh
        .services()
        .manifest_gate()
        .confirmed_manifest()
        .expect("gate");
    assert!(
        matches!(unconfirmed, ConfirmedManifest::Unconfirmed(_)),
        "a confirmation that cannot complete leaves the manifest unconfirmed: {unconfirmed:?}"
    );
}

/// #3643 (re-review P1): a checkpoint whose manifest publication became
/// visible but was reported uncertain leaves the cached segment references
/// describing the OLD snapshot. The open's queued `ReconcileToAttested` then
/// reads the NEW manifest; it must establish the new snapshot's references
/// (not trust the stale cache), keep every segment the new snapshot
/// references, and a strict reopen must still find them all.
#[test]
fn an_uncertain_but_visible_checkpoint_keeps_its_segments_through_the_open_reconcile() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0x71);
    // Establish a live checkpoint, then reopen with its reconcile task still pending.
    let mut first = open_runtime(branch, backend);
    first
        .execute_durable_commit(durable_batch(branch, b"review", b"old"), generation_guard())
        .expect("initial commit");
    let initial =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(23)).expect("request");
    assert_eq!(
        first
            .checkpoint(&initial)
            .expect("initial checkpoint")
            .status(),
        LifecycleCheckpointStatus::Completed
    );
    drop(first);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"review", b"value"),
            generation_guard(),
        )
        .expect("commit");
    backend
        .manifest_visible_then_uncertain
        .store(true, Ordering::SeqCst);
    let request =
        LifecycleCheckpointRequest::new(branch, 2, Timestamp::from_micros(24)).expect("request");
    let outcome = runtime.checkpoint(&request).expect("uncertain outcome");
    assert_eq!(
        outcome.status(),
        LifecycleCheckpointStatus::SnapshotVisibilityUncertain
    );
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("visible manifest");
    assert_eq!(
        manifest.snapshot_id(),
        Some(2),
        "fault happened after visibility"
    );
    let prefix = ObjectLayout::timeline_prefix().expect("prefix");
    let before = backend.list_prefix(&prefix).expect("list");
    assert!(!before.is_empty(), "checkpoint wrote timeline segments");
    // Drain the actual retention/reconcile work queued by reopen.
    let mut reconciled = false;
    for _ in 0..4 {
        match runtime
            .run_next_retention_maintenance()
            .expect("queued retention")
        {
            Some(outcome) if outcome.task_kind() == MaintenanceTaskKind::SnapshotPruning => {
                reconciled = true;
                break;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(reconciled, "the open's queued reconcile ran");
    let expected_live = segments_sealed_by(&before, 2);
    assert!(
        !expected_live.is_empty(),
        "checkpoint 2 wrote its new history"
    );
    for object in &expected_live {
        assert!(
            backend.read_object(object).is_ok(),
            "reconcile used stale cached refs and deleted manifest-live segment {object:?}"
        );
    }
    // ...and it did establish them rather than give up: the tail the first
    // snapshot sealed, which the new snapshot no longer references, is gone.
    assert!(
        !segments_sealed_by(&before, 1).is_empty(),
        "the first checkpoint sealed a tail"
    );
    let after = backend.list_prefix(&prefix).expect("list");
    assert!(
        segments_sealed_by(&after, 1).is_empty(),
        "the reconcile reclaimed the superseded tail: {after:?}"
    );
    // Strict recovery loads every segment the attested snapshot references.
    drop(runtime);
    let _reopened = open_runtime(branch, backend);
}
