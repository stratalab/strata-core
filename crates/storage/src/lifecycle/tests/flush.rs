use super::*;
use crate::backend::{
    Backend, BackendAppend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata,
    BackendRange, BackendResult, BackendWriterGuard, PublishDurability, PublishError,
    PublishFailureKind, PublishMode, PublishOutcome, PublishResult,
    DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::facts::BranchTableDescriptor;
use crate::branch::read::{BranchOwnedTable, BranchRowSource};
use crate::branch::state::BranchLocalState;
use crate::commit::{
    CommitBatch, CommitBatchOptions, CommitBranchGeneration, CommitBranchGenerationGuard,
    CommitDurabilityMode, CommitExpiry, CommitManualTimestampSource, CommitMutation, CommitOrigin,
    CommitRetentionHint, CommitRuntimeConfig, CommitValidationFacts,
};
use crate::lifecycle::flush::{
    flush_branch_drain_with, flush_cache_branch, flush_durable_branch,
    install_prepared_durable_flush, prepare_durable_flush_with_budget, FlushDrainRequest,
};
use crate::lifecycle::flush::{flush_zone_cut_keys, prepare_durable_flush_with_cuts_for_test};
use crate::object::{ObjectName, ObjectPrefix};
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId};
use crate::service::{
    DatabaseManifestService, TableManifestService, TableObjectReaderService, TableObjectService,
    WalServiceConfig,
};
use crate::table::{
    sort_table_rows_by_key, ImmutableTableBuilder, ImmutableTableReader, TableBuilderConfig,
    TableIdentity, TableReaderConfig, TableRow,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use strata_core::{BranchId, CommitVersion, Timestamp};

const FLUSH_DATABASE_ID: [u8; 16] = [0x4f; 16];

#[test]
fn flush_request_validates_components_and_target_level() {
    let branch = branch_id(0x61);
    let seed = FlushTableIdentitySeed::new("flush-seed").expect("seed");
    let object_id = FlushTableObjectId::new("flush-object").expect("object id");

    let request =
        FlushFrozenRequest::new(branch, Some(3), seed.clone(), object_id.clone()).expect("request");
    assert_eq!(request.branch_id(), branch);
    assert_eq!(request.frozen_index(), Some(3));
    assert_eq!(request.table_identity_seed(), &seed);
    assert_eq!(request.table_object_id(), &object_id);

    assert_eq!(
        FlushTableIdentitySeed::new(""),
        Err(LifecycleError::InvalidConfig {
            field: "table identity seed",
            reason: "flush component must not be empty",
        })
    );
    assert_eq!(
        FlushTableObjectId::new("bad/component"),
        Err(LifecycleError::InvalidConfig {
            field: "table object id",
            reason: "flush component must be a single object component",
        })
    );
    assert_eq!(
        FlushFrozenRequest::new_for_level(
            branch,
            None,
            seed,
            object_id,
            crate::branch::facts::BranchLevel::new(1),
        ),
        Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush target level must be zero",
        })
    );
}

#[test]
fn flush_named_frozen_index_must_exist() {
    let branch = branch_id(0x69);
    let mut state = frozen_branch(branch, put_row(branch, b"indexed", 9, 9_000, b"value"));

    let error = flush_cache_branch(&mut state, &flush_request(branch, Some(1)))
        .expect_err("missing frozen index");

    assert_eq!(
        error.code(),
        "failed_precondition.lifecycle.maintenance_task"
    );
    assert_eq!(state.frozen_table_count(), 1);
    assert_eq!(state.owned_table_count(), 0);
}

#[test]
fn flush_without_frozen_state_is_deferred() {
    let branch = branch_id(0x62);
    let mut state = BranchLocalState::empty(branch);
    let request = flush_request(branch, None);

    let outcome = flush_cache_branch(&mut state, &request).expect("flush outcome");

    assert!(outcome.deferred_no_frozen_state());
    assert_eq!(outcome.branch_id(), branch);
    assert_eq!(outcome.frozen_index(), None);
    assert_eq!(outcome.rows_flushed(), 0);
    assert!(outcome.table_identity().is_none());
    assert!(outcome.table_facts().is_none());
    assert!(outcome.failure().is_none());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Deferred
    );
    assert!(state.is_empty());
}

#[test]
fn cache_flush_replaces_oldest_frozen_table_and_preserves_reads() {
    let branch = branch_id(0x63);
    let mut state = BranchLocalState::empty(branch);
    let older_key = physical_key(branch, b"older");
    let newer_key = physical_key(branch, b"newer");
    let older = put_row(branch, b"older", 1, 1_000, b"older-value");
    let newer = put_row(branch, b"newer", 2, 2_000, b"newer-value");

    state
        .append_committed_row(older.clone())
        .expect("append older");
    state.rotate_active();
    state
        .append_committed_row(newer.clone())
        .expect("append newer");
    state.rotate_active();

    let outcome = flush_cache_branch(&mut state, &flush_request(branch, None)).expect("flush");

    assert!(outcome.completed());
    assert_eq!(outcome.frozen_index(), Some(1));
    assert_eq!(outcome.rows_flushed(), 1);
    assert_eq!(outcome.table_facts().expect("table facts").row_count(), 1);
    assert!(outcome.table_object().is_none());
    assert_eq!(outcome.maintenance_outcome().affected_objects(), 0);
    assert_eq!(state.frozen_table_count(), 1);
    assert_eq!(state.owned_table_count(), 1);

    let view = state.capture_read_view().expect("view");
    let older_visible = view
        .latest(&older_key)
        .expect("older read")
        .expect("older visible");
    assert_eq!(older_visible.row(), &older);
    assert_eq!(
        older_visible.source(),
        BranchRowSource::OwnedTable {
            level: crate::branch::facts::BranchLevel::ZERO,
            table_index: 0,
        }
    );
    let newer_visible = view
        .latest(&newer_key)
        .expect("newer read")
        .expect("newer visible");
    assert_eq!(newer_visible.row(), &newer);
    assert_eq!(newer_visible.source(), BranchRowSource::Frozen { index: 0 });
}

#[test]
fn cache_flush_drain_flushes_all_currently_frozen_tables() {
    let branch = branch_id(0x7a);
    let mut state = multi_frozen_branch(
        branch,
        [
            put_row(branch, b"drain-first", 1, 1_000, b"first"),
            put_row(branch, b"drain-second", 2, 2_000, b"second"),
            put_row(branch, b"drain-third", 3, 3_000, b"third"),
        ],
    );

    let outcome = flush_branch_drain_with(
        &mut state,
        &flush_drain_request(branch),
        |branch, request| Ok(flush_cache_branch(branch, request)?.maintenance_outcome()),
    )
    .expect("flush drain");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(outcome.branch_id(), branch);
    assert_eq!(outcome.frozen_tables_discovered(), 3);
    assert_eq!(outcome.completed_flushes(), 3);
    assert_eq!(outcome.deferred_flushes(), 0);
    assert_eq!(outcome.failed_flushes(), 0);
    assert_eq!(outcome.skipped_flushes(), 0);
    assert_eq!(outcome.post_drain_frozen_tables(), 0);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(maintenance.stats().maintenance_tasks(), 3);
    assert_eq!(state.frozen_table_count(), 0);
    assert_eq!(state.owned_levels()[0].len(), 3);
}

#[test]
fn cache_flush_drain_without_frozen_state_is_skipped() {
    let branch = branch_id(0x7b);
    let mut state = BranchLocalState::empty(branch);

    let outcome = flush_branch_drain_with(
        &mut state,
        &flush_drain_request(branch),
        |branch, request| Ok(flush_cache_branch(branch, request)?.maintenance_outcome()),
    )
    .expect("flush drain");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(outcome.branch_id(), branch);
    assert_eq!(outcome.frozen_tables_discovered(), 0);
    assert_eq!(outcome.completed_flushes(), 0);
    assert_eq!(outcome.skipped_flushes(), 1);
    assert_eq!(outcome.post_drain_frozen_tables(), 0);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(
        maintenance.reason(),
        Some("flush drain has no frozen state to publish")
    );
}

#[test]
fn cache_flush_drain_retries_when_new_frozen_state_appears_during_drain() {
    let branch = branch_id(0x7c);
    let mut state = frozen_branch(branch, put_row(branch, b"drain-seed", 1, 1_000, b"seed"));
    let mut injected = false;
    let request = flush_drain_request(branch).with_freeze_during_drain_retry_limit(1);

    let outcome = flush_branch_drain_with(&mut state, &request, |branch, request| {
        let outcome = flush_cache_branch(branch, request)?.maintenance_outcome();
        if !injected {
            injected = true;
            branch
                .append_committed_row(put_row(
                    branch.branch_id(),
                    b"drain-injected",
                    2,
                    2_000,
                    b"new",
                ))
                .expect("append injected row");
            branch.rotate_active();
        }
        Ok(outcome)
    })
    .expect("flush drain");

    assert_eq!(outcome.frozen_tables_discovered(), 1);
    assert_eq!(outcome.completed_flushes(), 2);
    assert_eq!(outcome.freeze_during_drain_retries(), 1);
    assert_eq!(outcome.post_drain_frozen_tables(), 0);
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Completed
    );
    assert_eq!(state.frozen_table_count(), 0);
    assert_eq!(state.owned_levels()[0].len(), 2);
}

#[test]
fn cache_flush_drain_defers_when_freeze_retry_limit_is_exhausted() {
    let branch = branch_id(0x7d);
    let mut state = frozen_branch(branch, put_row(branch, b"drain-limit", 1, 1_000, b"seed"));
    let request = flush_drain_request(branch).with_freeze_during_drain_retry_limit(0);

    let outcome = flush_branch_drain_with(&mut state, &request, |branch, request| {
        let outcome = flush_cache_branch(branch, request)?.maintenance_outcome();
        branch
            .append_committed_row(put_row(
                branch.branch_id(),
                b"drain-limit-new",
                2,
                2_000,
                b"new",
            ))
            .expect("append injected row");
        branch.rotate_active();
        Ok(outcome)
    })
    .expect("flush drain");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(outcome.frozen_tables_discovered(), 1);
    assert_eq!(outcome.completed_flushes(), 1);
    assert_eq!(outcome.freeze_during_drain_retries(), 0);
    assert_eq!(outcome.deferred_flushes(), 1);
    assert_eq!(outcome.post_drain_frozen_tables(), 1);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Deferred);
    assert!(maintenance.recovery_health().is_some());
    assert_eq!(
        maintenance.reason(),
        Some("flush drain left deferred frozen state")
    );
    assert_eq!(state.frozen_table_count(), 1);
    assert_eq!(state.owned_levels()[0].len(), 1);
}

#[test]
fn cache_flush_replaces_named_table_and_keeps_other_frozen_order() {
    let branch = branch_id(0x6a);
    let mut state = BranchLocalState::empty(branch);
    let first = put_row(branch, b"first", 1, 1_000, b"first-value");
    let second = put_row(branch, b"second", 2, 2_000, b"second-value");
    let third = put_row(branch, b"third", 3, 3_000, b"third-value");
    for row in [&first, &second, &third] {
        state.append_committed_row(row.clone()).expect("append row");
        state.rotate_active();
    }

    let outcome = flush_cache_branch(&mut state, &flush_request(branch, Some(1))).expect("flush");

    assert!(outcome.completed());
    assert_eq!(outcome.frozen_index(), Some(1));
    assert_eq!(state.frozen_table_count(), 2);
    assert_eq!(state.owned_table_count(), 1);
    let view = state.capture_read_view().expect("view");
    assert_eq!(
        view.latest(&physical_key(branch, b"second"))
            .expect("second read")
            .expect("second visible")
            .source(),
        BranchRowSource::OwnedTable {
            level: crate::branch::facts::BranchLevel::ZERO,
            table_index: 0,
        }
    );
    assert_eq!(
        view.latest(&physical_key(branch, b"third"))
            .expect("third read")
            .expect("third visible")
            .source(),
        BranchRowSource::Frozen { index: 0 }
    );
    assert_eq!(
        view.latest(&physical_key(branch, b"first"))
            .expect("first read")
            .expect("first visible")
            .source(),
        BranchRowSource::Frozen { index: 1 }
    );
}

#[test]
fn cache_flush_preserves_tombstones_and_commit_timestamps() {
    let branch = branch_id(0x6b);
    let key = physical_key(branch, b"deleted");
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, b"deleted", 1, 1_000, b"live"))
        .expect("append live row");
    state.rotate_active();
    state
        .append_committed_row(tombstone_row(branch, b"deleted", 2, 9_000))
        .expect("append tombstone");
    state.rotate_active();

    let outcome = flush_cache_branch(&mut state, &flush_request(branch, Some(0))).expect("flush");

    assert!(outcome.completed());
    assert_eq!(
        outcome.table_facts().expect("facts").commit_range().max(),
        CommitVersion::new(2)
    );
    assert_eq!(
        state.facts().expect("state facts").timestamp_max(),
        Some(Timestamp::from_micros(9_000))
    );
    let view = state.capture_read_view().expect("view");
    assert_eq!(view.latest(&key).expect("latest"), None);
    assert_eq!(
        view.at_version(&key, CommitVersion::new(1))
            .expect("version read")
            .expect("old row")
            .row()
            .value(),
        b"live"
    );
}

#[test]
fn cache_flush_install_failure_leaves_frozen_state_unchanged() {
    let branch = branch_id(0x7b);
    let row = put_row(branch, b"duplicate", 4, 4_000, b"value");
    let request = flush_request(branch, None);
    let identity = flush_cache_branch(&mut frozen_branch(branch, row.clone()), &request)
        .expect("identity flush")
        .table_identity()
        .expect("identity")
        .clone();
    let mut state = BranchLocalState::empty(branch);
    let duplicate = owned_table_for_row(
        branch,
        identity,
        put_row(branch, b"existing", 3, 3_000, b"existing"),
    );
    state
        .install_l0_table(duplicate)
        .expect("install existing table");
    state.append_committed_row(row).expect("append frozen row");
    state.rotate_active();
    let before = state.clone();

    let outcome = flush_cache_branch(&mut state, &request).expect("flush");

    assert!(outcome.failed_before_publication());
    assert!(outcome.failure().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Failed
    );
    assert_eq!(
        outcome
            .maintenance_outcome()
            .source_error()
            .expect("source error")
            .code(),
        "failed_precondition.lifecycle.branch_runtime"
    );
    assert_eq!(state, before);
}

#[test]
fn repeated_default_flush_after_success_is_deferred() {
    let branch = branch_id(0x6c);
    let mut state = frozen_branch(branch, put_row(branch, b"repeat", 10, 10_000, b"value"));
    let request = flush_request(branch, None);

    let completed = flush_cache_branch(&mut state, &request).expect("first flush");
    let deferred = flush_cache_branch(&mut state, &request).expect("second flush");

    assert!(completed.completed());
    assert!(deferred.deferred_no_frozen_state());
    assert_eq!(state.frozen_table_count(), 0);
    assert_eq!(state.owned_table_count(), 1);
}

#[test]
fn cache_runtime_flushes_explicitly_rotated_state_only() {
    let branch = branch_id(0x64);
    let backend = crate::backend::memory::MemoryBackend::new();
    let mut runtime = LifecycleCacheRuntime::open(
        cache_open_request(branch),
        &backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(4_000)),
    )
    .expect("cache runtime");
    let key = physical_key(branch, b"runtime");

    runtime
        .execute_cache_commit(
            put_batch(branch, key.clone(), b"runtime-value".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");

    let deferred = runtime
        .flush_frozen(&flush_request(branch, None))
        .expect("deferred flush");
    assert!(deferred.deferred_no_frozen_state());
    assert_eq!(runtime.branch_state().active_row_count(), 1);
    assert_eq!(runtime.branch_state().frozen_table_count(), 0);

    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    let flushed = runtime
        .flush_frozen(&flush_request(branch, None))
        .expect("flush");
    assert!(flushed.completed());
    assert_eq!(runtime.branch_state().active_row_count(), 0);
    assert_eq!(runtime.branch_state().frozen_table_count(), 0);
    assert_eq!(runtime.branch_state().owned_table_count(), 1);

    let visible = runtime
        .read_view()
        .expect("view")
        .latest(&key)
        .expect("read")
        .expect("visible row");
    assert_eq!(visible.row().value(), b"runtime-value");
}

#[test]
fn queued_cache_flush_task_runs_through_executor() {
    let branch = branch_id(0x75);
    let backend = crate::backend::memory::MemoryBackend::new();
    let mut runtime = LifecycleCacheRuntime::open(
        cache_open_request(branch),
        &backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(4_000)),
    )
    .expect("cache runtime");
    let key = physical_key(branch, b"queued-cache");

    runtime
        .execute_cache_commit(
            put_batch(branch, key.clone(), b"queued-cache-value".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");

    let maintenance = runtime
        .run_next_flush_maintenance()
        .expect("run flush")
        .expect("maintenance outcome");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(maintenance.affected_objects(), 0);
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
    assert_eq!(runtime.maintenance_status().stats().completed(), 1);
    assert_eq!(runtime.branch_state().frozen_table_count(), 0);
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert_eq!(
        runtime
            .read_view()
            .expect("view")
            .latest(&key)
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"queued-cache-value"
    );
}

#[test]
fn duplicate_flush_task_coalesces_by_branch() {
    let branch = branch_id(0x7b);
    let backend = crate::backend::memory::MemoryBackend::new();
    let mut runtime = LifecycleCacheRuntime::open(
        cache_open_request(branch),
        &backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(4_000)),
    )
    .expect("cache runtime");

    let first = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("first flush task");
    let second = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("second flush task");

    assert!(first.was_enqueued());
    assert!(second.was_coalesced());
    assert_eq!(second.task_id(), first.task_id());
    assert_eq!(runtime.maintenance_status().pending_tasks(), 1);
}

#[test]
fn flush_task_canceled_before_start_does_not_build_or_publish() {
    let branch = branch_id(0x7c);
    let backend = crate::backend::memory::MemoryBackend::new();
    let mut runtime = LifecycleCacheRuntime::open(
        cache_open_request(branch),
        &backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(4_000)),
    )
    .expect("cache runtime");
    runtime
        .execute_cache_commit(
            put_batch(
                branch,
                physical_key(branch, b"cancel-flush"),
                b"value".to_vec(),
            ),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");

    let close = runtime.close().expect("close cancels ordinary work");

    assert_eq!(close.stats().maintenance_tasks(), 1);
    assert_eq!(runtime.branch_state().frozen_table_count(), 1);
    assert_eq!(runtime.branch_state().owned_table_count(), 0);
}

#[test]
fn flush_task_rejected_after_close_requested() {
    let branch = branch_id(0x7d);
    let backend = crate::backend::memory::MemoryBackend::new();
    let mut runtime = LifecycleCacheRuntime::open(
        cache_open_request(branch),
        &backend,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        CommitManualTimestampSource::new(Timestamp::from_micros(4_000)),
    )
    .expect("cache runtime");
    runtime.close().expect("close runtime");

    let error = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect_err("closed runtime rejects flush task");

    assert_eq!(error.code(), "failed_precondition.lifecycle.state");
    assert_eq!(runtime.maintenance_status().pending_tasks(), 0);
}

#[test]
fn flush_task_failure_adds_health_debt() {
    let branch = branch_id(0x7e);
    let mut executor = LifecycleMaintenanceExecutor::new(1).expect("executor");
    executor
        .enqueue(open_state(), MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let mut runner = FailedFlushRunner;

    let outcome = executor
        .run_next(open_state(), &mut runner)
        .expect("run failed flush")
        .expect("flush outcome");

    assert_eq!(outcome.task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Failed);
    assert!(outcome.recovery_health().is_some());
    assert_eq!(executor.stats().failed(), 1);
}

#[test]
fn durable_flush_publishes_reopens_and_installs_table() {
    let branch = branch_id(0x65);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut state = frozen_branch(branch, put_row(branch, b"durable", 5, 5_000, b"stored"));
    let before = state.clone();
    let request = flush_request(branch, None);

    let outcome = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("durable flush");

    assert!(outcome.completed());
    assert_eq!(outcome.rows_flushed(), 1);
    assert!(outcome.table_object().is_some());
    assert!(outcome.object_facts().is_some());
    assert!(outcome.install_outcome().is_some());
    assert_eq!(outcome.maintenance_outcome().affected_objects(), 1);
    assert!(!outcome.maintenance_outcome().retryable());
    assert_eq!(state.frozen_table_count(), 0);
    assert_eq!(state.owned_table_count(), 1);

    let object = outcome.table_object().expect("object").clone();
    let expected_bytes = build_bytes_from_frozen(
        outcome.table_identity().expect("identity").clone(),
        &before.frozen()[0],
    );
    assert_eq!(backend.object_bytes(&object), Some(expected_bytes));
    assert_operation_order(
        &backend.operations(),
        FlushOperationKind::Publish,
        FlushOperationKind::Metadata,
    );
    assert_operation_order(
        &backend.operations(),
        FlushOperationKind::Metadata,
        FlushOperationKind::Range,
    );
    assert!(backend.operations().iter().all(|operation| matches!(
        operation.kind(),
        FlushOperationKind::Publish | FlushOperationKind::Metadata | FlushOperationKind::Range
    )));
    let view = state.capture_read_view().expect("view");
    assert_eq!(
        view.latest(&physical_key(branch, b"durable"))
            .expect("read")
            .expect("row")
            .row()
            .value(),
        b"stored"
    );
}

#[test]
fn queued_durable_flush_task_publishes_object_through_executor() {
    let branch = branch_id(0x76);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    let mut runtime = shell.complete_recovery(&recovery).expect("open runtime");
    let key = physical_key(branch, b"queued-durable");

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, key.clone(), b"queued-durable-value".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");
    let enqueue = runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");

    let maintenance = runtime
        .run_next_flush_maintenance()
        .expect("run flush")
        .expect("maintenance outcome");

    assert_eq!(maintenance.task_id(), Some(enqueue.task_id()));
    assert_eq!(maintenance.task_kind(), MaintenanceTaskKind::Flush);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Completed);
    assert_eq!(maintenance.affected_objects(), 1);
    // The completed flush triggers flush-driven WAL reclaim, which enqueues a (coalescing)
    // flush-watermark task — its coverage scan runs off-lock (BS5.3; the previous inline
    // scan-and-persist held the runtime lock through a durable manifest fsync per flush) —
    // plus the (coalescing) WAL-truncation task: two pending tasks.
    assert_eq!(runtime.maintenance_status().pending_tasks(), 2);
    assert_eq!(runtime.maintenance_status().stats().completed(), 1);
    assert_eq!(runtime.branch_state().frozen_table_count(), 0);
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    assert!(backend
        .operations()
        .iter()
        .any(|operation| operation.kind() == FlushOperationKind::Publish));
    assert_eq!(
        runtime
            .read_view()
            .expect("view")
            .latest(&key)
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"queued-durable-value"
    );
}

#[test]
fn durable_flush_does_not_persist_watermark_or_truncate_log() {
    let branch = branch_id(0x78);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    let mut runtime = shell.complete_recovery(&recovery).expect("open runtime");
    runtime
        .execute_durable_commit(
            durable_put_batch(
                branch,
                physical_key(branch, b"watermark"),
                b"value".to_vec(),
            ),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");

    let outcome = runtime
        .flush_frozen(&flush_request(branch, None))
        .expect("flush");

    assert!(outcome.completed());
    assert_eq!(
        outcome.table_facts().expect("facts").commit_range().max(),
        CommitVersion::new(1)
    );
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.flushed_through_commit_id(), None);
    assert_eq!(manifest.snapshot_watermark(), None);
}

#[test]
fn durable_flush_publishes_table_manifest_after_table_install() {
    let branch = branch_id(0x85);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut runtime = open_durable_runtime(backend, branch);
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, physical_key(branch, b"manifest"), b"value".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");

    let outcome = runtime
        .flush_frozen(&flush_request(branch, None))
        .expect("flush");

    assert!(outcome.completed());
    assert_eq!(runtime.branch_state().owned_table_count(), 1);
    let manifest = TableManifestService::new(backend)
        .load_required(branch)
        .expect("table manifest");
    assert_eq!(manifest.levels().len(), 1);
    assert_eq!(
        manifest.levels()[0].level(),
        crate::branch::facts::BranchLevel::ZERO
    );
    assert_eq!(manifest.levels()[0].tables().len(), 1);
    let table = &manifest.levels()[0].tables()[0];
    assert_eq!(
        table.table_identity(),
        outcome.table_identity().expect("flushed table identity")
    );
    assert_eq!(
        table.object(),
        outcome.table_object().expect("flushed table object")
    );
}

#[test]
fn durable_flush_manifest_preserves_existing_reachable_tables() {
    let branch = branch_id(0x86);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut runtime = open_durable_runtime(backend, branch);
    for (key, value) in [
        (b"manifest-first".as_slice(), b"first".as_slice()),
        (b"manifest-second".as_slice(), b"second".as_slice()),
    ] {
        runtime
            .execute_durable_commit(
                durable_put_batch(branch, physical_key(branch, key), value.to_vec()),
                CommitBranchGenerationGuard::exact(
                    CommitBranchGeneration::new(1).expect("generation"),
                ),
            )
            .expect("commit");
        runtime
            .rotate_active_for_maintenance()
            .expect("rotate active");
        runtime
            .flush_frozen(&flush_request(branch, None))
            .expect("flush");
    }

    let manifest = TableManifestService::new(backend)
        .load_required(branch)
        .expect("table manifest");

    assert_eq!(manifest.levels().len(), 1);
    assert_eq!(manifest.levels()[0].tables().len(), 2);
    assert_eq!(runtime.branch_state().owned_table_count(), 2);
}

#[test]
fn durable_flush_manifest_publish_failure_keeps_rows_visible_and_records_debt() {
    let branch = branch_id(0x87);
    let backend: &'static FlushBackend =
        crate::testkit::leak_static(FlushBackend::with_table_manifest_publish_failure(
            PublishFailureKind::FailedBeforeVisibility,
        ));
    let mut runtime = open_durable_runtime(backend, branch);
    let key = physical_key(branch, b"manifest-fail");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, key.clone(), b"value".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");

    let outcome = runtime
        .flush_frozen(&flush_request(branch, None))
        .expect("flush remains visible despite manifest debt");

    assert!(outcome.completed());
    assert_eq!(
        runtime
            .read_view()
            .expect("view")
            .latest(&key)
            .expect("read")
            .expect("visible")
            .row()
            .value(),
        b"value"
    );
    assert_eq!(runtime.current_recovery_health().fault_count(), 1);
    assert!(TableManifestService::new(backend)
        .load_current(branch)
        .expect("load table manifest")
        .is_none());
}

#[test]
fn durable_flush_drain_manifest_publish_failure_records_partial_progress_health_debt() {
    let branch = branch_id(0x89);
    let backend: &'static FlushBackend =
        crate::testkit::leak_static(FlushBackend::with_table_manifest_publish_failure(
            PublishFailureKind::FailedBeforeVisibility,
        ));
    let mut runtime = open_durable_runtime(backend, branch);
    let first_key = physical_key(branch, b"drain-manifest-fail-first");
    let second_key = physical_key(branch, b"drain-manifest-fail-second");

    runtime
        .execute_durable_commit(
            durable_put_batch(branch, first_key.clone(), b"first".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("first commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate first active");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, second_key.clone(), b"second".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("second commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate second active");
    assert_eq!(runtime.branch_state().frozen_table_count(), 2);

    let outcome = runtime
        .run_next_flush_maintenance()
        .expect("flush maintenance")
        .expect("flush outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Deferred);
    assert_eq!(outcome.stats().maintenance_tasks(), 2);
    assert_eq!(
        outcome.reason(),
        Some("flush drain left deferred frozen state")
    );
    assert!(outcome.source_error().is_some());
    assert!(outcome.recovery_health().is_some());
    assert_eq!(runtime.current_recovery_health().fault_count(), 1);
    assert_eq!(runtime.branch_state().owned_levels()[0].len(), 1);
    assert_eq!(runtime.branch_state().frozen_table_count(), 1);
    assert!(TableManifestService::new(backend)
        .load_current(branch)
        .expect("load table manifest")
        .is_none());

    let view = runtime.read_view().expect("read view");
    assert_eq!(
        view.latest(&first_key)
            .expect("first read")
            .expect("first visible")
            .row()
            .value(),
        b"first"
    );
    assert_eq!(
        view.latest(&second_key)
            .expect("second read")
            .expect("second visible")
            .row()
            .value(),
        b"second"
    );
}

#[test]
fn durable_flush_manifest_publish_uncertain_reports_uncertainty() {
    let branch = branch_id(0x88);
    let backend: &'static FlushBackend = crate::testkit::leak_static(
        FlushBackend::with_table_manifest_publish_failure(PublishFailureKind::VisibilityUnknown),
    );
    let mut runtime = open_durable_runtime(backend, branch);
    runtime
        .execute_durable_commit(
            durable_put_batch(
                branch,
                physical_key(branch, b"manifest-uncertain"),
                b"value".to_vec(),
            ),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue flush");
    let outcome = runtime
        .run_next_flush_maintenance()
        .expect("flush maintenance")
        .expect("maintenance outcome");

    assert_eq!(outcome.status(), MaintenanceOutcomeStatus::Completed);
    assert!(outcome.source_error().is_some());
    assert_eq!(
        outcome.source_error().expect("source error").code(),
        "ambiguous_commit.lifecycle.table_manifest_publication"
    );
    assert!(outcome.recovery_health().is_some());
}

#[test]
fn cache_flush_does_not_publish_table_manifest() {
    let branch = branch_id(0x89);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut state = frozen_branch(
        branch,
        put_row(branch, b"cache-manifest", 30, 30_000, b"value"),
    );

    let outcome = flush_cache_branch(&mut state, &flush_request(branch, None)).expect("flush");

    assert!(outcome.completed());
    assert!(TableManifestService::new(backend)
        .load_current(branch)
        .expect("load table manifest")
        .is_none());
    assert!(backend.operations().is_empty());
}

#[test]
fn wal_retention_proof_is_not_constructed_by_flush() {
    let branch = branch_id(0x81);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    let mut runtime = shell.complete_recovery(&recovery).expect("open runtime");
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, physical_key(branch, b"proof"), b"value".to_vec()),
            CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation")),
        )
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate active");

    let outcome = runtime
        .flush_frozen(&flush_request(branch, None))
        .expect("flush");

    assert!(outcome.completed());
    assert!(!outcome.maintenance_outcome().checkpoint_required());
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.flushed_through_commit_id(), None);
    assert_eq!(manifest.snapshot_watermark(), None);
}

#[test]
fn successful_flush_reports_candidate_commit_max() {
    let branch = branch_id(0x82);
    let mut state = BranchLocalState::empty(branch);
    state
        .append_committed_row(put_row(branch, b"candidate-a", 2, 2_000, b"a"))
        .expect("append first");
    state
        .append_committed_row(put_row(branch, b"candidate-b", 7, 7_000, b"b"))
        .expect("append second");
    state.rotate_active();

    let outcome = flush_cache_branch(&mut state, &flush_request(branch, None)).expect("flush");

    assert!(outcome.completed());
    assert_eq!(
        outcome.table_facts().expect("facts").commit_range().max(),
        CommitVersion::new(7)
    );
}

#[test]
fn failed_flush_for_wrong_branch_does_not_persist_watermark() {
    let branch = branch_id(0x79);
    let other = branch_id(0x7a);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    let mut runtime = shell.complete_recovery(&recovery).expect("open runtime");

    let error = runtime
        .flush_frozen(&flush_request(other, None))
        .expect_err("wrong branch rejects");

    // The handler now routes by `request.branch_id()`; an unknown branch
    // fails at the registry lookup (commit runtime layer) rather than at
    // the maintenance task validator that used to gate the prior single-
    // branch implementation.
    assert_eq!(error.code(), "failed_precondition.lifecycle.commit_runtime");
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.flushed_through_commit_id(), None);
    assert_eq!(manifest.snapshot_watermark(), None);
}

#[test]
fn branch_absence_does_not_advance_flush_watermark() {
    let branch = branch_id(0x83);
    let absent = branch_id(0x84);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    let mut runtime = shell.complete_recovery(&recovery).expect("open runtime");
    let operations_before = backend.operations();

    let error = runtime
        .flush_frozen(&flush_request(absent, None))
        .expect_err("absent branch rejects");

    // Per-branch routing surfaces an absent branch as a registry lookup
    // failure (commit runtime layer) rather than a maintenance task
    // validator rejection on the seeded branch.
    assert_eq!(error.code(), "failed_precondition.lifecycle.commit_runtime");
    assert_eq!(backend.operations(), operations_before);
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("manifest");
    assert_eq!(manifest.flushed_through_commit_id(), None);
    assert_eq!(manifest.snapshot_watermark(), None);
}

#[test]
fn durable_publish_failure_leaves_frozen_state_unchanged() {
    let branch = branch_id(0x66);
    let backend: &'static FlushBackend = crate::testkit::leak_static(
        FlushBackend::with_publish_failure(PublishFailureKind::FailedBeforeVisibility),
    );
    let mut state = frozen_branch(branch, put_row(branch, b"failure", 6, 6_000, b"value"));
    let before = state.clone();

    let error = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &flush_request(branch, None),
    )
    .expect_err("publish failure");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    assert_eq!(state, before);
}

#[test]
fn durable_reopen_failure_reports_published_not_installed() {
    let branch = branch_id(0x68);
    let backend: &'static FlushBackend =
        crate::testkit::leak_static(FlushBackend::with_range_failure());
    let mut state = frozen_branch(branch, put_row(branch, b"partial", 8, 8_000, b"value"));
    let before = state.clone();

    let outcome = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &flush_request(branch, None),
    )
    .expect("partial outcome");

    assert!(outcome.published_not_installed());
    assert_eq!(outcome.rows_flushed(), 1);
    assert!(outcome.table_object().is_some());
    assert!(outcome.object_facts().is_some());
    assert!(outcome.failure().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Failed
    );
    assert!(outcome.maintenance_outcome().retryable());
    assert_eq!(outcome.maintenance_outcome().affected_objects(), 1);
    assert_eq!(
        outcome.maintenance_outcome().affected_object_names(),
        &[outcome.table_object().expect("object").as_str().to_owned()]
    );
    assert_eq!(
        outcome
            .maintenance_outcome()
            .source_error()
            .expect("source error")
            .code(),
        "ambiguous_commit.lifecycle.flush_publication_orphan"
    );
    assert!(outcome
        .maintenance_outcome()
        .source_error()
        .expect("source error")
        .source()
        .is_some());
    assert_eq!(state, before);
}

#[test]
fn durable_publish_visibility_uncertainty_is_typed() {
    let branch = branch_id(0x77);
    let backend: &'static FlushBackend = crate::testkit::leak_static(
        FlushBackend::with_publish_failure(PublishFailureKind::VisibilityUnknown),
    );
    let mut state = frozen_branch(branch, put_row(branch, b"uncertain", 21, 21_000, b"value"));

    let error = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &flush_request(branch, None),
    )
    .expect_err("publish uncertainty");

    assert_eq!(error.code(), "ambiguous_commit.lifecycle.flush_publication");
    assert!(error.source().is_some());
    assert_eq!(state.frozen_table_count(), 1);
    assert_eq!(state.owned_table_count(), 0);
}

#[test]
fn durable_invalid_publish_metadata_preserves_service_source() {
    let branch = branch_id(0x6d);
    let backend: &'static FlushBackend =
        crate::testkit::leak_static(FlushBackend::with_invalid_publish_metadata());
    let mut state = frozen_branch(branch, put_row(branch, b"metadata", 11, 11_000, b"value"));
    let before = state.clone();

    let error = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &flush_request(branch, None),
    )
    .expect_err("invalid metadata");

    assert_eq!(error.code(), "failed_precondition.lifecycle.service");
    assert!(error.source().is_some());
    assert_eq!(state, before);
}

#[test]
fn durable_reopen_wrong_branch_table_reports_partial_publication() {
    let branch = branch_id(0x6e);
    let other = branch_id(0x6f);
    let original = put_row(branch, b"wrong-branch", 12, 12_000, b"value");
    let replacement = put_row(other, b"wrong-branch", 12, 12_000, b"value");
    let replacement_bytes = built_bytes_for_row("replacement-table", replacement);
    let backend: &'static FlushBackend =
        crate::testkit::leak_static(FlushBackend::with_replacement_bytes(replacement_bytes));
    let mut state = frozen_branch(branch, original);
    let before = state.clone();

    let outcome = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &flush_request(branch, None),
    )
    .expect("partial outcome");

    assert!(outcome.published_not_installed());
    assert!(outcome.failure().is_some());
    assert_eq!(
        outcome.maintenance_outcome().status(),
        MaintenanceOutcomeStatus::Failed
    );
    assert_eq!(state, before);
}

#[test]
fn durable_install_failure_reports_orphaned_object_fact() {
    let branch = branch_id(0x70);
    let row = put_row(branch, b"install", 13, 13_000, b"value");
    let request = flush_request(branch, None);
    let identity = flush_cache_branch(&mut frozen_branch(branch, row.clone()), &request)
        .expect("identity flush")
        .table_identity()
        .expect("identity")
        .clone();
    let mut state = frozen_branch(branch, row);
    let collision = owned_table_for_row(
        branch,
        identity,
        put_row(branch, b"collision", 14, 14_000, b"value"),
    );
    state
        .install_owned_table_at_level(crate::branch::facts::BranchLevel::ZERO, collision)
        .expect("collision table");
    let before = state.clone();
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());

    let outcome = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("partial outcome");

    assert!(outcome.published_not_installed());
    assert!(outcome.object_facts().is_some());
    assert!(outcome.table_object().is_some());
    let failure = outcome.failure().expect("orphan failure");
    assert_eq!(
        failure.code(),
        "ambiguous_commit.lifecycle.flush_publication_orphan"
    );
    assert!(failure.source().is_some());
    assert_eq!(state, before);
}

#[test]
fn existing_conflicting_object_fails_closed_without_removing_frozen_rows() {
    let branch = branch_id(0x71);
    let other = branch_id(0x72);
    let row = put_row(branch, b"conflict", 15, 15_000, b"value");
    let request = flush_request(branch, None);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut first = frozen_branch(branch, row.clone());
    let first_outcome = flush_durable_branch(
        &mut first,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("first flush");
    let object = first_outcome.table_object().expect("object").clone();
    let replacement = built_bytes_for_row(
        "conflicting-existing",
        put_row(other, b"conflict", 15, 15_000, b"value"),
    );
    assert_eq!(
        replacement.len(),
        backend.object_bytes(&object).expect("existing bytes").len()
    );
    backend.replace_object(&object, replacement);
    let mut retry = frozen_branch(branch, row);
    let before_retry = retry.clone();

    let outcome = flush_durable_branch(
        &mut retry,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("conflict outcome");

    assert!(outcome.published_not_installed());
    assert!(outcome.failure().is_some());
    assert!(!outcome.maintenance_outcome().retryable());
    assert_eq!(retry, before_retry);
}

#[test]
fn durable_flush_retries_existing_matching_object() {
    let branch = branch_id(0x67);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let row = put_row(branch, b"retry", 7, 7_000, b"value");
    let request = flush_request(branch, None);
    let mut first = frozen_branch(branch, row.clone());
    let mut second = frozen_branch(branch, row);

    let first_outcome = flush_durable_branch(
        &mut first,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("first flush");
    let first_object = first_outcome.table_object().expect("object").clone();

    let retry_outcome = flush_durable_branch(
        &mut second,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("retry flush");

    assert!(retry_outcome.completed());
    assert_eq!(retry_outcome.table_object(), Some(&first_object));
    assert_eq!(second.frozen_table_count(), 0);
    assert_eq!(second.owned_table_count(), 1);
}

#[test]
fn adopted_flush_output_vanishing_mid_build_is_a_sweep_race_not_a_failure() {
    let branch = branch_id(0x74);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let row = put_row(branch, b"swept-adopt", 9, 9_000, b"value");
    let request = flush_request(branch, None);
    // An abandoned earlier attempt: the object is published, the frozen state
    // on the real branch still queued (the throwaway state stands in for the
    // attempt whose install never landed).
    let mut abandoned = frozen_branch(branch, row.clone());
    let first_outcome = flush_durable_branch(
        &mut abandoned,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("first flush");
    let object = first_outcome.table_object().expect("object").clone();
    // The retry adopts the existing content-identical object; a concurrent
    // table-object sweep — for which the abandoned object is legitimate
    // garbage — unlinks it between the adoption and the build's read of it.
    backend.vanish_object_on_next_read(object);
    let mut retry = frozen_branch(branch, row);
    let before = retry.clone();

    let error = flush_durable_branch(
        &mut retry,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect_err("adopted object swept mid-build is the benign sweep race");

    assert!(
        error.is_rewrite_output_sweep_race(),
        "expected the sweep-race classification, got: {error:?}"
    );
    assert_eq!(
        error.code(),
        "unavailable.lifecycle.rewrite_output_sweep_race"
    );
    assert_eq!(retry, before);
}

#[test]
fn fresh_flush_output_vanishing_mid_build_still_fails_closed() {
    let branch = branch_id(0x75);
    let row = put_row(branch, b"fresh-vanish", 11, 11_000, b"value");
    let request = flush_request(branch, None);
    // Learn the deterministic object name from a scout run on a separate
    // backend (identical content derives an identical name).
    let scout_backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut scout = frozen_branch(branch, row.clone());
    let scout_outcome = flush_durable_branch(
        &mut scout,
        &TableObjectService::new(scout_backend),
        &TableObjectReaderService::new(scout_backend),
        &request,
    )
    .expect("scout flush");
    let object = scout_outcome.table_object().expect("object").clone();
    // A FRESH publish whose object disappears before the build's read is not
    // the adoption race — nothing else may touch a reserved fresh output, so
    // this keeps failing closed.
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    backend.vanish_object_on_next_read(object);
    let mut state = frozen_branch(branch, row);

    let outcome = flush_durable_branch(
        &mut state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("fresh vanish outcome");

    assert!(outcome.published_not_installed());
    assert_eq!(
        outcome
            .maintenance_outcome()
            .source_error()
            .expect("source error")
            .code(),
        "ambiguous_commit.lifecycle.flush_publication_orphan"
    );
}

#[test]
fn flush_identity_is_deterministic_and_tracks_commit_facts() {
    let branch = branch_id(0x73);
    let request = flush_request(branch, None);
    let row = put_row(branch, b"identity", 16, 16_000, b"value");
    let first = flush_cache_branch(&mut frozen_branch(branch, row.clone()), &request)
        .expect("first flush")
        .table_identity()
        .expect("first identity")
        .clone();
    let second = flush_cache_branch(&mut frozen_branch(branch, row), &request)
        .expect("second flush")
        .table_identity()
        .expect("second identity")
        .clone();
    let changed_commit = flush_cache_branch(
        &mut frozen_branch(branch, put_row(branch, b"identity", 17, 17_000, b"value")),
        &request,
    )
    .expect("changed commit")
    .table_identity()
    .expect("changed identity")
    .clone();
    let changed_value = flush_cache_branch(
        &mut frozen_branch(branch, put_row(branch, b"identity", 16, 16_000, b"other")),
        &request,
    )
    .expect("changed value")
    .table_identity()
    .expect("value identity")
    .clone();
    let other_branch = branch_id(0x74);
    let changed_branch = flush_cache_branch(
        &mut frozen_branch(
            other_branch,
            put_row(other_branch, b"identity", 16, 16_000, b"value"),
        ),
        &flush_request(other_branch, None),
    )
    .expect("changed branch")
    .table_identity()
    .expect("branch identity")
    .clone();

    let changed_key = flush_cache_branch(
        &mut frozen_branch(
            branch,
            put_row(branch, b"other-identity", 16, 16_000, b"value"),
        ),
        &request,
    )
    .expect("changed key")
    .table_identity()
    .expect("key identity")
    .clone();

    assert_eq!(first, second);
    assert_eq!(first, changed_value);
    assert_ne!(first, changed_commit);
    assert_ne!(first, changed_branch);
    // BS4 provenance fix: two frozen tables that share a row count and commit range but cover
    // different keys must get distinct identities. Otherwise the derived flush object id collides and
    // `publish_or_load_existing` loads the first table's stale object, whose rows would not match the
    // second table's summary (the extras/rows provenance mismatch the branch-owned oracle catches).
    assert_ne!(
        first, changed_key,
        "different key spans must yield different flush identities"
    );
    // The identity is the structured facts (`{seed}-{branch}-frozen-{rows}-{min}-{max}`) plus a
    // 16-hex key-span digest suffix.
    let expected_prefix = format!("flush-seed-{branch}-frozen-1-16-16-");
    assert!(
        first.as_str().starts_with(&expected_prefix),
        "identity {} must start with {expected_prefix}",
        first.as_str()
    );
    let digest = &first.as_str()[expected_prefix.len()..];
    assert_eq!(
        digest.len(),
        16,
        "key-span digest must be 16 hex chars: {digest}"
    );
    assert!(
        digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "key-span digest must be hex: {digest}"
    );
    assert!(!first.as_str().contains('/'));
}

#[test]
fn flush_object_identity_is_stable_when_frozen_position_changes() {
    let branch = branch_id(0x75);
    let target = put_row(branch, b"stable-position", 18, 18_000, b"value");
    let newer = put_row(branch, b"newer-frozen", 19, 19_000, b"newer");
    let request = flush_request(branch, None);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shifted = BranchLocalState::empty(branch);
    shifted
        .append_committed_row(target.clone())
        .expect("append target");
    shifted.rotate_active();
    shifted
        .append_committed_row(newer)
        .expect("append newer frozen");
    shifted.rotate_active();
    assert_eq!(shifted.frozen_table_count(), 2);
    let mut unshifted = frozen_branch(branch, target);

    let first = flush_durable_branch(
        &mut shifted,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("first flush");
    let second = flush_durable_branch(
        &mut unshifted,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("second flush");

    assert_eq!(first.frozen_index(), Some(1));
    assert_eq!(second.frozen_index(), Some(0));
    assert_eq!(first.table_identity(), second.table_identity());
    assert_eq!(first.table_object(), second.table_object());
}

fn flush_request(branch: BranchId, frozen_index: Option<usize>) -> FlushFrozenRequest {
    FlushFrozenRequest::new(
        branch,
        frozen_index,
        FlushTableIdentitySeed::new("flush-seed").expect("seed"),
        FlushTableObjectId::new("flush-object").expect("object id"),
    )
    .expect("flush request")
}

fn flush_drain_request(branch: BranchId) -> FlushDrainRequest {
    FlushDrainRequest::new(
        branch,
        FlushTableIdentitySeed::new("flush-drain-seed").expect("seed"),
        FlushTableObjectId::new("flush-drain-object").expect("object id"),
    )
}

fn cache_open_request(branch: BranchId) -> LifecycleCacheOpenRequest {
    LifecycleCacheOpenRequest::new(
        StorageOpenPlan::new(
            StorageMode::Cache,
            LifecycleCodecId::identity(),
            RecoveryStrictness::Strict,
            LifecycleConfig::default(),
        )
        .expect("open plan"),
        branch,
        CommitBranchGeneration::new(1).expect("generation"),
    )
    .expect("cache request")
}

fn durable_open_request(branch: BranchId) -> LifecycleDurableLocalOpenRequest {
    LifecycleDurableLocalOpenRequest::new(
        StorageOpenPlan::new(
            StorageMode::DurableLocalStandard,
            LifecycleCodecId::identity(),
            RecoveryStrictness::Strict,
            LifecycleConfig::default(),
        )
        .expect("open plan"),
        FLUSH_DATABASE_ID,
        branch,
        CommitBranchGeneration::new(1).expect("generation"),
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        WalServiceConfig::default(),
    )
    .expect("durable request")
}

fn open_durable_runtime(
    backend: &'static FlushBackend,
    branch: BranchId,
) -> LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource> {
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    shell.complete_recovery(&recovery).expect("open runtime")
}

fn put_batch(branch: BranchId, key: PhysicalKey, value: Vec<u8>) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            key,
            value,
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::new(
            CommitDurabilityMode::Cache,
            crate::commit::CommitConflictValidationMode::Skip,
            crate::commit::CommitDuplicateKeyPolicy::Reject,
            crate::commit::CommitTimestampPolicy::Explicit(Timestamp::from_micros(4_000)),
            CommitOrigin::StorageRuntime,
        ),
    )
}

fn durable_put_batch(branch: BranchId, key: PhysicalKey, value: Vec<u8>) -> CommitBatch {
    CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            key,
            value,
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::new(
            CommitDurabilityMode::Standard,
            crate::commit::CommitConflictValidationMode::Skip,
            crate::commit::CommitDuplicateKeyPolicy::Reject,
            crate::commit::CommitTimestampPolicy::Explicit(Timestamp::from_micros(9_000)),
            CommitOrigin::StorageRuntime,
        ),
    )
}

#[test]
fn prepared_durable_flush_installs_by_identity_after_frozen_set_shifts() {
    // BS5.3b: the install matches its frozen table by sealed-memtable identity
    // (O(1)), not by index or row comparison — so a freeze that lands between
    // the off-lock build and the install (the pipelined publish gap) must not
    // confuse it: the prepared flush replaces ITS OWN input, and the newer
    // frozen table stays frozen.
    let branch = branch_id(0x67);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut live = frozen_branch(branch, put_row(branch, b"first", 5, 5_000, b"first-value"));
    let request = flush_request(branch, None);

    // Off-lock build against a snapshot clone (shares the sealed memtable Arc).
    let snapshot = live.clone();
    let prepared = prepare_durable_flush_with_budget(
        &snapshot,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
        None,
        None,
        None,
    )
    .expect("prepare flush")
    .expect("prepared flush");

    // The live branch moves on before the install: another row freezes.
    live.append_committed_row(put_row(branch, b"second", 6, 6_000, b"second-value"))
        .expect("append second row");
    live.rotate_active();
    assert_eq!(live.frozen_table_count(), 2);

    let outcome = install_prepared_durable_flush(&mut live, prepared);

    assert!(outcome.completed());
    assert_eq!(
        live.frozen_table_count(),
        1,
        "the newer freeze stays frozen"
    );
    assert_eq!(
        live.owned_table_count(),
        1,
        "the built table installed at L0"
    );
}

#[test]
fn prepared_durable_flush_fails_closed_when_its_frozen_table_is_gone() {
    // BS5.3b fail-closed arm: if the prepared flush's input frozen table is no
    // longer present at install time (flushed by another path), the install
    // must refuse rather than replace some other frozen table.
    let branch = branch_id(0x68);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut live = frozen_branch(branch, put_row(branch, b"only", 5, 5_000, b"only-value"));
    let request = flush_request(branch, None);

    let snapshot = live.clone();
    let prepared = prepare_durable_flush_with_budget(
        &snapshot,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
        None,
        None,
        None,
    )
    .expect("prepare flush")
    .expect("prepared flush");

    // The live branch flushes the same frozen table through another path.
    let competing = flush_durable_branch(
        &mut live,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
    )
    .expect("competing flush");
    assert!(competing.completed());
    assert_eq!(live.frozen_table_count(), 0);

    let outcome = install_prepared_durable_flush(&mut live, prepared);

    assert!(
        !outcome.completed(),
        "stale prepared flush must not install"
    );
    assert_eq!(live.frozen_table_count(), 0);
    assert_eq!(
        live.owned_table_count(),
        1,
        "only the competing flush's table is installed"
    );
}

fn frozen_branch(branch: BranchId, row: StorageRow) -> BranchLocalState {
    let mut state = BranchLocalState::empty(branch);
    state.append_committed_row(row).expect("append row");
    state.rotate_active();
    state
}

fn multi_frozen_branch<const N: usize>(
    branch: BranchId,
    rows: [StorageRow; N],
) -> BranchLocalState {
    let mut state = BranchLocalState::empty(branch);
    for row in rows {
        state.append_committed_row(row).expect("append row");
        state.rotate_active();
    }
    state
}

fn put_row(
    branch: BranchId,
    user_key: &[u8],
    version: u64,
    timestamp: u64,
    value: &[u8],
) -> StorageRow {
    StorageRow::put(
        physical_key(branch, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
        Timestamp::EPOCH,
        value.to_vec(),
    )
}

fn tombstone_row(branch: BranchId, user_key: &[u8], version: u64, timestamp: u64) -> StorageRow {
    StorageRow::tombstone(
        physical_key(branch, user_key),
        CommitVersion::new(version),
        Timestamp::from_micros(timestamp),
    )
}

fn physical_key(branch: BranchId, user_key: &[u8]) -> PhysicalKey {
    PhysicalKey::new(
        branch,
        "flush",
        StorageSpaceId::engine(0x44).expect("engine space"),
        user_key.to_vec(),
    )
    .expect("physical key")
}

fn open_state() -> LifecycleStateMachine {
    let mut state = LifecycleStateMachine::new();
    state
        .transition(LifecycleTransitionTrigger::OpenRequested)
        .expect("open requested");
    state
        .transition(LifecycleTransitionTrigger::CacheOpenReady)
        .expect("cache open ready");
    state
}

fn branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; BranchId::BYTE_LEN])
}

fn build_bytes_from_frozen(identity: TableIdentity, frozen: &crate::table::FrozenTable) -> Vec<u8> {
    // W2.2: mirror the production flush build (bloom filter included).
    ImmutableTableBuilder::new(
        crate::lifecycle::compaction::lifecycle_table_builder_config(None).expect("builder config"),
    )
    .expect("builder")
    .build_from_frozen(identity, frozen)
    .expect("built table")
    .into_bytes()
}

fn built_bytes_for_row(identity: &str, row: StorageRow) -> Vec<u8> {
    let identity = TableIdentity::new(identity).expect("identity");
    let mut rows = vec![TableRow::new(row)];
    sort_table_rows_by_key(&mut rows);
    // W2.2: mirror the production flush build (bloom filter included).
    ImmutableTableBuilder::new(
        crate::lifecycle::compaction::lifecycle_table_builder_config(None).expect("builder config"),
    )
    .expect("builder")
    .build_from_rows(identity, &rows)
    .expect("built table")
    .into_bytes()
}

fn owned_table_for_row(
    branch: BranchId,
    identity: TableIdentity,
    row: StorageRow,
) -> BranchOwnedTable {
    let mut rows = vec![TableRow::new(row)];
    sort_table_rows_by_key(&mut rows);
    let artifact = ImmutableTableBuilder::new(TableBuilderConfig::default())
        .expect("builder")
        .build_from_rows(identity.clone(), &rows)
        .expect("built table");
    let reader = ImmutableTableReader::open_bytes(
        identity.clone(),
        artifact.into_bytes(),
        TableReaderConfig::default(),
    )
    .expect("reader");
    let descriptor = BranchTableDescriptor::new(
        identity,
        reader.facts().clone(),
        crate::branch::facts::BranchLevel::ZERO,
    )
    .expect("descriptor");
    let extras =
        crate::table::TableSummaryExtras::from_rows(reader.rows()).expect("table summary extras");
    BranchOwnedTable::new(branch, descriptor, reader, extras).expect("owned table")
}

fn assert_operation_order(
    operations: &[FlushOperation],
    first: FlushOperationKind,
    second: FlushOperationKind,
) {
    let first_index = operations
        .iter()
        .position(|operation| operation.kind() == first)
        .expect("first operation");
    let second_index = operations
        .iter()
        .position(|operation| operation.kind() == second)
        .expect("second operation");
    assert!(first_index < second_index);
}

#[derive(Debug)]
struct FlushBackend {
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    operations: Mutex<Vec<FlushOperation>>,
    lock_held: Arc<AtomicBool>,
    publish_failure: Option<PublishFailureKind>,
    /// A2 (#2524): fail table-object publishes AFTER this many succeed —
    /// the multi-output orphan arm (segment k fails with k-1 published).
    publish_failure_after: Option<usize>,
    table_manifest_publish_failure: Option<PublishFailureKind>,
    range_failure: bool,
    invalid_publish_metadata: bool,
    replacement_bytes: Option<Vec<u8>>,
    /// The next read (data or metadata) of this object deletes it and reports
    /// `NotFound` — a concurrent table-object sweep unlinking an orphan between
    /// a retry's adoption of it and the build's read of it.
    vanish_on_read: Mutex<Option<ObjectName>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FlushOperation {
    Range(ObjectName),
    Metadata(ObjectName),
    Publish(ObjectName, PublishMode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushOperationKind {
    Range,
    Metadata,
    Publish,
}

impl FlushOperation {
    const fn kind(&self) -> FlushOperationKind {
        match self {
            Self::Range(_) => FlushOperationKind::Range,
            Self::Metadata(_) => FlushOperationKind::Metadata,
            Self::Publish(_, _) => FlushOperationKind::Publish,
        }
    }
}

impl FlushBackend {
    fn new() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            operations: Mutex::new(Vec::new()),
            lock_held: Arc::new(AtomicBool::new(false)),
            publish_failure: None,
            publish_failure_after: None,
            table_manifest_publish_failure: None,
            range_failure: false,
            invalid_publish_metadata: false,
            replacement_bytes: None,
            vanish_on_read: Mutex::new(None),
        }
    }

    fn vanish_object_on_next_read(&self, object: ObjectName) {
        *self.vanish_on_read.lock().expect("vanish") = Some(object);
    }

    fn take_vanish(&self, name: &ObjectName) -> bool {
        let mut vanish = self.vanish_on_read.lock().expect("vanish");
        if vanish.as_ref() == Some(name) {
            *vanish = None;
            self.objects.lock().expect("objects").remove(name);
            return true;
        }
        false
    }

    fn with_publish_failure(kind: PublishFailureKind) -> Self {
        Self {
            publish_failure: Some(kind),
            ..Self::new()
        }
    }

    fn with_publish_failure_after(successes: usize) -> Self {
        Self {
            publish_failure_after: Some(successes),
            ..Self::new()
        }
    }

    fn with_table_manifest_publish_failure(kind: PublishFailureKind) -> Self {
        Self {
            table_manifest_publish_failure: Some(kind),
            ..Self::new()
        }
    }

    fn with_invalid_publish_metadata() -> Self {
        Self {
            invalid_publish_metadata: true,
            ..Self::new()
        }
    }

    fn with_range_failure() -> Self {
        Self {
            range_failure: true,
            ..Self::new()
        }
    }

    fn with_replacement_bytes(bytes: Vec<u8>) -> Self {
        Self {
            replacement_bytes: Some(bytes),
            ..Self::new()
        }
    }

    fn operations(&self) -> Vec<FlushOperation> {
        self.operations.lock().expect("operations").clone()
    }

    fn object_bytes(&self, object: &ObjectName) -> Option<Vec<u8>> {
        self.objects.lock().expect("objects").get(object).cloned()
    }

    fn replace_object(&self, object: &ObjectName, bytes: Vec<u8>) {
        self.objects
            .lock()
            .expect("objects")
            .insert(object.clone(), bytes);
    }

    fn record(&self, operation: FlushOperation) {
        self.operations.lock().expect("operations").push(operation);
    }
}

impl Backend for FlushBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::from_slice(DURABLE_LOCAL_MODE_REQUIREMENTS)
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        if self.take_vanish(name) {
            return Err(BackendError::new(
                BackendErrorKind::NotFound,
                "object not found",
            ));
        }
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.record(FlushOperation::Range(name.clone()));
        if self.range_failure {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "range read failed",
            ));
        }
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
        let mut names = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .filter(|name| name.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.record(FlushOperation::Metadata(name.clone()));
        if self.take_vanish(name) {
            return Err(BackendError::new(
                BackendErrorKind::NotFound,
                "object not found",
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
        if self.lock_held.swap(true, Ordering::SeqCst) {
            return Err(BackendError::new(
                BackendErrorKind::Unavailable,
                "writer lock already held",
            ));
        }
        Ok(BackendWriterGuard::new(
            name.clone(),
            FlushWriterLock {
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
        self.record(FlushOperation::Publish(name.clone(), mode));
        if is_table_manifest_object(name) {
            if let Some(kind) = self.table_manifest_publish_failure {
                return Err(PublishError::new(
                    name.clone(),
                    kind,
                    BackendError::new(BackendErrorKind::Unavailable, "table manifest failed"),
                ));
            }
        }
        if let Some(kind) = self.publish_failure {
            return Err(PublishError::new(
                name.clone(),
                kind,
                BackendError::new(BackendErrorKind::Unavailable, "publish failed"),
            ));
        }
        if let Some(successes) = self.publish_failure_after {
            let published = self.objects.lock().expect("objects").len();
            if published >= successes {
                return Err(PublishError::new(
                    name.clone(),
                    PublishFailureKind::FailedBeforeVisibility,
                    BackendError::new(BackendErrorKind::Unavailable, "publish failed"),
                ));
            }
        }
        let mut objects = self.objects.lock().expect("objects");
        if mode == PublishMode::Create && objects.contains_key(name) {
            return Err(PublishError::precondition_failed(
                name,
                "object already exists",
            ));
        }
        let stored = self.replacement_bytes.as_deref().unwrap_or(bytes);
        objects.insert(name.clone(), stored.to_vec());
        let metadata_size = if self.invalid_publish_metadata {
            bytes.len().saturating_add(1) as u64
        } else {
            stored.len() as u64
        };
        Ok(PublishOutcome::new(
            name.clone(),
            BackendMetadata::new(metadata_size, None),
            PublishDurability::Durable,
        ))
    }
}

fn is_table_manifest_object(name: &ObjectName) -> bool {
    name.as_str().starts_with("tables/") && name.as_str().ends_with("/manifest")
}

struct FlushWriterLock {
    locked: Arc<AtomicBool>,
}

impl Drop for FlushWriterLock {
    fn drop(&mut self) {
        self.locked.store(false, Ordering::SeqCst);
    }
}

struct FailedFlushRunner;

impl MaintenanceTaskRunner for FailedFlushRunner {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        assert_eq!(task.kind(), MaintenanceTaskKind::Flush);
        Ok(MaintenanceOutcome::new(
            MaintenanceTaskKind::Flush,
            MaintenanceOutcomeStatus::Failed,
        ))
    }
}

// ---- A2 (#2524): flush zone cuts ----

/// The encoded physical key of a test row — the comparison space the cut
/// policy and L1 span map share.
fn encoded_physical(branch: BranchId, user_key: &[u8]) -> Vec<u8> {
    crate::table::TableRow::new(put_row(branch, user_key, 1, 1, b"x"))
        .key()
        .physical_key_bytes()
        .to_vec()
}

fn one_frozen_table_branch(branch: BranchId, rows: Vec<StorageRow>) -> BranchLocalState {
    let mut state = BranchLocalState::empty(branch);
    for row in rows {
        state.append_committed_row(row).expect("append row");
    }
    state.rotate_active();
    state
}

#[test]
fn flush_zone_cut_keys_cut_where_whole_tables_are_skipped() {
    let branch = branch_id(0x80);
    let state = one_frozen_table_branch(
        branch,
        vec![
            put_row(branch, b"aa", 1, 100, b"low"),
            put_row(branch, b"zz", 2, 200, b"high"),
        ],
    );
    let frozen = &state.frozen()[0];
    let spans = vec![
        (
            encoded_physical(branch, b"bb"),
            encoded_physical(branch, b"cc"),
            40,
        ),
        (
            encoded_physical(branch, b"dd"),
            encoded_physical(branch, b"ee"),
            40,
        ),
    ];

    // Both tables sit entirely inside the (aa, zz) gap: 80 bytes skipped.
    assert_eq!(
        flush_zone_cut_keys(&spans, frozen, 64, 4),
        vec![encoded_physical(branch, b"zz")],
    );
    // Below the threshold: no cut.
    assert!(flush_zone_cut_keys(&spans, frozen, 81, 4).is_empty());
    // No L1 at all (cold start): no cut.
    assert!(flush_zone_cut_keys(&[], frozen, 1, 4).is_empty());
    // A single output slot leaves nothing to cut.
    assert!(flush_zone_cut_keys(&spans, frozen, 64, 1).is_empty());
}

#[test]
fn flush_zone_cut_keys_ignore_edge_straddling_tables() {
    let branch = branch_id(0x81);
    let state = one_frozen_table_branch(
        branch,
        vec![
            put_row(branch, b"gg", 1, 100, b"low"),
            put_row(branch, b"pp", 2, 200, b"high"),
        ],
    );
    let frozen = &state.frozen()[0];
    // Straddles the lower edge (first key sorts at/below "gg").
    let lower_straddler = vec![(
        encoded_physical(branch, b"aa"),
        encoded_physical(branch, b"hh"),
        1_000,
    )];
    assert!(flush_zone_cut_keys(&lower_straddler, frozen, 1, 4).is_empty());
    // Straddles the upper edge (last key sorts at/above "pp").
    let upper_straddler = vec![(
        encoded_physical(branch, b"hh"),
        encoded_physical(branch, b"zz"),
        1_000,
    )];
    assert!(flush_zone_cut_keys(&upper_straddler, frozen, 1, 4).is_empty());
}

#[test]
fn flush_zone_cut_keys_pick_the_largest_gaps_within_the_output_cap() {
    let branch = branch_id(0x82);
    let state = one_frozen_table_branch(
        branch,
        vec![
            put_row(branch, b"aa", 1, 100, b"a"),
            put_row(branch, b"gg", 2, 200, b"g"),
            put_row(branch, b"mm", 3, 300, b"m"),
            put_row(branch, b"tt", 4, 400, b"t"),
        ],
    );
    let frozen = &state.frozen()[0];
    let spans = vec![
        // Inside (aa, gg): 32 bytes.
        (
            encoded_physical(branch, b"bb"),
            encoded_physical(branch, b"cc"),
            32,
        ),
        // Inside (gg, mm): 96 bytes.
        (
            encoded_physical(branch, b"hh"),
            encoded_physical(branch, b"ii"),
            96,
        ),
        // Inside (mm, tt): 64 bytes.
        (
            encoded_physical(branch, b"nn"),
            encoded_physical(branch, b"oo"),
            64,
        ),
    ];

    // Three qualifying gaps, two cut slots: the two largest win, returned in
    // ascending key order.
    assert_eq!(
        flush_zone_cut_keys(&spans, frozen, 32, 3),
        vec![
            encoded_physical(branch, b"mm"),
            encoded_physical(branch, b"tt"),
        ],
    );
}

#[test]
fn segmented_flush_installs_key_disjoint_outputs() {
    let branch = branch_id(0x83);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    // Two versions of the low key prove a cut can never split one physical
    // key; the high key lands in its own segment.
    let mut state = one_frozen_table_branch(
        branch,
        vec![
            put_row(branch, b"aa", 1, 100, b"low-v1"),
            put_row(branch, b"aa", 3, 300, b"low-v3"),
            put_row(branch, b"zz", 2, 200, b"high"),
        ],
    );
    let request = flush_request(branch, None);

    let prepared = prepare_durable_flush_with_cuts_for_test(
        &state.clone(),
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
        None,
        None,
        None,
        &[encoded_physical(branch, b"zz")],
    )
    .expect("prepare segmented flush")
    .expect("prepared flush");

    let outcome = install_prepared_durable_flush(&mut state, prepared);
    assert!(outcome.completed());
    assert_eq!(outcome.tables().len(), 2, "one output table per segment");
    assert_eq!(outcome.rows_flushed(), 3);
    assert_eq!(state.frozen_table_count(), 0);
    assert_eq!(state.owned_table_count(), 2);
    // A2's watermark invariant: the segments' commit ranges interleave
    // ([1,3] and [2,2]) yet their union covers every version — the
    // recovery release fail-safe must accept the interval.
    assert!(
        crate::lifecycle::checkpoint::branch_durable_ranges_cover_interval(
            state.owned_levels(),
            state.inherited_layers(),
            CommitVersion::ZERO,
            CommitVersion::new(3),
        ),
        "interleaved segment commit ranges must union-cover the flushed interval",
    );
}

#[test]
fn adopted_segmented_flush_output_vanishing_mid_build_is_a_sweep_race() {
    let branch = branch_id(0x85);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let rows = vec![
        put_row(branch, b"aa", 1, 100, b"low"),
        put_row(branch, b"zz", 2, 200, b"high"),
    ];
    let request = flush_request(branch, None);
    // The abandoned earlier attempt publishes both segment objects; its
    // install never lands (the prepared value is dropped).
    let abandoned = one_frozen_table_branch(branch, rows.clone());
    let prepared = prepare_durable_flush_with_cuts_for_test(
        &abandoned,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
        None,
        None,
        None,
        &[encoded_physical(branch, b"zz")],
    )
    .expect("prepare abandoned segmented flush")
    .expect("prepared flush");
    drop(prepared);
    let first_object = backend
        .operations()
        .into_iter()
        .find_map(|operation| match operation {
            FlushOperation::Publish(name, _) => Some(name),
            _ => None,
        })
        .expect("first published segment");
    // The retry adopts the first segment's content-identical object; a
    // concurrent sweep unlinks it between the adoption and the build's read.
    backend.vanish_object_on_next_read(first_object);
    let state = one_frozen_table_branch(branch, rows);

    let error = prepare_durable_flush_with_cuts_for_test(
        &state,
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
        None,
        None,
        None,
        &[encoded_physical(branch, b"zz")],
    )
    .expect_err("adopted segment swept mid-build is the benign sweep race");

    assert!(
        error.is_rewrite_output_sweep_race(),
        "expected the sweep-race classification, got: {error:?}"
    );
}

#[test]
fn segmented_flush_publish_failure_reports_every_published_output() {
    let branch = branch_id(0x84);
    // First table-object publish succeeds; the second fails — the orphan
    // outcome must name the published first segment.
    let backend: &'static FlushBackend =
        crate::testkit::leak_static(FlushBackend::with_publish_failure_after(1));
    let mut state = one_frozen_table_branch(
        branch,
        vec![
            put_row(branch, b"aa", 1, 100, b"low"),
            put_row(branch, b"zz", 2, 200, b"high"),
        ],
    );
    let request = flush_request(branch, None);

    let prepared = prepare_durable_flush_with_cuts_for_test(
        &state.clone(),
        &TableObjectService::new(backend),
        &TableObjectReaderService::new(backend),
        &request,
        None,
        None,
        None,
        &[encoded_physical(branch, b"zz")],
    )
    .expect("prepare segmented flush")
    .expect("prepared flush");

    let outcome = install_prepared_durable_flush(&mut state, prepared);
    assert!(!outcome.completed());
    assert!(outcome.published_not_installed());
    let maintenance = outcome.maintenance_outcome();
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
    assert_eq!(
        maintenance.affected_object_names().len(),
        1,
        "the one published segment is reported for reclaim accounting",
    );
    assert_eq!(
        state.frozen_table_count(),
        1,
        "the frozen input survives a failed segmented flush",
    );
    assert_eq!(state.owned_table_count(), 0);
}

/// The off-lock flush-watermark design is optimistic by contract: the proof
/// carries its build-time manifest epoch, and a concurrent flush advancing
/// the manifest rejects the stale proof so the next wake recomputes. That
/// rejection is the designed retry path — the apply fold must record it as
/// a deferral, not a permanent task failure.
#[test]
fn stale_flush_watermark_proof_defers_instead_of_failing() {
    let branch = branch_id(0x7a);
    let backend: &'static FlushBackend = crate::testkit::leak_static(FlushBackend::new());
    let mut shell = LifecycleDurableLocalShell::assemble(
        durable_open_request(branch),
        backend,
        CommitManualTimestampSource::new(Timestamp::from_micros(9_000)),
    )
    .expect("durable shell");
    let recovery_request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .expect("recovery outcome");
    let mut runtime = shell.complete_recovery(&recovery).expect("open runtime");
    let generation =
        CommitBranchGenerationGuard::exact(CommitBranchGeneration::new(1).expect("generation"));

    // First flush cycle: publishes a table manifest and auto-enqueues the
    // coalescing flush-watermark task.
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, physical_key(branch, b"stale-a"), b"one".to_vec()),
            generation,
        )
        .expect("first commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate first");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue first flush");
    let first_flush = runtime
        .run_next_flush_maintenance()
        .expect("run first flush")
        .expect("first flush outcome");
    assert_eq!(first_flush.status(), MaintenanceOutcomeStatus::Completed);

    // Capture the off-lock coverage inputs and compute the proof against the
    // CURRENT manifest epoch.
    let step = runtime
        .start_next_background_flush_watermark_maintenance()
        .expect("start watermark step")
        .expect("watermark task is pending");
    let DurableBackgroundMaintenanceStep::FlushWatermarkCompute(inputs) = step else {
        panic!("expected an off-lock coverage compute step, got a completed outcome");
    };
    let computed = inputs.compute_coverage();
    assert!(computed.is_some(), "coverage must be provable at capture");

    // Stale the proof: a second flush cycle advances the table-manifest
    // sequence past the epoch the proof was built against.
    runtime
        .execute_durable_commit(
            durable_put_batch(branch, physical_key(branch, b"stale-b"), b"two".to_vec()),
            generation,
        )
        .expect("second commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate second");
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::flush(branch))
        .expect("enqueue second flush");
    let second_flush = runtime
        .run_next_flush_maintenance()
        .expect("run second flush")
        .expect("second flush outcome");
    assert_eq!(second_flush.status(), MaintenanceOutcomeStatus::Completed);

    // Applying the now-stale proof is the documented optimistic-concurrency
    // rejection: a deferral that the next wake retries, never a failure.
    let outcome = runtime
        .apply_flush_watermark_coverage(&inputs, computed)
        .expect("apply returns an outcome envelope")
        .expect("the pending watermark task is redeemed");
    assert_eq!(
        outcome.status(),
        MaintenanceOutcomeStatus::Deferred,
        "stale proof must defer: {outcome:?}"
    );
    assert!(
        outcome.source_error().is_some(),
        "the deferral carries the staleness error for diagnostics"
    );
    assert_eq!(
        runtime.maintenance_status().stats().failed(),
        0,
        "a stale proof is not a task failure: {:?}",
        runtime.maintenance_status()
    );
}

/// #2553's third site (#2778): a flush whose output races the table-object
/// sweep is a benign scheduling race — the frozen input stays queued and the
/// next drain publishes fresh bytes. The drain must count it as a deferral,
/// exactly like the locked-publish and compaction-install arms, never as a
/// task failure.
#[test]
fn flush_drain_defers_when_the_output_races_a_table_object_sweep() {
    let branch = branch_id(0x7d);
    let mut state = frozen_branch(branch, put_row(branch, b"sweep-race", 1, 1_000, b"row"));

    let outcome = flush_branch_drain_with(
        &mut state,
        &flush_drain_request(branch),
        |_branch, _request| {
            Err(LifecycleError::RewriteOutputRacedSweep {
                object: crate::object::ObjectName::new("tables/sweep-race-object")
                    .expect("object name"),
            })
        },
    )
    .expect("flush drain");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(
        outcome.failed_flushes(),
        0,
        "a benign race is not a failure"
    );
    // The raced flush defers, and `with_post_drain_frozen_tables` adds its
    // own deferral for the retained frozen state — both are deferrals.
    assert!(outcome.deferred_flushes() >= 1);
    assert_eq!(
        maintenance.status(),
        MaintenanceOutcomeStatus::Deferred,
        "sweep race defers: {maintenance:?}"
    );
    assert!(maintenance.retryable());
    assert!(
        maintenance.source_error().is_some(),
        "the race is preserved for diagnostics"
    );
    assert_eq!(
        state.frozen_table_count(),
        1,
        "the frozen input stays queued for the retried drain"
    );
}

/// Direction control: genuine flush errors still fail the drain loudly.
#[test]
fn flush_drain_still_fails_on_genuine_flush_errors() {
    let branch = branch_id(0x7e);
    let mut state = frozen_branch(branch, put_row(branch, b"real-error", 1, 1_000, b"row"));

    let outcome = flush_branch_drain_with(
        &mut state,
        &flush_drain_request(branch),
        |_branch, _request| {
            Err(LifecycleError::lower_layer(
                LifecycleLowerLayer::Backend,
                "backend write failed",
            ))
        },
    )
    .expect("flush drain");
    let maintenance = outcome.maintenance_outcome();

    assert_eq!(outcome.failed_flushes(), 1);
    assert_eq!(maintenance.status(), MaintenanceOutcomeStatus::Failed);
}
