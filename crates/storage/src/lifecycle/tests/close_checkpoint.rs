//! Space-reclamation contract §3.1 (slice 7): the close-time checkpoint.
//!
//! A clean close publishes one multi-branch snapshot covering every branch's
//! uncovered rows (the product default plus an empty never-flushed root, the
//! engine's `_system_` shape), truncates the covered WAL behind it and prunes
//! the snapshots it supersedes — so the next open replays nothing. Every
//! deferral leaves the WAL exactly as the durable record the next open
//! replays.

use super::checkpoint::shared::{
    assemble_shell, branch_id, durable_batch, generation_guard, open_runtime, physical_key,
    CheckpointBackendEvent, CheckpointTestBackend,
};
use super::*;
use crate::commit::{CommitBranchGeneration, CommitManualTimestampSource};
use crate::layout::ObjectLayout;
use crate::service::{DatabaseManifestService, SnapshotService};
use strata_core::{BranchId, CommitVersion};

fn wal_objects(backend: &CheckpointTestBackend) -> Vec<String> {
    let prefix = ObjectLayout::wal_prefix().expect("wal prefix");
    backend
        .object_snapshot()
        .keys()
        .filter(|object| object.as_str().starts_with(prefix.as_str()))
        .map(|object| object.as_str().to_owned())
        .collect()
}

fn snapshot_ids(backend: &CheckpointTestBackend) -> Vec<u64> {
    SnapshotService::new(backend)
        .list_snapshots()
        .expect("list snapshots")
        .iter()
        .map(crate::service::SnapshotObject::snapshot_id)
        .collect()
}

fn events_since(backend: &CheckpointTestBackend, from: usize) -> Vec<CheckpointBackendEvent> {
    backend.events()[from..].to_vec()
}

/// Reopens the store and reports how many WAL records recovery replayed.
fn reopen_counting_replay(
    branch: BranchId,
    backend: &'static CheckpointTestBackend,
) -> (
    LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    usize,
) {
    let mut shell = assemble_shell(branch, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    let replayed = outcome.wal().record_count();
    let runtime = shell.complete_recovery(&outcome).expect("open runtime");
    (runtime, replayed)
}

fn commit(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    key: &'static [u8],
) {
    runtime
        .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
        .expect("durable commit");
}

fn row_is_present(
    runtime: &LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    key: &'static [u8],
) -> bool {
    runtime
        .read_view_for_branch(branch)
        .expect("read view")
        .latest(&physical_key(branch, key))
        .expect("latest read")
        .is_some()
}

/// The engine's shape: the seeded branch plus an empty root that received a
/// commit and never flushed. One close, one snapshot covering both, the WAL
/// truncated to the fresh active segment, and a reopen that replays nothing
/// yet reads every row.
#[test]
fn clean_close_checkpoints_every_branch_and_truncates_the_wal() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xe1);
    let extra = branch_id(0xe2);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-multi-a");
    commit(&mut runtime, initial, b"close-multi-b");
    runtime
        .create_branch(
            extra,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create the empty root");
    commit(&mut runtime, extra, b"close-multi-root");
    commit(&mut runtime, initial, b"close-multi-c");
    let wal_before = wal_objects(backend);
    let events_before = backend.event_count();

    let close = runtime.close().expect("clean close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Attempted(LifecycleCheckpointStatus::Completed)
    );
    let events = events_since(backend, events_before);
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == CheckpointBackendEvent::SnapshotCreate)
            .count(),
        1,
        "one snapshot covers both branches: {events:?}"
    );
    assert!(
        events.contains(&CheckpointBackendEvent::ObjectDelete),
        "the covered WAL segment is truncated: {events:?}"
    );
    assert_eq!(snapshot_ids(backend), vec![1]);
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(manifest.snapshot_id(), Some(1));
    assert_eq!(
        manifest.snapshot_watermark(),
        Some(4),
        "the snapshot covers every commit of both branches"
    );
    let wal_after = wal_objects(backend);
    assert!(
        wal_after.iter().all(|object| !wal_before.contains(object)),
        "every pre-close segment is covered and deleted: before={wal_before:?} after={wal_after:?}"
    );
    assert!(
        wal_after.len() <= 1,
        "at most the fresh active segment remains: {wal_after:?}"
    );

    let (reopened, replayed) = reopen_counting_replay(initial, backend);
    assert_eq!(replayed, 0, "the next open replays nothing");
    for (branch, key) in [
        (initial, b"close-multi-a" as &'static [u8]),
        (initial, b"close-multi-b"),
        (initial, b"close-multi-c"),
        (extra, b"close-multi-root"),
    ] {
        assert!(
            row_is_present(&reopened, branch, key),
            "row {key:?} on {branch:?} survives through the snapshot"
        );
    }
}

/// The fresh-fork window (#2798): a COW child that owns nothing cannot be
/// serialized, so the registry defers and the close leaves the WAL as the
/// durable record.
#[test]
fn close_checkpoint_defers_in_the_fresh_fork_window() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xe3);
    let fork = branch_id(0xe4);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-fork-base");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate initial");
    runtime
        .flush_frozen(
            &FlushFrozenRequest::new(
                initial,
                None,
                FlushTableIdentitySeed::new("close-fork-seed").expect("seed"),
                FlushTableObjectId::new("close-fork-object").expect("object id"),
            )
            .expect("flush request"),
        )
        .expect("flush initial");
    runtime
        .fork_current(
            initial,
            fork,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("fork");
    commit(&mut runtime, initial, b"close-fork-tail");
    let wal_before = wal_objects(backend);
    let events_before = backend.event_count();

    let close = runtime.close().expect("clean close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Skipped(CloseCheckpointSkip::Structural(
            crate::lifecycle::checkpoint::CheckpointStructuralDeferral::UnmaterializedInheritedLayers
        ))
    );
    let events = events_since(backend, events_before);
    assert!(!events.contains(&CheckpointBackendEvent::SnapshotCreate));
    assert!(!events.contains(&CheckpointBackendEvent::ObjectDelete));
    assert_eq!(wal_objects(backend), wal_before, "the WAL is untouched");
    assert!(snapshot_ids(backend).is_empty());
}

/// A non-seeded branch holding a durable table base no longer defers the
/// close checkpoint (space-reclamation contract §3.2, slice 12): one snapshot
/// covers both branches and records both as durable-base holders, the WAL is
/// truncated behind it, and the next open replays nothing.
#[test]
fn close_checkpoint_completes_with_a_flushed_non_seeded_branch() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xe5);
    let extra = branch_id(0xe6);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-flushed-root-a");
    runtime
        .create_branch(
            extra,
            CommitBranchGeneration::new(1).expect("generation"),
            None,
        )
        .expect("create the root");
    commit(&mut runtime, extra, b"close-flushed-root-base");
    runtime
        .rotate_active_for_branch_for_maintenance(extra)
        .expect("rotate extra");
    runtime
        .flush_frozen(
            &FlushFrozenRequest::new(
                extra,
                None,
                FlushTableIdentitySeed::new("close-flushed-root-seed").expect("seed"),
                FlushTableObjectId::new("close-flushed-root-object").expect("object id"),
            )
            .expect("flush request"),
        )
        .expect("flush extra");
    commit(&mut runtime, initial, b"close-flushed-root-b");
    let wal_before = wal_objects(backend);
    let events_before = backend.event_count();

    let close = runtime.close().expect("clean close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Attempted(LifecycleCheckpointStatus::Completed)
    );
    let events = events_since(backend, events_before);
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == CheckpointBackendEvent::SnapshotCreate)
            .count(),
        1,
        "one snapshot covers both branches: {events:?}"
    );
    assert_eq!(snapshot_ids(backend), vec![1]);
    let wal_after = wal_objects(backend);
    assert!(
        wal_after.iter().all(|object| !wal_before.contains(object)),
        "every pre-close segment is covered and deleted: before={wal_before:?} after={wal_after:?}"
    );

    let mut shell = assemble_shell(initial, backend).expect("shell");
    let request =
        LifecycleRecoveryRequest::from_open_plan(shell.open_plan()).expect("recovery request");
    let outcome = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&request)
        .expect("recovery outcome");
    assert_eq!(
        outcome.wal().record_count(),
        0,
        "the next open replays nothing"
    );
    assert_eq!(
        outcome.checkpoint().durable_base_branches(),
        Some(&[extra][..]),
        "the flushed non-seeded branch alone holds a durable base"
    );
    let reopened = shell.complete_recovery(&outcome).expect("open runtime");
    assert!(reopened.current_recovery_health_for_test().is_healthy());
    assert!(row_is_present(&reopened, initial, b"close-flushed-root-a"));
    assert!(row_is_present(&reopened, initial, b"close-flushed-root-b"));
    assert!(row_is_present(&reopened, extra, b"close-flushed-root-base"));
}

/// Nothing above the retention watermark: a close right after a checkpoint
/// that already covers the visible version publishes no second snapshot.
#[test]
fn close_checkpoint_skips_when_nothing_sits_above_the_watermark() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xe7);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-nothing-new");
    let checkpoint = runtime
        .checkpoint_for_explicit_maintenance(initial, true)
        .expect("explicit checkpoint");
    assert_eq!(checkpoint.status(), LifecycleCheckpointStatus::Completed);
    assert_eq!(snapshot_ids(backend), vec![1]);
    let events_before = backend.event_count();

    let close = runtime.close().expect("clean close");

    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Skipped(CloseCheckpointSkip::NothingNew)
    );
    let events = events_since(backend, events_before);
    assert!(!events.contains(&CheckpointBackendEvent::SnapshotCreate));
    assert_eq!(snapshot_ids(backend), vec![1]);
}

/// A delta over the snapshot payload cap defers at close (the flush-first
/// retry belongs to the next session's growth chain); nothing is published or
/// deleted, and the next open replays every record.
#[test]
fn close_checkpoint_over_the_delta_cap_leaves_the_wal_for_the_next_session() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xe8);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-over-cap-a");
    commit(&mut runtime, initial, b"close-over-cap-b");
    runtime.set_checkpoint_delta_cap_for_test(1);
    let wal_before = wal_objects(backend);
    let events_before = backend.event_count();

    let close = runtime.close().expect("clean close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Attempted(LifecycleCheckpointStatus::DeferredDeltaExceedsCap)
    );
    let events = events_since(backend, events_before);
    assert!(!events.contains(&CheckpointBackendEvent::SnapshotCreate));
    assert!(!events.contains(&CheckpointBackendEvent::ObjectDelete));
    let wal_after = wal_objects(backend);
    assert!(
        wal_before.iter().all(|object| wal_after.contains(object)),
        "every pre-close segment survives: before={wal_before:?} after={wal_after:?}"
    );
    assert!(snapshot_ids(backend).is_empty());

    let (reopened, replayed) = reopen_counting_replay(initial, backend);
    assert_eq!(replayed, 2, "the next open replays both commits");
    assert!(row_is_present(&reopened, initial, b"close-over-cap-b"));
}

/// A disabled close-time reclaim budget skips the checkpoint before any
/// work: no snapshot, no rotation, no truncation.
#[test]
fn disabled_reclaim_budget_skips_the_close_checkpoint() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xe9);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-disabled");
    let wal_before = wal_objects(backend);
    let events_before = backend.event_count();

    let close = runtime
        .close_with_reclaim_budget(LifecycleCloseReclaimBudget::Disabled)
        .expect("clean close");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Skipped(CloseCheckpointSkip::Disabled)
    );
    let events = events_since(backend, events_before);
    assert!(!events.contains(&CheckpointBackendEvent::SnapshotCreate));
    assert!(!events.contains(&CheckpointBackendEvent::ObjectDelete));
    assert_eq!(wal_objects(backend), wal_before, "the WAL is untouched");
    assert!(snapshot_ids(backend).is_empty());
}

/// A snapshot publish that fails at close is refused, not retried and not a
/// close failure: no covered segment is deleted before a manifest re-point
/// that never happened, and the next open replays the WAL.
#[test]
fn close_checkpoint_snapshot_publish_failure_leaves_the_wal_intact() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xea);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-publish-fault-a");
    commit(&mut runtime, initial, b"close-publish-fault-b");
    backend.fail_snapshot_publish();
    let wal_before = wal_objects(backend);
    let events_before = backend.event_count();

    let close = runtime
        .close()
        .expect("a refused checkpoint keeps the close clean");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(close.checkpoint(), CloseCheckpointReport::Refused);
    let events = events_since(backend, events_before);
    assert!(
        !events.contains(&CheckpointBackendEvent::ObjectDelete),
        "nothing is deleted without a published snapshot: {events:?}"
    );
    let wal_after = wal_objects(backend);
    assert!(
        wal_before.iter().all(|object| wal_after.contains(object)),
        "every pre-close segment survives: before={wal_before:?} after={wal_after:?}"
    );
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(manifest.snapshot_id(), None);

    let (reopened, replayed) = reopen_counting_replay(initial, backend);
    assert_eq!(replayed, 2, "the next open replays both commits");
    assert!(row_is_present(&reopened, initial, b"close-publish-fault-a"));
    assert!(row_is_present(&reopened, initial, b"close-publish-fault-b"));
    assert_eq!(
        reopened.visible_version(),
        CommitVersion::new(2),
        "recovery restores the visible version from the WAL"
    );
}

/// A truncation that cannot delete the covered segment is the checkpoint's
/// health debt, not a close failure: the snapshot is attested, the segment
/// stays for a later pass, and the next open replays nothing.
#[test]
fn close_checkpoint_truncation_failure_is_health_debt_not_a_close_failure() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xeb);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-truncate-fault");
    backend.fail_delete();
    let wal_before = wal_objects(backend);

    let close = runtime
        .close()
        .expect("a failed truncation keeps the close clean");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Attempted(LifecycleCheckpointStatus::Completed)
    );
    assert!(
        !runtime.current_recovery_health_for_test().is_healthy(),
        "the failed truncation is recorded as health debt: {:?}",
        runtime.current_recovery_health_for_test()
    );
    let wal_after = wal_objects(backend);
    assert!(
        wal_before.iter().all(|object| wal_after.contains(object)),
        "the undeletable segment stays: before={wal_before:?} after={wal_after:?}"
    );
    assert_eq!(snapshot_ids(backend), vec![1]);

    let (reopened, replayed) = reopen_counting_replay(initial, backend);
    assert_eq!(
        replayed, 0,
        "the attested snapshot covers the leftover segment"
    );
    assert!(row_is_present(&reopened, initial, b"close-truncate-fault"));
}

/// A close that fails AFTER its checkpoint completed (the final WAL sync)
/// retries; the retry sees nothing above the watermark the checkpoint moved
/// and publishes no second snapshot.
#[test]
fn close_retry_after_a_completed_checkpoint_publishes_no_second_snapshot() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xec);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-retry-nothing-new");
    // Sync calls during the close: the reclaim rotation seals the active
    // segment (1), then the WAL close syncs the fresh one (2).
    backend.fail_sync_on_call(backend.sync_calls() + 2);

    let error = runtime
        .close()
        .expect_err("the WAL close sync fails after the checkpoint");
    assert_eq!(runtime.state(), LifecycleState::Closing, "{error:?}");
    assert_eq!(snapshot_ids(backend), vec![1], "the checkpoint completed");

    let close = runtime.close().expect("the retry closes");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Skipped(CloseCheckpointSkip::NothingNew)
    );
    assert_eq!(snapshot_ids(backend), vec![1], "no second snapshot");
}

/// A snapshot published whose manifest re-point failed (the #3612 shape)
/// leaves an orphan at the id the close allocated; a retried close allocates
/// the next id, completes, and the superseded prune removes the orphan.
#[test]
fn close_retry_after_a_partial_publish_advances_the_snapshot_id() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let initial = branch_id(0xed);
    let mut runtime = open_runtime(initial, backend);
    commit(&mut runtime, initial, b"close-retry-partial");
    // Manifest replaces during the checkpoint: the active-segment persist
    // (1), then the re-point after the snapshot publish (2).
    backend.fail_manifest_replacement_on_call(backend.manifest_replace_calls() + 2);
    backend.fail_sync_on_call(backend.sync_calls() + 2);

    let error = runtime
        .close()
        .expect_err("the WAL close sync fails after the partial checkpoint");
    assert_eq!(runtime.state(), LifecycleState::Closing, "{error:?}");
    assert_eq!(snapshot_ids(backend), vec![1], "the orphan snapshot landed");
    assert_eq!(
        DatabaseManifestService::new(backend)
            .load_required()
            .expect("database manifest")
            .snapshot_id(),
        None,
        "the manifest never attested it"
    );

    let close = runtime.close().expect("the retry closes");

    assert_eq!(close.status(), CloseOutcomeStatus::Complete);
    assert_eq!(
        close.checkpoint(),
        CloseCheckpointReport::Attempted(LifecycleCheckpointStatus::Completed)
    );
    let manifest = DatabaseManifestService::new(backend)
        .load_required()
        .expect("database manifest");
    assert_eq!(
        manifest.snapshot_id(),
        Some(2),
        "the retry allocated the next id"
    );
    assert_eq!(
        snapshot_ids(backend),
        vec![2],
        "the superseded prune removed the orphan"
    );
}
