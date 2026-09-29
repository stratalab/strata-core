//! Space-reclamation contract §3.5 (slice 2): the durable runtime's footprint
//! facts, asserted against the runners that would act on them.

use super::checkpoint::shared::{
    branch_id, durable_batch, generation_guard, open_runtime, open_runtime_with_wal_segment_size,
    CheckpointBackendEvent, CheckpointTestBackend,
};
use super::*;
use crate::backend::Backend;
use crate::commit::CommitManualTimestampSource;
use crate::layout::ObjectLayout;
use crate::lifecycle::{classify_wal_segment, LifecycleFootprintWatermark, WalSegmentClass};
use crate::object::ObjectName;
use std::collections::BTreeMap;
use strata_core::{BranchId, CommitVersion, Timestamp};

#[test]
fn classify_wal_segment_truth_table() {
    use WalSegmentClass::{Reclaimable, Tail};

    let covered = Some(CommitVersion::new(10));
    for (case, sealed_max_commit, covered_through, expected) in [
        ("active or above (unsized)", None, covered, Tail),
        (
            "sealed, covered exactly",
            Some(CommitVersion::new(10)),
            covered,
            Reclaimable,
        ),
        (
            "sealed, covered below",
            Some(CommitVersion::new(9)),
            covered,
            Reclaimable,
        ),
        (
            "sealed, empty segment",
            Some(CommitVersion::ZERO),
            covered,
            Reclaimable,
        ),
        (
            "sealed, above the proof",
            Some(CommitVersion::new(11)),
            covered,
            Tail,
        ),
        ("sealed, no proof", Some(CommitVersion::new(1)), None, Tail),
        ("active, no proof", None, None, Tail),
    ] {
        assert_eq!(
            classify_wal_segment(sealed_max_commit, covered_through),
            expected,
            "{case}"
        );
    }
}

fn flush_request(branch: BranchId, tag: &str) -> FlushFrozenRequest {
    FlushFrozenRequest::new(
        branch,
        None,
        FlushTableIdentitySeed::new(format!("footprint-{tag}-{branch}")).expect("seed"),
        FlushTableObjectId::new(format!("footprint-object-{tag}-{branch}")).expect("object"),
    )
    .expect("flush request")
}

fn flush_one_table(
    runtime: &mut LifecycleDurableLocalRuntime<'static, CommitManualTimestampSource>,
    branch: BranchId,
    key: &'static [u8],
    tag: &str,
) -> ObjectName {
    runtime
        .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
        .expect("commit");
    runtime
        .rotate_active_for_maintenance()
        .expect("rotate for flush");
    runtime
        .flush_frozen(&flush_request(branch, tag))
        .expect("flush frozen rows")
        .table_object()
        .expect("flushed table object")
        .clone()
}

fn object_bytes(backend: &CheckpointTestBackend, object: &ObjectName) -> u64 {
    backend
        .object_snapshot()
        .get(object)
        .map(|bytes| bytes.len() as u64)
        .expect("object present")
}

fn wal_bytes_by_object(backend: &CheckpointTestBackend) -> BTreeMap<ObjectName, u64> {
    let prefix = ObjectLayout::wal_prefix().expect("wal prefix");
    backend
        .object_snapshot()
        .into_iter()
        .filter(|(name, _)| name.as_str().starts_with(prefix.as_str()))
        .map(|(name, bytes)| (name, bytes.len() as u64))
        .collect()
}

fn listing_events(backend: &CheckpointTestBackend) -> usize {
    backend
        .events()
        .iter()
        .filter(|event| matches!(event, CheckpointBackendEvent::ObjectList))
        .count()
}

#[test]
fn live_facts_sum_the_catalog_and_read_only_the_caches() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd1);
    let mut runtime = open_runtime(branch, backend);
    let first = flush_one_table(&mut runtime, branch, b"live-a", "a");
    let second = flush_one_table(&mut runtime, branch, b"live-b", "b");
    let expected_bytes = object_bytes(backend, &first) + object_bytes(backend, &second);
    // Warm the WAL growth cache the way a commit would, then read it back
    // through the live tier without a scan.
    let sampled = runtime.current_wal_growth_facts().expect("growth facts");
    let events_before = backend.events().len();

    let live = runtime.footprint_live_facts();

    assert_eq!(
        backend.events().len(),
        events_before,
        "the live tier does no backend I/O"
    );
    assert_eq!(live.live_table_objects(), 2);
    assert_eq!(live.live_table_bytes(), expected_bytes);
    assert_eq!(live.wal(), Some(sampled));
    assert_eq!(
        live.wal_retention_watermark(),
        LifecycleFootprintWatermark::Known(None),
        "seeded from the open-time manifest: no checkpoint yet"
    );
    assert_eq!(live.pending_reclaim_tasks(), 0);
}

#[test]
fn live_facts_report_cold_caches_instead_of_warming_them() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd2);
    let mut runtime = open_runtime(branch, backend);
    runtime
        .execute_durable_commit(durable_batch(branch, b"cold", b"value"), generation_guard())
        .expect("commit");
    // A checkpoint completion invalidates the retention watermark cache; a
    // truncation invalidates the WAL growth cache.
    runtime.invalidate_retention_watermark_cache();
    runtime.services().wal().invalidate_sealed_retention();
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue mark");
    let listings_before = listing_events(backend);

    let live = runtime.footprint_live_facts();

    assert_eq!(listing_events(backend), listings_before);
    assert_eq!(
        live.wal(),
        None,
        "a cold WAL cache is reported, never scanned"
    );
    assert_eq!(
        live.wal_retention_watermark(),
        LifecycleFootprintWatermark::Cold
    );
    assert_eq!(
        live.pending_reclaim_tasks(),
        1,
        "the queued mark is reclaim work"
    );
}

#[test]
fn audit_unreferenced_objects_are_exactly_what_the_sweep_stages() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd3);
    let mut runtime = open_runtime(branch, backend);
    let live = flush_one_table(&mut runtime, branch, b"audit-live", "live");
    // A table object no manifest references: a prior session's leftover.
    let stray = ObjectLayout::table_object(&branch.to_string(), 0, "footprint-stray")
        .expect("stray object");
    let stray_bytes = vec![0x5a; 777];
    backend
        .write_object(&stray, &stray_bytes)
        .expect("plant stray table object");

    let before = runtime.footprint_audit_facts().expect("audit");

    assert_eq!(before.unreferenced(), Some((1, stray_bytes.len() as u64)));
    assert_eq!(before.quarantined_objects(), 0);
    assert_eq!(before.quarantined_bytes(), 0);
    assert!(object_bytes(backend, &live) > 0);

    // The mark and the sweep act on the same candidate set.
    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch))
        .expect("enqueue mark");
    let mark = runtime
        .run_next_retention_maintenance()
        .expect("run mark")
        .expect("mark outcome");
    assert_eq!(mark.status(), MaintenanceOutcomeStatus::Completed);
    let sweep = runtime
        .run_next_quarantine_maintenance()
        .expect("run sweep")
        .expect("sweep outcome");
    assert_eq!(sweep.status(), MaintenanceOutcomeStatus::Completed);

    let after = runtime
        .footprint_audit_facts()
        .expect("audit after the sweep");

    assert_eq!(after.unreferenced(), Some((0, 0)));
    assert_eq!(after.quarantined_objects(), 1);
    assert_eq!(after.quarantined_bytes(), stray_bytes.len() as u64);
    assert!(
        backend.read_object(&live).is_ok(),
        "the live table is untouched"
    );
}

#[test]
fn audit_under_data_loss_health_names_nothing_unreferenced() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd4);
    let mut runtime = open_runtime(branch, backend);
    flush_one_table(&mut runtime, branch, b"health", "health");
    let stray =
        ObjectLayout::table_object(&branch.to_string(), 0, "footprint-stray").expect("stray");
    backend
        .write_object(&stray, &[0x5a; 64])
        .expect("plant stray table object");
    runtime.record_recovery_health_for_test(
        &RecoveryHealth::degraded(
            RecoveryDegradationClass::DataLoss,
            vec![
                RecoveryFault::new(RecoveryFaultKind::MissingSnapshotObject, "missing")
                    .expect("fault"),
            ],
        )
        .expect("health"),
    );

    let audit = runtime.footprint_audit_facts().expect("audit");

    assert_eq!(
        audit.unreferenced(),
        None,
        "an incomplete proof proves nothing dead"
    );
}

#[test]
fn audit_snapshots_count_the_superseded_family_the_prune_reclaims() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd5);
    let mut runtime = open_runtime(branch, backend);
    for (snapshot_id, key) in [
        (1, b"snap-a" as &'static [u8]),
        (2, b"snap-b" as &'static [u8]),
    ] {
        runtime
            .execute_durable_commit(durable_batch(branch, key, b"value"), generation_guard())
            .expect("commit");
        let request =
            LifecycleCheckpointRequest::new(branch, snapshot_id, Timestamp::from_micros(50))
                .expect("request");
        let outcome = runtime.checkpoint(&request).expect("checkpoint");
        assert_eq!(outcome.status(), LifecycleCheckpointStatus::Completed);
    }
    let older = ObjectLayout::snapshot(1).expect("snapshot 1");
    let newer = ObjectLayout::snapshot(2).expect("snapshot 2");
    let older_bytes = object_bytes(backend, &older);
    let newer_bytes = object_bytes(backend, &newer);
    // #3643: each checkpoint sealed its branch's timeline tail; the older one
    // is superseded with its snapshot (the newer references only its own).
    let tail = |sealing| {
        ObjectLayout::timeline_segment(crate::layout::TimelineSegmentId {
            sealing_snapshot_id: sealing,
            ordinal: 0,
        })
        .expect("tail segment")
    };
    let older_tail = object_bytes(backend, &tail(1));
    let newer_tail = object_bytes(backend, &tail(2));

    let before = runtime.footprint_audit_facts().expect("audit");

    assert_eq!(before.snapshot_objects(), 2);
    assert_eq!(before.snapshot_bytes(), older_bytes + newer_bytes);
    assert_eq!(before.superseded_snapshots(), 1);
    assert_eq!(before.superseded_snapshot_bytes(), older_bytes);
    assert_eq!(before.timeline_segments(), (2, older_tail + newer_tail));
    assert_eq!(before.superseded_timeline_segments(), (1, older_tail));

    // The chained prune (slice 5) reclaims exactly the superseded part.
    while let Some(outcome) = runtime
        .run_next_retention_maintenance()
        .expect("retention lane")
    {
        if outcome.task_kind() == MaintenanceTaskKind::SnapshotPruning {
            break;
        }
    }
    let after = runtime
        .footprint_audit_facts()
        .expect("audit after the prune");

    assert_eq!(after.snapshot_objects(), 1);
    assert_eq!(after.snapshot_bytes(), newer_bytes);
    assert_eq!(after.timeline_segments(), (1, newer_tail));
    assert_eq!(after.superseded_timeline_segments(), (0, 0));
    assert_eq!(after.superseded_snapshots(), 0);
    assert_eq!(after.superseded_snapshot_bytes(), 0);
}

#[test]
fn audit_wal_split_is_exactly_what_the_truncation_pass_deletes() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd6);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 4096);
    for index in 0..6u8 {
        let key: &'static [u8] =
            crate::testkit::leak_static(format!("wal-split-{index}").into_bytes()).as_slice();
        let value: &'static [u8] = crate::testkit::leak_static(vec![index; 1500]).as_slice();
        runtime
            .execute_durable_commit(durable_batch(branch, key, value), generation_guard())
            .expect("commit");
    }
    let sealed_before = wal_bytes_by_object(backend);
    assert!(
        sealed_before.len() >= 3,
        "several segments rolled: {sealed_before:?}"
    );
    // No proof yet: nothing is reclaimable.
    let unproven = runtime
        .footprint_audit_facts()
        .expect("audit before checkpoint");
    assert_eq!(unproven.wal_reclaimable_bytes(), 0);
    assert_eq!(
        unproven.wal_tail_bytes(),
        sealed_before.values().sum::<u64>(),
        "without a proof every segment is tail"
    );

    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(51)).expect("request");
    let checkpoint = runtime.checkpoint(&request).expect("checkpoint");
    assert_eq!(checkpoint.status(), LifecycleCheckpointStatus::Completed);
    // The proof covers every record, the active segment's included — and the
    // active segment is still tail, exactly as the delete pass protects it.
    let covered = runtime
        .footprint_audit_facts()
        .expect("audit with a full proof");
    let active = ObjectLayout::wal_segment(runtime.services().wal().active_segment_id())
        .expect("active segment object");
    let active_bytes = wal_bytes_by_object(backend)
        .get(&active)
        .copied()
        .expect("active segment is listed");
    assert_eq!(
        covered.wal_tail_bytes(),
        active_bytes,
        "only the active segment is tail"
    );
    // One record above the proof keeps the active segment a genuine tail, so
    // the truncation pass deletes only what the audit calls reclaimable.
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"wal-split-tail", b"tail"),
            generation_guard(),
        )
        .expect("tail commit");
    let before = runtime
        .footprint_audit_facts()
        .expect("audit after checkpoint");
    let wal_before = wal_bytes_by_object(backend);
    assert!(before.wal_reclaimable_bytes() > 0);
    assert_eq!(
        before.wal_reclaimable_bytes() + before.wal_tail_bytes(),
        wal_before.values().sum::<u64>()
    );

    runtime
        .enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        .expect("enqueue truncation");
    let truncation = runtime
        .run_next_wal_truncation_maintenance()
        .expect("run truncation")
        .expect("truncation outcome");
    assert_eq!(truncation.status(), MaintenanceOutcomeStatus::Completed);

    let wal_after = wal_bytes_by_object(backend);
    let deleted: u64 = wal_before
        .iter()
        .filter(|(name, _)| !wal_after.contains_key(*name))
        .map(|(_, bytes)| bytes)
        .sum();
    assert_eq!(deleted, before.wal_reclaimable_bytes());
    let after = runtime
        .footprint_audit_facts()
        .expect("audit after truncation");
    assert_eq!(after.wal_reclaimable_bytes(), 0);
    assert_eq!(after.wal_tail_bytes(), wal_after.values().sum::<u64>());
}

/// A segment listed at the start of an audit can be gone by its read or stat
/// (a concurrent truncation): the audit drops it, exactly as the delete pass
/// and the sealed-retention scan do, instead of failing.
#[test]
fn audit_tolerates_wal_segments_that_vanish_between_listing_and_read() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd7);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 4096);
    for index in 0..6u8 {
        let key: &'static [u8] =
            crate::testkit::leak_static(format!("wal-vanish-{index}").into_bytes()).as_slice();
        let value: &'static [u8] = crate::testkit::leak_static(vec![index; 1500]).as_slice();
        runtime
            .execute_durable_commit(durable_batch(branch, key, value), generation_guard())
            .expect("commit");
    }
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(52)).expect("request");
    assert_eq!(
        runtime.checkpoint(&request).expect("checkpoint").status(),
        LifecycleCheckpointStatus::Completed
    );
    let wal = wal_bytes_by_object(backend);
    let active_id = runtime.services().wal().active_segment_id();
    let active = ObjectLayout::wal_segment(active_id).expect("active segment");
    let sealed = ObjectLayout::wal_segment(active_id - 1).expect("a sealed segment");
    let complete = runtime.footprint_audit_facts().expect("audit");
    assert_eq!(
        complete.wal_reclaimable_bytes() + complete.wal_tail_bytes(),
        wal.values().sum::<u64>()
    );

    // The sealed segment vanishes at its read: its bytes leave the reclaimable side.
    backend.vanish_object_on_next_read(sealed.clone());
    let without_sealed = runtime
        .footprint_audit_facts()
        .expect("audit survives a vanished read");
    assert_eq!(
        without_sealed.wal_reclaimable_bytes(),
        complete.wal_reclaimable_bytes() - wal[&sealed]
    );
    assert_eq!(without_sealed.wal_tail_bytes(), complete.wal_tail_bytes());

    // #3666: the active segment is never listed or stat'ed — it is counted at
    // the writer's logical length — so a vanish armed on it changes nothing.
    backend.vanish_object_on_next_read(active.clone());
    let with_active_armed = runtime
        .footprint_audit_facts()
        .expect("audit never reads the active segment");
    assert_eq!(
        with_active_armed.wal_tail_bytes(),
        complete.wal_tail_bytes()
    );
    assert!(
        wal_bytes_by_object(backend).contains_key(&active),
        "the armed vanish never fired: the audit did not touch the active segment"
    );
}

/// The tolerance is for `NotFound` only: any other failure to read a sealed
/// segment is the audit's error, never a dropped segment — a silently smaller
/// figure would misreport the debt. The active segment is never stat'ed
/// (#3666), so a fault armed on it cannot fail or shrink the audit.
#[test]
fn audit_propagates_wal_segment_read_and_stat_failures_other_than_not_found() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd8);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 4096);
    for index in 0..6u8 {
        let key: &'static [u8] =
            crate::testkit::leak_static(format!("wal-fault-{index}").into_bytes()).as_slice();
        let value: &'static [u8] = crate::testkit::leak_static(vec![index; 1500]).as_slice();
        runtime
            .execute_durable_commit(durable_batch(branch, key, value), generation_guard())
            .expect("commit");
    }
    let request =
        LifecycleCheckpointRequest::new(branch, 1, Timestamp::from_micros(53)).expect("request");
    assert_eq!(
        runtime.checkpoint(&request).expect("checkpoint").status(),
        LifecycleCheckpointStatus::Completed
    );
    let active_id = runtime.services().wal().active_segment_id();
    let active = ObjectLayout::wal_segment(active_id).expect("active segment");
    let sealed = ObjectLayout::wal_segment(active_id - 1).expect("a sealed segment");

    backend.fail_object_on_next_read(sealed);
    assert!(
        runtime.footprint_audit_facts().is_err(),
        "a sealed segment read failure propagates instead of dropping the segment"
    );

    // Once the sealed fault is spent the audit is whole again — and a fault
    // armed on the active segment never fires, because the audit counts it
    // at the writer's logical length instead of stat'ing it.
    backend.fail_object_on_next_read(active);
    let complete = runtime
        .footprint_audit_facts()
        .expect("audit never stats the active segment");
    assert_eq!(
        complete.wal_reclaimable_bytes() + complete.wal_tail_bytes(),
        wal_bytes_by_object(backend).values().sum::<u64>()
    );
}

/// A segment listed ABOVE the active one (a crash leftover the delete pass
/// never reads) is only sized: its stat bytes join the tail. The `NotFound`
/// tolerance holds there too — a segment that vanishes before its stat is
/// dropped — while any other stat failure is the audit's error.
#[test]
fn audit_sizes_a_segment_above_the_active_one_and_tolerates_only_not_found() {
    let backend: &'static CheckpointTestBackend =
        crate::testkit::leak_static(CheckpointTestBackend::new());
    let branch = branch_id(0xd9);
    let mut runtime = open_runtime_with_wal_segment_size(branch, backend, 4096);
    runtime
        .execute_durable_commit(
            durable_batch(branch, b"wal-above", b"value"),
            generation_guard(),
        )
        .expect("commit");
    let before = runtime.footprint_audit_facts().expect("audit");
    let active_id = runtime.services().wal().active_segment_id();
    let above = ObjectLayout::wal_segment(active_id + 1).expect("segment above the active one");
    backend
        .write_object(&above, &[0xa5; 100])
        .expect("plant the leftover segment");

    let with_above = runtime.footprint_audit_facts().expect("audit");
    assert_eq!(with_above.wal_tail_bytes(), before.wal_tail_bytes() + 100);
    assert_eq!(
        with_above.wal_reclaimable_bytes(),
        before.wal_reclaimable_bytes()
    );

    backend.vanish_object_on_next_read(above.clone());
    let vanished = runtime
        .footprint_audit_facts()
        .expect("a segment gone before its stat is dropped");
    assert_eq!(vanished.wal_tail_bytes(), before.wal_tail_bytes());

    backend
        .write_object(&above, &[0xa5; 100])
        .expect("plant the leftover segment again");
    backend.fail_object_on_next_read(above);
    assert!(
        runtime.footprint_audit_facts().is_err(),
        "a stat failure other than NotFound propagates"
    );
}
