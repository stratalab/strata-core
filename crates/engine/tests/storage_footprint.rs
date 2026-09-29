//! Space-reclamation contract §3.5 (slice 9): the engine's storage footprint —
//! one canonical path from the storage diagnostics to `Database`, database-
//! global facts, and a typed refusal where no durable objects exist.
#![cfg(feature = "localfs")]

mod common;

use common::{
    assert_no_storage_leak, assert_status, branch, key, open_cache_database, open_durable_database,
    space, value,
};
use strata_engine::{Database, EngineErrorClass, FootprintDetail, ReclaimOutcome};

fn write_rows(db: &mut Database, branch_name: &str, prefix: &str, rows: u32) {
    let mut kv = db.kv(branch(branch_name), space("app")).expect("kv opens");
    for index in 0..rows {
        kv.put(
            key(format!("{prefix}-{index:04}").as_bytes()),
            value(&[b'v'; 256]),
        )
        .expect("write");
    }
}

/// The audit facts a clean database shows: nothing unreferenced or
/// quarantined, at most one superseded snapshot, and no WAL below the
/// retention watermark left undeleted.
#[cfg(feature = "testkit")]
fn assert_clean(footprint: &strata_engine::StorageFootprint) {
    assert_eq!(footprint.detail, FootprintDetail::Audit);
    assert_eq!(footprint.unreferenced_objects, Some(0), "{footprint:?}");
    assert_eq!(footprint.unreferenced_bytes, Some(0), "{footprint:?}");
    assert_eq!(footprint.quarantined_objects, Some(0), "{footprint:?}");
    assert_eq!(footprint.quarantined_bytes, Some(0), "{footprint:?}");
    assert!(
        footprint
            .superseded_snapshots
            .is_some_and(|count| count <= 1),
        "{footprint:?}"
    );
    assert_eq!(footprint.wal_reclaimable_bytes, Some(0), "{footprint:?}");
}

#[test]
fn storage_footprint_on_a_cache_database_is_unsupported() {
    let mut db = open_cache_database().expect("cache open");
    let error = db
        .storage_footprint(None, FootprintDetail::Live)
        .expect_err("a cache database holds no durable objects");
    assert_status(
        &error,
        EngineErrorClass::Unavailable,
        "unsupported.engine.persistence_capability",
        true,
    );
    assert_no_storage_leak(&error);
}

#[test]
fn storage_footprint_requires_a_known_branch() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut db = open_durable_database(dir.path()).expect("durable open");
    let error = db
        .storage_footprint(Some(&branch("missing")), FootprintDetail::Live)
        .expect_err("an unknown branch is refused before any storage call");
    assert_status(
        &error,
        EngineErrorClass::NotFound,
        "not_found.engine.branch",
        false,
    );
}

/// A reopened database's reclaim wake reaches the engine's ledger view on the
/// production (background) scheduler: the open's reconcile prune is reported
/// as a typed pass. The worker runs it off the caller's thread, so the read
/// polls with a generous bound rather than assuming it has already run.
#[test]
fn storage_footprint_reports_the_open_wake_prune_on_the_background_scheduler() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let mut db = open_durable_database(dir.path()).expect("durable open");
        write_rows(&mut db, "default", "wake", 8);
        db.close().expect("clean close");
    }
    let mut db = open_durable_database(dir.path()).expect("reopen");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let prune = loop {
        let footprint = db
            .storage_footprint(None, FootprintDetail::Live)
            .expect("footprint");
        if let Some(prune) = footprint.reclaim.last_snapshot_prune {
            break prune;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the open wake's reconcile prune never reached the ledger: {footprint:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    // One attested snapshot and nothing beside it: the prune had nothing to
    // remove, and said so.
    assert_eq!(prune.outcome, ReclaimOutcome::Nothing, "{prune:?}");
    assert_eq!(prune.deferral, None, "{prune:?}");
    assert_eq!(prune.bytes_reclaimed, 0, "{prune:?}");
}

/// The live tier costs no backend I/O and carries no audit fact.
#[test]
fn storage_footprint_live_tier_carries_no_audit_fact() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut db = open_durable_database(dir.path()).expect("durable open");
    write_rows(&mut db, "default", "live", 8);
    let footprint = db
        .storage_footprint(None, FootprintDetail::Live)
        .expect("live footprint");
    assert_eq!(footprint.detail, FootprintDetail::Live);
    assert_eq!(footprint.live_table_objects, 0, "nothing flushed yet");
    assert_eq!(footprint.live_table_bytes, 0);
    assert_eq!(footprint.unreferenced_objects, None);
    assert_eq!(footprint.quarantined_bytes, None);
    assert_eq!(footprint.snapshot_objects, None);
    assert_eq!(footprint.wal_tail_bytes, None);
    assert_eq!(footprint.total_bytes, None);
    assert_eq!(footprint.reclaim.total_bytes_reclaimed, 0);
    let default_footprint = db
        .storage_footprint(Some(&branch("default")), FootprintDetail::Live)
        .expect("live footprint");
    assert_eq!(
        default_footprint, footprint,
        "the facts are database-global"
    );
}

#[cfg(feature = "testkit")]
mod inline {
    use super::*;
    use strata_engine::{
        DatabaseOpenOutcome, DurableLocalOpenOptions, MaintenanceScheduling, ReclaimOutcome,
    };

    fn open_inline(path: &std::path::Path) -> Database {
        Database::open_local(
            path,
            DurableLocalOpenOptions::new().with_maintenance_scheduling_policy_for_test(
                MaintenanceScheduling::DeterministicInline,
            ),
        )
        .map(DatabaseOpenOutcome::into_database)
        .expect("durable open")
    }

    /// The footprint is database-global: both user branches' durable bases
    /// count, and naming either branch (or none) reports the same facts.
    #[test]
    fn storage_footprint_covers_both_engine_branches() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut db = open_inline(dir.path());
        write_rows(&mut db, "default", "d", 64);
        db.branches()
            .expect("branches")
            .create(branch("feature"))
            .expect("create feature");
        write_rows(&mut db, "feature", "f", 64);
        db.flush_storage_branch_for_test(&branch("default"))
            .expect("flush default");
        db.flush_storage_branch_for_test(&branch("feature"))
            .expect("flush feature");

        let all = db
            .storage_footprint(None, FootprintDetail::Audit)
            .expect("audit footprint");
        assert_clean(&all);
        assert_eq!(
            all.live_table_objects, 2,
            "one durable table per flushed user branch: {all:?}"
        );
        assert!(all.live_table_bytes > 0, "{all:?}");
        assert_eq!(
            all.snapshot_objects,
            Some(1),
            "the creation checkpoint is the one snapshot: {all:?}"
        );
        assert!(all.snapshot_bytes.is_some_and(|bytes| bytes > 0), "{all:?}");
        assert_eq!(all.superseded_snapshots, Some(0), "{all:?}");
        // #3643: the snapshot references a timeline tail per branch.
        assert!(
            all.timeline_segment_objects.is_some_and(|count| count > 0),
            "{all:?}"
        );
        assert_eq!(all.superseded_timeline_segments, Some(0), "{all:?}");
        assert!(
            all.wal_retained_bytes.is_some_and(|bytes| bytes > 0),
            "{all:?}"
        );
        assert!(
            all.wal_tail_bytes.is_some_and(|bytes| bytes > 0),
            "the session's rows sit in the WAL tail: {all:?}"
        );
        assert_eq!(
            all.total_bytes,
            Some(
                all.live_table_bytes
                    + all.snapshot_bytes.expect("audit")
                    + all.timeline_segment_bytes.expect("audit")
                    + all.wal_reclaimable_bytes.expect("audit")
                    + all.wal_tail_bytes.expect("audit")
            ),
            "a clean database's total is its tables, its snapshot, its timeline segments and \
             the WAL on disk: {all:?}"
        );
        for name in ["default", "feature"] {
            let scoped = db
                .storage_footprint(Some(&branch(name)), FootprintDetail::Audit)
                .expect("audit footprint");
            assert_eq!(scoped.live_table_objects, all.live_table_objects);
            assert_eq!(scoped.live_table_bytes, all.live_table_bytes);
            assert_eq!(scoped.unreferenced_objects, all.unreferenced_objects);
        }
    }

    /// Debt the audit can see, and the ledger's account of reclaiming it: two
    /// flushed tables a compaction supersedes are unreferenced or quarantined
    /// until the chained mark → sweep → purge drains on the next commit.
    #[cfg(feature = "localfs")]
    #[test]
    fn storage_footprint_audit_reports_debt_and_the_reclaim_that_settles_it() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut db = open_inline(dir.path());
        write_rows(&mut db, "default", "a", 32);
        db.flush_storage_branch_for_test(&branch("default"))
            .expect("first flush");
        write_rows(&mut db, "default", "b", 32);
        db.flush_storage_branch_for_test(&branch("default"))
            .expect("second flush");
        let before_compaction = db
            .storage_footprint(None, FootprintDetail::Audit)
            .expect("audit footprint");
        assert_eq!(before_compaction.live_table_objects, 2);
        assert_clean(&before_compaction);

        db.force_storage_branch_compaction_for_test(&branch("default"))
            .expect("compaction");
        let debt = db
            .storage_footprint(None, FootprintDetail::Audit)
            .expect("audit footprint");
        // #3646: the audit counts each file once — the two superseded inputs
        // as unreferenced, only the output as a table in use. The Live tier,
        // which does no listing, still counts all three catalogued objects.
        assert_eq!(
            debt.live_table_objects, 1,
            "the audit's table facts exclude what it counts as unreferenced: {debt:?}"
        );
        let live = db
            .storage_footprint(None, FootprintDetail::Live)
            .expect("live footprint");
        assert_eq!(live.live_table_objects, 3, "{live:?}");
        assert_eq!(
            debt.unreferenced_objects,
            Some(2),
            "both superseded inputs are debt until the chain drains: {debt:?}"
        );
        assert!(
            debt.unreferenced_bytes.is_some_and(|bytes| bytes > 0),
            "{debt:?}"
        );
        assert_eq!(debt.quarantined_objects, Some(0), "{debt:?}");
        assert_eq!(
            debt.reclaim.pending_reclaim_tasks,
            Some(1),
            "the compaction queued its mark: {debt:?}"
        );
        assert_eq!(debt.reclaim.last_mark, None, "{debt:?}");

        // The next commit's inline drain runs the chained mark → sweep →
        // purge; the audit is clean again and the ledger accounts for it.
        write_rows(&mut db, "default", "c", 1);
        let settled = db
            .storage_footprint(None, FootprintDetail::Audit)
            .expect("audit footprint");
        assert_clean(&settled);
        let sweep = settled.reclaim.last_sweep.expect("the sweep ran");
        assert_eq!(sweep.outcome, ReclaimOutcome::Reclaimed, "{settled:?}");
        assert_eq!(sweep.deferral, None);
        assert!(sweep.bytes_reclaimed > 0, "{settled:?}");
        assert_eq!(sweep.objects_affected, 2, "{settled:?}");
        assert!(settled.reclaim.total_bytes_reclaimed > 0, "{settled:?}");
        assert!(settled.reclaim.reclaimed_passes >= 1, "{settled:?}");
        assert!(settled.reclaim.total_passes >= 2, "{settled:?}");
        assert_eq!(
            settled.reclaim.pending_reclaim_tasks,
            Some(0),
            "{settled:?}"
        );
        assert!(
            settled.total_bytes.is_some_and(|total| {
                total < debt.total_bytes.expect("audit total with warm WAL facts")
            }),
            "the settled database is smaller: {debt:?} -> {settled:?}"
        );
    }

    /// #3619 (promoted from its gate-7 pin): once the reclaim chain moves the
    /// two compaction inputs out of the table family, the Live tier counts
    /// only the compaction output, and the ledger's freed total counts their
    /// bytes once (the purge), not again for the sweep that staged them.
    #[test]
    fn purged_objects_leave_the_live_tier_and_are_counted_once() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut db = open_inline(dir.path());
        write_rows(&mut db, "default", "a", 32);
        db.flush_storage_branch_for_test(&branch("default"))
            .expect("first flush");
        write_rows(&mut db, "default", "b", 32);
        db.flush_storage_branch_for_test(&branch("default"))
            .expect("second flush");
        db.force_storage_branch_compaction_for_test(&branch("default"))
            .expect("compaction");
        write_rows(&mut db, "default", "c", 1);
        let settled = db
            .storage_footprint(None, FootprintDetail::Audit)
            .expect("audit footprint");
        assert_eq!(settled.unreferenced_objects, Some(0), "{settled:?}");
        assert_eq!(settled.quarantined_objects, Some(0), "{settled:?}");
        assert_eq!(
            settled.live_table_objects, 1,
            "only the compaction output is live: {settled:?}"
        );
        let purge = settled.reclaim.last_purge.expect("the purge ran");
        assert!(purge.bytes_reclaimed > 0, "{settled:?}");
        assert_eq!(
            settled.reclaim.total_bytes_reclaimed, purge.bytes_reclaimed,
            "the inputs' bytes are counted once: {settled:?}"
        );
    }

    /// Write, close, reopen read-only, reopen with a write: at every stage the
    /// audit sees a clean database and the live base survives.
    #[test]
    fn footprint_after_write_close_reopen() {
        let dir = tempfile::tempdir().expect("tmp");
        let root = dir.path().join("db");
        {
            let mut db = open_inline(&root);
            write_rows(&mut db, "default", "w", 64);
            db.flush_storage_branch_for_test(&branch("default"))
                .expect("flush");
            let footprint = db
                .storage_footprint(None, FootprintDetail::Audit)
                .expect("audit footprint");
            assert_clean(&footprint);
            assert_eq!(footprint.live_table_objects, 1);
            db.close().expect("clean close");
        }
        let live_table_bytes = {
            let mut db = open_inline(&root);
            let footprint = db
                .storage_footprint(None, FootprintDetail::Audit)
                .expect("audit footprint after a read-only reopen");
            assert_clean(&footprint);
            assert_eq!(footprint.live_table_objects, 1);
            assert!(footprint.live_table_bytes > 0, "{footprint:?}");
            let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
            assert!(kv.get(&key(b"w-0000")).expect("read").is_some());
            drop(kv);
            db.close().expect("clean close");
            footprint.live_table_bytes
        };
        {
            let mut db = open_inline(&root);
            write_rows(&mut db, "default", "x", 16);
            let footprint = db
                .storage_footprint(None, FootprintDetail::Audit)
                .expect("audit footprint after a writing reopen");
            assert_clean(&footprint);
            assert_eq!(footprint.live_table_objects, 1);
            assert_eq!(footprint.live_table_bytes, live_table_bytes);
            db.close().expect("clean close");
        }
    }
}
