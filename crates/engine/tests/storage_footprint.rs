//! Space-reclamation contract §3.5 (slice 9): the engine's storage footprint —
//! one canonical path from the storage diagnostics to `Database`, database-
//! global facts, and a typed refusal where no durable objects exist.
#![cfg(feature = "localfs")]

mod common;

use common::{
    assert_no_storage_leak, assert_status, branch, key, open_cache_database, open_durable_database,
    space, value,
};
use strata_engine::{Database, EngineErrorClass, FootprintDetail};

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
                    + all.wal_reclaimable_bytes.expect("audit")
                    + all.wal_tail_bytes.expect("audit")
            ),
            "a clean database's total is its tables, its snapshot and the WAL on disk: {all:?}"
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
        assert_eq!(
            debt.live_table_objects, 3,
            "the superseded inputs stay catalogued beside the output until the purge: {debt:?}"
        );
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

    /// Gate-7 pin for #3619: the durable table catalogue never drops a purged
    /// object, so the Live tier keeps counting the two compaction inputs the
    /// chain deleted. Asserts the CURRENT behavior; promote it to
    /// `live_table_objects == 1` when #3619 is fixed.
    #[test]
    fn pin_3619_purged_objects_stay_catalogued() {
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
            settled.live_table_objects, 3,
            "#3619: purged inputs stay catalogued until reopen: {settled:?}"
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
