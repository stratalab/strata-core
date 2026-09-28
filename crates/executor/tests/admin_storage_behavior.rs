//! Space-reclamation contract §3.5 (slice 10): the `admin.storage` command —
//! the engine's storage footprint on the wire, with the audit tier behind a
//! flag and a typed refusal on a cache database.

use strata_executor::{Command, Executor, Output};
use tempfile::TempDir;

fn storage(executor: &mut Executor, branch: Option<&str>, audit: bool) -> Output {
    executor
        .execute(Command::Storage {
            branch: branch.map(str::to_owned),
            audit,
        })
        .expect("storage footprint")
}

#[test]
fn storage_on_a_cache_database_is_refused_with_the_registered_code() {
    let mut executor = Executor::open_cache().expect("cache executor");
    let error = executor
        .execute(Command::Storage {
            branch: None,
            audit: false,
        })
        .expect_err("a cache database holds no durable objects");
    assert_eq!(error.code(), "unsupported.engine.persistence_capability");
}

#[test]
fn storage_requires_a_known_branch() {
    let temp = TempDir::new().expect("temp dir");
    let mut executor =
        Executor::open_durable_local(temp.path().join("db")).expect("durable executor");
    let error = executor
        .execute(Command::Storage {
            branch: Some("missing".to_owned()),
            audit: false,
        })
        .expect_err("an unknown branch is refused");
    assert_eq!(error.code(), "not_found.engine.branch");
}

#[test]
fn storage_reports_the_live_tier_without_audit_facts() {
    let temp = TempDir::new().expect("temp dir");
    let mut executor =
        Executor::open_durable_local(temp.path().join("db")).expect("durable executor");
    let Output::Storage(live) = storage(&mut executor, None, false) else {
        panic!("storage returns the storage output");
    };
    assert!(!live.audit);
    assert_eq!(
        live.live_table_objects, 0,
        "nothing flushed on a fresh open"
    );
    assert_eq!(live.live_table_bytes, 0);
    assert_eq!(live.unreferenced_objects, None);
    assert_eq!(live.snapshot_objects, None);
    assert_eq!(live.wal_tail_bytes, None);
    assert_eq!(live.total_bytes, None);
    // The background worker owns the open's reclaim wake, so its queue may
    // still hold that task here; the count is known either way.
    assert!(live.reclaim.pending_reclaim_tasks.is_some());

    let Output::Storage(mut scoped) = storage(&mut executor, Some("default"), false) else {
        panic!("storage returns the storage output");
    };
    // The footprint facts are database-global. The reclaim ledger is left out
    // of the comparison: the background worker may run the open's wake
    // between the two reads, and the ledger legitimately moves when it does.
    scoped.reclaim = live.reclaim;
    assert_eq!(scoped, live, "the facts are database-global");
}

#[test]
fn storage_audit_gathers_the_listing_backed_facts_and_a_total() {
    let temp = TempDir::new().expect("temp dir");
    let mut executor =
        Executor::open_durable_local(temp.path().join("db")).expect("durable executor");
    let Output::Storage(audit) = storage(&mut executor, None, true) else {
        panic!("storage returns the storage output");
    };
    assert!(audit.audit);
    assert_eq!(audit.unreferenced_objects, Some(0));
    assert_eq!(audit.unreferenced_bytes, Some(0));
    assert_eq!(audit.quarantined_objects, Some(0));
    assert_eq!(
        audit.snapshot_objects,
        Some(1),
        "the creation checkpoint is the one snapshot: {audit:?}"
    );
    // #3643: it references a timeline tail segment for each of the two
    // branches (`default` and `_system_`, whose histories differ).
    assert_eq!(audit.timeline_segment_objects, Some(2), "{audit:?}");
    assert!(audit.timeline_segment_bytes.is_some_and(|bytes| bytes > 0));
    assert_eq!(audit.superseded_timeline_segments, Some(0));
    assert!(audit.snapshot_bytes.is_some_and(|bytes| bytes > 0));
    assert_eq!(audit.superseded_snapshots, Some(0));
    assert_eq!(audit.wal_reclaimable_bytes, Some(0));
    assert!(audit.wal_tail_bytes.is_some());
    assert_eq!(
        audit.total_bytes,
        Some(
            audit.live_table_bytes
                + audit.snapshot_bytes.expect("audit")
                + audit.timeline_segment_bytes.expect("audit")
                + audit.wal_reclaimable_bytes.expect("audit")
                + audit.wal_tail_bytes.expect("audit")
        ),
        "the total takes the WAL from the audit's own listing: {audit:?}"
    );
    // The open's reconcile prune runs on the background worker; whether it
    // has run yet is timing, so the ledger is only checked for shape here.
    assert!(audit.reclaim.pending_reclaim_tasks.is_some(), "{audit:?}");

    // The wire shape: tagged by command name, audit-only fields present.
    let json = serde_json::to_value(Output::Storage(audit)).expect("serialize");
    assert_eq!(json["type"], "storage");
    assert_eq!(json["data"]["audit"], true);
    assert_eq!(json["data"]["snapshot_objects"], 1);
    assert_eq!(json["data"]["timeline_segment_objects"], 2);
    assert!(json["data"]["reclaim"]["total_passes"].is_u64());
    let round_trip: Output = serde_json::from_value(json).expect("deserialize");
    assert!(matches!(round_trip, Output::Storage(_)));
}

#[test]
fn storage_command_name_and_read_classification() {
    let command = Command::Storage {
        branch: None,
        audit: true,
    };
    assert_eq!(command.name(), "storage");
    assert!(!command.is_write());
    let json = serde_json::to_value(&command).expect("serialize");
    assert_eq!(json, serde_json::json!({"type": "storage", "audit": true}));
    let live: Command = serde_json::from_value(serde_json::json!({"type": "storage"}))
        .expect("audit defaults to false");
    assert!(matches!(live, Command::Storage { audit: false, .. }));
}

#[cfg(feature = "testkit")]
mod inline {
    use super::*;
    use strata_engine::{DurableLocalOpenOptions, MaintenanceScheduling};
    use strata_executor::types::{AdminReclaimOutcome, Bytes};

    /// With maintenance inline, the open's reconcile prune drains at the
    /// first commit, so the ledger's per-family passes reach the wire
    /// deterministically: the prune that found nothing to remove.
    #[test]
    fn storage_audit_carries_the_ledger_passes_once_the_open_wake_drains() {
        let temp = TempDir::new().expect("temp dir");
        let mut executor = Executor::open_durable_local_with_options(
            temp.path().join("db"),
            DurableLocalOpenOptions::new().with_maintenance_scheduling_policy_for_test(
                MaintenanceScheduling::DeterministicInline,
            ),
        )
        .expect("durable executor");
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: Bytes::from(b"k".to_vec()),
                value: Bytes::from(b"v".to_vec()),
            })
            .expect("a commit drains the open wake inline");
        let Output::Storage(audit) = storage(&mut executor, None, true) else {
            panic!("storage returns the storage output");
        };
        let prune = audit
            .reclaim
            .last_snapshot_prune
            .expect("the open's reconcile prune ran at the first commit");
        assert_eq!(prune.outcome, AdminReclaimOutcome::Nothing, "{audit:?}");
        assert_eq!(prune.deferral, None);
        assert_eq!(prune.objects_affected, 1, "{audit:?}");
        assert_eq!(audit.reclaim.pending_reclaim_tasks, Some(0), "{audit:?}");
        assert!(audit.reclaim.total_passes >= 1, "{audit:?}");
        assert_eq!(audit.snapshot_objects, Some(1), "{audit:?}");
    }
}
