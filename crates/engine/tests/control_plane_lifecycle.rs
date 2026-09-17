//! Control-plane lifecycle tests: fail-closed behavior and interrupted-operation
//! recovery, driven through the persistence fault seam.
//!
//! These rely on the testkit fault-injection surface, so the whole binary
//! compiles away without the `testkit` feature.
#![cfg(feature = "testkit")]

mod common;

#[cfg(feature = "localfs")]
use common::open_durable_database;
use common::{assert_status, branch, open_cache_database, space};
use strata_engine::testkit::StorageFaultKind;
use strata_engine::{AdminHealthStatus, ControlHealthStatus, EngineErrorClass};
#[cfg(feature = "localfs")]
use strata_engine::{KvKey, KvValue};

/// When a branch operation fails and its pending-marker cleanup also fails, the
/// control plane fails closed: inspection still works and reports the plane as
/// unavailable (admin health is Degraded), but writes are rejected.
#[test]
fn fail_closed_control_plane_degrades_health_and_rejects_work() {
    let mut db = open_cache_database().expect("cache database opens");

    // The storage branch action fails, and the following cleanup commit (the
    // second commit, after the pending marker is written) fails too — which is
    // what forces the control plane closed.
    db.inject_branch_fault_for_test(StorageFaultKind::Unavailable);
    db.inject_commit_fault_after_for_test(1, StorageFaultKind::Unavailable);
    assert!(
        db.branches()
            .expect("branch service opens")
            .create(branch("feature"))
            .is_err(),
        "branch creation should fail"
    );

    // Inspection still works and reports every control area as unavailable.
    let diagnostics = db
        .control_diagnostics(None)
        .expect("diagnostics remain available");
    assert_eq!(
        diagnostics.identity_status(),
        ControlHealthStatus::Unavailable
    );
    assert_eq!(
        diagnostics.registry_status(),
        ControlHealthStatus::Unavailable
    );
    assert_eq!(
        diagnostics.branch_catalog_status(),
        ControlHealthStatus::Unavailable
    );

    // Admin health rolls this up to Degraded — reads and inspection still work.
    assert_eq!(
        db.admin().expect("admin opens").health(None).status,
        AdminHealthStatus::Degraded
    );

    // Further work is rejected with a structured control-plane error.
    let Err(rejected) = db.kv(branch("default"), space("default")) else {
        panic!("the fail-closed control plane must reject further work");
    };
    assert_status(
        &rejected,
        EngineErrorClass::Unavailable,
        "unavailable.engine.control_plane",
        true,
    );
}

/// A branch creation interrupted after its durable pending marker is written
/// leaves that marker behind; reopening the database must recover (roll the
/// interrupted operation back) and open, rather than bricking the whole
/// database with a data-loss error (finding F2).
#[cfg(feature = "localfs")]
#[test]
fn interrupted_branch_creation_recovers_on_durable_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut db = open_durable_database(dir.path()).expect("durable opens");
        db.inject_branch_fault_for_test(StorageFaultKind::Unavailable);
        db.inject_commit_fault_after_for_test(1, StorageFaultKind::Unavailable);
        // The pending marker commits durably; the branch action and cleanup fail.
        assert!(
            db.branches()
                .expect("branch service opens")
                .create(branch("feature"))
                .is_err(),
            "branch creation should fail, leaving a durable pending marker"
        );
        db.close().expect("close succeeds");
    }

    // Recovery un-bricks the database: reopen succeeds and the interrupted
    // branch is absent.
    let mut db = open_durable_database(dir.path()).expect("recovery reopens the database");
    assert!(
        db.branches()
            .expect("branch service opens")
            .list()
            .expect("branch list succeeds")
            .iter()
            .all(|summary| summary.name().as_str() != "feature"),
        "the interrupted branch must not be published after recovery"
    );
    // The pre-existing default branch and normal work are intact, and the name
    // is free to be created cleanly.
    db.kv(branch("default"), space("default"))
        .expect("default branch is usable after recovery");
    db.branches()
        .expect("branch service opens")
        .create(branch("feature"))
        .expect("the interrupted name can be created cleanly after recovery");
    db.close().expect("close succeeds");
}

/// A fork that fails AFTER its storage branch is durably created (here, the
/// describe that follows a successful current-fork) must roll the storage
/// branch back rather than orphan it. Otherwise the branch name is permanently
/// poisoned — a clean retry would hit `already_exists` in storage (finding U12).
#[cfg(feature = "localfs")]
#[test]
fn fork_failure_after_storage_creation_rolls_back_and_leaves_name_reusable() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut db = open_durable_database(dir.path()).expect("durable opens");

    // Give the default branch a commit so a current-fork actually forks storage
    // (reaching the post-fork describe) rather than taking the empty-history
    // path that never describes.
    db.kv(branch("default"), space("default"))
        .expect("kv service opens")
        .put(
            KvKey::new(b"seed".to_vec()).expect("valid key"),
            KvValue::new(b"v".to_vec()),
        )
        .expect("seed commit");

    // The fork succeeds at the storage layer; the following describe fails.
    db.inject_branch_fault_after_for_test(1, StorageFaultKind::Unavailable);
    assert!(
        db.branches()
            .expect("branch service opens")
            .fork_current(&branch("default"), branch("feature"))
            .is_err(),
        "the fork should fail on the post-fork describe"
    );

    // The name is not poisoned: the storage branch was rolled back, so a clean
    // retry succeeds (before the fix this failed with already_exists).
    db.branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("the fork name is reusable after rollback");
    assert!(
        db.branches()
            .expect("branch service opens")
            .list()
            .expect("branch list succeeds")
            .iter()
            .any(|summary| summary.name().as_str() == "feature"),
        "the clean retry published the branch"
    );
    db.close().expect("close succeeds");
}

/// V1 cutover (hard rule 42): pre-V1 development databases are rejected
/// with the structured layout code, never silently overlaid.
#[test]
fn pre_v1_database_layout_is_rejected_with_layout_code() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    std::fs::write(tempdir.path().join("strata.toml"), b"pre-v1").expect("marker");
    let Err(error) = strata_engine::Database::open_local(
        tempdir.path(),
        strata_engine::DurableLocalOpenOptions::new(),
    ) else {
        panic!("pre-V1 layout must refuse");
    };
    assert_eq!(error.code(), "failed_precondition.engine.layout_version");
    assert!(
        !tempdir.path().join("manifest").exists(),
        "refusal must not create V1 layout objects"
    );
}
