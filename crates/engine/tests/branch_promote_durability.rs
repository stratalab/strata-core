//! Branch promotion durable crash-window conformance (M12D2, conformance #9/#11).
//!
//! A promotion commits target data, then publishes the merge edge, as two
//! commits. These tests drive the persistence fault seam to crash the database
//! between them and prove reopen recovery reconciles the promotion intent: the
//! promoted data is never exposed without lineage, and lineage is reconstructed
//! from committed branch-control rows.
//!
//! The fault seam and durable reopen both need testkit + localfs, so the binary
//! compiles away without them.
#![cfg(all(feature = "testkit", feature = "localfs"))]

mod common;

use common::{assert_branch_value, branch, key, open_durable_database, space, value};
use strata_engine::testkit::StorageFaultKind;
use strata_engine::PromotionStrategy;

/// Seeds `default` with `k=base`, forks `feature`, and changes `k=promoted` on
/// the source only — a one-sided change a strict promotion applies cleanly as a
/// single mutation.
fn seed_one_sided(db: &mut strata_engine::Database) {
    {
        let mut kv = db
            .kv(branch("default"), space("default"))
            .expect("kv opens");
        kv.put(key(b"k"), value(b"base")).expect("base");
    }
    db.branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    let mut kv = db
        .kv(branch("feature"), space("default"))
        .expect("kv opens");
    kv.put(key(b"k"), value(b"promoted"))
        .expect("feature change");
}

/// Crash AFTER the promotion intent (commit 1) and the target data commit
/// (commit 2), but BEFORE the merge-edge publish (commit 3): reopen recovery
/// must finalize the edge so the promoted data is visible WITH lineage (#9/#11).
#[cfg(feature = "localfs")]
#[test]
fn promotion_interrupted_before_lineage_publish_finalizes_on_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut db = open_durable_database(dir.path()).expect("durable opens");
        seed_one_sided(&mut db);

        db.inject_commit_fault_after_for_test(2, StorageFaultKind::Unavailable);
        assert!(
            db.branches()
                .expect("branch service opens")
                .promote(
                    &branch("feature"),
                    &branch("default"),
                    PromotionStrategy::Strict,
                )
                .is_err(),
            "the merge-edge publish fails, leaving a durable promotion intent",
        );
        db.close().expect("close succeeds");
    }

    let mut db = open_durable_database(dir.path()).expect("recovery reopens the database");
    // The promoted data is present, and recovery finalized its authoritative
    // lineage rather than exposing the data without it.
    assert_branch_value(&mut db, "default", "default", b"k", b"promoted");
    let summary = db
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .expect("default summary");
    let edge = summary
        .merge_parent()
        .expect("recovery finalized the merge edge");
    assert_eq!(edge.source_name().as_str(), "feature");

    // The source frontier captured in the recovered intent must survive
    // finalization: advancing the source and re-promoting diffs against
    // source-at-merge, so the new change applies cleanly instead of
    // false-conflicting against a fork base. (Regression guard that the
    // crash-recovery path carries the source-frontier version, not just the
    // live promote path.)
    {
        let mut kv = db
            .kv(branch("feature"), space("default"))
            .expect("kv opens");
        kv.put(key(b"k"), value(b"promoted2"))
            .expect("feature advances past the recovered merge");
    }
    let second = db
        .branches()
        .expect("branch service opens")
        .promote(
            &branch("feature"),
            &branch("default"),
            PromotionStrategy::Strict,
        )
        .expect("repeated promote after recovery must not false-conflict");
    assert!(second.conflicts().is_empty());
    assert_branch_value(&mut db, "default", "default", b"k", b"promoted2");
    db.close().expect("close succeeds");
}

/// Crash AFTER the promotion intent (commit 1) but BEFORE the target data commit
/// (commit 2): the live path clears the intent, so reopen finds no promotion in
/// flight — the target is unchanged and carries no lineage (#9, roll-back side).
#[cfg(feature = "localfs")]
#[test]
fn promotion_interrupted_before_data_commit_leaves_the_target_unchanged() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut db = open_durable_database(dir.path()).expect("durable opens");
        seed_one_sided(&mut db);

        db.inject_commit_fault_after_for_test(1, StorageFaultKind::Unavailable);
        assert!(
            db.branches()
                .expect("branch service opens")
                .promote(
                    &branch("feature"),
                    &branch("default"),
                    PromotionStrategy::Strict,
                )
                .is_err(),
            "the target data commit fails, rolling the promotion back",
        );
        db.close().expect("close succeeds");
    }

    let mut db = open_durable_database(dir.path()).expect("recovery reopens the database");
    // Nothing was promoted: the target keeps its pre-promotion value and records
    // no merge edge.
    assert_branch_value(&mut db, "default", "default", b"k", b"base");
    let summary = db
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .expect("default summary");
    assert!(
        summary.merge_parent().is_none(),
        "a rolled-back promotion must not record a merge edge",
    );
    db.close().expect("close succeeds");
}
