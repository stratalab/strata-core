//! Deleting a fork source while its children still depend on it (#3196).
//!
//! The durable runtime refuses, because a layer-less fork's rows are
//! re-materialized from its source at recovery — deleting the source arms a
//! permanent recovery failure. Cache mode has no recovery, so it deliberately
//! keeps unrestricted deletes.
//!
//! That divergence is a design decision, not a defect. What was a defect is
//! how the refusal reached the caller: `failed_precondition.engine.persistence`
//! — "persistence is temporarily unable to accept the request", hinting "wait
//! for the database to become ready, then retry". Nothing about waiting helps.
//! The caller has to delete the child first, and the refusal now says so.

mod common;

use strata_engine::{Database, KvKey, KvValue};

use common::{branch, open_cache_database, open_durable_database, space};

fn seed_and_fork(database: &mut Database) {
    // The source must not be the default branch: that has its own refusal, and
    // it would mask the one under test.
    database
        .branches()
        .expect("branch service")
        .create(branch("parent"))
        .expect("create the fork source");

    let version = database
        .kv(branch("parent"), space("default"))
        .expect("kv service")
        .put(
            KvKey::new("row").expect("key"),
            KvValue::new(b"one".to_vec()),
        )
        .expect("seed write")
        .commit()
        .version();
    database
        .kv(branch("parent"), space("default"))
        .expect("kv service")
        .put(
            KvKey::new("row").expect("key"),
            KvValue::new(b"two".to_vec()),
        )
        .expect("second write");

    database
        .branches()
        .expect("branch service")
        .fork_at_version(&branch("parent"), branch("child"), version)
        .expect("historical fork");
}

#[test]
fn durable_delete_of_a_fork_source_names_the_children_that_block_it() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open");
    seed_and_fork(&mut durable);

    let error = durable
        .branches()
        .expect("branch service")
        .delete(&branch("parent"))
        .expect_err("the fork source cannot be deleted while its child lives");
    let status = error.status();

    assert_eq!(
        status.code(),
        "failed_precondition.engine.branch_has_children"
    );
    let fix = status.suggested_fix();
    assert!(
        fix.to_lowercase().contains("child"),
        "the remedy must name the children, not tell the caller to wait: {fix:?}"
    );

    // Direction control: once the child is gone, the delete proceeds.
    durable
        .branches()
        .expect("branch service")
        .delete(&branch("child"))
        .expect("delete the child");
    durable
        .branches()
        .expect("branch service")
        .delete(&branch("parent"))
        .expect("the source is deletable once nothing depends on it");
}

/// Cache mode has no recovery to protect, so it keeps unrestricted deletes.
/// Pinned so the divergence stays a decision rather than an accident.
#[test]
fn cache_delete_of_a_fork_source_is_unrestricted() {
    let mut cache = open_cache_database().expect("cache open");
    seed_and_fork(&mut cache);

    cache
        .branches()
        .expect("branch service")
        .delete(&branch("parent"))
        .expect("cache mode deletes a fork source while its child lives");
}
