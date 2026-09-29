//! Product branch semantics tests.

mod common;

use strata_core::{CommitVersion, Timestamp};
use strata_engine::{
    BranchName, BranchStatus, CacheOpenOptions, Database, DurableLocalOpenOptions, EngineErrorClass,
};

use common::{
    assert_branch_value, assert_no_storage_type_in_engine_error, branch, key, open_cache_database,
    open_durable_database, reopen_durable_database_after_drop, reopen_local_after_drop, space,
    value,
};

#[test]
fn branch_ids_match_stable_product_derivation() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let default = database
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .expect("default branch exists");
    assert_eq!(default.branch_id().as_bytes(), &[0; 16]);

    let main = database
        .branches()
        .expect("branch service opens")
        .create(branch("main"))
        .expect("main branch created");
    assert_eq!(
        main.branch().branch_id().as_bytes(),
        &[
            0x1f, 0x64, 0xc0, 0x67, 0xac, 0xf2, 0x50, 0x34, 0xa5, 0xd0, 0x92, 0xd5, 0x76, 0x41,
            0x38, 0xec,
        ]
    );

    let uuid_name = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    let uuid = database
        .branches()
        .expect("branch service opens")
        .create(branch(uuid_name))
        .expect("UUID branch created");
    assert_eq!(
        uuid.branch().branch_id().as_bytes(),
        &[
            0xf4, 0x7a, 0xc1, 0x0b, 0x58, 0xcc, 0x43, 0x72, 0xa5, 0x67, 0x0e, 0x02, 0xb2, 0xc3,
            0xd4, 0x79,
        ]
    );
}

#[test]
fn branch_name_validation_rejects_reserved_aliases_and_control_names() {
    for rejected in [
        "",
        "   ",
        "_system_",
        "_internal",
        "main\nline",
        "00000000-0000-0000-0000-000000000000",
        "01010101-0101-0101-0101-010101010101",
        "f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0",
    ] {
        let error = BranchName::new(rejected).expect_err("branch name rejected");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_no_storage_type_in_engine_error(&error);
    }
}

#[test]
fn branch_names_that_alias_another_branchs_storage_identity_are_rejected() {
    // "main" derives the v5 id 1f64c067-…-38ec; the UUID-form name equal to
    // those bytes derives the same id, so the two would share a storage branch
    // (finding U8). Whichever is created second must be rejected with a
    // structured already_exists error, not a raw storage generation conflict.
    let aliasing_uuid = "1f64c067-acf2-5034-a5d0-92d5764138ec";

    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .create(branch("main"))
        .expect("main branch created");
    let error = database
        .branches()
        .expect("branch service opens")
        .create(branch(aliasing_uuid))
        .expect_err("aliasing UUID-form branch rejected");
    assert_eq!(error.code(), "already_exists.engine.branch");

    // The collision is also detected against a deleted branch: recreate the
    // scenario in reverse and delete the first branch before the second create.
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .create(branch(aliasing_uuid))
        .expect("UUID branch created");
    database
        .branches()
        .expect("branch service opens")
        .delete(&branch(aliasing_uuid))
        .expect("UUID branch deleted");
    let error = database
        .branches()
        .expect("branch service opens")
        .create(branch("main"))
        .expect_err("aliasing name rejected even against a deleted branch");
    assert_eq!(error.code(), "already_exists.engine.branch");
}

#[test]
fn reserved_branch_identity_aliases_cannot_be_selected_as_defaults() {
    for rejected in [
        "01010101-0101-0101-0101-010101010101",
        "f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0",
    ] {
        let error = CacheOpenOptions::new()
            .with_default_branch(rejected)
            .expect_err("reserved default alias rejected");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_no_storage_type_in_engine_error(&error);
    }
}

#[test]
fn root_branch_create_starts_empty_and_isolated() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"shared"), value(b"default"))
        .expect("default put succeeds");

    let created = database
        .branches()
        .expect("branch service opens")
        .create(branch("scratch"))
        .expect("root branch created");
    assert_eq!(created.branch().name().as_str(), "scratch");
    assert_eq!(created.branch().generation(), 1);
    assert_eq!(created.branch().status(), BranchStatus::Active);
    assert!(created.branch().parent().is_none());

    assert!(database
        .kv(branch("scratch"), space("default"))
        .expect("scratch KV opens")
        .get(&key(b"shared"))
        .expect("scratch read succeeds")
        .is_none());
    database
        .kv(branch("scratch"), space("default"))
        .expect("scratch KV opens")
        .put(key(b"scratch-only"), value(b"scratch"))
        .expect("scratch put succeeds");
    assert_branch_value(
        &mut database,
        "scratch",
        "default",
        b"scratch-only",
        b"scratch",
    );
    assert!(database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .get(&key(b"scratch-only"))
        .expect("default read succeeds")
        .is_none());
}

#[test]
fn durable_reopen_after_branch_delete_remains_usable() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut database = open_durable_database(dir.path()).expect("durable open succeeds");
        database
            .branches()
            .expect("branch service opens")
            .create(branch("scratch"))
            .expect("branch created");
        database
            .branches()
            .expect("branch service opens")
            .delete(&branch("scratch"))
            .expect("branch deleted");
    }

    // The first handle was dropped, not closed: wait out a detached worker's
    // writer lock (#2837, #3546).
    let mut reopened =
        reopen_durable_database_after_drop(dir.path()).expect("durable reopen succeeds");
    reopened
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("docs"))
        .expect("space create succeeds after branch delete");
    assert!(reopened
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("docs"))
        .expect("space exists succeeds"));
}

#[test]
fn fork_current_from_empty_source_still_reports_parent() {
    let mut database = open_cache_database().expect("cache open succeeds");

    let forked = database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("empty source fork succeeds");
    let parent = forked.branch().parent().expect("parent facts");
    assert_eq!(parent.name().as_str(), "default");
    assert_eq!(parent.branch_id().as_bytes(), &[0; 16]);
    assert_eq!(parent.generation(), 1);
    assert!(parent.fork_version() >= CommitVersion::new(1));

    database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"late"), value(b"default"))
        .expect("default put succeeds");
    assert!(database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens")
        .get(&key(b"late"))
        .expect("feature read succeeds")
        .is_none());
}

#[test]
fn fork_current_inherits_source_frontier_and_reports_parent() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"shared"), value(b"base"))
        .expect("base put succeeds");

    let forked = database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork succeeds");
    let parent = forked.branch().parent().expect("parent facts");
    assert_eq!(parent.name().as_str(), "default");
    assert_eq!(parent.generation(), 1);
    assert!(parent.fork_version() >= CommitVersion::new(1));

    assert_branch_value(&mut database, "feature", "default", b"shared", b"base");
    database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"late"), value(b"default"))
        .expect("late put succeeds");
    assert!(database
        .kv(branch("feature"), space("default"))
        .expect("feature KV opens")
        .get(&key(b"late"))
        .expect("feature read succeeds")
        .is_none());
}

#[test]
fn historical_forks_resolve_version_and_timestamp_boundaries() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let first = database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"history"), value(b"one"))
        .expect("first put succeeds")
        .commit();
    database
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(b"history"), value(b"two"))
        .expect("second put succeeds");

    let by_version = database
        .branches()
        .expect("branch service opens")
        .fork_at_version(&branch("default"), branch("by-version"), first.version())
        .expect("fork at version succeeds");
    assert_eq!(
        by_version
            .branch()
            .parent()
            .expect("parent facts")
            .fork_version(),
        first.version()
    );
    assert_branch_value(&mut database, "by-version", "default", b"history", b"one");

    let by_timestamp = database
        .branches()
        .expect("branch service opens")
        .fork_at_timestamp(
            &branch("default"),
            branch("by-timestamp"),
            Timestamp::from_micros(first.timestamp().as_micros()),
        )
        .expect("fork at timestamp succeeds");
    assert_eq!(
        by_timestamp
            .branch()
            .parent()
            .expect("parent facts")
            .fork_timestamp(),
        Some(first.timestamp())
    );
    assert_branch_value(&mut database, "by-timestamp", "default", b"history", b"one");
}

#[test]
fn delete_and_recreate_advances_generation_without_resurrecting_rows() {
    let mut database = open_cache_database().expect("cache open succeeds");
    database
        .branches()
        .expect("branch service opens")
        .create(branch("scratch"))
        .expect("scratch created");
    database
        .kv(branch("scratch"), space("default"))
        .expect("scratch KV opens")
        .put(key(b"old"), value(b"old"))
        .expect("old put succeeds");

    let deleted = database
        .branches()
        .expect("branch service opens")
        .delete(&branch("scratch"))
        .expect("scratch deleted");
    assert_eq!(deleted.branch().status(), BranchStatus::Deleted);
    assert_eq!(deleted.generation_before(), Some(1));
    assert_eq!(deleted.generation_after(), Some(1));
    assert!(database
        .branches()
        .expect("branch service opens")
        .list()
        .expect("branch list succeeds")
        .iter()
        .all(|summary| summary.name().as_str() != "scratch"));

    let error = database
        .kv(branch("scratch"), space("default"))
        .map(|_| ())
        .expect_err("deleted branch write rejected at service construction");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");

    let recreated = database
        .branches()
        .expect("branch service opens")
        .create(branch("scratch"))
        .expect("scratch recreated");
    assert_eq!(recreated.branch().generation(), 2);
    assert!(database
        .kv(branch("scratch"), space("default"))
        .expect("scratch KV opens")
        .get(&key(b"old"))
        .expect("scratch read succeeds")
        .is_none());
}

#[test]
fn configured_default_branch_is_protected_and_persisted() {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("db");
    let options = DurableLocalOpenOptions::new()
        .with_default_branch("main")
        .expect("valid default branch");

    {
        let mut database = Database::open_local(&path, options)
            .expect("durable open succeeds")
            .into_database();
        assert_eq!(database.default_branch().as_str(), "main");
        database
            .branches()
            .expect("branch service opens")
            .create(branch("spare"))
            .expect("spare branch created");

        let error = database
            .branches()
            .expect("branch service opens")
            .delete(&branch("main"))
            .expect_err("default branch delete rejected");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.branch_delete");
        database.close().expect("close succeeds");
    }

    {
        let mut database = Database::open_local(&path, DurableLocalOpenOptions::new())
            .expect("durable reopen succeeds")
            .into_database();
        assert_eq!(database.default_branch().as_str(), "main");
        assert!(database
            .branches()
            .expect("branch service opens")
            .get(&branch("main"))
            .is_ok());
    }

    let conflicting = DurableLocalOpenOptions::new()
        .with_default_branch("other")
        .expect("valid default branch");
    // The handle above was dropped, not closed (#2837, #3546).
    let Err(error) = reopen_local_after_drop(&path, &conflicting) else {
        panic!("conflicting default branch rejected");
    };
    assert_eq!(error.class(), EngineErrorClass::IncompatibleLayout);
    assert_eq!(error.code(), "failed_precondition.engine.default_branch");
}

#[test]
fn cache_open_can_select_non_literal_default_branch() {
    let options = CacheOpenOptions::new()
        .with_default_branch("main")
        .expect("valid default branch");
    let mut database = Database::open_cache(options)
        .expect("cache open succeeds")
        .into_database();

    assert_eq!(database.default_branch().as_str(), "main");
    assert!(database
        .branches()
        .expect("branch service opens")
        .get(&branch("main"))
        .is_ok());
    assert!(database
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .is_err());
}
