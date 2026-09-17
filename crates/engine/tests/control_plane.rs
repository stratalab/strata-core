//! Control-plane bootstrap and durable reopen tests.

mod common;

#[cfg(feature = "localfs")]
use strata_engine::DurableLocalOpenOptions;
use strata_engine::{
    CacheOpenOptions, ControlHealthStatus, Database, DatabaseOpenTarget, EngineErrorClass,
    ProductSpace,
};

#[cfg(feature = "localfs")]
use common::assert_branch_value;
use common::{assert_default_branch_exists, branch, key, space, value};

#[test]
fn cache_open_bootstraps_default_branch() {
    let outcome = Database::open_cache(CacheOpenOptions::new()).expect("cache open succeeds");
    assert_eq!(outcome.summary().target(), DatabaseOpenTarget::Cache);
    assert!(outcome.summary().created());
    assert!(!outcome.summary().durable());

    let mut database = outcome.into_database();
    assert_default_branch_exists(&mut database);
    let close = database.close().expect("cache close succeeds");
    assert!(!close.durable());
    assert!(!close.durable_synced());
    assert!(!close.idempotent());

    let close = database.close().expect("second cache close succeeds");
    assert!(!close.durable());
    assert!(!close.durable_synced());
    assert!(close.idempotent());
}

#[cfg(feature = "localfs")]
#[test]
fn durable_open_reopen_preserves_control_plane_and_kv() {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("db");

    {
        let outcome = Database::open_local(&path, DurableLocalOpenOptions::new())
            .expect("durable open succeeds");
        assert_eq!(outcome.summary().target(), DatabaseOpenTarget::DurableLocal);
        assert!(outcome.summary().created());
        assert!(outcome.summary().durable());

        let mut database = outcome.into_database();
        database
            .kv(branch("default"), space("default"))
            .expect("KV service opens")
            .put(key(b"key-a"), value(b"value-a"))
            .expect("put succeeds");
        database
            .kv(branch("default"), space("default"))
            .expect("KV service opens")
            .put_batch([
                (key(b"batch-a"), value(b"batch-value-a")),
                (key(b"batch-b"), value(b"batch-value-b")),
            ])
            .expect("batch put succeeds");
        database
            .branches()
            .expect("branch service opens")
            .fork_current(&branch("default"), branch("feature"))
            .expect("branch create succeeds");
        database
            .kv(branch("feature"), space("default"))
            .expect("KV service opens")
            .put(key(b"key-b"), value(b"value-b"))
            .expect("feature put succeeds");
        let close = database.close().expect("close succeeds");
        assert!(close.durable());
        assert!(!close.idempotent());
    }

    {
        let outcome = Database::open_local(&path, DurableLocalOpenOptions::new())
            .expect("durable reopen succeeds");
        assert_eq!(outcome.summary().target(), DatabaseOpenTarget::DurableLocal);
        assert!(!outcome.summary().created());
        assert!(outcome.summary().durable());

        let mut database = outcome.into_database();
        let branches = database
            .branches()
            .expect("branch service opens")
            .list()
            .expect("branch list succeeds");
        assert!(branches
            .iter()
            .any(|branch| branch.name().as_str() == "default"));
        assert!(branches
            .iter()
            .any(|branch| branch.name().as_str() == "feature"));
        assert_branch_value(&mut database, "default", "default", b"key-a", b"value-a");
        assert_branch_value(
            &mut database,
            "default",
            "default",
            b"batch-a",
            b"batch-value-a",
        );
        assert_branch_value(
            &mut database,
            "default",
            "default",
            b"batch-b",
            b"batch-value-b",
        );
        assert_branch_value(&mut database, "feature", "default", b"key-b", b"value-b");
    }
}

#[cfg(feature = "localfs")]
#[test]
fn durable_reopen_validates_registered_user_space_catalog() {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("db");

    {
        let database = Database::open_local(&path, DurableLocalOpenOptions::new())
            .expect("durable open succeeds")
            .into_database();
        database
            .kv(branch("default"), space("tenant"))
            .expect("tenant KV service opens")
            .put(key(b"tenant-key"), value(b"tenant-value"))
            .expect("tenant put succeeds");
    }

    {
        let mut database = Database::open_local(&path, DurableLocalOpenOptions::new())
            .expect("durable reopen validates space catalog")
            .into_database();
        assert_branch_value(
            &mut database,
            "default",
            "tenant",
            b"tenant-key",
            b"tenant-value",
        );
    }
}

#[test]
fn control_diagnostics_report_core_health_and_requested_space_catalog() {
    let mut database = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database();
    let default_branch = branch("default");

    let diagnostics = database
        .control_diagnostics(Some(&default_branch))
        .expect("control diagnostics succeed");
    assert_eq!(diagnostics.identity_status(), ControlHealthStatus::Healthy);
    assert_eq!(diagnostics.registry_status(), ControlHealthStatus::Healthy);
    assert_eq!(
        diagnostics.branch_catalog_status(),
        ControlHealthStatus::Healthy
    );
    assert_eq!(diagnostics.default_branch().as_str(), "default");
    assert_eq!(diagnostics.active_branch_count(), 1);
    let space_catalog = diagnostics
        .space_catalog()
        .expect("space catalog diagnostics present");
    assert_eq!(space_catalog.branch().as_str(), "default");
    assert_eq!(space_catalog.status(), ControlHealthStatus::Healthy);
    assert_eq!(space_catalog.space_count(), Some(1));

    database
        .kv(branch("default"), space("tenant"))
        .expect("tenant KV service opens")
        .put(key(b"tenant-key"), value(b"tenant-value"))
        .expect("tenant put succeeds");
    let diagnostics = database
        .control_diagnostics(Some(&default_branch))
        .expect("control diagnostics succeed");
    assert_eq!(
        diagnostics
            .space_catalog()
            .expect("space catalog diagnostics present")
            .space_count(),
        Some(2)
    );

    let missing_branch = branch("missing");
    let diagnostics = database
        .control_diagnostics(Some(&missing_branch))
        .expect("missing branch diagnostics succeed");
    let space_catalog = diagnostics
        .space_catalog()
        .expect("space catalog diagnostics present");
    assert_eq!(space_catalog.branch().as_str(), "missing");
    assert_eq!(space_catalog.status(), ControlHealthStatus::Missing);
    assert_eq!(space_catalog.space_count(), None);
}

#[test]
fn control_diagnostics_report_branch_local_space_catalog_for_create_and_fork() {
    let mut database = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database();

    database
        .kv(branch("default"), space("tenant"))
        .expect("tenant KV service opens")
        .put(key(b"tenant-key"), value(b"tenant-value"))
        .expect("tenant put succeeds");
    database
        .branches()
        .expect("branch service opens")
        .create(branch("scratch"))
        .expect("empty branch create succeeds");
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("child"))
        .expect("forked branch create succeeds");

    let default_branch = branch("default");
    let scratch_branch = branch("scratch");
    let child_branch = branch("child");
    let default_diagnostics = database
        .control_diagnostics(Some(&default_branch))
        .expect("default diagnostics succeed");
    let scratch_diagnostics = database
        .control_diagnostics(Some(&scratch_branch))
        .expect("scratch diagnostics succeed");
    let child_diagnostics = database
        .control_diagnostics(Some(&child_branch))
        .expect("child diagnostics succeed");

    assert_eq!(
        default_diagnostics
            .space_catalog()
            .expect("default space diagnostics present")
            .space_count(),
        Some(2)
    );
    assert_eq!(
        scratch_diagnostics
            .space_catalog()
            .expect("scratch space diagnostics present")
            .space_count(),
        Some(1)
    );
    assert_eq!(
        child_diagnostics
            .space_catalog()
            .expect("child space diagnostics present")
            .space_count(),
        Some(2)
    );
}

#[test]
fn control_diagnostics_do_not_expose_raw_keys_or_deferred_systems() {
    let mut database = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database();
    let diagnostics = database
        .control_diagnostics(Some(&branch("default")))
        .expect("control diagnostics succeed");
    let text = format!("{diagnostics:?}");

    for forbidden in [
        "identity:local-instance",
        "registry:storage-spaces",
        "registry:migrations",
        "branch:index",
        "branch:default",
        "space:index",
        "space:reserved",
        "recipe",
        "search",
        "retrieval",
        "shadow",
        "derived",
        "cache",
    ] {
        assert!(
            !text.contains(forbidden),
            "diagnostics exposed deferred or raw control detail: {forbidden}"
        );
    }
}

#[test]
fn system_branch_is_not_listed() {
    let mut database = Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database();
    let branches = database
        .branches()
        .expect("branch service opens")
        .list()
        .expect("branch list succeeds");
    assert!(branches
        .iter()
        .all(|summary| summary.name().as_str() != "_system_"));
}

#[test]
fn system_branch_is_reserved_for_product_apis() {
    let error =
        strata_engine::BranchName::new("_system_").expect_err("system branch name is reserved");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.branch_name_reserved");
}

#[test]
fn system_space_is_reserved_for_product_apis() {
    let error = ProductSpace::new("_system_").expect_err("system space is reserved");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.product_space_reserved"
    );
}
