//! Runtime lifecycle tests for the database handle.

mod common;

use common::{assert_no_storage_leak, assert_status, branch, open_cache_database, space};
#[cfg(feature = "localfs")]
use common::{key, open_durable_database, value};
#[cfg(feature = "localfs")]
use strata_engine::DurableLocalOpenOptions;
use strata_engine::{CacheOpenOptions, Database, EngineError, EngineErrorClass};

fn assert_closed(error: Option<EngineError>) {
    let error = error.expect("accessor must reject a closed handle");
    assert_status(
        &error,
        EngineErrorClass::ClosedRuntime,
        "failed_precondition.engine.runtime_closed",
        false,
    );
}

/// Every service accessor — data capabilities, branch, space, admin, and
/// diagnostics alike — rejects a closed handle before doing any work.
#[test]
fn every_accessor_rejects_after_close() {
    let mut db = open_cache_database().expect("cache database opens");
    db.close().expect("close succeeds");

    assert_closed(db.branches().err());
    assert_closed(db.kv(branch("default"), space("default")).err());
    assert_closed(db.json(branch("default"), space("default")).err());
    assert_closed(db.vector(branch("default"), space("default")).err());
    assert_closed(db.event(branch("default"), space("default")).err());
    assert_closed(db.graph(branch("default"), space("default")).err());
    assert_closed(db.spaces(branch("default")).err());
    assert_closed(db.admin().err());
    assert_closed(db.control_diagnostics(None).err());
}

/// Closing twice is idempotent and the second close reports it.
#[test]
fn second_close_is_idempotent() {
    let mut db = open_cache_database().expect("cache database opens");
    let first = db.close().expect("first close succeeds");
    assert!(!first.idempotent());
    let second = db.close().expect("second close succeeds");
    assert!(second.idempotent());
}

/// A configured non-default branch is the database default, is created at
/// generation 1, and the literal `default` branch is not created.
#[test]
fn open_with_configured_default_branch_is_reflected() {
    let options = CacheOpenOptions::new()
        .with_default_branch("main")
        .expect("valid default branch");
    let mut db = Database::open_cache(options)
        .expect("cache opens")
        .into_database();
    assert_eq!(db.default_branch().as_str(), "main");

    {
        let mut admin = db.admin().expect("admin opens");
        assert_eq!(
            admin
                .info(None)
                .expect("info succeeds")
                .default_branch
                .as_str(),
            "main"
        );
    }

    let branches = db.branches().expect("branch service opens");
    assert_eq!(
        branches
            .get(&branch("main"))
            .expect("configured default exists")
            .generation(),
        1
    );
    let error = branches
        .get(&branch("default"))
        .expect_err("the literal default branch was not created");
    assert_status(
        &error,
        EngineErrorClass::NotFound,
        "not_found.engine.branch",
        false,
    );
}

/// Reopening a durable database with a different default branch is rejected,
/// while reopening with the persisted default succeeds.
#[cfg(feature = "localfs")]
#[test]
fn reopen_with_mismatched_default_branch_is_rejected() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let options = DurableLocalOpenOptions::new()
            .with_default_branch("main")
            .expect("valid default branch");
        let mut db = Database::open_local(dir.path(), options)
            .expect("durable opens")
            .into_database();
        db.close().expect("close succeeds");
    }

    let mismatched = DurableLocalOpenOptions::new()
        .with_default_branch("other")
        .expect("valid default branch");
    let error = Database::open_local(dir.path(), mismatched)
        .err()
        .expect("a mismatched default is rejected");
    assert_status(
        &error,
        EngineErrorClass::IncompatibleLayout,
        "failed_precondition.engine.default_branch",
        false,
    );

    let matching = DurableLocalOpenOptions::new()
        .with_default_branch("main")
        .expect("valid default branch");
    let db = Database::open_local(dir.path(), matching)
        .expect("matching default reopens")
        .into_database();
    assert_eq!(db.default_branch().as_str(), "main");
}

/// A memory budget below the minimum supported size is rejected at open, with a
/// product error that does not leak storage internals.
#[test]
fn memory_budget_below_minimum_is_rejected() {
    let options = CacheOpenOptions::new().with_memory_budget(1);
    let error = Database::open_cache(options)
        .err()
        .expect("an undersized budget is rejected");
    assert_status(
        &error,
        EngineErrorClass::InvalidInput,
        "invalid_argument.engine.persistence",
        false,
    );
    assert_no_storage_leak(&error);

    let options = CacheOpenOptions::new().with_memory_budget(8 * 1024 * 1024);
    Database::open_cache(options).expect("a sufficient budget opens");
}

/// A committed write survives dropping the durable handle without an explicit
/// close — commits are durable on success, so close is not required to persist.
#[cfg(feature = "localfs")]
#[test]
fn dropped_durable_handle_preserves_committed_data() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let db = open_durable_database(dir.path()).expect("durable opens");
        db.kv(branch("default"), space("default"))
            .expect("kv opens")
            .put(key(b"k"), value(b"v"))
            .expect("put commits");
        // The handle is dropped here without an explicit close().
    }

    let db = open_durable_database(dir.path()).expect("durable reopens");
    let stored = db
        .kv(branch("default"), space("default"))
        .expect("kv opens")
        .get(&key(b"k"))
        .expect("read succeeds");
    assert_eq!(
        stored.expect("committed value survives drop").as_bytes(),
        b"v"
    );
}

/// Closing a durable database reports a synced, durable outcome.
#[cfg(feature = "localfs")]
#[test]
fn durable_close_reports_synced() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut db = open_durable_database(dir.path()).expect("durable opens");
    let close = db.close().expect("close succeeds");
    assert!(close.durable());
    assert!(close.durable_synced());
}
