//! Runtime integration coverage for the public `strata_executor::Strata` handle.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};

use tempfile::tempdir;

use strata_core::id::CommitVersion;
use strata_core::{BranchId, Value};
use strata_engine::database::{OpenSpec, OPEN_DATABASES};
use strata_engine::{Database, SearchSubsystem};
use strata_executor::{
    AccessMode, Command, Error, IpcServer, OpenOptions, Output, Session, Strata,
};
use strata_storage::{Key, Namespace};
use strata_vector::VectorSubsystem;

static PRODUCT_OPEN_TEST_LOCK: Mutex<()> = Mutex::new(());
const LOCKED_WITHOUT_SOCKET_MESSAGE: &str = "Database is locked by another process. Run `strata up` to enable shared access, or use --follower for read-only access.";

fn product_open_guard() -> MutexGuard<'static, ()> {
    PRODUCT_OPEN_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn output_has_int(output: &Output, expected: i64) -> bool {
    match output {
        Output::Maybe(Some(Value::Int(value))) => *value == expected,
        Output::MaybeVersioned(Some(versioned)) => versioned.value == Value::Int(expected),
        _ => false,
    }
}

fn event_payload(key: &str, value: Value) -> Value {
    Value::object([(key.to_string(), value)].into_iter().collect())
}

fn assert_access_denied(error: Error) {
    assert!(
        matches!(error, Error::AccessDenied { .. }),
        "expected access denied, got {error:?}"
    );
}

fn assert_session_write_denied(session: &mut Session) {
    let error = session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "blocked-session-write".into(),
            value: Value::Int(1),
        })
        .expect_err("read-only session should reject writes");
    assert_access_denied(error);
}

fn assert_builtin_recipes_available(session: &mut Session) {
    let output = session
        .execute(Command::RecipeList { branch: None })
        .expect("recipe list should succeed");
    match output {
        Output::Keys(names) => {
            for expected in ["default", "keyword", "semantic", "hybrid", "graph", "rag"] {
                assert!(
                    names.contains(&expected.to_string()),
                    "missing built-in recipe {expected}; got {names:?}"
                );
            }
        }
        other => panic!("expected recipe names, got {other:?}"),
    }
}

fn assert_product_subsystems(database: &Arc<Database>) {
    assert_eq!(
        database.installed_subsystem_names(),
        ["graph", "vector", "search"],
        "product open should preserve graph/vector/search subsystem order"
    );
}

fn assert_branch_metadata_absent(database: &Arc<Database>, branch: &str) {
    let namespace = Arc::new(Namespace::for_branch(BranchId::from_bytes([0; 16])));
    let key = Key::new_branch_with_id(namespace, branch);
    let stored = database
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .expect("branch metadata read should succeed");
    assert!(
        stored.is_none(),
        "follower open should not create branch metadata for {branch}"
    );
}

fn create_disk_db_with_default_branch(
    path: &std::path::Path,
    default_branch: &str,
) -> std::sync::Arc<Database> {
    Database::open_runtime(
        OpenSpec::primary(path)
            .with_subsystem(strata_engine::GraphSubsystem)
            .with_subsystem(VectorSubsystem)
            .with_subsystem(SearchSubsystem)
            .with_default_branch(default_branch),
    )
    .expect("disk database should open")
}

fn create_disk_db_without_default_branch(path: &std::path::Path) -> std::sync::Arc<Database> {
    Database::open_runtime(
        OpenSpec::primary(path)
            .with_subsystem(strata_engine::GraphSubsystem)
            .with_subsystem(VectorSubsystem)
            .with_subsystem(SearchSubsystem),
    )
    .expect("disk database should open")
}

#[test]
fn strata_handle_uses_current_context_for_typed_methods() {
    let _guard = product_open_guard();
    let mut db = Strata::cache().expect("cache db should open");
    let database = db.database();
    assert_product_subsystems(&database);
    db.set_space("analytics").expect("space should update");

    let version = db
        .kv_put("status", Value::String("ok".into()))
        .expect("kv put should succeed");
    assert!(version > 0);

    db.json_set("doc:1", "$", Value::Bool(true))
        .expect("json set should succeed");
    db.event_append("system.init", event_payload("ok", Value::Bool(true)))
        .expect("event append should succeed");

    let value = db.kv_get("status").expect("kv get should succeed");
    assert_eq!(value, Some(Value::String("ok".into())));

    let mut session = Session::new(db.database());

    let default_kv = session
        .execute(Command::KvGet {
            branch: Some("default".into()),
            space: Some("default".into()),
            key: "status".into(),
            as_of: None,
        })
        .expect("default-space kv read should succeed");
    assert!(matches!(
        default_kv,
        Output::Maybe(None) | Output::MaybeVersioned(None)
    ));

    let analytics_json = session
        .execute(Command::JsonGet {
            branch: Some("default".into()),
            space: Some("analytics".into()),
            key: "doc:1".into(),
            path: "$".into(),
            as_of: None,
        })
        .expect("analytics json read should succeed");
    match analytics_json {
        Output::Maybe(Some(Value::Bool(true))) => {}
        Output::MaybeVersioned(Some(versioned)) => {
            assert_eq!(versioned.value, Value::Bool(true));
        }
        other => panic!("expected bool JSON value, got {other:?}"),
    }

    let default_events = session
        .execute(Command::EventGetByType {
            branch: Some("default".into()),
            space: Some("default".into()),
            event_type: "system.init".into(),
            limit: None,
            after_sequence: None,
            as_of: None,
        })
        .expect("default-space event read should succeed");
    match default_events {
        Output::VersionedValues(events) => assert!(events.is_empty()),
        other => panic!("expected VersionedValues, got {other:?}"),
    }

    let analytics_events = session
        .execute(Command::EventGetByType {
            branch: Some("default".into()),
            space: Some("analytics".into()),
            event_type: "system.init".into(),
            limit: None,
            after_sequence: None,
            as_of: None,
        })
        .expect("analytics event read should succeed");
    match analytics_events {
        Output::VersionedValues(events) => assert_eq!(events.len(), 1),
        other => panic!("expected VersionedValues, got {other:?}"),
    }
}

// These tests intentionally clear the engine's in-process open registry to
// force `Strata::open()` down the IPC path. Keeping them in a separate
// integration binary prevents that global mutation from interfering with the
// main executor command/session suite.

#[test]
fn ipc_open_and_transaction_round_trip_use_new_session_path() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    let database = primary.database();
    let access_mode = primary.access_mode();

    let mut server =
        IpcServer::start(dir.path(), database, access_mode).expect("server should start");
    OPEN_DATABASES.lock().clear();
    let mut remote = Strata::open(dir.path()).expect("remote open should use ipc");
    assert!(remote.is_ipc(), "handle should be IPC-backed");
    remote
        .set_space("analytics")
        .expect("space should update on remote handle");

    let mut session = remote.session().expect("session should open");
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .expect("transaction should begin");
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "counter".into(),
            value: Value::Int(9),
        })
        .expect("remote kv put should succeed");

    let output = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "counter".into(),
            as_of: None,
        })
        .expect("remote kv get should succeed");
    assert!(output_has_int(&output, 9));

    session
        .execute(Command::TxnCommit)
        .expect("transaction should commit");

    let mut check = Session::new(primary.database());
    let output = check
        .execute(Command::KvGet {
            branch: Some("default".into()),
            space: Some("analytics".into()),
            key: "counter".into(),
            as_of: None,
        })
        .expect("primary read should succeed");
    assert!(output_has_int(&output, 9));

    server.shutdown();
}

#[test]
fn ipc_handle_typed_methods_round_trip() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    let database = primary.database();
    let access_mode = primary.access_mode();

    let mut server =
        IpcServer::start(dir.path(), database, access_mode).expect("server should start");
    OPEN_DATABASES.lock().clear();
    let mut remote = Strata::open(dir.path()).expect("remote open should use ipc");
    assert!(remote.is_ipc(), "handle should be IPC-backed");
    remote
        .set_space("analytics")
        .expect("space should update on remote handle");

    let version = remote
        .kv_put("typed-counter", Value::Int(11))
        .expect("typed kv put should succeed");
    assert!(version > 0);
    remote
        .json_set("typed-doc", "$", Value::Bool(true))
        .expect("typed json set should succeed");
    remote
        .event_append("typed.audit", event_payload("ok", Value::Bool(true)))
        .expect("typed event append should succeed");

    assert_eq!(
        remote
            .kv_get("typed-counter")
            .expect("typed kv get should succeed"),
        Some(Value::Int(11))
    );

    let mut session = remote.session().expect("remote session should open");
    let json_output = session
        .execute(Command::JsonGet {
            branch: None,
            space: None,
            key: "typed-doc".into(),
            path: "$".into(),
            as_of: None,
        })
        .expect("typed json read should succeed");
    match json_output {
        Output::Maybe(Some(Value::Bool(true))) => {}
        Output::MaybeVersioned(Some(versioned)) => {
            assert_eq!(versioned.value, Value::Bool(true));
        }
        other => panic!("expected bool JSON value, got {other:?}"),
    }

    let event_output = session
        .execute(Command::EventGetByType {
            branch: None,
            space: None,
            event_type: "typed.audit".into(),
            limit: None,
            after_sequence: None,
            as_of: None,
        })
        .expect("typed event read should succeed");
    match event_output {
        Output::VersionedValues(events) => assert_eq!(events.len(), 1),
        other => panic!("expected VersionedValues, got {other:?}"),
    }

    server.shutdown();
}

#[test]
fn ipc_session_returns_error_after_server_shutdown() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    let database = primary.database();
    let access_mode = primary.access_mode();

    let mut server =
        IpcServer::start(dir.path(), database, access_mode).expect("server should start");
    OPEN_DATABASES.lock().clear();
    let remote = Strata::open(dir.path()).expect("remote open should use ipc");
    assert!(remote.is_ipc(), "handle should be IPC-backed");
    server.shutdown();

    let error = match remote.session() {
        Ok(_) => panic!("session creation should fail after shutdown"),
        Err(error) => error,
    };
    assert!(matches!(error, strata_executor::Error::Io { .. }));
}

#[test]
fn ipc_shutdown_cleans_up_socket_and_pid_files_and_allows_restart() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    let database = primary.database();
    let access_mode = primary.access_mode();
    drop(primary);

    let socket_path = dir.path().join("strata.sock");
    let pid_path = dir.path().join("strata.pid");

    let mut server =
        IpcServer::start(dir.path(), database.clone(), access_mode).expect("server should start");
    assert!(
        socket_path.exists(),
        "socket file should exist while running"
    );
    assert!(pid_path.exists(), "pid file should exist while running");

    server.shutdown();
    assert!(
        !socket_path.exists(),
        "shutdown should remove the socket file before drop"
    );
    assert!(
        !pid_path.exists(),
        "shutdown should remove the pid file before drop"
    );

    let mut restarted =
        IpcServer::start(dir.path(), database, access_mode).expect("server should restart cleanly");
    restarted.shutdown();
}

#[test]
fn new_handle_uses_runtime_default_branch_and_independent_context() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let seeded = create_disk_db_with_default_branch(dir.path(), "main");
    seeded
        .shutdown()
        .expect("seeded database should shut down cleanly");
    drop(seeded);

    let mut db = Strata::open(dir.path()).expect("runtime handle should open");
    assert_eq!(db.current_branch(), "main");
    assert_eq!(db.info().expect("info should load").default_branch, "main");

    let mut admin = db.session().expect("session should open");
    admin
        .execute(Command::BranchCreate {
            branch_id: Some("feature".into()),
            metadata: None,
        })
        .expect("feature branch should be created");

    db.set_branch("feature")
        .expect("feature should become current");
    db.set_space("analytics")
        .expect("analytics should be valid");
    db.kv_put("scoped", Value::Int(7))
        .expect("scoped write should succeed");

    let handle = db.new_handle().expect("new handle should open");
    assert_eq!(handle.current_branch(), "main");
    assert_eq!(handle.current_space(), "default");
    assert_eq!(
        handle
            .kv_get("scoped")
            .expect("main/default read should succeed"),
        None
    );

    handle
        .kv_put("global", Value::Int(9))
        .expect("main/default write should succeed");
    assert_eq!(
        db.kv_get("global")
            .expect("feature/analytics read should succeed"),
        None
    );
}

#[test]
fn close_releases_local_runtime_for_clean_reopen() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");

    let db = Strata::open(dir.path()).expect("primary db should open");
    let database = db.database();
    assert!(
        !database.is_follower(),
        "default disk open should be primary"
    );
    assert_product_subsystems(&database);
    assert!(!db.is_ipc(), "default disk open should be local");
    assert_eq!(db.access_mode(), AccessMode::ReadWrite);
    assert_eq!(db.current_branch(), "default");
    let mut session = db.session().expect("session should open");
    assert_builtin_recipes_available(&mut session);
    db.kv_put("persisted", Value::Int(5))
        .expect("write should succeed");
    db.close().expect("close should succeed");

    let reopened = Strata::open(dir.path()).expect("database should reopen after close");
    assert_eq!(
        reopened
            .kv_get("persisted")
            .expect("persisted read should succeed"),
        Some(Value::Int(5))
    );
    reopened.close().expect("reopened handle should close");
}

#[test]
fn read_only_disk_open_is_local_and_rejects_writes() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");

    let db = Strata::open(dir.path()).expect("primary db should open");
    db.kv_put("visible", Value::Int(13))
        .expect("seed write should succeed");
    db.close().expect("db close should succeed");

    let read_only = Strata::open_with(
        dir.path(),
        OpenOptions::new().access_mode(AccessMode::ReadOnly),
    )
    .expect("read-only primary product open should succeed");
    let database = read_only.database();
    assert!(
        !database.is_follower(),
        "read-only primary open should not become a follower"
    );
    assert_product_subsystems(&database);
    assert!(!read_only.is_ipc(), "read-only open should remain local");
    assert_eq!(read_only.access_mode(), AccessMode::ReadOnly);
    assert_eq!(
        read_only
            .kv_get("visible")
            .expect("read-only handle should read"),
        Some(Value::Int(13))
    );

    let error = read_only
        .kv_put("blocked", Value::Int(14))
        .expect_err("read-only product handle should reject writes");
    assert_access_denied(error);
    let mut session = read_only
        .session()
        .expect("read-only product session should open");
    assert_session_write_denied(&mut session);
    read_only.close().expect("read-only handle should close");
}

#[test]
fn follower_open_forces_read_only_and_rejects_writes() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    primary
        .kv_put("visible", Value::Int(31))
        .expect("seed write should succeed");
    primary.close().expect("primary db should close");

    let follower = Strata::open_with(dir.path(), OpenOptions::new().follower(true))
        .expect("follower product open should succeed");
    let database = follower.database();
    assert!(
        database.is_follower(),
        "follower option should open a follower runtime"
    );
    assert_product_subsystems(&database);
    assert!(!follower.is_ipc(), "follower open should be local");
    assert_eq!(follower.access_mode(), AccessMode::ReadOnly);
    assert_eq!(
        follower
            .kv_get("visible")
            .expect("follower handle should read existing data"),
        Some(Value::Int(31))
    );

    let error = follower
        .kv_put("blocked", Value::Int(1))
        .expect_err("follower product handle should reject writes");
    assert_access_denied(error);
    let mut session = follower.session().expect("follower session should open");
    assert_session_write_denied(&mut session);
    follower.close().expect("follower handle should close");
}

#[test]
fn cache_open_is_local_read_write_and_seeds_builtin_recipes() {
    let _guard = product_open_guard();
    let db = Strata::cache().expect("cache db should open");
    let database = db.database();
    assert_product_subsystems(&database);

    assert!(!db.is_ipc(), "cache open should be local");
    assert_eq!(db.access_mode(), AccessMode::ReadWrite);
    assert_eq!(db.current_branch(), "default");

    let mut session = db.session().expect("cache session should open");
    assert_builtin_recipes_available(&mut session);

    db.kv_put("cache-write", Value::Int(21))
        .expect("cache write should succeed");
    db.close().expect("cache db should close");
}

#[test]
fn follower_open_does_not_bootstrap_missing_default_branch_state() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let seeded = create_disk_db_without_default_branch(dir.path());
    assert_eq!(
        seeded.default_branch_name(),
        None,
        "setup should not create a default branch marker"
    );
    assert_branch_metadata_absent(&seeded, "default");
    seeded
        .shutdown()
        .expect("seeded database should shut down cleanly");
    drop(seeded);

    let follower = Strata::open_with(dir.path(), OpenOptions::new().follower(true))
        .expect("follower product open should succeed");
    let database = follower.database();
    assert!(
        database.is_follower(),
        "follower option should open a follower runtime"
    );
    assert_product_subsystems(&database);
    assert_eq!(
        database.default_branch_name(),
        None,
        "follower product open should not write a default branch marker"
    );
    assert_branch_metadata_absent(&database, "default");
    follower.close().expect("follower handle should close");
}

#[test]
fn ipc_read_only_fallback_preserves_access_mode_and_rejects_writes() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    let database = primary.database();
    let mut server =
        IpcServer::start(dir.path(), database, primary.access_mode()).expect("server should start");

    OPEN_DATABASES.lock().clear();
    let remote = Strata::open_with(
        dir.path(),
        OpenOptions::new().access_mode(AccessMode::ReadOnly),
    )
    .expect("read-only remote open should use ipc");
    assert!(remote.is_ipc(), "handle should be IPC-backed");
    assert_eq!(remote.access_mode(), AccessMode::ReadOnly);

    let error = remote
        .kv_put("blocked-ipc", Value::Int(1))
        .expect_err("read-only IPC typed write should be rejected");
    assert_access_denied(error);
    let mut session = remote.session().expect("read-only IPC session should open");
    assert_session_write_denied(&mut session);

    server.shutdown();
}

#[test]
fn ipc_new_handle_refreshes_default_branch_after_public_fallback() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let seeded = create_disk_db_with_default_branch(dir.path(), "main");
    seeded
        .shutdown()
        .expect("seeded database should shut down cleanly");
    drop(seeded);

    let primary = Strata::open(dir.path()).expect("primary db should open");
    assert_eq!(primary.current_branch(), "main");
    let database = primary.database();
    let mut server =
        IpcServer::start(dir.path(), database, primary.access_mode()).expect("server should start");

    OPEN_DATABASES.lock().clear();
    let mut remote = Strata::open(dir.path()).expect("remote open should use ipc");
    assert!(remote.is_ipc(), "handle should be IPC-backed");
    assert_eq!(remote.current_branch(), "main");

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = remote.database();
    }));
    assert!(
        panic.is_err(),
        "database() should panic on IPC-backed Strata handles"
    );

    let mut admin = remote.session().expect("remote session should open");
    admin
        .execute(Command::BranchCreate {
            branch_id: Some("feature".into()),
            metadata: None,
        })
        .expect("feature branch should be created");
    remote
        .set_branch("feature")
        .expect("feature should become current on remote handle");
    remote
        .set_space("analytics")
        .expect("analytics should be valid");

    let fresh = remote
        .new_handle()
        .expect("new IPC handle should open through public fallback");
    assert!(fresh.is_ipc(), "new handle should remain IPC-backed");
    assert_eq!(fresh.current_branch(), "main");
    assert_eq!(fresh.current_space(), "default");

    server.shutdown();
}

#[test]
fn locked_disk_open_without_socket_returns_actionable_error() {
    let _guard = product_open_guard();
    let dir = tempdir().expect("tempdir should succeed");
    let primary = Strata::open(dir.path()).expect("primary db should open");
    let database = primary.database();
    drop(primary);

    let socket_path = dir.path().join("strata.sock");
    assert!(
        !socket_path.exists(),
        "test requires no IPC socket before fallback attempt"
    );

    OPEN_DATABASES.lock().clear();
    let error = match Strata::open(dir.path()) {
        Ok(_) => panic!("locked open without socket should fail"),
        Err(error) => error,
    };
    match error {
        Error::Internal { reason, .. } => {
            assert_eq!(reason, LOCKED_WITHOUT_SOCKET_MESSAGE);
        }
        other => panic!("expected internal lock error, got {other:?}"),
    }

    database.shutdown().expect("primary db should shut down");
}
