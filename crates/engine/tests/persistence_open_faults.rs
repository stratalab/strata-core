//! Engine-boundary classification of permanent open failures.
//!
//! A permanent, unrecoverable open condition must reach the caller as a
//! permanent status, never as a transient outage with retry advice: a caller
//! obeying "retry after the persistence layer is available" would otherwise
//! retry forever against a permanently broken database. Corruption is
//! non-retryable `Corruption`; a structurally invalid path is `InvalidInput`.
#![cfg(feature = "localfs")]

mod common;

use std::path::{Path, PathBuf};

use common::{assert_status, branch, key, open_durable_database, space, value};
use strata_engine::{
    Database, DatabaseOpenOutcome, DurabilityMode, DurableLocalOpenOptions, EngineErrorClass,
    ErrorClass, RetryPolicy,
};

/// Opens with `Always` durability so every acknowledged put is already on
/// disk when the session is abandoned WITHOUT a clean close. A clean close
/// checkpoints and truncates the WAL (space-reclamation contract §3.1,
/// slice 7), which would leave no durable log record for these tests to
/// damage; the abandoned session is the shape whose WAL still holds them.
fn open_always_synced(root: &Path) -> Database {
    Database::open_local(
        root,
        DurableLocalOpenOptions::new().with_durability(DurabilityMode::Always),
    )
    .map(DatabaseOpenOutcome::into_database)
    .expect("durable open")
}

/// The active WAL segment file, so a test can damage the durable log directly.
fn active_wal_segment(root: &Path) -> PathBuf {
    let wal_dir = root.join("wal");
    let mut best: Option<(String, PathBuf)> = None;
    for entry in std::fs::read_dir(&wal_dir).expect("wal dir").flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(id) = name.strip_suffix(".object@") else {
            continue;
        };
        if id.len() != 16 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let id = id.to_owned();
        if best
            .as_ref()
            .is_none_or(|(best_id, _)| id.as_str() > best_id.as_str())
        {
            best = Some((id, path));
        }
    }
    best.map(|(_, path)| path)
        .expect("an active WAL segment exists")
}

#[test]
fn opening_a_byte_corrupted_wal_reports_permanent_corruption() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("db");
    {
        let db = open_always_synced(&root);
        let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
        for index in 0u32..64 {
            kv.put(
                key(format!("corrupt-{index:04}").as_bytes()),
                value(&[b'v'; 64]),
            )
            .expect("seed write");
        }
        drop(kv);
        // Abandon the session: every record is synced, none is checkpointed.
        drop(db);
    }

    let segment = active_wal_segment(&root);
    let mut bytes = std::fs::read(&segment).expect("read wal segment");
    assert!(bytes.len() > 128, "wal segment too small to corrupt safely");
    let start = bytes.len() / 4;
    for offset in start..(start + 32).min(bytes.len()) {
        bytes[offset] ^= 0xff;
    }
    std::fs::write(&segment, &bytes).expect("write corrupted wal segment");

    let error = open_durable_database(&root)
        .err()
        .expect("a byte-corrupted WAL must fail to open loudly");

    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "corruption.engine.persistence_recovery",
        false,
    );
    assert_eq!(error.public_class(), ErrorClass::Corruption);
    assert_eq!(error.retry_policy(), RetryPolicy::Never);
}

#[test]
fn deleting_the_sole_wal_segment_refuses_to_open_instead_of_silently_erasing() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("db");
    {
        let db = open_always_synced(&root);
        let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
        kv.put(key(b"alpha"), value(b"1")).expect("seed write");
        kv.put(key(b"beta"), value(b"2")).expect("seed write");
        drop(kv);
        // Abandon the session: every record is synced, none is checkpointed.
        drop(db);
    }

    // Remove the sole WAL segment — the un-checkpointed rows live only there.
    // The manifest, written at creation, still records that this database
    // exists, so a reopen must refuse rather than silently reset to empty.
    let segment = active_wal_segment(&root);
    std::fs::remove_file(&segment).expect("remove the sole wal segment");

    let error = open_durable_database(&root).err().expect(
        "a database whose sole WAL segment was removed must refuse to open, \
         not silently recreate an empty log",
    );

    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "corruption.engine.persistence_recovery",
        false,
    );
    assert_eq!(error.public_class(), ErrorClass::Corruption);
    assert_eq!(error.retry_policy(), RetryPolicy::Never);
}

#[test]
fn opening_a_regular_file_as_a_database_reports_invalid_input() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("not-a-db");
    std::fs::write(&root, b"not a database directory").expect("write regular file");

    let error = open_durable_database(&root)
        .err()
        .expect("a regular file is not a valid database directory");

    assert_status(
        &error,
        EngineErrorClass::InvalidInput,
        "invalid_argument.engine.persistence",
        false,
    );
    assert_eq!(error.public_class(), ErrorClass::InvalidArgument);
    assert_eq!(error.retry_policy(), RetryPolicy::Never);
}

#[test]
fn opening_under_a_missing_parent_directory_reports_invalid_input() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("does-not-exist").join("db");

    let error = open_durable_database(&root)
        .err()
        .expect("a database path under a missing parent cannot be opened");

    assert_status(
        &error,
        EngineErrorClass::InvalidInput,
        "invalid_argument.engine.persistence",
        false,
    );
    assert_eq!(error.public_class(), ErrorClass::InvalidArgument);
    assert_eq!(error.retry_policy(), RetryPolicy::Never);
}
