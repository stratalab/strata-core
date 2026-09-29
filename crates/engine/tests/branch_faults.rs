//! Branch and open-contention fault tests — the race conditions that are
//! actually reachable through the V1 engine surface.
//!
//! The branch-semantics parity plan sketched threaded same-name races
//! (concurrent create, delete-vs-write, recreate-vs-stale-write). The shipped
//! V1 engine makes those unreachable **by construction**: `Database` is not
//! `Clone`, every service accessor takes `&mut self`, and the executor owns
//! the handle by value — so no two engine operations can overlap on one
//! database handle, and a stale service handle cannot outlive a delete
//! (borrowck refuses). The sequential forms (stale write after delete, empty
//! recreate with a bumped generation) are covered in `branch_semantics.rs`.
//!
//! What IS reachable — and was untested — is contention between *handles*:
//! two `Database` opens racing for the same durable path, in-process or
//! across processes, arbitrated by the storage writer lock. One winner,
//! typed refusals for the losers, and release on close are load-bearing
//! product promises (two winners would mean two WAL writers on one store).

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};

use strata_engine::{Database, DurableLocalOpenOptions, EngineErrorClass};

use common::{branch, key, space, value};

/// Every thread races `Database::open_local` on one fresh path: exactly one
/// may win; every loser must get a typed engine error (never a panic, never
/// a second winner), and the winner's handle must be fully functional while
/// the losers are refused.
#[test]
fn concurrent_duplicate_opens_have_exactly_one_winner() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("db");
    let winners = AtomicUsize::new(0);
    let losers = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                match Database::open_local(&path, DurableLocalOpenOptions::new()) {
                    Ok(outcome) => {
                        winners.fetch_add(1, Ordering::SeqCst);
                        // The winner is a real database, not merely a lock
                        // holder: it takes a write and reads it back while
                        // the losers are being refused.
                        let mut database = outcome.into_database();
                        let mut kv = database
                            .kv(branch("default"), space("app"))
                            .expect("winner kv service");
                        kv.put(key(b"won"), value(b"yes")).expect("winner write");
                        common::assert_branch_value(
                            &mut database,
                            "default",
                            "app",
                            b"won",
                            b"yes",
                        );
                    }
                    Err(error) => {
                        losers.fetch_add(1, Ordering::SeqCst);
                        // Typed refusal, and no storage vocabulary leaks
                        // through the engine error surface.
                        assert!(
                            !error.code().is_empty(),
                            "loser refusal must carry a stable code: {error:?}"
                        );
                        common::assert_no_storage_type_in_engine_error(&error);
                    }
                }
            });
        }
    });

    assert_eq!(
        winners.load(Ordering::SeqCst),
        1,
        "the writer lock must admit exactly one database handle"
    );
    assert_eq!(losers.load(Ordering::SeqCst), 7);
}

/// Closing (or dropping) the winning handle releases the writer lock: a
/// subsequent open succeeds and reads the winner's durable writes.
#[test]
fn writer_lock_releases_on_close_and_the_data_survives() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("db");

    {
        let first = Database::open_local(&path, DurableLocalOpenOptions::new())
            .expect("first open")
            .into_database();
        first
            .kv(branch("default"), space("app"))
            .expect("kv")
            .put(key(b"held"), value(b"1"))
            .expect("write under the lock");

        // While the first handle lives, a second open is refused with a
        // typed error — same contract as the threaded race, held statically.
        let second = Database::open_local(&path, DurableLocalOpenOptions::new());
        let error = second.err().expect("second open must be refused");
        assert!(
            !error.code().is_empty(),
            "refusal must carry a stable code: {error:?}"
        );
        assert_ne!(
            error.class(),
            EngineErrorClass::Internal,
            "lock contention is an expected condition, not an internal fault: {error:?}"
        );
        drop(first);
    }

    // `first` was dropped, not closed: wait out a detached worker's writer
    // lock (#2837, #3546).
    let mut reopened =
        common::reopen_durable_database_after_drop(&path).expect("open after release");
    common::assert_branch_value(&mut reopened, "default", "app", b"held", b"1");
}

/// Loser retry protocol: a refused opener that waits for the winner to
/// finish must succeed on retry — refusal is contention, not corruption.
#[test]
fn refused_opener_succeeds_after_the_winner_departs() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("db");

    let winner = Database::open_local(&path, DurableLocalOpenOptions::new())
        .expect("winner open")
        .into_database();
    Database::open_local(&path, DurableLocalOpenOptions::new())
        .err()
        .expect("contender refused while the winner holds the lock");
    drop(winner);

    for attempt in 0..50 {
        match Database::open_local(&path, DurableLocalOpenOptions::new()) {
            Ok(_) => return,
            Err(error) => {
                assert!(
                    attempt < 49,
                    "open never succeeded after the winner departed: {error:?}"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}
