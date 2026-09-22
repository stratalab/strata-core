//! #3502 Slice D2 — the engine's opt-in MVCC version-retention surface,
//! end to end.
//!
//! `DurableLocalOpenOptions::with_version_retention` is the user-facing opt-in:
//! `KeepAll` (the default) retains unbounded history, `KeepRecentVersions`
//! prunes versions older than `visible - window` during compaction and
//! publishes a retained-history floor. These tests open a real durable
//! database through the public API, drive a deterministic compaction through
//! the pruning dispatch, and prove the whole chain from the open option to the
//! read boundary: an `as_of` read of a pruned version surfaces
//! `history_unavailable.engine.persistence_history`, while the default keeps
//! every version readable.
//!
//! The compaction seam is only built under the `testkit` feature (it reaches a
//! storage seam exposed through `strata-storage/testkit`), and pruning is a
//! durable-branch behavior, so the whole binary compiles away without both.
#![cfg(all(feature = "testkit", feature = "localfs"))]

mod common;

use common::{branch, key, space, value};
use strata_core::CommitVersion;
use strata_engine::{Database, DatabaseOpenOutcome, DurableLocalOpenOptions, VersionRetention};

const HISTORY_CODE: &str = "history_unavailable.engine.persistence_history";

fn open(path: &std::path::Path, retention: VersionRetention) -> Database {
    Database::open_local(
        path,
        DurableLocalOpenOptions::new().with_version_retention(retention),
    )
    .map(DatabaseOpenOutcome::into_database)
    .expect("durable open")
}

/// Commits one value for key `k` on the default branch and returns its version.
fn commit(db: &mut Database, k: &[u8], v: &[u8]) -> CommitVersion {
    db.kv(branch("default"), space("default"))
        .expect("default KV opens")
        .put(key(k), value(v))
        .expect("put succeeds")
        .commit()
        .version()
}

/// With an opt-in keep-newer-than-1 window, a durable compaction prunes
/// versions older than `visible - window`; the oldest version is dropped and
/// an `as_of` read of it RAISES, while the latest value still reads.
#[test]
fn opt_in_retention_prunes_old_versions_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut db = open(
        dir.path(),
        VersionRetention::KeepRecentVersions { window: 1 },
    );

    // Two batches of three versions, each flushed into its own L0 table — two
    // tables, below the flush-followup pressure threshold, so no compaction
    // races. Then FORCE one compaction (deterministically, at visible =
    // versions[5]); under the keep-newer-than-1 window it prunes versions
    // below `visible - 1`, keeping one below-floor survivor. The oldest
    // version is dropped.
    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(commit(&mut db, b"k", &[b'v', index]));
    }
    db.flush_storage_branch_for_test(&branch("default"))
        .expect("flush first table");
    for index in 3..6u8 {
        versions.push(commit(&mut db, b"k", &[b'v', index]));
    }
    db.flush_storage_branch_for_test(&branch("default"))
        .expect("flush second table");
    db.force_storage_branch_compaction_for_test(&branch("default"))
        .expect("force pruning compaction");

    let error = db
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .get_at_version(&key(b"k"), versions[0])
        .expect_err("oldest version was pruned");
    assert_eq!(error.code(), HISTORY_CODE);

    let latest = db
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .get(&key(b"k"))
        .expect("latest read succeeds")
        .expect("latest value exists");
    assert_eq!(latest.as_bytes(), &[b'v', 5]);
}

/// Control: the default `KeepAll` policy never prunes — the oldest version is
/// still readable exactly after the same compaction sequence.
#[test]
fn default_keep_all_retention_keeps_old_versions_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut db = open(dir.path(), VersionRetention::KeepAll);

    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(commit(&mut db, b"k", &[b'v', index]));
    }
    db.flush_storage_branch_for_test(&branch("default"))
        .expect("flush first table");
    for index in 3..6u8 {
        versions.push(commit(&mut db, b"k", &[b'v', index]));
    }
    db.flush_storage_branch_for_test(&branch("default"))
        .expect("flush second table");
    db.force_storage_branch_compaction_for_test(&branch("default"))
        .expect("force compaction");

    let oldest = db
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .get_at_version(&key(b"k"), versions[0])
        .expect("oldest version retained under KeepAll")
        .expect("oldest value exists");
    assert_eq!(oldest.as_bytes(), &[b'v', 0]);
}

/// #3502 Slice E3 — default-posture proof. A database opened with the PLAIN
/// defaults (no `with_version_retention` call at all) never prunes: after the
/// same re-write-heavy churn and a forced compaction, an `as_of` read of the
/// OLDEST version still returns its exact value. V1 ships pruning opt-in
/// (`KeepAll` default), so unbounded time-travel history is the promise a user
/// gets without opting into anything — this pins that no compaction erodes it.
#[test]
fn default_options_never_prune_history() {
    let dir = tempfile::tempdir().expect("tempdir");
    // The default a real caller gets: no `with_version_retention` call.
    let mut db = Database::open_local(dir.path(), DurableLocalOpenOptions::new())
        .map(DatabaseOpenOutcome::into_database)
        .expect("durable open");

    let mut versions = Vec::new();
    for index in 0..3u8 {
        versions.push(commit(&mut db, b"k", &[b'v', index]));
    }
    db.flush_storage_branch_for_test(&branch("default"))
        .expect("flush first table");
    for index in 3..6u8 {
        versions.push(commit(&mut db, b"k", &[b'v', index]));
    }
    db.flush_storage_branch_for_test(&branch("default"))
        .expect("flush second table");
    db.force_storage_branch_compaction_for_test(&branch("default"))
        .expect("force compaction");

    // Every version — including the oldest — still reads exactly: the default
    // published no retained-history floor, so nothing was pruned.
    for (index, version) in versions.iter().enumerate() {
        let value = db
            .kv(branch("default"), space("default"))
            .expect("default KV opens")
            .get_at_version(&key(b"k"), *version)
            .expect("default retains every version")
            .expect("value exists at every retained version");
        assert_eq!(
            value.as_bytes(),
            &[b'v', u8::try_from(index).expect("small index")]
        );
    }
}

/// #3519 end-to-end: under the default `KeepAll` posture, a flushed commit's
/// history survives a clean close+reopen. In the real product the `_system_`
/// branch always exists, so close DEFERS its checkpoint — and after W3.1c
/// elided the `COMMIT_TIMELINE` rows the flushed commits' version→timestamp
/// facts live only on their data rows. A retained `as_of` read of the oldest
/// version must still return its exact value after reopen (the read-time
/// fallback rebuilds the timeline from those rows), rather than raising
/// `history_unavailable` as it did before the fix.
#[test]
fn keep_all_history_survives_close_and_reopen_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut versions = Vec::new();
    {
        let mut db = open(dir.path(), VersionRetention::KeepAll);
        for index in 0..4u8 {
            versions.push(commit(&mut db, b"k", &[b'v', index]));
        }
        db.flush_storage_branch_for_test(&branch("default"))
            .expect("flush");
        let before = db
            .kv(branch("default"), space("default"))
            .expect("default KV opens")
            .get_at_version(&key(b"k"), versions[0])
            .expect("oldest version reads before reopen")
            .expect("value exists before reopen");
        assert_eq!(before.as_bytes(), &[b'v', 0]);
        db.close().expect("clean close");
    }

    let db = common::open_durable_database(dir.path()).expect("durable reopen");
    let oldest = db
        .kv(branch("default"), space("default"))
        .expect("default KV opens")
        .get_at_version(&key(b"k"), versions[0])
        .expect("oldest version retained across reopen under KeepAll")
        .expect("oldest value exists after reopen");
    assert_eq!(oldest.as_bytes(), &[b'v', 0]);
}
