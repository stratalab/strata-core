//! Open-time failure classification.
//!
//! Permanent, unrecoverable open conditions must not masquerade as a transient
//! outage with retry advice. A byte-corrupted WAL is corruption (a permanent
//! `FailedPrecondition`), and a structurally invalid database path is an
//! invalid argument — never a retryable lower-layer outage.
#![cfg(feature = "localfs")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use strata_core::BranchId;
use strata_storage::api::{
    CommitBatch, CommitMutation, CommitOptions, MaintenanceRequest, MaintenanceScope,
    MaintenanceSummaryStatus, MaintenanceTask, StorageApiErrorClass, StorageDurabilityPolicy,
    StorageKey, StorageRuntime, StorageSpaceId, StorageValue,
};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

fn temp_root(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "strata-open-fault-{name}-{}-{}",
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("clear old temp dir");
    }
    path
}

fn default_branch() -> BranchId {
    BranchId::from_bytes([0x01; BranchId::BYTE_LEN])
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
fn corrupt_wal_record_reports_permanent_corruption_not_transient_outage() {
    let root = temp_root("corrupt-wal");
    // Write enough acknowledged rows to fill the log with complete interior
    // records, so a mid-file byte flip lands inside a checksummed record body
    // (a hard corruption) rather than at the trailing record (a torn tail).
    {
        let runtime = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Always)
            .expect("durable open")
            .into_runtime();
        let branch = default_branch();
        let space = StorageSpaceId::new(vec![0x20]).expect("engine space");
        for index in 0u32..64 {
            let key = StorageKey::new(format!("corrupt-{index:04}").into_bytes()).expect("key");
            let batch = CommitBatch::new(
                branch,
                vec![CommitMutation::Put {
                    storage_space: space.clone(),
                    key,
                    value: StorageValue::new(vec![b'v'; 64]),
                    ttl: None,
                }],
                CommitOptions::default(),
            )
            .expect("commit batch");
            runtime.commit(&batch).expect("durable commit");
        }
        // Drop (not close) to leave the synced WAL for recovery without a
        // close-time checkpoint truncating it.
    }

    let segment = active_wal_segment(&root);
    let mut bytes = std::fs::read(&segment).expect("read wal segment");
    assert!(bytes.len() > 128, "wal segment too small to corrupt safely");
    // Damage a run inside the first quarter of the log — well past the segment
    // header, well before the trailing record.
    let start = bytes.len() / 4;
    for offset in start..(start + 32).min(bytes.len()) {
        bytes[offset] ^= 0xff;
    }
    std::fs::write(&segment, &bytes).expect("write corrupted wal segment");

    let error = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Always)
        .expect_err("a byte-corrupted WAL must fail to open, not silently recover");

    assert_eq!(
        error.class(),
        StorageApiErrorClass::FailedPrecondition,
        "corrupt WAL must be a permanent precondition failure, not a transient `Internal` outage; got code {}",
        error.code()
    );
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.recovery_degraded",
        "corrupt WAL must surface the recovery-degraded code so the engine maps it to non-retryable corruption"
    );
}

#[test]
fn opening_a_regular_file_as_a_database_reports_invalid_argument() {
    let root = temp_root("file-as-db");
    std::fs::create_dir_all(root.parent().expect("temp parent")).expect("temp parent dir");
    std::fs::write(&root, b"not a database directory").expect("write regular file");

    let error = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect_err("a regular file is not a valid database directory");

    assert_eq!(
        error.class(),
        StorageApiErrorClass::InvalidArgument,
        "a file-as-database path is an invalid argument, not a transient outage; got code {}",
        error.code()
    );
}

#[test]
fn opening_under_a_missing_parent_directory_reports_invalid_argument() {
    let root = temp_root("missing-parent")
        .join("does-not-exist")
        .join("db");

    let error = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect_err("a database path under a missing parent cannot be opened");

    assert_eq!(
        error.class(),
        StorageApiErrorClass::InvalidArgument,
        "a missing-parent path is an invalid argument, not a transient outage; got code {}",
        error.code()
    );
}

/// Commits one row so a later open can prove it reached the same store.
#[cfg(unix)]
fn commit_marker(runtime: &StorageRuntime<'_>, name: &str) {
    let key = StorageKey::new(name.as_bytes().to_vec()).expect("key");
    let batch = CommitBatch::new(
        default_branch(),
        vec![CommitMutation::Put {
            storage_space: StorageSpaceId::new(vec![0x20]).expect("engine space"),
            key,
            value: StorageValue::new(vec![b'v'; 16]),
            ttl: None,
        }],
        CommitOptions::default(),
    )
    .expect("commit batch");
    runtime.commit(&batch).expect("durable commit");
}

/// #3008: a database path that is a symlink to a directory names that
/// directory. It opens, and the layout lands in the real directory — not
/// beside the link, and not refused as "not a directory".
#[cfg(unix)]
#[test]
fn opening_through_a_symlink_to_a_directory_opens_the_real_directory() {
    let real = temp_root("symlink-real");
    std::fs::create_dir_all(&real).expect("real database directory");
    let link = temp_root("symlink-link");
    std::os::unix::fs::symlink(&real, &link).expect("symlink to the database directory");

    let mut runtime = StorageRuntime::open_durable_local(&link, StorageDurabilityPolicy::Always)
        .expect("a symlink to a directory is a valid database path")
        .into_runtime();
    commit_marker(&runtime, "through-link");
    runtime.close().expect("clean close");

    assert!(
        std::fs::symlink_metadata(&link)
            .expect("link still present")
            .file_type()
            .is_symlink(),
        "opening through the link must not replace the link"
    );
    assert!(
        real.join("wal").is_dir(),
        "the durable layout is written into the link's target directory"
    );

    // Reopening through the real path opens the same store the link created.
    let reopened = StorageRuntime::open_durable_local(&real, StorageDurabilityPolicy::Always)
        .expect("reopen through the real path");
    drop(reopened);
}

/// #3008: two opens of one directory — one through a symlink, one through the
/// real path — contend on the same writer lock. Following the link must never
/// let a second writer in beside the first.
#[cfg(unix)]
#[test]
fn a_symlinked_open_and_a_real_path_open_contend_on_one_writer_lock() {
    let real = temp_root("symlink-lock-real");
    std::fs::create_dir_all(&real).expect("real database directory");
    let link = temp_root("symlink-lock-link");
    std::os::unix::fs::symlink(&real, &link).expect("symlink to the database directory");

    let through_link = StorageRuntime::open_durable_local(&link, StorageDurabilityPolicy::Standard)
        .expect("open through the link")
        .into_runtime();
    let error = StorageRuntime::open_durable_local(&real, StorageDurabilityPolicy::Standard)
        .expect_err("the real path names the directory the link's writer already holds");
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.writer_lock",
        "the real-path open must contend on the link open's writer lock"
    );
    drop(through_link);

    let through_real = StorageRuntime::open_durable_local(&real, StorageDurabilityPolicy::Standard)
        .expect("open through the real path")
        .into_runtime();
    let error = StorageRuntime::open_durable_local(&link, StorageDurabilityPolicy::Standard)
        .expect_err("the link names the directory the real-path writer already holds");
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.writer_lock",
        "the link open must contend on the real-path open's writer lock"
    );
    drop(through_real);
}

/// #3008: a dangling symlink still refuses with the typed path-shape code; it
/// names no directory, and the refused open must not create one at the target.
#[cfg(unix)]
#[test]
fn opening_through_a_dangling_symlink_reports_invalid_argument() {
    let target = temp_root("dangling-target");
    let link = temp_root("dangling-link");
    std::os::unix::fs::symlink(&target, &link).expect("dangling symlink");

    let error = StorageRuntime::open_durable_local(&link, StorageDurabilityPolicy::Standard)
        .expect_err("a dangling symlink names no database directory");
    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
    assert!(
        std::fs::symlink_metadata(&target).is_err(),
        "a refused open must not materialize the dangling link's target"
    );
}

/// #3008: a symlink to a regular file is refused exactly like the file itself.
#[cfg(unix)]
#[test]
fn opening_through_a_symlink_to_a_file_reports_invalid_argument() {
    let file = temp_root("symlink-file-target");
    std::fs::create_dir_all(file.parent().expect("temp parent")).expect("temp parent dir");
    std::fs::write(&file, b"not a database directory").expect("write regular file");
    let link = temp_root("symlink-file-link");
    std::os::unix::fs::symlink(&file, &link).expect("symlink to a file");

    let error = StorageRuntime::open_durable_local(&link, StorageDurabilityPolicy::Standard)
        .expect_err("a symlink to a file is not a database directory");
    assert_eq!(error.class(), StorageApiErrorClass::InvalidArgument);
    assert_eq!(error.code(), "invalid_argument.storage_api.argument");
}

/// Deletes every WAL segment object under `wal/`, leaving sidecar metadata
/// and every other artifact untouched (the #2765 sabotage).
fn delete_all_wal_segments(root: &Path) {
    let wal_dir = root.join("wal");
    let mut deleted = 0u32;
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
        std::fs::remove_file(&path).expect("delete wal segment");
        deleted += 1;
    }
    assert!(deleted > 0, "the stage produced no wal segments to delete");
}

/// Seeds a durable store with acknowledged commits, publishes a checkpoint
/// (so the manifest attests durable history, as every engine-level database
/// does from its creation barrier), and closes cleanly.
fn stage_cleanly_closed_store(root: &Path) {
    let mut runtime = StorageRuntime::open_durable_local(root, StorageDurabilityPolicy::Standard)
        .expect("durable open")
        .into_runtime();
    let branch = default_branch();
    let space = StorageSpaceId::new(vec![0x20]).expect("engine space");
    for index in 0u32..8 {
        let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
        let batch = CommitBatch::new(
            branch,
            vec![CommitMutation::Put {
                storage_space: space.clone(),
                key,
                value: StorageValue::new(vec![b'v'; 64]),
                ttl: None,
            }],
            CommitOptions::default(),
        )
        .expect("commit batch");
        runtime.commit(&batch).expect("durable commit");
    }
    let summary = runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("checkpoint request");
    assert_eq!(
        summary.status(),
        MaintenanceSummaryStatus::Completed,
        "the staging checkpoint must publish so the manifest attests history"
    );
    runtime.close().expect("clean close");
}

/// #2765: an existing database whose manifest attests a published checkpoint
/// must refuse to open when every WAL segment is gone — recreating a fresh
/// empty log silently presents a gutted store as a healthy empty database.
#[test]
fn missing_wal_segments_after_clean_close_report_permanent_corruption() {
    let root = temp_root("missing-wal");
    stage_cleanly_closed_store(&root);
    delete_all_wal_segments(&root);

    let error = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect_err("a checkpoint-attested database with no WAL segments must refuse to open");

    assert_eq!(
        error.class(),
        StorageApiErrorClass::FailedPrecondition,
        "missing WAL on an attested database is permanent corruption, not a fresh store; got code {}",
        error.code()
    );
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.recovery_degraded",
        "missing WAL must surface the recovery-degraded code so the engine maps it to non-retryable corruption"
    );
}

/// #2765, whole-directory variant: removing `wal/` entirely is the same
/// absence and must refuse identically.
#[test]
fn missing_wal_directory_after_clean_close_reports_permanent_corruption() {
    let root = temp_root("missing-wal-dir");
    stage_cleanly_closed_store(&root);
    std::fs::remove_dir_all(root.join("wal")).expect("remove wal dir");

    let error = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect_err("a checkpoint-attested database with no wal/ directory must refuse to open");

    assert_eq!(
        error.class(),
        StorageApiErrorClass::FailedPrecondition,
        "missing wal/ on an attested database is permanent corruption; got code {}",
        error.code()
    );
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.recovery_degraded",
        "the directory variant must classify identically to the segment variant"
    );
}

/// The refusal must not overfire: before any checkpoint publishes (a torn
/// first creation — manifest written, crash before the log or the creation
/// checkpoint landed), `snapshot_watermark` is absent, nothing acknowledged
/// could exist, and reopen must still recreate the log and succeed.
#[test]
fn unattested_store_with_missing_wal_still_opens_fresh() {
    let root = temp_root("torn-creation");
    {
        // Create and drop without close: no checkpoint publishes, so the
        // manifest attests nothing.
        let _runtime = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
            .expect("durable open")
            .into_runtime();
    }
    delete_all_wal_segments(&root);

    let runtime = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect("an unattested store recreates its log")
        .into_runtime();
    drop(runtime);
}

/// #2690: after acknowledged commits and a clean close, the close-time durable
/// commit watermark attests the log even when NO checkpoint exists — deleting
/// every WAL segment must refuse, not reopen as fresh. (This is the store the
/// #2765 manifest-attestation guard cannot see: its manifest never recorded a
/// checkpoint, so the WAL was the sole durable record.)
#[test]
fn unattested_store_with_commits_refuses_open_when_wal_is_deleted() {
    let root = temp_root("watermark-sole");
    {
        let mut runtime =
            StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
                .expect("durable open")
                .into_runtime();
        let branch = default_branch();
        let space = StorageSpaceId::new(vec![0x20]).expect("engine space");
        for index in 0u32..4 {
            let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
            let batch = CommitBatch::new(
                branch,
                vec![CommitMutation::Put {
                    storage_space: space.clone(),
                    key,
                    value: StorageValue::new(vec![b'v'; 32]),
                    ttl: None,
                }],
                CommitOptions::default(),
            )
            .expect("commit batch");
            runtime.commit(&batch).expect("durable commit");
        }
        runtime
            .close()
            .expect("clean close publishes the commit watermark");
    }
    delete_all_wal_segments(&root);

    let error = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect_err("the commit watermark attests data the log can no longer provide");
    assert_eq!(
        error.class(),
        StorageApiErrorClass::FailedPrecondition,
        "watermark-attested loss is permanent corruption; got code {}",
        error.code()
    );
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.recovery_degraded",
        "watermark-attested loss surfaces the recovery-degraded code"
    );
}

/// #2766: unlinking the active WAL segment while the handle is live makes the
/// final close sync fail against a vanished object — permanently. The close
/// must classify as corruption (`retryable: false` at the engine boundary),
/// never as a retryable outage: the inode is gone and no retry can bring the
/// acknowledged bytes back. Reopen then refuses per the #2765 guard rather
/// than presenting a healthy empty store.
#[test]
fn unlinking_active_wal_makes_close_report_permanent_corruption() {
    let root = temp_root("live-unlink-close");
    let mut runtime = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect("durable open")
        .into_runtime();
    let branch = default_branch();
    let space = StorageSpaceId::new(vec![0x20]).expect("engine space");
    for index in 0u32..8 {
        let key = StorageKey::new(format!("acked-{index:04}").into_bytes()).expect("key");
        let batch = CommitBatch::new(
            branch,
            vec![CommitMutation::Put {
                storage_space: space.clone(),
                key,
                value: StorageValue::new(vec![b'v'; 32]),
                ttl: None,
            }],
            CommitOptions::default(),
        )
        .expect("commit batch");
        runtime.commit(&batch).expect("durable commit");
    }
    // Publish a checkpoint so the manifest attests history (as every
    // engine-level database does from creation), making the reopen half of
    // the defect observable through the #2765 guard.
    let summary = runtime
        .maintenance(&MaintenanceRequest::new(
            MaintenanceTask::Checkpoint,
            MaintenanceScope::Global,
        ))
        .expect("checkpoint request");
    assert_eq!(summary.status(), MaintenanceSummaryStatus::Completed);

    delete_all_wal_segments(&root);

    let error = runtime
        .close()
        .expect_err("closing over a vanished WAL object cannot succeed");
    assert_eq!(
        error.class(),
        StorageApiErrorClass::FailedPrecondition,
        "a vanished WAL object is permanent loss, not a retryable outage; got code {}",
        error.code()
    );
    assert_eq!(
        error.code(),
        "failed_precondition.storage_api.recovery_degraded",
        "the close failure must carry the recovery-degraded code so the engine maps it to \
         non-retryable corruption"
    );
    drop(runtime);

    let reopen = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Standard)
        .expect_err("reopen after the loss must refuse, never present a healthy empty store");
    assert_eq!(
        reopen.code(),
        "failed_precondition.storage_api.recovery_degraded"
    );
}

/// #2766: after the WAL writer observes a permanent sync failure (the active
/// object vanished), subsequent writes must be refused — never admitted and
/// acknowledged against a log that no sync point can ever cover again.
#[test]
fn writes_after_a_lost_wal_are_refused() {
    let root = temp_root("live-unlink-writes");
    let mut runtime = StorageRuntime::open_durable_local(&root, StorageDurabilityPolicy::Always)
        .expect("durable open")
        .into_runtime();
    let branch = default_branch();
    let space = StorageSpaceId::new(vec![0x20]).expect("engine space");
    let put = |runtime: &mut StorageRuntime<'_>, name: &str| {
        let key = StorageKey::new(name.as_bytes().to_vec()).expect("key");
        let batch = CommitBatch::new(
            branch,
            vec![CommitMutation::Put {
                storage_space: space.clone(),
                key,
                value: StorageValue::new(vec![b'v'; 32]),
                ttl: None,
            }],
            CommitOptions::default(),
        )
        .expect("commit batch");
        runtime.commit(&batch).map(|_| ())
    };
    put(&mut runtime, "pre-unlink").expect("acked before the unlink");

    delete_all_wal_segments(&root);

    // The first post-unlink commit hits the covering sync and cannot attest
    // durability: ambiguous, never acknowledged.
    let first = put(&mut runtime, "post-unlink-first")
        .expect_err("the sync against a vanished object cannot attest durability");
    assert_eq!(
        first.code(),
        "ambiguous_commit.storage_api.durable_uncertain",
        "the first failing sync is durability-uncertain; got code {}",
        first.code()
    );

    // Every later write must be refused, never acknowledged: the unresolved
    // durable fact recorded by the failed group blocks admission until the
    // database is reopened (and reopen classifies the loss).
    let second = put(&mut runtime, "post-unlink-second")
        .expect_err("writes after an unresolved durable failure must be refused");
    assert_eq!(
        second.code(),
        "ambiguous_commit.storage_api.durable_uncertain",
        "admission is blocked on the unresolved durable fact; got code {}",
        second.code()
    );

    // Close after the unresolved durable failure refuses on the unresolved
    // fact (its own established contract — reopen is the resolution path);
    // the standard-mode sibling test owns the close-time loss classification.
    runtime
        .close()
        .expect_err("closing with an unresolved durable commit cannot succeed");
}
