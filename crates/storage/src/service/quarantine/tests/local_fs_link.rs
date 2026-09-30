//! #3721: the quarantine stage on the real local-filesystem backend. The
//! quarantine object is the source's own inode under a second name — the stage
//! writes no quarantine payload bytes — and each crash window (the durable disk
//! state a fault leaves behind) recovers through the ordinary retry and purge.

use super::*;
use crate::backend::local_fs::LocalFsBackend;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const BYTES: &[u8] = b"immutable-table-bytes";

fn request(branch_id: BranchId, source_object: ObjectName) -> QuarantineObjectRequest {
    QuarantineObjectRequest::new(
        branch_id,
        DATABASE_ID,
        CODEC_ID,
        "table0002",
        source_object,
        Timestamp::from_micros(1_700_000_000_000_000),
        QuarantineGate::Safe,
    )
}

fn quarantine_object(branch_id: BranchId) -> ObjectName {
    ObjectLayout::quarantine_object(&branch_id.to_string(), "table0002").expect("quarantine object")
}

/// The on-disk file of an object (`<name>.object@`).
fn file(root: &Path, name: &ObjectName) -> PathBuf {
    root.join(format!("{}.object@", name.as_str()))
}

/// `(inode, link count)`, or `None` when the file is absent.
fn inode(path: &Path) -> Option<(u64, u64)> {
    std::fs::metadata(path)
        .ok()
        .map(|metadata| (metadata.ino(), metadata.nlink()))
}

/// A published source table object, and its inode.
fn seeded() -> (tempfile::TempDir, LocalFsBackend, ObjectName, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = LocalFsBackend::new(dir.path());
    let source_object = table_source_object("table0002");
    backend
        .publish_object(&source_object, BYTES, PublishMode::Create)
        .expect("publish source");
    let (source_inode, links) = inode(&file(dir.path(), &source_object)).expect("source file");
    assert_eq!(links, 1);
    (dir, backend, source_object, source_inode)
}

fn purge(
    service: &QuarantineService<'_>,
    branch_id: BranchId,
) -> super::super::QuarantinePurgeReport {
    let token = service
        .load_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect("inventory token")
        .token();
    service
        .purge_quarantine(QuarantinePurgeRequest::new(
            branch_id,
            DATABASE_ID,
            CODEC_ID,
            QuarantineGate::Safe,
            Some(token),
        ))
        .expect("purge")
}

#[test]
fn the_local_fs_stage_moves_the_source_inode_and_writes_no_quarantine_bytes() {
    let (dir, backend, source_object, source_inode) = seeded();
    let branch_id = branch_id();
    let quarantine_object = quarantine_object(branch_id);
    let service = QuarantineService::new(&backend);

    let report = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("stage");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
    assert_eq!(report.byte_count(), BYTES.len() as u64);
    // The quarantine object IS the source file: same inode, now its only name.
    // A copy would be a fresh inode — this is the no-bytes-written proof.
    assert_eq!(
        inode(&file(dir.path(), &quarantine_object)),
        Some((source_inode, 1))
    );
    assert_eq!(inode(&file(dir.path(), &source_object)), None);
    assert_eq!(
        backend.read_object(&quarantine_object).expect("read"),
        BYTES
    );
    // #3692: the source's emptied directories went with it; the family root stays.
    let source_dir = file(dir.path(), &source_object)
        .parent()
        .expect("parent")
        .to_path_buf();
    assert!(!source_dir.exists(), "{}", source_dir.display());
    assert!(dir.path().join("tables").is_dir());
    assert_eq!(
        service
            .reconcile_branch_quarantine(branch_id, DATABASE_ID, CODEC_ID)
            .expect("reconcile")
            .kind(),
        QuarantineReconciliationKind::CleanInventory
    );

    // Round trip: the purge reclaims the moved bytes and empties the inventory.
    let purged = purge(&service, branch_id);
    assert_eq!(purged.deleted().len(), 1);
    assert_eq!(purged.reclaimed_bytes(), BYTES.len() as u64);
    assert_eq!(inode(&file(dir.path(), &quarantine_object)), None);
    assert!(service
        .load_required_inventory(branch_id, DATABASE_ID, CODEC_ID)
        .expect("inventory")
        .inventory()
        .is_empty());
}

/// Crash window 1: the inventory lists the entry, the link never happened.
/// The source is untouched; the purge clears the entry and a fresh stage moves
/// the source — exactly the copy stage's window and recovery.
#[test]
fn a_crash_before_the_link_keeps_the_source_and_recovers_by_purge_then_restage() {
    let (dir, backend, source_object, source_inode) = seeded();
    let branch_id = branch_id();
    let quarantine_object = quarantine_object(branch_id);
    let service = QuarantineService::new(&backend);
    backend
        .inject_targeted_final_publish_fault(quarantine_object.as_str().to_owned())
        .expect("arm");

    let report = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("stage");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinePublishFailed
    );
    assert_eq!(
        inode(&file(dir.path(), &source_object)),
        Some((source_inode, 1))
    );
    assert_eq!(inode(&file(dir.path(), &quarantine_object)), None);
    assert_eq!(
        service
            .reconcile_branch_quarantine(branch_id, DATABASE_ID, CODEC_ID)
            .expect("reconcile")
            .kind(),
        QuarantineReconciliationKind::MissingQuarantineObject
    );

    let purged = purge(&service, branch_id);
    assert_eq!(purged.already_missing().len(), 1);
    assert_eq!(
        inode(&file(dir.path(), &source_object)),
        Some((source_inode, 1)),
        "the purge never touches the source"
    );

    let restaged = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("restage");
    assert_eq!(
        restaged.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleted
    );
    assert_eq!(
        inode(&file(dir.path(), &quarantine_object)),
        Some((source_inode, 1))
    );
}

/// Crash window 2: linked, but the quarantine parent's fsync failed. Both
/// names are the same inode; the retry's byte comparison passes and deletes
/// the source (the copy stage's "copy visible, source present" window).
#[test]
fn a_crash_after_the_link_before_its_durability_recovers_by_the_retry() {
    let (dir, backend, source_object, source_inode) = seeded();
    let branch_id = branch_id();
    let quarantine_object = quarantine_object(branch_id);
    let service = QuarantineService::new(&backend);
    backend
        .inject_targeted_publish_fault_visible_unconfirmed(quarantine_object.as_str().to_owned())
        .expect("arm");

    let report = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("stage");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinePublishUncertain
    );
    assert_eq!(
        inode(&file(dir.path(), &source_object)),
        Some((source_inode, 2))
    );
    assert_eq!(
        inode(&file(dir.path(), &quarantine_object)),
        Some((source_inode, 2))
    );

    let retry = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("retry");
    assert_eq!(retry.status(), QuarantineObjectStatus::SourceDeleteRetried);
    assert_eq!(inode(&file(dir.path(), &source_object)), None);
    assert_eq!(
        inode(&file(dir.path(), &quarantine_object)),
        Some((source_inode, 1))
    );
}

/// Crash window 3: linked durably, the source unlink failed. The retry
/// deletes the source; the purge reclaims the bytes.
#[test]
fn a_crash_after_the_link_before_the_source_delete_recovers_by_the_retry_then_purge() {
    let (dir, backend, source_object, source_inode) = seeded();
    let branch_id = branch_id();
    let quarantine_object = quarantine_object(branch_id);
    let service = QuarantineService::new(&backend);
    backend.inject_before_removal_delete_fault().expect("arm");

    let report = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("stage");

    assert_eq!(
        report.status(),
        QuarantineObjectStatus::QuarantinedSourceDeleteFailed
    );
    assert_eq!(
        inode(&file(dir.path(), &source_object)),
        Some((source_inode, 2))
    );

    let retry = service
        .quarantine_object(&request(branch_id, source_object.clone()))
        .expect("retry");
    assert_eq!(retry.status(), QuarantineObjectStatus::SourceDeleteRetried);
    assert_eq!(inode(&file(dir.path(), &source_object)), None);

    let purged = purge(&service, branch_id);
    assert_eq!(purged.deleted().len(), 1);
    assert_eq!(inode(&file(dir.path(), &quarantine_object)), None);
}
