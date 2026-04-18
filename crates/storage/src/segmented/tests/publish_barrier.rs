//! Regression tests for the manifest-publication barrier (SG-001, SG-002, SG-003).
//!
//! These tests arm the storage-level fault-injection hooks in
//! `crate::test_hooks` to force a manifest publish or directory fsync
//! failure, then assert that:
//!
//! - The failing operation returns `Err(StorageError::ManifestPublish)` or
//!   `Err(StorageError::DirFsync)` (no silent `Ok(())` from a swallowed warn).
//! - For compaction paths: every old input `.sst` file is still on disk — the
//!   delete loop is strictly gated on publish success.
//!
//! The hooks in `crate::test_hooks` are thread-local, so tests do not need
//! external serialization to isolate their injections from one another.
//!
//! Covers: SG-001 (publish not a barrier), SG-002 (compaction-delete after
//! unobserved publish), SG-003 (dir fsync swallowed).

use super::*;
use crate::error::StorageError;
use crate::test_hooks;
use std::collections::BTreeSet;
use std::io;

/// Collect the set of `.sst` filenames in a branch directory.
fn sst_names(branch_dir: &std::path::Path) -> BTreeSet<String> {
    std::fs::read_dir(branch_dir)
        .map(|iter| {
            iter.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn seed_two_segments(store: &SegmentedStore, b: &BranchId) {
    for commit in 1..=2u64 {
        seed(
            store,
            Key::new(
                Arc::new(Namespace::new(*b, "default".to_string())),
                TypeTag::KV,
                format!("k{commit}").into_bytes(),
            ),
            Value::Int(commit as i64),
            commit,
        );
        store.rotate_memtable(b);
        store.flush_oldest_frozen(b).unwrap();
    }
}

/// Assert that every file present before an operation is still present after,
/// i.e. the operation did not delete any pre-existing `.sst` file. New files
/// (e.g. an orphaned compaction output) are allowed.
fn assert_no_files_deleted(
    branch_dir: &std::path::Path,
    before: &BTreeSet<String>,
    context: &str,
) {
    let after = sst_names(branch_dir);
    let deleted: Vec<_> = before.difference(&after).collect();
    assert!(
        deleted.is_empty(),
        "{context}: failed publish must not delete any old input — missing files: {deleted:?}",
    );
}

// ── Compaction paths — old inputs must survive a failed publish ────────

#[test]
fn compact_branch_refuses_delete_on_publish_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    seed_two_segments(&store, &b);

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let before = sst_names(&branch_dir);
    assert_eq!(before.len(), 2, "two input segments before compaction");

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.compact_branch(&b, CommitVersion(0));
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
    assert_no_files_deleted(&branch_dir, &before, "compact_branch");
}

#[test]
fn compact_tier_refuses_delete_on_publish_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 4096);
    let b = branch();
    seed_two_segments(&store, &b);

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let before = sst_names(&branch_dir);
    assert_eq!(before.len(), 2, "two input segments before compaction");

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.compact_tier(&b, &[0, 1], CommitVersion(0));
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
    assert_no_files_deleted(&branch_dir, &before, "compact_tier");
}

#[test]
fn compact_l0_to_l1_refuses_delete_on_publish_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 4096);
    let b = branch();
    seed_two_segments(&store, &b);

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let before = sst_names(&branch_dir);
    assert_eq!(before.len(), 2, "two input segments before compaction");

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.compact_l0_to_l1(&b, CommitVersion(0));
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
    assert_no_files_deleted(&branch_dir, &before, "compact_l0_to_l1");
}

#[test]
fn compact_level_refuses_delete_on_publish_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 4096);
    let b = branch();
    seed_two_segments(&store, &b);

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let before = sst_names(&branch_dir);
    assert_eq!(before.len(), 2, "two input segments before compaction");

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.compact_level(&b, 0, CommitVersion(0));
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
    assert_no_files_deleted(&branch_dir, &before, "compact_level");
}

// ── Non-compaction publish paths ──────────────────────────────────────

#[test]
fn flush_oldest_frozen_surfaces_publish_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.flush_oldest_frozen(&b);
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
}

#[test]
fn fork_branch_surfaces_publish_failure() {
    // Seed a parent with flushed segments so that the fork dest publish is
    // the next `write_branch_manifest` that runs (source-side publish only
    // triggers when inline-flush is needed).
    let (_tmp_dir, store) = setup_parent_with_segments(&[("k1", 1, 1)]);

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.fork_branch(&parent_branch(), &child_branch());
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
}

#[test]
fn materialize_layer_surfaces_publish_failure() {
    let (_tmp_dir, store) = setup_parent_with_segments(&[("k1", 1, 1), ("k2", 2, 2)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    assert_eq!(store.inherited_layer_count(&child_branch()), 1);

    test_hooks::inject_manifest_publish_failure(io::ErrorKind::PermissionDenied);
    let result = store.materialize_layer(&child_branch(), 0);
    test_hooks::clear_manifest_publish_failure();

    match result {
        Err(StorageError::ManifestPublish { .. }) => {}
        other => panic!("expected ManifestPublish, got {other:?}"),
    }
}

// ── Dir fsync — write_manifest surfaces DirFsync (SG-003) ─────────────

#[test]
fn write_manifest_surfaces_dir_fsync_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);

    test_hooks::inject_dir_fsync_failure(io::ErrorKind::Other);
    let result = store.flush_oldest_frozen(&b);
    test_hooks::clear_dir_fsync_failure();

    // The dir-fsync failure surfaces via `write_manifest` → is returned
    // by `write_branch_manifest` unchanged (DirFsync variant carries its
    // own dir context) → bubbles up through flush.
    match result {
        Err(StorageError::DirFsync { .. }) => {}
        other => panic!("expected DirFsync, got {other:?}"),
    }
}
