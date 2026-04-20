use std::path::{Path, PathBuf};

use super::*;

fn kv_key_for(branch_id: BranchId, name: &str) -> Key {
    Key::new(
        Arc::new(Namespace::new(branch_id, "default".to_string())),
        TypeTag::KV,
        name.as_bytes().to_vec(),
    )
}

fn seed_branch(store: &SegmentedStore, branch_id: BranchId, key: &str, value: i64, version: u64) {
    store
        .put_with_version_mode(
            kv_key_for(branch_id, key),
            Value::Int(value),
            CommitVersion(version),
            None,
            WriteMode::Append,
        )
        .unwrap();
}

fn flush_entries(store: &SegmentedStore, branch_id: &BranchId, entries: &[(&str, i64, u64)]) {
    for &(key, value, version) in entries {
        seed_branch(store, *branch_id, key, value, version);
    }
    store.rotate_memtable(branch_id);
    store.flush_oldest_frozen(branch_id).unwrap();
}

fn branch_dir(root: &Path, branch_id: &BranchId) -> PathBuf {
    root.join(super::hex_encode_branch(branch_id))
}

fn sst_paths(root: &Path, branch_id: &BranchId) -> Vec<PathBuf> {
    let dir = branch_dir(root, branch_id);
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .ok()
        .into_iter()
        .flat_map(std::iter::Iterator::flatten)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .collect();
    paths.sort();
    paths
}

fn reopen_store(root: &Path) -> SegmentedStore {
    let store = SegmentedStore::with_dir(root.to_path_buf(), 0);
    store.recover_segments().unwrap();
    store
}

fn assert_int_value(store: &SegmentedStore, branch_id: BranchId, key: &str, expected: i64) {
    let value = store
        .get_versioned(&kv_key_for(branch_id, key), CommitVersion::MAX)
        .unwrap()
        .unwrap_or_else(|| {
            panic!(
                "missing key {key} on branch {}",
                super::hex_encode_branch(&branch_id)
            )
        });
    assert_eq!(value.value, Value::Int(expected));
}

fn compact_level_merge_fixture(store: &SegmentedStore, branch_id: &BranchId) {
    flush_entries(store, branch_id, &[("a", 1, 1), ("b", 2, 1)]);
    store.compact_level(branch_id, 0, CommitVersion(0)).unwrap();
    store.compact_level(branch_id, 1, CommitVersion(0)).unwrap();

    flush_entries(store, branch_id, &[("b", 20, 2), ("c", 30, 2)]);
    store.compact_level(branch_id, 0, CommitVersion(0)).unwrap();

    assert_eq!(store.level_segment_count(branch_id, 1), 1);
    assert_eq!(store.level_segment_count(branch_id, 2), 1);
}

#[test]
fn flush_manifest_publish_failure_reopen_keeps_pre_publish_state() {
    crate::test_hooks::clear_manifest_publish_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b);

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.flush_oldest_frozen(&b).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert!(sst_paths(dir.path(), &b).is_empty());
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.branch_segment_count(&b), 0);
    assert!(reopened
        .get_versioned(&kv_key("a"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn flush_dir_fsync_failure_reopen_keeps_installed_segment() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b);

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    assert!(store.flush_oldest_frozen(&b).unwrap());
    assert!(store.publish_health().is_some());
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.branch_segment_count(&b), 1);
    assert_int_value(&reopened, b, "a", 1);
}

#[test]
fn compact_branch_manifest_publish_failure_reopen_keeps_pre_publish_state() {
    crate::test_hooks::clear_manifest_publish_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_entries(&store, &b, &[("a", 1, 1)]);
    flush_entries(&store, &b, &[("b", 2, 2)]);
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(inputs.len(), 2);

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.compact_branch(&b, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.l0_segment_count(&b), 2);
    assert_int_value(&reopened, b, "a", 1);
    assert_int_value(&reopened, b, "b", 2);
}

#[test]
fn compact_branch_dir_fsync_failure_reopen_keeps_forward_progress() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_entries(&store, &b, &[("a", 1, 1)]);
    flush_entries(&store, &b, &[("b", 2, 2)]);
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(inputs.len(), 2);

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let err = store.compact_branch(&b, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::DirFsync { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.branch_segment_count(&b), 1);
    assert_int_value(&reopened, b, "a", 1);
    assert_int_value(&reopened, b, "b", 2);
}

#[test]
fn compact_tier_manifest_publish_failure_reopen_keeps_pre_publish_state() {
    crate::test_hooks::clear_manifest_publish_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    for commit in 1..=4_u64 {
        let key = format!("k{commit}");
        flush_entries(&store, &b, &[(&key, commit as i64, commit)]);
    }
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(inputs.len(), 4);

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store
        .compact_tier(&b, &[0, 1, 2, 3], CommitVersion(0))
        .unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.l0_segment_count(&b), 4);
    for commit in 1..=4_i64 {
        assert_int_value(&reopened, b, &format!("k{commit}"), commit);
    }
}

#[test]
fn compact_tier_dir_fsync_failure_reopen_keeps_forward_progress() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    for commit in 1..=4_u64 {
        let key = format!("k{commit}");
        flush_entries(&store, &b, &[(&key, commit as i64, commit)]);
    }
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(inputs.len(), 4);

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let err = store
        .compact_tier(&b, &[0, 1, 2, 3], CommitVersion(0))
        .unwrap_err();
    assert!(matches!(err, crate::StorageError::DirFsync { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.branch_segment_count(&b), 1);
    for commit in 1..=4_i64 {
        assert_int_value(&reopened, b, &format!("k{commit}"), commit);
    }
}

#[test]
fn compact_l0_to_l1_manifest_publish_failure_reopen_keeps_pre_publish_state() {
    crate::test_hooks::clear_manifest_publish_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_entries(&store, &b, &[("a", 1, 1)]);
    flush_entries(&store, &b, &[("b", 2, 2)]);
    flush_entries(&store, &b, &[("c", 3, 3)]);
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(inputs.len(), 3);

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.l0_segment_count(&b), 3);
    assert_eq!(reopened.l1_segment_count(&b), 0);
    assert_int_value(&reopened, b, "a", 1);
    assert_int_value(&reopened, b, "b", 2);
    assert_int_value(&reopened, b, "c", 3);
}

#[test]
fn compact_l0_to_l1_dir_fsync_failure_reopen_keeps_forward_progress() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_entries(&store, &b, &[("a", 1, 1)]);
    flush_entries(&store, &b, &[("b", 2, 2)]);
    flush_entries(&store, &b, &[("c", 3, 3)]);
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(inputs.len(), 3);

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let err = store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::DirFsync { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.l0_segment_count(&b), 0);
    assert!(reopened.l1_segment_count(&b) >= 1);
    assert_int_value(&reopened, b, "a", 1);
    assert_int_value(&reopened, b, "b", 2);
    assert_int_value(&reopened, b, "c", 3);
}

#[test]
fn compact_level_trivial_move_manifest_publish_failure_reopen_keeps_pre_publish_state() {
    crate::test_hooks::clear_manifest_publish_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_entries(&store, &b, &[("x", 1, 1)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.compact_level(&b, 1, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.level_segment_count(&b, 1), 1);
    assert_eq!(reopened.level_segment_count(&b, 2), 0);
    assert_int_value(&reopened, b, "x", 1);
}

#[test]
fn compact_level_trivial_move_dir_fsync_failure_reopen_keeps_forward_progress() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_entries(&store, &b, &[("x", 1, 1)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    let inputs = sst_paths(dir.path(), &b);
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let err = store.compact_level(&b, 1, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::DirFsync { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.level_segment_count(&b, 1), 0);
    assert_eq!(reopened.level_segment_count(&b, 2), 1);
    assert_int_value(&reopened, b, "x", 1);
}

#[test]
fn compact_level_merge_manifest_publish_failure_reopen_keeps_pre_publish_state() {
    crate::test_hooks::clear_manifest_publish_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    compact_level_merge_fixture(&store, &b);
    let inputs = sst_paths(dir.path(), &b);

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.compact_level(&b, 1, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.level_segment_count(&b, 1), 1);
    assert_eq!(reopened.level_segment_count(&b, 2), 1);
    assert_int_value(&reopened, b, "a", 1);
    assert_int_value(&reopened, b, "b", 20);
    assert_int_value(&reopened, b, "c", 30);
}

#[test]
fn compact_level_merge_dir_fsync_failure_reopen_keeps_forward_progress() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    compact_level_merge_fixture(&store, &b);
    let inputs = sst_paths(dir.path(), &b);

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let err = store.compact_level(&b, 1, CommitVersion(0)).unwrap_err();
    assert!(matches!(err, crate::StorageError::DirFsync { .. }));
    assert!(inputs.iter().all(|path| path.exists()));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.level_segment_count(&b, 1), 0);
    assert!(reopened.level_segment_count(&b, 2) >= 1);
    assert_int_value(&reopened, b, "a", 1);
    assert_int_value(&reopened, b, "b", 20);
    assert_int_value(&reopened, b, "c", 30);
}

#[test]
fn fork_manifest_publish_failure_reopen_keeps_child_absent() {
    crate::test_hooks::clear_manifest_publish_failure();

    let (dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);

    let err = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert!(reopened.branches.get(&child_branch()).is_none());
    assert_int_value(&reopened, parent_branch(), "a", 1);
    assert_int_value(&reopened, parent_branch(), "b", 2);
}

#[test]
fn fork_dir_fsync_failure_reopen_keeps_child_visible() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let (dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);

    let (fork_version, shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    assert!(fork_version > CommitVersion::ZERO);
    assert!(shared > 0);
    assert!(store.publish_health().is_some());
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.inherited_layer_count(&child_branch()), 1);
    let child_value = reopened
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(child_value.value, Value::Int(1));
}

#[test]
fn materialize_manifest_publish_failure_reopen_keeps_inherited_layer() {
    crate::test_hooks::clear_manifest_publish_failure();

    let (dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.materialize_layer(&child_branch(), 0).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.inherited_layer_count(&child_branch()), 1);
    let child_value = reopened
        .get_versioned(&child_kv("b"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(child_value.value, Value::Int(2));
}

#[test]
fn materialize_dir_fsync_failure_reopen_keeps_materialized_state() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let (dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(result.entries_materialized, 2);
    assert!(store.publish_health().is_some());
    drop(store);

    let reopened = reopen_store(dir.path());
    assert_eq!(reopened.inherited_layer_count(&child_branch()), 0);
    assert!(reopened.branch_segment_count(&child_branch()) > 0);
    let child_value = reopened
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(child_value.value, Value::Int(1));
}
