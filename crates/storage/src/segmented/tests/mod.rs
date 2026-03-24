//! Tests for SegmentedStore, split by category.

pub(in crate::segmented) use super::compaction::*;
pub(crate) use super::*;
pub(crate) use strata_core::types::{Namespace, TypeTag};

pub fn branch() -> BranchId {
    BranchId::from_bytes([1; 16])
}

pub fn ns() -> Arc<Namespace> {
    Arc::new(Namespace::new(branch(), "default".to_string()))
}

pub fn kv_key(name: &str) -> Key {
    Key::new(ns(), TypeTag::KV, name.as_bytes().to_vec())
}

pub fn seed(store: &SegmentedStore, key: Key, value: Value, version: u64) {
    store
        .put_with_version_mode(key, value, version, None, WriteMode::Append)
        .unwrap();
}

// Shared helpers for COW branching tests (fork, materialize, lifecycle, concurrency)

pub fn parent_branch() -> BranchId {
    BranchId::from_bytes([10; 16])
}

pub fn child_branch() -> BranchId {
    BranchId::from_bytes([20; 16])
}

pub fn parent_ns() -> Arc<Namespace> {
    Arc::new(Namespace::new(parent_branch(), "default".to_string()))
}

pub fn child_ns() -> Arc<Namespace> {
    Arc::new(Namespace::new(child_branch(), "default".to_string()))
}

pub fn parent_kv(name: &str) -> Key {
    Key::new(parent_ns(), TypeTag::KV, name.as_bytes().to_vec())
}

pub fn child_kv(name: &str) -> Key {
    Key::new(child_ns(), TypeTag::KV, name.as_bytes().to_vec())
}

/// Helper: snapshot the parent's segments and attach as an inherited layer
/// on the child branch. Creates the child branch if needed.
pub fn attach_inherited_layer(
    store: &SegmentedStore,
    source_branch_id: BranchId,
    child_branch_id: BranchId,
    fork_version: u64,
) {
    let source = store.branches.get(&source_branch_id).unwrap();
    let snapshot = source.version.load_full();
    drop(source);
    store
        .branches
        .entry(child_branch_id)
        .or_insert_with(BranchState::new);
    let mut child = store.branches.get_mut(&child_branch_id).unwrap();
    child.inherited_layers.push(InheritedLayer {
        source_branch_id,
        fork_version,
        segments: snapshot,
        status: LayerStatus::Active,
    });
}

/// Set up a store where parent has flushed segments with given key-value pairs.
pub fn setup_parent_with_segments(
    entries: &[(&str, i64, u64)],
) -> (tempfile::TempDir, SegmentedStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    for &(key, val, ver) in entries {
        store
            .put_with_version_mode(
                parent_kv(key),
                Value::Int(val),
                ver,
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();
    (dir, store)
}

mod basic;
mod batch;
mod compact;
mod concurrency;
mod flush;
mod fork;
mod leveled;
mod lifecycle;
mod materialize;
