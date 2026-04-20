use std::sync::atomic::Ordering;

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

#[test]
fn fork_after_restart_propagates_rebuilt_max_version() {
    let dir = tempfile::tempdir().unwrap();
    let parent = BranchId::from_bytes([41; 16]);
    let child = BranchId::from_bytes([42; 16]);
    let grandchild = BranchId::from_bytes([43; 16]);

    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    for version in [7_u64, 19, 42] {
        seed_branch(
            &store,
            parent,
            &format!("k{version:02}"),
            version as i64,
            version,
        );
        store.rotate_memtable(&parent);
        store.flush_oldest_frozen(&parent).unwrap();
    }
    drop(store);

    let reopened = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = reopened.recover_segments().unwrap();
    assert_eq!(
        outcome.branch_versions.get(&parent),
        Some(&CommitVersion(42))
    );
    assert!(matches!(outcome.health, RecoveryHealth::Healthy));

    let parent_state = reopened.branches.get(&parent).unwrap();
    assert_eq!(
        parent_state.max_version.load(Ordering::Acquire),
        42,
        "recovery must rebuild parent branch.max_version before post-restart forks"
    );
    drop(parent_state);

    let (fork_version, shared) = reopened.fork_branch(&parent, &child).unwrap();
    assert_eq!(fork_version, CommitVersion(42));
    assert!(shared > 0);

    let child_state = reopened.branches.get(&child).unwrap();
    assert_eq!(
        child_state.max_version.load(Ordering::Acquire),
        42,
        "forked child must inherit the rebuilt max_version so subsequent forks stay correct"
    );
    drop(child_state);

    let (grandchild_fork_version, grandchild_shared) =
        reopened.fork_branch(&child, &grandchild).unwrap();
    assert_eq!(grandchild_fork_version, CommitVersion(42));
    assert!(grandchild_shared > 0);
}
