//! SE4 regression tests: branch-resurrection race fix (SG-010, SG-011) +
//! contract pins for SG-012 and SG-015.
//!
//! **SG-010 / SG-011** — an in-flight compaction or `materialize_layer` must
//! not resurrect a branch that `clear_branch` removed during the long-I/O gap
//! between snapshot and install. The fix replaces `entry().or_insert_with`
//! with `get_mut()`; on `None` the op cleans up its outputs and returns
//! `StorageError::BranchDeletedDuringOp`.
//!
//! **SG-012 / SG-015** — pin the current caller-serialized compaction
//! contract and the current best-effort cleanup behavior of `clear_branch`
//! interleaved with compaction, so Tranche 4 has a green baseline to reshape
//! against.
//!
//! Race determinism comes from `crate::test_hooks::install_pause`, which
//! parks the install phase so the test thread can land its `clear_branch`
//! first. See `crates/storage/src/test_hooks.rs`.

use super::*;
use crate::error::{BranchOp, StorageError};
use crate::test_hooks::{arm, pause_tag, release, wait_until_entered};
use std::sync::{Arc, Barrier};

/// Unique branch-id per test so parallel runs don't collide on the install-
/// pause keyspace (which is `(op_tag, branch_id)`). The byte tag is chosen
/// to be disjoint from `branch()` / `parent_branch()` / `child_branch()` and
/// from every other test in this file.
fn test_branch(tag: u8) -> BranchId {
    BranchId::from_bytes([tag; 16])
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Populate a branch with two L0 segments so compaction has real inputs.
fn seed_two_l0_segments(store: &SegmentedStore, b: BranchId) {
    let ns = Arc::new(Namespace::new(b, "default".to_string()));
    for i in 0_i64..50 {
        store
            .put_with_version_mode(
                Key::new(
                    Arc::clone(&ns),
                    TypeTag::KV,
                    format!("a{i:04}").into_bytes(),
                ),
                Value::Int(i),
                CommitVersion(1),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();
    for i in 0_i64..50 {
        store
            .put_with_version_mode(
                Key::new(
                    Arc::clone(&ns),
                    TypeTag::KV,
                    format!("b{i:04}").into_bytes(),
                ),
                Value::Int(i),
                CommitVersion(2),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();
}

fn flush_branch_data(store: &SegmentedStore, b: BranchId, entries: &[(&str, i64, u64)]) {
    let ns = Arc::new(Namespace::new(b, "default".to_string()));
    for &(key_name, value, commit) in entries {
        store
            .put_with_version_mode(
                Key::new(Arc::clone(&ns), TypeTag::KV, key_name.as_bytes().to_vec()),
                Value::Int(value),
                CommitVersion(commit),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();
}

fn count_sst_files(branch_dir: &std::path::Path) -> usize {
    if !branch_dir.exists() {
        return 0;
    }
    std::fs::read_dir(branch_dir).map_or(0, |rd| {
        rd.filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
            .count()
    })
}

fn assert_branch_deleted_during_op(err: &StorageError, expected_op: BranchOp) {
    match err {
        StorageError::BranchDeletedDuringOp { op, .. } => assert_eq!(*op, expected_op),
        other => panic!("expected BranchDeletedDuringOp({expected_op:?}), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SG-010: compaction resurrection race
// ---------------------------------------------------------------------------

#[test]
fn compact_branch_does_not_resurrect_cleared_branch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = test_branch(0xA0);
    seed_two_l0_segments(&store, b);

    let branch_hex = hex_encode_branch(&b);
    let branch_dir = dir.path().join(&branch_hex);
    let sst_before = count_sst_files(&branch_dir);
    assert_eq!(sst_before, 2);

    let pause = arm(pause_tag::COMPACT_BRANCH, b);
    let s = Arc::clone(&store);
    let handle = std::thread::spawn(move || s.compact_branch(&b, CommitVersion(0)));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&b));
    release(&pause);

    let result = handle.join().unwrap();
    drop(pause);

    let err = result.expect_err("compaction must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::CompactBranch);

    assert!(
        !store.branches.contains_key(&b),
        "branch must stay absent after clear_branch",
    );
    // New compaction output file was cleaned up on the refusal path.
    // (The two original L0 segments were removed by clear_branch.)
    assert_eq!(
        count_sst_files(&branch_dir),
        0,
        "no orphan .sst files after refused compaction",
    );
}

#[test]
fn compact_l0_to_l1_does_not_resurrect_cleared_branch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = test_branch(0xA1);
    seed_two_l0_segments(&store, b);

    let branch_hex = hex_encode_branch(&b);
    let branch_dir = dir.path().join(&branch_hex);

    let pause = arm(pause_tag::COMPACT_L0_TO_L1, b);
    let s = Arc::clone(&store);
    let handle = std::thread::spawn(move || s.compact_l0_to_l1(&b, CommitVersion(0)));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&b));
    release(&pause);

    let result = handle.join().unwrap();
    drop(pause);

    let err = result.expect_err("L0→L1 compaction must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::CompactL0ToL1);

    assert!(!store.branches.contains_key(&b));
    assert_eq!(count_sst_files(&branch_dir), 0);
}

#[test]
fn compact_level_does_not_resurrect_cleared_branch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = test_branch(0xA2);
    seed_two_l0_segments(&store, b);

    let branch_hex = hex_encode_branch(&b);
    let branch_dir = dir.path().join(&branch_hex);

    // compact_level(0, ...) pushes L0 → L1; hits the atomic-swap path.
    let pause = arm(pause_tag::COMPACT_LEVEL, b);
    let s = Arc::clone(&store);
    let handle = std::thread::spawn(move || s.compact_level(&b, 0, CommitVersion(0)));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&b));
    release(&pause);

    let result = handle.join().unwrap();
    drop(pause);

    let err = result.expect_err("compact_level must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::CompactLevel);

    assert!(!store.branches.contains_key(&b));
    assert_eq!(count_sst_files(&branch_dir), 0);
}

#[test]
fn compact_level_trivial_move_does_not_resurrect_cleared_branch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = test_branch(0xA5);

    // Create exactly one L1 segment and no L2 overlap so compact_level(1, ...)
    // takes the trivial-move path rather than the full rewrite path.
    flush_branch_data(&store, b, &[("x", 1, 1)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    let branch_hex = hex_encode_branch(&b);
    let branch_dir = dir.path().join(&branch_hex);

    let pause = arm(pause_tag::COMPACT_LEVEL, b);
    let s = Arc::clone(&store);
    let handle = std::thread::spawn(move || s.compact_level(&b, 1, CommitVersion(0)));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&b));
    release(&pause);

    let result = handle.join().unwrap();
    drop(pause);

    let err = result.expect_err("trivial-move compact_level must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::CompactLevel);

    assert!(!store.branches.contains_key(&b));
    assert_eq!(count_sst_files(&branch_dir), 0);
}

#[test]
fn compact_tier_does_not_resurrect_cleared_branch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = test_branch(0xA3);
    seed_two_l0_segments(&store, b);

    let branch_hex = hex_encode_branch(&b);
    let branch_dir = dir.path().join(&branch_hex);

    let pause = arm(pause_tag::COMPACT_TIER, b);
    let s = Arc::clone(&store);
    let handle = std::thread::spawn(move || s.compact_tier(&b, &[0, 1], CommitVersion(0)));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&b));
    release(&pause);

    let result = handle.join().unwrap();
    drop(pause);

    let err = result.expect_err("compact_tier must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::CompactTier);

    assert!(!store.branches.contains_key(&b));
    assert_eq!(count_sst_files(&branch_dir), 0);
}

// ---------------------------------------------------------------------------
// SG-011: materialize_layer resurrection race
// ---------------------------------------------------------------------------

#[test]
fn materialize_layer_does_not_resurrect_cleared_branch() {
    // Dedicated parent/child ids for this test — avoid the shared
    // `parent_branch()` / `child_branch()` helpers so parallel tests that use
    // them can't collide on the install-pause keyspace.
    let parent = test_branch(0xB0);
    let child = test_branch(0xB1);

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let parent_ns = Arc::new(Namespace::new(parent, "default".to_string()));
    for (k, v, ver) in [("a", 1, 1), ("b", 2, 2)] {
        store
            .put_with_version_mode(
                Key::new(Arc::clone(&parent_ns), TypeTag::KV, k.as_bytes().to_vec()),
                Value::Int(v),
                CommitVersion(ver),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&parent);
    store.flush_oldest_frozen(&parent).unwrap();

    store.branches.entry(child).or_insert_with(BranchState::new);
    store.fork_branch(&parent, &child).unwrap();
    assert_eq!(store.inherited_layer_count(&child), 1);

    let child_hex = hex_encode_branch(&child);
    let child_dir = dir.path().join(&child_hex);

    let pause = arm(pause_tag::MATERIALIZE_LAYER, child);
    let s = Arc::clone(&store);
    let handle = std::thread::spawn(move || s.materialize_layer(&child, 0));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&child));
    release(&pause);

    let result = handle.join().unwrap();
    drop(pause);

    let err = result.expect_err("materialize_layer must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::MaterializeLayer);

    assert!(!store.branches.contains_key(&child));
    // Any materialize output was cleaned up on the refusal path.
    assert_eq!(
        count_sst_files(&child_dir),
        0,
        "no orphan .sst files after refused materialize",
    );
}

// ---------------------------------------------------------------------------
// SG-015: same-branch compactions serialized by caller → deterministic
// ---------------------------------------------------------------------------

#[test]
fn serialized_same_branch_compactions_are_deterministic() {
    // Two serialized compact_branch calls on the same branch must leave the
    // store in a deterministic state. This pins the existing caller-serialized
    // contract; explicit per-branch compaction locking is T4's choice.

    #[derive(Debug, PartialEq)]
    struct DeterministicOutcome {
        first: Option<CompactionResult>,
        second: Option<CompactionResult>,
        level_counts: Vec<usize>,
        entries: Vec<(Vec<u8>, Value, u64)>,
    }

    fn branch_entries(store: &SegmentedStore, b: &BranchId) -> Vec<(Vec<u8>, Value, u64)> {
        let mut entries: Vec<_> = store
            .list_branch(b)
            .into_iter()
            .map(|(key, value)| (key.user_key.into_vec(), value.value, value.version.as_u64()))
            .collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    fn level_counts(store: &SegmentedStore, b: &BranchId) -> Vec<usize> {
        let branch = store.branches.get(b).unwrap();
        let ver = branch.version.load();
        ver.levels.iter().map(std::vec::Vec::len).collect()
    }

    fn run_once() -> DeterministicOutcome {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();
        seed_two_l0_segments(&store, b);
        let first = store.compact_branch(&b, CommitVersion(0)).unwrap();
        let second = store.compact_branch(&b, CommitVersion(0)).unwrap();
        DeterministicOutcome {
            first,
            second,
            level_counts: level_counts(&store, &b),
            entries: branch_entries(&store, &b),
        }
    }

    let a = run_once();
    let b = run_once();
    assert_eq!(a, b, "serialized compact_branch outcome must be stable");
    assert!(a.first.is_some(), "first compaction should perform work");
    assert_eq!(
        a.second, None,
        "second compaction should be a deterministic no-op"
    );
    assert_eq!(
        a.level_counts[0], 1,
        "two compactions should deterministically collapse to one L0 segment",
    );
}

// ---------------------------------------------------------------------------
// SG-012: clear_branch interleaved with compaction → clean reopen
// ---------------------------------------------------------------------------

#[test]
fn clear_branch_interleaved_with_compaction_reopens_clean() {
    // After compaction is refused mid-flight by clear_branch, a reopen must:
    //   (a) not see the deleted branch,
    //   (b) not find orphan .sst files in the branch dir,
    //   (c) report healthy recovery.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = test_branch(0xA4);
    seed_two_l0_segments(&store, b);

    let branch_hex = hex_encode_branch(&b);
    let branch_dir = dir.path().join(&branch_hex);

    let pause = arm(pause_tag::COMPACT_BRANCH, b);
    let s = Arc::clone(&store);
    let b_clone = b;
    let handle = std::thread::spawn(move || s.compact_branch(&b_clone, CommitVersion(0)));

    wait_until_entered(&pause);
    assert!(store.clear_branch(&b));
    release(&pause);

    let race_result = handle.join().unwrap();
    drop(pause);

    // Confirm the race actually hit the refusal path — otherwise the reopen
    // assertions below would pass trivially for the wrong reason.
    let err = race_result.expect_err("compaction must refuse to resurrect");
    assert_branch_deleted_during_op(&err, BranchOp::CompactBranch);

    drop(store);

    // Reopen.
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let outcome = store2.recover_segments().unwrap();

    assert!(
        !store2.branches.contains_key(&b),
        "reopened store must not contain the cleared branch",
    );
    assert_eq!(
        count_sst_files(&branch_dir),
        0,
        "no orphan .sst files after reopen (best-effort cleanup + GC succeeded)",
    );
    // Recovery is healthy when there is nothing on disk to recover for this branch.
    assert!(
        matches!(outcome.health, crate::segmented::RecoveryHealth::Healthy),
        "expected Healthy recovery, got {:?}",
        outcome.health,
    );
}

// ---------------------------------------------------------------------------
// Sanity: arming a pause has zero effect when the op isn't invoked
// ---------------------------------------------------------------------------

#[test]
fn install_pause_only_triggers_matching_op_and_branch() {
    // Arm COMPACT_L0_TO_L1 on the test branch but run compact_branch (different
    // tag) — it must not block. Then arm the matching tag on a DIFFERENT
    // branch_id and run compact_branch on our branch — still must not block,
    // proving the key is (op_tag, branch_id) not just op_tag.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = test_branch(0xC0);
    seed_two_l0_segments(&store, b);

    // (a) unrelated tag, same branch — must not trigger.
    let pause_a = arm(pause_tag::COMPACT_L0_TO_L1, b);
    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let done = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            b1.wait();
            store.compact_branch(&b, CommitVersion(0))
        });
        barrier.wait();
        h.join().unwrap()
    });
    done.expect("compact_branch must complete when unrelated tag is armed");
    drop(pause_a);

    // (b) same tag, unrelated branch — must not trigger.
    let other_branch = test_branch(0xC1);
    let pause_b = arm(pause_tag::COMPACT_BRANCH, other_branch);

    // Give ourselves two fresh L0 segments to compact again.
    seed_two_l0_segments(&store, b);
    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let done = std::thread::scope(|scope| {
        let h = scope.spawn(|| {
            b1.wait();
            store.compact_branch(&b, CommitVersion(0))
        });
        barrier.wait();
        h.join().unwrap()
    });
    done.expect("compact_branch must complete when arm targets a different branch");
    drop(pause_b);
}
