//! B3.2 — generation-safe branch write paths.
//!
//! Exercises the `BranchService` → `BranchControlStore` wiring landed in
//! B3.2. Each test observes the control record through the canonical
//! `BranchService::control_record` accessor so the generation bump,
//! lifecycle flip, and fork anchor propagation are asserted against the
//! engine's own authority, not inferred from downstream side-effects.
//!
//! Invariants covered (from `docs/design/branching/b3-phasing-plan.md`):
//!
//! - Same-name `create` / `delete` cycles bump `BranchGeneration`
//!   monotonically without reusing previous lifecycle identities.
//! - `delete` flips the record to `Deleted` and clears the active
//!   pointer atomically with the legacy metadata purge.
//! - `fork` records the parent's live `BranchRef` (generation-aware)
//!   as the fork anchor, so a fork taken after recreate references the
//!   new generation, not the tombstoned one.
//! - Storage-fork-first ordering (AD6) holds even when the KV
//!   transaction aborts — an orphaned storage fork is harmless and the
//!   subsequent create-after-failure still works.
//! - Concurrent create-after-delete races are serialized by the
//!   branch-commit lock and generation allocation rather than producing
//!   duplicate active records.

#![cfg(not(miri))]

use crate::common::*;
use std::sync::{Arc, Barrier};
use std::thread;
use strata_core::BranchId;
use strata_core::Value;
use strata_engine::BranchRef;
use strata_engine::{primitives::extensions::KVStoreExt, BranchLifecycleStatus};

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

/// Seed a single KV write on `name` so the storage layer materialises a
/// branch — COW fork fails if the source has no memtable/segments.
fn seed_storage(test_db: &TestDb, name: &str) {
    let kv = test_db.kv();
    let branch_id = resolve(name);
    kv.put(&branch_id, "default", "_seed_", Value::Int(1))
        .expect("seed write succeeds");
}

// ============================================================================
// Generation bump across delete + recreate
// ============================================================================

#[test]
fn create_allocates_gen_zero_active_record() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("main").unwrap();

    let rec = branches
        .control_record("main")
        .unwrap()
        .expect("create writes an active control record");
    assert_eq!(rec.branch, BranchRef::new(resolve("main"), 0));
    assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
    assert!(rec.fork.is_none(), "root creates have no fork anchor");
}

#[test]
fn delete_flips_record_to_deleted_and_clears_active_pointer() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("scratch").unwrap();
    assert!(branches.control_record("scratch").unwrap().is_some());

    branches.delete("scratch").unwrap();

    // Active-pointer is cleared → find_active_by_name returns None.
    assert!(
        branches.control_record("scratch").unwrap().is_none(),
        "deleting a branch must clear its active pointer"
    );

    // Recreate — must allocate gen 1 (the tombstone is still at gen 0).
    branches.create("scratch").unwrap();
    let rec = branches.control_record("scratch").unwrap().unwrap();
    assert_eq!(
        rec.branch,
        BranchRef::new(resolve("scratch"), 1),
        "recreate after delete must bump generation to 1"
    );
    assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
}

#[test]
fn two_full_delete_recreate_cycles_reach_generation_two() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("cycle").unwrap();
    branches.delete("cycle").unwrap();
    branches.create("cycle").unwrap(); // gen 1
    branches.delete("cycle").unwrap();
    branches.create("cycle").unwrap(); // gen 2

    let rec = branches.control_record("cycle").unwrap().unwrap();
    assert_eq!(
        rec.branch,
        BranchRef::new(resolve("cycle"), 2),
        "two full cycles must yield generation 2"
    );
}

#[test]
fn recreate_reuses_branch_id_but_distinct_branch_ref() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("alt").unwrap();
    let before = branches.control_record("alt").unwrap().unwrap().branch;

    branches.delete("alt").unwrap();
    branches.create("alt").unwrap();
    let after = branches.control_record("alt").unwrap().unwrap().branch;

    assert_eq!(before.id, after.id, "BranchId is deterministic from name");
    assert_ne!(
        before, after,
        "distinct lifecycle instances must produce distinct BranchRefs"
    );
}

// ============================================================================
// Fork anchors pick up the parent's live generation
// ============================================================================

#[test]
fn fork_anchor_references_parent_generation_zero() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("main").unwrap();
    seed_storage(&test_db, "main");
    let parent_ref = branches.control_record("main").unwrap().unwrap().branch;

    branches.fork("main", "feature-a").unwrap();

    let child = branches.control_record("feature-a").unwrap().unwrap();
    let anchor = child.fork.expect("child fork has an anchor");
    assert_eq!(anchor.parent, parent_ref);
    assert_eq!(
        anchor.parent,
        BranchRef::new(resolve("main"), 0),
        "fork from a gen-0 parent must capture generation 0"
    );
}

#[test]
fn fork_after_recreate_anchors_to_new_parent_generation() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    // main@gen0 → feature-a anchored at {main, 0}
    branches.create("main").unwrap();
    seed_storage(&test_db, "main");
    branches.fork("main", "feature-a").unwrap();
    let feature_a_anchor = branches
        .control_record("feature-a")
        .unwrap()
        .unwrap()
        .fork
        .unwrap();
    assert_eq!(feature_a_anchor.parent.generation, 0);

    // Recycle main → gen 1
    branches.delete("feature-a").unwrap();
    branches.delete("main").unwrap();
    branches.create("main").unwrap();
    seed_storage(&test_db, "main");
    let new_parent = branches.control_record("main").unwrap().unwrap().branch;
    assert_eq!(new_parent.generation, 1);

    // Fork from main@gen1 — anchor must point at {main, 1}, not {main, 0}.
    branches.fork("main", "feature-b").unwrap();
    let feature_b = branches.control_record("feature-b").unwrap().unwrap();
    let anchor = feature_b.fork.expect("fork anchor present");
    assert_eq!(anchor.parent, new_parent);
    assert_eq!(
        anchor.parent.generation, 1,
        "post-recreate fork must pick up the parent's new generation"
    );

    // The earlier feature-a anchor is still correctly pinned to gen 0
    // (even though gen 0 is tombstoned — lineage is immutable).
    assert_eq!(feature_a_anchor.parent.generation, 0);
}

#[test]
fn child_gets_fresh_generation_independent_of_parent() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("main").unwrap();
    seed_storage(&test_db, "main");
    branches.fork("main", "first-child").unwrap();
    branches.fork("main", "second-child").unwrap();

    let first = branches.control_record("first-child").unwrap().unwrap();
    let second = branches.control_record("second-child").unwrap().unwrap();
    assert_eq!(first.branch.generation, 0);
    assert_eq!(
        second.branch.generation, 0,
        "sibling forks each start at their own gen 0"
    );
    assert_ne!(first.branch.id, second.branch.id);
}

#[test]
fn forked_child_recreate_bumps_child_generation() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("main").unwrap();
    seed_storage(&test_db, "main");
    branches.fork("main", "feature").unwrap();
    assert_eq!(
        branches
            .control_record("feature")
            .unwrap()
            .unwrap()
            .branch
            .generation,
        0
    );

    branches.delete("feature").unwrap();
    branches.fork("main", "feature").unwrap();

    let rec = branches.control_record("feature").unwrap().unwrap();
    assert_eq!(
        rec.branch.generation, 1,
        "refork after delete must allocate a new child generation"
    );
    let anchor = rec.fork.unwrap();
    assert_eq!(
        anchor.parent.generation, 0,
        "parent still on its original generation"
    );
}

// ============================================================================
// Invariants: legacy metadata + control record remain coherent
// ============================================================================

#[test]
fn legacy_metadata_and_control_record_stay_in_sync() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("dual").unwrap();

    // Both surfaces report the branch.
    assert!(branches.exists("dual").unwrap());
    assert!(branches.control_record("dual").unwrap().is_some());
    assert!(branches.list().unwrap().contains(&"dual".to_string()));

    branches.delete("dual").unwrap();

    // Both surfaces agree that the branch is gone.
    assert!(!branches.exists("dual").unwrap());
    assert!(branches.control_record("dual").unwrap().is_none());
    assert!(!branches.list().unwrap().contains(&"dual".to_string()));
}

#[test]
fn create_duplicate_live_branch_is_rejected() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("dup").unwrap();
    let err = branches.create("dup").unwrap_err();
    assert!(
        err.to_string().contains("already exists"),
        "duplicate create must surface a clean error; got: {err}"
    );

    // The existing record is untouched.
    let rec = branches.control_record("dup").unwrap().unwrap();
    assert_eq!(rec.branch.generation, 0);
}

// ============================================================================
// Concurrent create-after-delete
// ============================================================================

#[test]
fn concurrent_create_after_delete_serializes_via_commit_lock() {
    let test_db = TestDb::new();
    let db = test_db.db.clone();
    db.branches().create("race").unwrap();
    db.branches().delete("race").unwrap();

    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = db.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.branches().create("race")
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for h in handles {
        match h.join().unwrap() {
            Ok(_) => successes += 1,
            Err(_) => failures += 1,
        }
    }

    assert_eq!(successes, 1, "exactly one create must win the race");
    assert_eq!(failures, 3, "the other three must surface duplicate errors");

    // The winner's record: gen 1 (one prior full cycle).
    let rec = db.branches().control_record("race").unwrap().unwrap();
    assert_eq!(rec.branch.generation, 1);
    assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
}

#[test]
fn recreate_cycles_from_cross_thread_creates_allocate_monotonic_generations() {
    // Three sequential delete+create cycles where each `create` runs on
    // a fresh thread. Not a race test — each iteration joins before the
    // next begins. Purpose: prove that allocating a generation from
    // another thread (which sees the counter through an Arc<Database>,
    // not via thread-local state) still bumps monotonically.
    let test_db = TestDb::new();
    let db = test_db.db.clone();
    db.branches().create("spin").unwrap();

    for _ in 0..3 {
        db.branches().delete("spin").unwrap();
        let db2 = db.clone();
        let h = thread::spawn(move || db2.branches().create("spin"));
        h.join().unwrap().unwrap();
    }

    let rec = db.branches().control_record("spin").unwrap().unwrap();
    assert_eq!(
        rec.branch.generation, 3,
        "three full cycles yield gen 3 regardless of which thread performed the create"
    );
}

#[test]
fn stale_pre_delete_transaction_cannot_commit_into_recreated_generation() {
    let test_db = TestDb::new();
    let db = test_db.db.clone();
    let branches = db.branches();

    branches.create("guarded").unwrap();
    let branch_id = resolve("guarded");
    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.kv_put("late-write", Value::Int(7)).unwrap();

    branches.delete("guarded").unwrap();
    branches.create("guarded").unwrap();

    let err = txn.commit().unwrap_err();
    assert!(
        err.to_string().contains("lifecycle advanced")
            || err.to_string().contains("no longer has active generation"),
        "stale pre-delete txn must be rejected after recreate; got: {err}"
    );

    assert!(
        test_db
            .kv()
            .get(&branch_id, "default", "late-write")
            .unwrap()
            .is_none(),
        "stale transaction must not write into the recreated branch"
    );
    let rec = branches.control_record("guarded").unwrap().unwrap();
    assert_eq!(rec.branch.generation, 1);
}

#[test]
fn stale_pre_delete_transaction_cannot_commit_into_recreated_literal_default_branch() {
    let test_db = TestDb::new();
    let db = test_db.db.clone();
    let branches = db.branches();

    branches.create("default").unwrap();
    let branch_id = resolve("default");
    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.kv_put("late-default-write", Value::Int(9)).unwrap();

    branches.delete("default").unwrap();
    branches.create("default").unwrap();

    let err = txn.commit().unwrap_err();
    // Literal "default" shares the nil BranchId with internal global
    // branch-index operations, so its generation guard is lazily seeded via
    // the active-pointer read key rather than the eager per-branch guard used
    // by ordinary branches. The important invariant is rejection of the stale
    // pre-delete transaction, not the exact error variant.
    assert!(
        err.to_string().contains("lifecycle advanced")
            || err.to_string().contains("no longer has active generation")
            || err.to_string().contains("Validation failed"),
        "stale pre-delete txn on literal default branch must be rejected after recreate; got: {err}"
    );

    assert!(
        test_db
            .kv()
            .get(&branch_id, "default", "late-default-write")
            .unwrap()
            .is_none(),
        "stale transaction must not write into the recreated literal default branch"
    );
    let rec = branches.control_record("default").unwrap().unwrap();
    assert_eq!(rec.branch.generation, 1);
}

// ============================================================================
// Storage-first ordering: aborted KV txn leaves no control record
// ============================================================================

#[test]
fn create_duplicate_after_partial_failure_still_succeeds_once_cleared() {
    // AD6: when the KV transaction fails after storage fork commits, the
    // fork leaves harmless orphan storage state. The next attempt must
    // succeed because no control record was ever written.
    //
    // We exercise the same guarantee without injecting storage
    // failures: fork to an existing destination → the metadata-write
    // transaction fails, and subsequent successful fork to the same
    // name works cleanly.
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("main").unwrap();
    seed_storage(&test_db, "main");
    branches.create("feature").unwrap();

    // First fork fails because destination already exists.
    let err = branches.fork("main", "feature").unwrap_err();
    assert!(
        err.to_string().contains("already exists"),
        "fork to existing dest must surface a duplicate-destination error; got: {err}"
    );

    // No stray control record was created for the failed attempt —
    // feature's record remains the one from the earlier `create`.
    let rec = branches.control_record("feature").unwrap().unwrap();
    assert!(
        rec.fork.is_none(),
        "failed fork must not retroactively mutate the existing record"
    );

    // Delete + re-fork: clean path lands with a fork anchor.
    branches.delete("feature").unwrap();
    branches.fork("main", "feature").unwrap();

    let rec = branches.control_record("feature").unwrap().unwrap();
    assert_eq!(rec.branch.generation, 1, "second feature is gen 1");
    let anchor = rec.fork.expect("re-forked child has anchor");
    assert_eq!(anchor.parent, BranchRef::new(resolve("main"), 0));
}

// ============================================================================
// Reopen preserves generation counter
// ============================================================================

#[test]
fn recreate_after_reopen_continues_generation_sequence() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("persist").unwrap();
    test_db.db.branches().delete("persist").unwrap();
    test_db.db.branches().create("persist").unwrap();

    // After reopen, the persisted next-gen counter must still see the
    // existing lifecycle instances so a further recreate picks gen 2.
    test_db.reopen();

    test_db.db.branches().delete("persist").unwrap();
    test_db.db.branches().create("persist").unwrap();

    let rec = test_db
        .db
        .branches()
        .control_record("persist")
        .unwrap()
        .unwrap();
    assert_eq!(
        rec.branch.generation, 2,
        "generation counter must survive reopen"
    );
}
