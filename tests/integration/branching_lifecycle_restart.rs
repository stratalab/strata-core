//! B4.2 — lifecycle persistence and same-name recreate across reopen.
//!
//! Locks the post-reopen half of the B4.2 coverage bar:
//!
//! - An `Archived` lifecycle written before reopen is still observable
//!   after reopen and continues to refuse writes with `BranchArchived`.
//! - Same-name delete → reopen → recreate chains produce strictly
//!   monotone generations on the store, and `merge_base` against the
//!   new generation never surfaces a stale fork anchor from the
//!   tombstoned generation.
//! - The `next_generation` counter survives reopen so the monotonicity
//!   holds after more than one cycle.
//! - Bundle import onto a tombstoned target ignores the bundle's
//!   generation and allocates a fresh one (AD7), and that fresh
//!   generation survives a subsequent reopen.
//!
//! These complement `branching_control_store_recovery.rs` (which
//! proves recovery mechanics for the standard B3 flows) by focusing
//! on the lifecycle-gate invariants introduced in B4.

#![cfg(not(miri))]

use crate::common::branching::archive_branch_for_test;
use crate::common::*;
use std::sync::Arc;
use strata_core::value::Value;
use strata_core::{BranchRef, StrataError};
use strata_engine::bundle;
use tempfile::TempDir;

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

/// Seed a single row so the branch materialises in storage — required
/// for export and for some post-reopen introspection paths.
fn seed(db: &Arc<Database>, name: &str) {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", "_seed_", Value::Int(1))
        .expect("seed write succeeds");
}

// =============================================================================
// Archived lifecycle persists across reopen
// =============================================================================

#[test]
fn archived_lifecycle_persists_across_reopen() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("frozen").unwrap();
    seed(&test_db.db, "frozen");
    let archived_ref = archive_branch_for_test(&test_db.db, "frozen");

    test_db.reopen();

    // Control record still exists, still archived.
    let rec = test_db
        .db
        .branches()
        .control_record("frozen")
        .unwrap()
        .expect("archived record persists across reopen");
    assert_eq!(rec.branch, archived_ref, "reopen preserves BranchRef");
    assert!(
        matches!(rec.lifecycle, strata_core::branch::BranchLifecycleStatus::Archived),
        "reopen preserves Archived lifecycle"
    );
}

#[test]
fn post_reopen_archived_branch_refuses_writes_with_branch_archived() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("frozen").unwrap();
    seed(&test_db.db, "frozen");
    // Also set up a live source so we can exercise merge-as-target
    // against the archived branch post-reopen.
    test_db.db.branches().create("live").unwrap();
    seed(&test_db.db, "live");
    archive_branch_for_test(&test_db.db, "frozen");

    test_db.reopen();

    // Every gated mutation must surface the typed archived error after
    // reopen — the widened active-pointer (KD1) and the gate helpers
    // must both survive recovery, not just the persisted record. Cover
    // three gate surfaces: annotation (tag), annotation (add_note), and
    // structural (revert). A regression that touched just one surface
    // without migrating would be caught here.
    match test_db.db.branches().tag("frozen", "v1", None, None, None) {
        Err(StrataError::BranchArchived { name }) => {
            assert_eq!(name, "frozen", "typed error carries the archived name");
        }
        other => panic!("expected BranchArchived (tag) after reopen, got {other:?}"),
    }

    match test_db.db.branches().add_note(
        "frozen",
        strata_core::id::CommitVersion(1),
        "n",
        None,
        None,
    ) {
        Err(StrataError::BranchArchived { name }) => {
            assert_eq!(name, "frozen");
        }
        other => panic!("expected BranchArchived (add_note) after reopen, got {other:?}"),
    }

    match test_db.db.branches().revert(
        "frozen",
        strata_core::id::CommitVersion(1),
        strata_core::id::CommitVersion(1),
    ) {
        Err(StrataError::BranchArchived { name }) => {
            assert_eq!(name, "frozen");
        }
        other => panic!("expected BranchArchived (revert) after reopen, got {other:?}"),
    }
}

#[test]
fn post_reopen_archived_branch_is_still_deletable() {
    // KD1 rationale check: `delete` uses `require_visible`, so an
    // Archived branch must remain deletable even after the gate and
    // the widened pointer round-trip through reopen.
    let mut test_db = TestDb::new();
    test_db.db.branches().create("frozen").unwrap();
    seed(&test_db.db, "frozen");
    archive_branch_for_test(&test_db.db, "frozen");

    test_db.reopen();

    test_db
        .db
        .branches()
        .delete("frozen")
        .expect("Archived branch must be deletable after reopen");
    assert!(
        test_db.db.branches().control_record("frozen").unwrap().is_none(),
        "post-delete record is absent"
    );
}

// =============================================================================
// Same-name recreate across reopen: monotone generations, no stale lineage
// =============================================================================

#[test]
fn delete_reopen_recreate_allocates_monotone_generations() {
    let mut test_db = TestDb::new();

    // gen 0
    test_db.db.branches().create("rolling").unwrap();
    let gen0 = test_db
        .db
        .branches()
        .control_record("rolling")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(gen0.generation, 0);

    test_db.db.branches().delete("rolling").unwrap();
    test_db.reopen();
    test_db.db.branches().create("rolling").unwrap();

    let gen1 = test_db
        .db
        .branches()
        .control_record("rolling")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(
        gen1,
        BranchRef::new(resolve("rolling"), 1),
        "post-reopen recreate must advance generation"
    );

    test_db.db.branches().delete("rolling").unwrap();
    test_db.reopen();
    test_db.db.branches().create("rolling").unwrap();

    let gen2 = test_db
        .db
        .branches()
        .control_record("rolling")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(
        gen2.generation, 2,
        "next_generation counter survives multiple reopens"
    );
}

#[test]
fn merge_base_does_not_surface_tombstoned_generation_after_reopen() {
    // Setup: fork feature from main at gen 0, merge once, delete feature,
    // reopen, recreate feature at gen 1. `merge_base(main, feature)` on
    // the new generation must not return any point from the old
    // generation's lineage — edges are scoped by `BranchRef`, not by
    // name, so the tombstoned feature@gen0 edges must stay invisible.
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    seed(&test_db.db, "main");
    test_db.db.branches().fork("main", "feature").unwrap();
    test_db
        .kv()
        .put(&resolve("feature"), "default", "delta", Value::Int(1))
        .unwrap();
    test_db.db.branches().merge("feature", "main").unwrap();
    test_db.db.branches().delete("feature").unwrap();

    test_db.reopen();

    test_db.db.branches().create("feature").unwrap();
    let new_feature = test_db
        .db
        .branches()
        .control_record("feature")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(new_feature.generation, 1);

    // The new feature@gen1 was created as a fresh root (no fork), so
    // it has no lineage relationship to main. Merge base MUST be None.
    let mb = test_db
        .db
        .branches()
        .merge_base("main", "feature")
        .unwrap();
    assert!(
        mb.is_none(),
        "recreated branch without fork inherits no lineage — merge_base must be None, got {mb:?}"
    );
}

// =============================================================================
// next_generation counter monotonicity across reopens
// =============================================================================

#[test]
fn next_generation_counter_is_monotone_across_reopens() {
    let mut test_db = TestDb::new();

    for expected_gen in 0..4u64 {
        test_db.db.branches().create("cycle").unwrap();
        let rec = test_db
            .db
            .branches()
            .control_record("cycle")
            .unwrap()
            .unwrap();
        assert_eq!(
            rec.branch.generation, expected_gen,
            "cycle {expected_gen}: generation must equal {expected_gen}"
        );
        test_db.db.branches().delete("cycle").unwrap();
        test_db.reopen();
    }

    // One more create — should land at gen 4, confirming the pattern
    // survived the final reopen as well.
    test_db.db.branches().create("cycle").unwrap();
    let final_rec = test_db
        .db
        .branches()
        .control_record("cycle")
        .unwrap()
        .unwrap();
    assert_eq!(final_rec.branch.generation, 4);
}

// =============================================================================
// Bundle-import collision: tombstoned target + reopen preserves fresh gen
// =============================================================================

fn export_bundle(db: &Arc<Database>, branch: &str) -> (TempDir, std::path::PathBuf) {
    let bundle_dir = TempDir::new().unwrap();
    let path = bundle_dir
        .path()
        .join(format!("{branch}.branchbundle.tar.zst"));
    bundle::export_branch(db, branch, &path).expect("export succeeds");
    (bundle_dir, path)
}

#[test]
fn bundle_import_after_reopen_preserves_fresh_generation() {
    // Source DB carries `foo@gen3` — push through several cycles so the
    // bundle's `generation` field is non-trivial.
    let source_db = TestDb::new();
    for _ in 0..3 {
        source_db.db.branches().create("foo").unwrap();
        source_db.db.branches().delete("foo").unwrap();
    }
    source_db.db.branches().create("foo").unwrap(); // gen 3
    assert_eq!(
        source_db
            .db
            .branches()
            .control_record("foo")
            .unwrap()
            .unwrap()
            .branch
            .generation,
        3
    );
    let (_bundle_keepalive, bundle_path) = export_bundle(&source_db.db, "foo");

    // Target DB: has a single tombstone at gen 0 (one create + delete).
    let mut target_db = TestDb::new();
    target_db.db.branches().create("foo").unwrap();
    target_db.db.branches().delete("foo").unwrap();

    // Import — AD7 says target allocates gen 1 (from its own counter),
    // NOT bundle's gen 3.
    bundle::import_branch(&target_db.db, &bundle_path).unwrap();
    let after_import = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(after_import.generation, 1, "AD7: fresh gen ignores bundle's gen");

    // Reopen and assert the fresh generation persists, the bundle's
    // generation has not silently resurfaced, and a subsequent
    // delete+create still advances monotonically from gen 1.
    target_db.reopen();
    let after_reopen = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap()
        .branch;
    assert_eq!(
        after_reopen, after_import,
        "post-reopen BranchRef matches pre-reopen (gen 1, not bundle's gen 3)"
    );

    target_db.db.branches().delete("foo").unwrap();
    target_db.db.branches().create("foo").unwrap();
    let next_gen = target_db
        .db
        .branches()
        .control_record("foo")
        .unwrap()
        .unwrap()
        .branch
        .generation;
    assert_eq!(
        next_gen, 2,
        "post-import counter must advance monotonically from the allocated fresh gen"
    );
}

// =============================================================================
// Archived → reopen → recreate path preserves tombstoning semantics
// =============================================================================

#[test]
fn archived_branch_after_delete_and_reopen_allows_recreate() {
    // Archive a branch, then delete it via the production path (delete
    // uses `require_visible`, so Archived → Deleted is allowed), reopen,
    // recreate under the same name: must allocate a strictly-greater
    // generation, not reuse the archived instance's BranchRef.
    let mut test_db = TestDb::new();
    test_db.db.branches().create("layered").unwrap();
    seed(&test_db.db, "layered");
    let gen0 = test_db
        .db
        .branches()
        .control_record("layered")
        .unwrap()
        .unwrap()
        .branch;
    archive_branch_for_test(&test_db.db, "layered");
    test_db.db.branches().delete("layered").unwrap();

    test_db.reopen();

    test_db.db.branches().create("layered").unwrap();
    let gen1 = test_db
        .db
        .branches()
        .control_record("layered")
        .unwrap()
        .unwrap()
        .branch;
    assert_ne!(gen0, gen1, "post-reopen recreate must not reuse prior BranchRef");
    assert_eq!(gen1.generation, 1);
}
