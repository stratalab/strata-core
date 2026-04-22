//! B4.2 — mutation-path coverage matrix for the lifecycle write gate.
//!
//! Exercises every `BranchService` mutation (plus the bundle-import path)
//! against every lifecycle state a target / source can be in. The B4.1
//! gate is meant to be uniform: one helper (`require_writable_by_name`
//! or `require_visible_by_name`) runs at the top of every mutation, and
//! no production path should smuggle a gate-bypassing side effect past
//! it. B4.2 locks that uniformity.
//!
//! ## What each cell asserts
//!
//! - Correct typed error on the `StrataError` enum discriminant
//!   (`BranchArchived` / `BranchNotFoundByName` / `InvalidInput` for
//!   name-allocation duplicates). Matched on the variant, never on
//!   message text.
//! - No `BranchOpEvent` fires on reject paths.
//! - No lineage edge appends to the target on reject paths.
//! - Repeated refused calls do not deadlock (gate runs before any
//!   lock acquisition).
//!
//! ## State synthesis
//!
//! No production path produces the `Archived` lifecycle. The matrix
//! uses `common::branching::set_lifecycle_for_test` (a feature-gated
//! engine test-support hook) to flip a live record's `lifecycle`
//! field in place. `Deleted` state uses the production
//! `BranchService::delete` path. `Missing` is "never created".
//!
//! Single-branch ops (`materialize`) exercise all four states as
//! truly distinct setups: `Missing` skips creation entirely.
//!
//! Cross-matrix ops (fork / merge / `cherry_pick` /
//! `cherry_pick_from_diff`) require a live fork-parent pair to set up
//! the success cells, so the `Missing` label on a name that was first
//! created and then unwound collapses onto the `Deleted` setup path
//! (`reshape` calls `delete` for both). The gate surfaces the same
//! `BranchNotFoundByName` error in both cases, so the assertion
//! vocabulary matches; the distinction between "never existed" and
//! "tombstoned" is covered by the single-branch matrix cells.
//!
//! Source × target cross-matrix applies to ops that read a source and
//! write a target (fork / merge / `cherry_pick` / `cherry_pick_from_diff`).

#![cfg(not(miri))]

use crate::common::branching::{
    archive_branch_for_test, assert_no_side_effects_since, set_lifecycle_for_test,
    BranchSideEffectCheckpoint, CapturingBranchObserver,
};
use crate::common::*;
use std::sync::Arc;
use strata_core::branch::BranchLifecycleStatus;
use strata_core::id::CommitVersion;
use strata_core::value::Value;
use strata_core::{BranchRef, StrataError};
use strata_engine::{bundle, CherryPickFilter, Database, MergeOptions};

// =============================================================================
// Enum of lifecycle target states and helpers to synthesize them
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetState {
    Active,
    Archived,
    Deleted,
    Missing,
}

impl TargetState {
    fn all() -> [TargetState; 4] {
        [
            TargetState::Active,
            TargetState::Archived,
            TargetState::Deleted,
            TargetState::Missing,
        ]
    }

    fn label(self) -> &'static str {
        match self {
            TargetState::Active => "Active",
            TargetState::Archived => "Archived",
            TargetState::Deleted => "Deleted",
            TargetState::Missing => "Missing",
        }
    }
}

/// Build `name` into `state` on `db`. For `Active`, seeds a single KV
/// write so the storage layer materialises the branch (fork COW needs
/// a memtable on the source).
fn materialize(db: &Arc<Database>, name: &str, state: TargetState) -> Option<BranchRef> {
    match state {
        TargetState::Active => {
            db.branches().create(name).unwrap();
            seed(db, name);
            Some(db.branches().control_record(name).unwrap().unwrap().branch)
        }
        TargetState::Archived => {
            db.branches().create(name).unwrap();
            seed(db, name);
            Some(archive_branch_for_test(db, name))
        }
        TargetState::Deleted => {
            db.branches().create(name).unwrap();
            seed(db, name);
            let deleted_ref = db.branches().control_record(name).unwrap().unwrap().branch;
            db.branches().delete(name).unwrap();
            Some(deleted_ref)
        }
        TargetState::Missing => {
            // Intentionally no-op — the name must have no record at all.
            None
        }
    }
}

/// Write a single KV row so the branch's storage layer materialises —
/// required for fork COW and for `Archived` reads in source-side ops.
fn seed(db: &Arc<Database>, name: &str) {
    let bid = BranchId::from_user_name(name);
    KVStore::new(db.clone())
        .put(&bid, "default", "_seed_", Value::Int(1))
        .expect("seed write succeeds");
}

// =============================================================================
// Assertion helpers — match on enum variants, never on message text
// =============================================================================

fn assert_archived(result: Result<impl std::fmt::Debug, StrataError>, name: &str, scenario: &str) {
    match result {
        Err(StrataError::BranchArchived { name: n }) => {
            assert_eq!(
                n, name,
                "[{scenario}] BranchArchived carries the target branch name"
            );
        }
        Err(e) => panic!("[{scenario}] expected BranchArchived({name}), got: {e:?}"),
        Ok(v) => panic!("[{scenario}] expected BranchArchived({name}), got Ok({v:?})"),
    }
}

fn assert_not_found(result: Result<impl std::fmt::Debug, StrataError>, name: &str, scenario: &str) {
    match result {
        Err(StrataError::BranchNotFoundByName { name: n, .. }) => {
            assert_eq!(
                n, name,
                "[{scenario}] BranchNotFoundByName carries the target branch name"
            );
        }
        Err(e) => panic!("[{scenario}] expected BranchNotFoundByName({name}), got: {e:?}"),
        Ok(v) => panic!("[{scenario}] expected BranchNotFoundByName({name}), got Ok({v:?})"),
    }
}

fn assert_invalid_input_contains(
    result: Result<impl std::fmt::Debug, StrataError>,
    fragment: &str,
    scenario: &str,
) {
    match result {
        Err(StrataError::InvalidInput { message }) => {
            assert!(
                message.contains(fragment),
                "[{scenario}] expected InvalidInput containing {fragment:?}, got: {message}"
            );
        }
        Err(e) => panic!("[{scenario}] expected InvalidInput containing {fragment:?}, got: {e:?}"),
        Ok(v) => {
            panic!("[{scenario}] expected InvalidInput containing {fragment:?}, got Ok({v:?})")
        }
    }
}

/// Setup: open a fresh `TestDb` with a capturing observer already
/// registered. Returns the db wrapper and the observer handle.
fn setup() -> (TestDb, Arc<CapturingBranchObserver>) {
    let test_db = TestDb::new();
    let obs = CapturingBranchObserver::new();
    test_db.db.branch_op_observers().register(obs.clone());
    (test_db, obs)
}

/// Take a checkpoint on `target` and return it — for use around a
/// gate-refused call.
fn checkpoint(
    db: &Arc<Database>,
    observer: &CapturingBranchObserver,
    target: &str,
    target_ref: Option<BranchRef>,
) -> BranchSideEffectCheckpoint {
    BranchSideEffectCheckpoint::capture(db, observer, target, target_ref)
}

// =============================================================================
// Op 1/12 — create (name-allocation semantics)
// =============================================================================
//
// `create` is not a write to an existing lifecycle instance; it's a
// name allocation. Per B4/KD1, an `Active` or `Archived` live record
// blocks creation with a duplicate error. `Deleted` and `Missing`
// succeed — `Deleted` allocates a strictly-greater generation.

#[test]
fn create_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        let scenario = format!("create[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db.branches().create(name);
        match state {
            TargetState::Active | TargetState::Archived => {
                assert_invalid_input_contains(result, "already exists", &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Deleted => {
                result.unwrap_or_else(|e| {
                    panic!("[{scenario}] create after delete must succeed: {e:?}")
                });
                let rec = db.branches().control_record(name).unwrap().unwrap();
                assert_eq!(
                    rec.branch.generation, 1,
                    "[{scenario}] recreate advances generation"
                );
            }
            TargetState::Missing => {
                result.unwrap_or_else(|e| {
                    panic!("[{scenario}] create on missing must succeed: {e:?}")
                });
                let rec = db.branches().control_record(name).unwrap().unwrap();
                assert_eq!(
                    rec.branch.generation, 0,
                    "[{scenario}] first create is gen 0"
                );
            }
        }
    }
}

// =============================================================================
// Op 2/12 — delete
// =============================================================================
//
// `delete` requires the target to be **visible** (Active or Archived).
// `Deleted` / `Missing` → `BranchNotFoundByName`.

#[test]
fn delete_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        let scenario = format!("delete[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db.branches().delete(name);
        match state {
            TargetState::Active | TargetState::Archived => {
                result.unwrap_or_else(|e| {
                    panic!("[{scenario}] delete visible branch must succeed: {e:?}")
                });
                assert!(
                    db.branches().control_record(name).unwrap().is_none(),
                    "[{scenario}] post-delete live record is absent"
                );
            }
            TargetState::Deleted | TargetState::Missing => {
                assert_not_found(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
        }
    }
}

// =============================================================================
// Op 3/12 — fork (source + target cross-matrix)
// =============================================================================
//
// - source: visible (Active / Archived) passes, otherwise BranchNotFound.
// - destination: live record (Active / Archived) is a duplicate; no
//   live record (Deleted / Missing) passes (allocates fresh generation).

#[test]
fn fork_matrix() {
    for source_state in TargetState::all() {
        for dest_state in TargetState::all() {
            let (test_db, obs) = setup();
            let db = test_db.db.clone();

            // Source / dest must be distinct names.
            let source = "src";
            let dest = "dst";
            let _source_ref = materialize(&db, source, source_state);
            let dest_ref = materialize(&db, dest, dest_state);

            let scenario = format!(
                "fork[src={}, dst={}]",
                source_state.label(),
                dest_state.label()
            );
            let before = checkpoint(&db, &obs, dest, dest_ref);

            let result = db.branches().fork(source, dest);

            let source_visible =
                matches!(source_state, TargetState::Active | TargetState::Archived);
            let dest_has_live = matches!(dest_state, TargetState::Active | TargetState::Archived);

            if !source_visible {
                assert_not_found(result, source, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            } else if dest_has_live {
                assert_invalid_input_contains(result, "already exists", &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            } else {
                result.unwrap_or_else(|e| {
                    panic!(
                        "[{scenario}] fork with visible source and free dest must succeed: {e:?}"
                    )
                });
                let rec = db.branches().control_record(dest).unwrap().unwrap();
                assert!(rec.fork.is_some(), "[{scenario}] fork anchor populated");
            }
        }
    }
}

// =============================================================================
// Op 4/12 — merge (source + target cross-matrix)
// =============================================================================
//
// - source: visible (Active / Archived) passes, otherwise BranchNotFound.
// - target: Active writable; Archived → BranchArchived; Deleted/Missing → BranchNotFound.

#[test]
fn merge_matrix() {
    for source_state in TargetState::all() {
        for target_state in TargetState::all() {
            let (test_db, obs) = setup();
            let db = test_db.db.clone();

            // Merge needs a fork relationship; otherwise `merge` will
            // refuse on "unrelated branches" for the Active × Active
            // success path. Setup: create `target`, seed it, fork
            // `source` from it — then reshape both to the required
            // states.
            db.branches().create("base").unwrap();
            seed(&db, "base");
            let source_name = "source";
            let target_name = "target";
            db.branches().fork("base", source_name).unwrap();
            db.branches().fork(source_name, target_name).unwrap();

            // Extra source-side change so there's something to merge in
            // the Active × Active success case.
            KVStore::new(db.clone())
                .put(
                    &BranchId::from_user_name(source_name),
                    "default",
                    "delta",
                    Value::Int(99),
                )
                .unwrap();

            let _source_ref = reshape(&db, source_name, source_state);
            let target_ref = reshape(&db, target_name, target_state);

            let scenario = format!(
                "merge[src={}, tgt={}]",
                source_state.label(),
                target_state.label()
            );
            let before = checkpoint(&db, &obs, target_name, target_ref);

            let result =
                db.branches()
                    .merge_with_options(source_name, target_name, MergeOptions::default());

            let source_visible =
                matches!(source_state, TargetState::Active | TargetState::Archived);

            match (source_visible, target_state) {
                (false, _) => {
                    assert_not_found(result, source_name, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Archived) => {
                    assert_archived(result, target_name, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Deleted | TargetState::Missing) => {
                    assert_not_found(result, target_name, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Active) => {
                    result.unwrap_or_else(|e| {
                        panic!("[{scenario}] merge Active → Active must succeed: {e:?}")
                    });
                }
            }
        }
    }
}

// =============================================================================
// Op 5/12 — revert (single target)
// =============================================================================

#[test]
fn revert_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        let scenario = format!("revert[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db
            .branches()
            .revert(name, CommitVersion(1), CommitVersion(1));

        match state {
            TargetState::Archived => {
                assert_archived(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Deleted | TargetState::Missing => {
                assert_not_found(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Active => {
                // revert on an Active branch against an out-of-range
                // version may return Ok (no-op) or error, but it must
                // NOT produce BranchArchived / BranchNotFoundByName —
                // those are the gate-only signals we test here.
                if let Err(e) = result {
                    assert!(
                        !matches!(
                            e,
                            StrataError::BranchArchived { .. }
                                | StrataError::BranchNotFoundByName { .. }
                        ),
                        "[{scenario}] Active target must not yield gate errors, got: {e:?}"
                    );
                }
            }
        }
    }
}

// =============================================================================
// Op 6/12 — cherry_pick (source + target cross-matrix)
// =============================================================================

#[test]
fn cherry_pick_matrix() {
    for source_state in TargetState::all() {
        for target_state in TargetState::all() {
            let (test_db, obs) = setup();
            let db = test_db.db.clone();

            // Fork relationship so the Active × Active case has
            // a source → target pair that shares lineage and a live
            // key to copy.
            db.branches().create("base").unwrap();
            seed(&db, "base");
            let source = "src";
            let target = "tgt";
            db.branches().fork("base", source).unwrap();
            db.branches().fork(source, target).unwrap();
            KVStore::new(db.clone())
                .put(
                    &BranchId::from_user_name(source),
                    "default",
                    "pickable",
                    Value::Int(7),
                )
                .unwrap();

            let _source_ref = reshape(&db, source, source_state);
            let target_ref = reshape(&db, target, target_state);

            let scenario = format!(
                "cherry_pick[src={}, tgt={}]",
                source_state.label(),
                target_state.label()
            );
            let before = checkpoint(&db, &obs, target, target_ref);

            let result = db.branches().cherry_pick(
                source,
                target,
                &[("default".to_string(), "pickable".to_string())],
            );

            let source_visible =
                matches!(source_state, TargetState::Active | TargetState::Archived);

            match (source_visible, target_state) {
                (false, _) => {
                    assert_not_found(result, source, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Archived) => {
                    assert_archived(result, target, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Deleted | TargetState::Missing) => {
                    assert_not_found(result, target, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Active) => {
                    result.unwrap_or_else(|e| {
                        panic!("[{scenario}] cherry_pick Active → Active must succeed: {e:?}")
                    });
                }
            }
        }
    }
}

// =============================================================================
// Op 7/12 — cherry_pick_from_diff (source + target cross-matrix)
// =============================================================================

#[test]
fn cherry_pick_from_diff_matrix() {
    for source_state in TargetState::all() {
        for target_state in TargetState::all() {
            let (test_db, obs) = setup();
            let db = test_db.db.clone();

            db.branches().create("base").unwrap();
            seed(&db, "base");
            let source = "src";
            let target = "tgt";
            db.branches().fork("base", source).unwrap();
            db.branches().fork(source, target).unwrap();
            KVStore::new(db.clone())
                .put(
                    &BranchId::from_user_name(source),
                    "default",
                    "diff_picked",
                    Value::Int(42),
                )
                .unwrap();

            let _source_ref = reshape(&db, source, source_state);
            let target_ref = reshape(&db, target, target_state);

            let scenario = format!(
                "cherry_pick_from_diff[src={}, tgt={}]",
                source_state.label(),
                target_state.label()
            );
            let before = checkpoint(&db, &obs, target, target_ref);

            let result =
                db.branches()
                    .cherry_pick_from_diff(source, target, CherryPickFilter::default());

            let source_visible =
                matches!(source_state, TargetState::Active | TargetState::Archived);

            match (source_visible, target_state) {
                (false, _) => {
                    assert_not_found(result, source, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Archived) => {
                    assert_archived(result, target, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Deleted | TargetState::Missing) => {
                    assert_not_found(result, target, &scenario);
                    assert_no_side_effects_since(&db, &obs, &before, &scenario);
                }
                (true, TargetState::Active) => {
                    result.unwrap_or_else(|e| panic!("[{scenario}] cherry_pick_from_diff Active → Active must succeed: {e:?}"));
                }
            }
        }
    }
}

// =============================================================================
// Ops 8/12 & 9/12 — tag / untag (single target, KD4 annotation gate)
// =============================================================================

#[test]
fn tag_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        let scenario = format!("tag[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db.branches().tag(name, "v1", None, None, None);
        match state {
            TargetState::Archived => {
                assert_archived(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Deleted | TargetState::Missing => {
                assert_not_found(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Active => {
                result.unwrap_or_else(|e| panic!("[{scenario}] tag on Active must succeed: {e:?}"));
            }
        }
    }
}

#[test]
fn untag_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        // Lay down a tag on Active only so untag has something to hit.
        if matches!(state, TargetState::Active) {
            db.branches().tag(name, "v1", None, None, None).unwrap();
        }

        let scenario = format!("untag[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db.branches().untag(name, "v1");
        match state {
            TargetState::Archived => {
                assert_archived(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Deleted | TargetState::Missing => {
                assert_not_found(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Active => {
                let removed = result
                    .unwrap_or_else(|e| panic!("[{scenario}] untag on Active must succeed: {e:?}"));
                assert!(removed, "[{scenario}] laid-down tag should be removed");
            }
        }
    }
}

// =============================================================================
// Ops 10/12 & 11/12 — add_note / delete_note (single target, KD4)
// =============================================================================

#[test]
fn add_note_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        let scenario = format!("add_note[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db
            .branches()
            .add_note(name, CommitVersion(1), "a note", None, None);
        match state {
            TargetState::Archived => {
                assert_archived(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Deleted | TargetState::Missing => {
                assert_not_found(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Active => {
                result.unwrap_or_else(|e| {
                    panic!("[{scenario}] add_note on Active must succeed: {e:?}")
                });
            }
        }
    }
}

#[test]
fn delete_note_matrix() {
    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        let name = "target";
        let target_ref = materialize(&db, name, state);

        if matches!(state, TargetState::Active) {
            db.branches()
                .add_note(name, CommitVersion(1), "pre", None, None)
                .unwrap();
        }

        let scenario = format!("delete_note[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = db.branches().delete_note(name, CommitVersion(1));
        match state {
            TargetState::Archived => {
                assert_archived(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Deleted | TargetState::Missing => {
                assert_not_found(result, name, &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Active => {
                let removed = result.unwrap_or_else(|e| {
                    panic!("[{scenario}] delete_note on Active must succeed: {e:?}")
                });
                assert!(removed, "[{scenario}] pre-seeded note must be removed");
            }
        }
    }
}

// =============================================================================
// Op 12/12 — bundle::import_branch (single target; KD1 duplicate rule)
// =============================================================================
//
// Bundle import treats Active and Archived alike as "live duplicate
// name" per KD1; only Deleted and Missing accept the import. This
// locks AD7 — a tombstoned target allocates a fresh generation and
// does not honor the bundle's generation.

#[test]
fn bundle_import_matrix() {
    // Build one source bundle once, reuse across states.
    let source_db = TestDb::new();
    source_db.db.branches().create("payload").unwrap();
    seed(&source_db.db, "payload");
    let bundle_dir = tempfile::tempdir().expect("bundle tempdir");
    let bundle_path = bundle_dir.path().join("payload.branchbundle.tar.zst");
    bundle::export_branch(&source_db.db, "payload", &bundle_path).expect("export succeeds");

    for state in TargetState::all() {
        let (test_db, obs) = setup();
        let db = test_db.db.clone();
        // The bundle carries name "payload"; target state is on the
        // same name in the target DB.
        let name = "payload";
        let target_ref = materialize(&db, name, state);

        let scenario = format!("bundle_import[target={}]", state.label());
        let before = checkpoint(&db, &obs, name, target_ref);

        let result = bundle::import_branch(&db, &bundle_path);

        match state {
            TargetState::Active | TargetState::Archived => {
                assert_invalid_input_contains(result, "already exists", &scenario);
                assert_no_side_effects_since(&db, &obs, &before, &scenario);
            }
            TargetState::Missing => {
                result.unwrap_or_else(|e| {
                    panic!("[{scenario}] bundle import must succeed on Missing target: {e:?}")
                });
                let rec = db.branches().control_record(name).unwrap().unwrap();
                // Bundle carries generation 0 and target has no prior
                // lineage → AD7 preserves the bundle's generation.
                assert_eq!(
                    rec.branch.generation, 0,
                    "[{scenario}] fresh-target import preserves bundle generation verbatim"
                );
            }
            TargetState::Deleted => {
                result.unwrap_or_else(|e| {
                    panic!("[{scenario}] bundle import must succeed on Deleted target: {e:?}")
                });
                let rec = db.branches().control_record(name).unwrap().unwrap();
                // Tombstoned target → AD7 fresh-gen fallback. Target's
                // next-gen counter was advanced to 1 by the prior
                // tombstone, so the import lands at gen 1, NOT at the
                // bundle's gen 0.
                assert_eq!(
                    rec.branch.generation, 1,
                    "[{scenario}] post-tombstone import allocates fresh gen 1 (not bundle's gen 0)"
                );
            }
        }
    }
}

// =============================================================================
// Cross-cutting: repeated refused calls do not deadlock
// =============================================================================
//
// The gate must run before any lock acquisition (KD3). If it didn't,
// repeated refused calls could exhaust pool capacity or hold a
// per-branch lock across the refused path. Loop one target state per
// gate-variant (Archived → BranchArchived, Missing → BranchNotFound)
// and assert the call returns promptly every time.

#[test]
fn refused_calls_are_side_effect_free_and_do_not_deadlock() {
    let (test_db, obs) = setup();
    let db = test_db.db.clone();
    db.branches().create("target").unwrap();
    seed(&db, "target");
    archive_branch_for_test(&db, "target");
    let target_ref = Some(
        db.branches()
            .control_record("target")
            .unwrap()
            .unwrap()
            .branch,
    );

    let before = checkpoint(&db, &obs, "target", target_ref);

    for _ in 0..64 {
        let result = db.branches().tag("target", "v1", None, None, None);
        assert_archived(result, "target", "loop-archived-tag");
    }
    for _ in 0..64 {
        let result = db
            .branches()
            .revert("target", CommitVersion(1), CommitVersion(1));
        assert_archived(result, "target", "loop-archived-revert");
    }
    for _ in 0..64 {
        let result =
            db.branches()
                .merge_with_options("target", "nonexistent", MergeOptions::default());
        // source `target` is Archived-visible; target `nonexistent` is
        // Missing — so the failure surfaces on the target-side gate.
        assert_not_found(result, "nonexistent", "loop-missing-merge-target");
    }

    assert_no_side_effects_since(&db, &obs, &before, "repeat-refused-loop");
}

// =============================================================================
// Helpers — reshape a live branch into a non-Active state in place
// =============================================================================
//
// `reshape` is used by the cross-matrix setups that already built a
// live pair via `create + fork` to drive them into the target states.
// `Missing` is produced by calling `delete` twice so the fork+seed
// setup is unwound cleanly.

fn reshape(db: &Arc<Database>, name: &str, state: TargetState) -> Option<BranchRef> {
    match state {
        TargetState::Active => db
            .branches()
            .control_record(name)
            .unwrap()
            .map(|rec| rec.branch),
        TargetState::Archived => Some(set_lifecycle_for_test(
            db,
            name,
            BranchLifecycleStatus::Archived,
        )),
        TargetState::Deleted | TargetState::Missing => {
            // Cross-matrix setup needs a live fork-parent pair for the
            // Active × Active success cell, so we tear down with
            // `delete` even for the `Missing` label. Both states
            // resolve to the same gate outcome — `BranchNotFoundByName`
            // — so the assertion vocabulary matches. The truly
            // never-created `Missing` state is exercised by the
            // single-branch matrix cells (`materialize`).
            let deleted_ref = db.branches().control_record(name).unwrap().unwrap().branch;
            db.branches().delete(name).unwrap();
            Some(deleted_ref)
        }
    }
}
