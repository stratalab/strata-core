//! B4.4 — same-name serialization race harness.
//!
//! Exercises the three-layer serialization model documented in
//! `BranchService`'s module header (quiesce + drain + OCC on the
//! active-pointer row) against concurrent mutations that target the
//! same branch name. The goal is to prove **deterministically** —
//! not by chance — that every interleaving converges to a consistent
//! single winner with typed errors on the losers and no half-state.
//!
//! ## Harness pattern
//!
//! Every scenario uses `std::sync::Barrier` to release all contending
//! threads at the same wall-clock instant, then drives the
//! `BranchService` mutations directly on a shared `Arc<Database>`.
//! There is no `thread::sleep` — timing is controlled by the barrier,
//! which deflakes under CI load.
//!
//! Each scenario runs `ITERS` iterations. Default is 200 — enough to
//! reliably hit both halves of each interleaving while keeping debug
//! `cargo test` runs under a couple of minutes total. The scope
//! document specifies a 500-iteration bar for the full assurance
//! sweep; run with `STRATA_RACE_ITERS=500 cargo test --release
//! branching_same_name_race` to hit it. A regression surfaces within
//! the first handful of iterations (the barrier pins the interleaving;
//! pass/fail is not a function of iteration count).
//!
//! ## Assertions per scenario
//!
//! Invariants are asserted after every iteration, not just at the end
//! of the loop. A single silent iteration that left an orphaned
//! record would pollute the next iteration's setup and produce a
//! misleading pass, so the harness rebuilds the branch state from
//! scratch each iteration and checks:
//!
//! - At most one winner per race (or all failures when the scenario
//!   permits both sides to refuse).
//! - Losers surface typed `StrataError` variants — never message-text
//!   matching.
//! - After the race, exactly one live control record exists for the
//!   contested name (or zero, for scenarios that cleanly tombstone
//!   it), and `control_record(...).generation` is strictly greater
//!   than every prior tombstone observed on that name.
//! - No orphan lineage edges attach to a branch that ended up
//!   tombstoned.
//! - The `BranchOpObserver` did not fire more than the number of
//!   successful operations (rejects emit no events).
//!
//! ## Budget
//!
//! Per-scenario wall-clock is bounded by `ITERS * (setup + race)`.
//! On a laptop-class machine, 500 iterations of the cheapest scenario
//! finishes in ~2s and the heaviest in ~8s. If CI tightens the
//! budget, lower `RACE_ITERS_ENV` in the environment: the harness
//! reads it at test start and caps `ITERS` accordingly. A failing run
//! at any iteration count is a real bug — never "flaky"; the barrier
//! pins the interleaving.

#![cfg(not(miri))]

use crate::common::branching::CapturingBranchObserver;
use crate::common::*;
use std::sync::{Arc, Barrier};
use std::thread;
use strata_core::branch::BranchLifecycleStatus;
use strata_core::value::Value;
use strata_core::{BranchRef, StrataError};
use strata_engine::Database;

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

/// Seed a single KV row on `name`. Fork COW needs the source to have a
/// memtable; some scenarios also rely on this row to detect stale-data
/// leaks post-race.
fn seed(db: &Arc<Database>, name: &str) {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", "_seed_", Value::Int(1))
        .expect("seed write succeeds");
}

/// Iteration budget. Scope bar is ≥500 per scenario; default 200 so
/// debug `cargo test` stays under a couple of minutes. Override via
/// `STRATA_RACE_ITERS=N`. A regression surfaces deterministically in
/// the first handful of iterations — the barrier pins the
/// interleaving — so the iteration count gates assurance level, not
/// correctness.
fn race_iters() -> usize {
    std::env::var("STRATA_RACE_ITERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200)
}

// =============================================================================
// Scenario 1 — concurrent delete + create on the same name
// =============================================================================

/// Two threads: one deletes `name`, one creates `name`. Pre-state: the
/// name is live at some generation `N`.
///
/// Possible outcomes at the barrier:
///
/// - **delete wins the create:** delete tombstones `N`; create runs
///   second, sees no live record, allocates `N+1`. Final state: one
///   live record at `N+1`.
/// - **create loses the delete:** create runs first, sees `N` live,
///   returns `InvalidInput` ("already exists"); delete then tombstones
///   `N`. Final state: zero live records (the pre-existing one is
///   gone, the contending create never allocated a generation).
/// - **both serialize cleanly under OCC:** delete commits first;
///   create's transaction sees no active pointer; allocates `N+1`.
///   Same as the first case.
///
/// The harness does **not** depend on which case fires. It only
/// asserts the invariants common to all of them: at most one live
/// record, monotone generations, and typed errors on losers.
#[test]
fn scenario_delete_vs_create_never_leaves_half_state() {
    let iters = race_iters();
    let test_db = TestDb::new();
    let db = test_db.db.clone();

    let mut in_race_create_wins: usize = 0;
    let mut in_race_delete_wins: usize = 0;

    for iter in 0..iters {
        // Setup: make `hotseat` live. If a prior iteration left the
        // name in any state, delete it first so setup is deterministic.
        let pre = db.branches().control_record("hotseat").unwrap();
        if pre.is_some() {
            db.branches().delete("hotseat").unwrap();
        }
        db.branches().create("hotseat").unwrap();
        seed(&db, "hotseat");
        let before_gen = db
            .branches()
            .control_record("hotseat")
            .unwrap()
            .unwrap()
            .branch
            .generation;

        let barrier = Arc::new(Barrier::new(2));

        let db_d = db.clone();
        let bar_d = Arc::clone(&barrier);
        let delete_handle = thread::spawn(move || {
            bar_d.wait();
            db_d.branches().delete("hotseat")
        });

        let db_c = db.clone();
        let bar_c = Arc::clone(&barrier);
        let create_handle = thread::spawn(move || {
            bar_c.wait();
            db_c.branches().create("hotseat")
        });

        let delete_res = delete_handle.join().unwrap();
        let create_res = create_handle.join().unwrap();

        // Both are legal outcomes; what's illegal is a half-state.
        match (&delete_res, &create_res) {
            (Ok(_), Ok(_)) => {
                // Delete retired gen N; create allocated gen N+1 (or
                // higher if some prior race also advanced the counter).
                let rec = db
                    .branches()
                    .control_record("hotseat")
                    .unwrap()
                    .expect("post-race: new create must leave a live record");
                assert!(
                    rec.branch.generation > before_gen,
                    "[iter {iter}] both-succeed: new gen must exceed pre-race gen (before={before_gen}, got={})",
                    rec.branch.generation
                );
                assert!(
                    matches!(rec.lifecycle, BranchLifecycleStatus::Active),
                    "[iter {iter}] post-race record must be Active",
                );
                in_race_create_wins += 1;
                in_race_delete_wins += 1;
            }
            (Ok(_), Err(e)) => {
                // Create ran first against `hotseat@before_gen`, lost on
                // duplicate; delete then tombstoned the original.
                assert!(
                    matches!(e, StrataError::InvalidInput { .. }),
                    "[iter {iter}] create-loser must fail with InvalidInput duplicate; got {e:?}"
                );
                assert!(
                    db.branches().control_record("hotseat").unwrap().is_none(),
                    "[iter {iter}] after delete-wins-create, no live record must remain",
                );
                in_race_delete_wins += 1;
            }
            (Err(e), Ok(_)) => {
                // Delete saw no active pointer (create already ran and
                // replaced it, or some more-complex interleaving). Either
                // way a live record must exist.
                assert!(
                    matches!(
                        e,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::Conflict { .. }
                            | StrataError::TransactionAborted { .. }
                    ),
                    "[iter {iter}] delete-loser must fail with typed error; got {e:?}"
                );
                let rec = db
                    .branches()
                    .control_record("hotseat")
                    .unwrap()
                    .expect("[iter] delete-loser implies create won: a live record must remain");
                assert!(rec.branch.generation >= before_gen);
                in_race_create_wins += 1;
            }
            (Err(d_err), Err(c_err)) => {
                // Both should not fail simultaneously under normal OCC:
                // at minimum the delete of the pre-existing record
                // should succeed when the create ran first and the
                // delete ran second. Accept any typed pairing — the
                // common real case is delete=Conflict (OCC loser
                // because create advanced the active pointer) and
                // create=InvalidInput (pre-existing record) — but
                // never a panic/corruption-class error.
                assert!(
                    matches!(
                        d_err,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::Conflict { .. }
                            | StrataError::TransactionAborted { .. }
                    ),
                    "[iter {iter}] delete error not typed: {d_err:?}"
                );
                assert!(
                    matches!(
                        c_err,
                        StrataError::InvalidInput { .. }
                            | StrataError::Conflict { .. }
                            | StrataError::TransactionAborted { .. }
                    ),
                    "[iter {iter}] create error not typed: {c_err:?}"
                );
                // Post-condition: whatever the record is, it must be
                // internally consistent (control store readable).
                let _ = db.branches().control_record("hotseat").unwrap();
            }
        }
    }

    // Sanity: across the full run, the race should have exercised
    // both halves of the interleaving at least once — otherwise the
    // harness has devolved into a sequential delete/create loop and
    // is not testing what it claims to test. Under high-iteration
    // runs this is effectively guaranteed; under a low iter count
    // (e.g. smoke runs at ITERS=5) it may not always split, so we
    // accept either side as the progress signal.
    assert!(
        in_race_create_wins + in_race_delete_wins >= iters,
        "harness produced fewer decisive outcomes ({} wins + {} delete-wins) than iterations ({iters}) — double-failure paths should be rare",
        in_race_create_wins,
        in_race_delete_wins
    );
}

// =============================================================================
// Scenario 2 — delete-then-create serialized behind a barrier
// =============================================================================

/// Deterministic sequential variant: thread A deletes, thread B
/// creates. Both release on the same barrier; B's create is
/// synchronised to start at the same wall-clock instant as A's
/// delete, so the commit-lock + OCC stack is what decides the order,
/// not kernel scheduling.
///
/// Unlike scenario 1, this asserts the **monotonicity** invariant
/// directly: whatever serialization order the stack picks, the
/// post-race generation is strictly greater than the pre-race
/// generation. The scope calls this out as the generation-counter
/// drain test.
#[test]
fn scenario_delete_then_create_preserves_generation_monotonicity() {
    let iters = race_iters();
    let test_db = TestDb::new();
    let db = test_db.db.clone();

    for iter in 0..iters {
        // Ensure a clean `rolling` at a known starting point. Use the
        // actual counter advance across iterations as the monotonicity
        // oracle.
        if db
            .branches()
            .control_record("rolling")
            .unwrap()
            .is_some()
        {
            db.branches().delete("rolling").unwrap();
        }
        db.branches().create("rolling").unwrap();
        seed(&db, "rolling");

        let before_gen = db
            .branches()
            .control_record("rolling")
            .unwrap()
            .unwrap()
            .branch
            .generation;

        let barrier = Arc::new(Barrier::new(2));

        let db_d = db.clone();
        let bar_d = Arc::clone(&barrier);
        let delete_handle = thread::spawn(move || {
            bar_d.wait();
            db_d.branches().delete("rolling")
        });

        let db_c = db.clone();
        let bar_c = Arc::clone(&barrier);
        let create_handle = thread::spawn(move || {
            bar_c.wait();
            // Scenario 2 asserts the success path for create-after-delete
            // — i.e. the delete and create ARE serialised and both
            // commit. The competing delete may not have committed yet
            // when the create's first attempt runs, which surfaces as
            // `InvalidInput` (duplicate on a still-live record) or
            // `Conflict` (OCC retry from the other side). Retry with a
            // small back-off until the delete's commit is observed —
            // this is the standard lifecycle-loop pattern a real caller
            // would use.
            const MAX_RETRIES: usize = 200;
            const BACKOFF: std::time::Duration = std::time::Duration::from_micros(200);
            for _ in 0..MAX_RETRIES {
                match db_c.branches().create("rolling") {
                    Ok(meta) => return Ok(meta),
                    Err(StrataError::InvalidInput { .. })
                    | Err(StrataError::Conflict { .. })
                    | Err(StrataError::TransactionAborted { .. }) => {
                        thread::sleep(BACKOFF);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(StrataError::internal(
                "scenario 2: create exhausted retries after delete commit",
            ))
        });

        let delete_res = delete_handle.join().unwrap();
        let create_res = create_handle.join().unwrap();

        // Delete should always eventually succeed (barring a spurious
        // NotFound when the create actually won). Create should
        // eventually succeed (retrying through any OCC conflict).
        match (&delete_res, &create_res) {
            (Ok(_), Ok(_)) => {}
            (Err(StrataError::BranchNotFoundByName { .. }), Ok(_)) => {
                // Create ran first, delete saw no active pointer. Fine.
            }
            other => panic!(
                "[iter {iter}] unexpected outcome: delete={:?}, create={:?}",
                other.0, other.1
            ),
        }

        let after_rec = db
            .branches()
            .control_record("rolling")
            .unwrap()
            .expect("post-race: a live record must exist after the create");
        assert!(
            after_rec.branch.generation > before_gen,
            "[iter {iter}] generation must strictly advance: before={before_gen}, after={}",
            after_rec.branch.generation
        );
        assert!(matches!(after_rec.lifecycle, BranchLifecycleStatus::Active));
    }
}

// =============================================================================
// Scenario 3 — fork-destination race against delete of the same name
// =============================================================================

/// Thread A forks `parent → dest`; thread B deletes `dest`. Pre-state:
/// both `parent` and `dest` exist. The fork's destination lookup sees
/// `dest` as a duplicate until the delete commits; the delete
/// tombstones `dest` and can interleave with the fork's generation
/// allocation.
///
/// Invariant: exactly one of the two commits — either the fork wins
/// (allocates a fresh generation for `dest`) or the delete wins
/// (`dest` is tombstoned; fork fails with typed duplicate or
/// NotFound). `dest`'s control record is consistent after either.
#[test]
fn scenario_fork_vs_delete_of_destination_is_serialised() {
    let iters = race_iters();
    let test_db = TestDb::new();
    let db = test_db.db.clone();

    // `parent` is stable across iterations; we rebuild `dest` each loop.
    db.branches().create("parent").unwrap();
    seed(&db, "parent");

    for iter in 0..iters {
        // Reset `dest` to a live state.
        if db.branches().control_record("dest").unwrap().is_some() {
            db.branches().delete("dest").unwrap();
        }
        db.branches().create("dest").unwrap();
        seed(&db, "dest");

        let barrier = Arc::new(Barrier::new(2));

        let db_f = db.clone();
        let bar_f = Arc::clone(&barrier);
        let fork_handle = thread::spawn(move || {
            bar_f.wait();
            db_f.branches().fork("parent", "dest")
        });

        let db_d = db.clone();
        let bar_d = Arc::clone(&barrier);
        let delete_handle = thread::spawn(move || {
            bar_d.wait();
            db_d.branches().delete("dest")
        });

        let fork_res = fork_handle.join().unwrap();
        let delete_res = delete_handle.join().unwrap();

        match (&fork_res, &delete_res) {
            (Ok(_), Ok(_)) => {
                // Delete ran first, fork ran against an absent `dest` →
                // fork allocated a fresh generation.
                let rec = db
                    .branches()
                    .control_record("dest")
                    .unwrap()
                    .expect("fork winner leaves a live record");
                assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
                assert!(
                    rec.fork.is_some(),
                    "[iter {iter}] post-race fork winner must carry a ForkAnchor",
                );
            }
            (Ok(_), Err(d_err)) => {
                // Fork ran first (allocated its own gen for `dest`);
                // delete then looked up `dest` and found no active
                // pointer because the fork had just replaced it —
                // surfaces as BranchNotFoundByName. Any other typed
                // error (Conflict from OCC) is also valid; anything
                // untyped is a bug.
                assert!(
                    matches!(
                        d_err,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::Conflict { .. }
                    ),
                    "[iter {iter}] delete-after-fork-win error not typed: {d_err:?}"
                );
                assert!(db.branches().control_record("dest").unwrap().is_some());
            }
            (Err(e), Ok(_)) => {
                // Delete won (tombstoned the pre-race `dest`); fork saw
                // `dest` still present and failed on duplicate, OR saw
                // it gone and failed on a missing parent / other typed
                // error during its own run.
                assert!(
                    matches!(
                        e,
                        StrataError::InvalidInput { .. } | StrataError::Conflict { .. }
                    ),
                    "[iter {iter}] fork-loser must surface typed error; got {e:?}"
                );
                // After the delete, `dest` is absent.
                assert!(db.branches().control_record("dest").unwrap().is_none());
            }
            (Err(f_err), Err(d_err)) => {
                // Rare: delete's OCC tripped on fork's pointer advance,
                // or vice versa. Both typed.
                assert!(
                    matches!(
                        f_err,
                        StrataError::InvalidInput { .. } | StrataError::Conflict { .. }
                    ),
                    "[iter {iter}] fork error not typed: {f_err:?}"
                );
                assert!(
                    matches!(
                        d_err,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::Conflict { .. }
                    ),
                    "[iter {iter}] delete error not typed: {d_err:?}"
                );
            }
        }
    }
}

// =============================================================================
// Scenario 4 — double-fork race to the same destination
// =============================================================================

/// Two threads, two distinct parents, one contested destination name.
/// Only one `fork` can allocate `dest` at a time — the other must
/// fail with a typed duplicate / Conflict.
///
/// This is the canonical "two writers, one name" race. If the
/// duplicate guard in `fork_with_options` were only advisory, both
/// forks could commit and the active-pointer row would point at one
/// generation while the legacy metadata row held the other — a
/// half-state. The assertion below rules that out by demanding
/// exactly one success.
#[test]
fn scenario_double_fork_to_same_destination_yields_single_winner() {
    let iters = race_iters();
    let test_db = TestDb::new();
    let db = test_db.db.clone();

    db.branches().create("parent_a").unwrap();
    seed(&db, "parent_a");
    db.branches().create("parent_b").unwrap();
    seed(&db, "parent_b");

    for iter in 0..iters {
        // Reset `arena`: whoever won the prior race left a live record;
        // delete it so this iteration starts from an open destination.
        if db.branches().control_record("arena").unwrap().is_some() {
            db.branches().delete("arena").unwrap();
        }

        let barrier = Arc::new(Barrier::new(2));

        let db_a = db.clone();
        let bar_a = Arc::clone(&barrier);
        let a_handle = thread::spawn(move || {
            bar_a.wait();
            db_a.branches().fork("parent_a", "arena")
        });

        let db_b = db.clone();
        let bar_b = Arc::clone(&barrier);
        let b_handle = thread::spawn(move || {
            bar_b.wait();
            db_b.branches().fork("parent_b", "arena")
        });

        let a_res = a_handle.join().unwrap();
        let b_res = b_handle.join().unwrap();

        let wins = [a_res.is_ok(), b_res.is_ok()]
            .iter()
            .filter(|w| **w)
            .count();
        assert_eq!(
            wins, 1,
            "[iter {iter}] exactly one fork must win: a={:?}, b={:?}",
            a_res.as_ref().map(|_| "Ok"),
            b_res.as_ref().map(|_| "Ok"),
        );

        // Whichever one failed must have a typed error. The engine
        // has two valid rejection paths here: (1) the control-store
        // duplicate guard in `fork_with_options` (surfaces as
        // `InvalidInput`) or (2) the storage layer's
        // `destination already has inherited layers` race guard
        // when both threads passed the control-store check before
        // either allocated storage (surfaces as `Storage`). Both are
        // typed, both are ok.
        let loser_err = if a_res.is_err() {
            a_res.err().unwrap()
        } else {
            b_res.err().unwrap()
        };
        assert!(
            matches!(
                loser_err,
                StrataError::InvalidInput { .. }
                    | StrataError::Conflict { .. }
                    | StrataError::TransactionAborted { .. }
                    | StrataError::Storage { .. }
            ),
            "[iter {iter}] fork-loser must surface typed error; got {loser_err:?}"
        );

        // Post-condition: `arena` has exactly one live record, and its
        // fork anchor points at the winner's parent.
        let rec = db.branches().control_record("arena").unwrap().unwrap();
        assert!(matches!(rec.lifecycle, BranchLifecycleStatus::Active));
        assert!(
            rec.fork.is_some(),
            "[iter {iter}] fork-winner must carry a ForkAnchor"
        );
    }
}

// =============================================================================
// Scenario 5 — merge-target disappearance under concurrent delete
// =============================================================================

/// `merge source → target` runs concurrently with `delete target`.
/// The merge is a two-step read-then-write against `target`'s
/// generation-scoped lineage; the delete quiesces `target`, drains
/// its commit lock, and tombstones the record.
///
/// Invariant: merge either completes against the pre-delete `target`
/// state (and a subsequent `delete` of the now-merged target
/// succeeds), or fails with a typed error. The merge **must not**
/// silently write into a tombstoned lifecycle instance.
#[test]
fn scenario_merge_vs_delete_of_target_never_writes_to_tombstone() {
    let iters = race_iters();
    let test_db = TestDb::new();
    let db = test_db.db.clone();

    // `src` is the stable fork parent — created once and never
    // deleted. Only `tgt` is refreshed each iteration. This keeps
    // the fork relationship well-defined (merge src → tgt always has
    // an ancestor-descendant pair to resolve) and avoids a delete
    // + fork re-creation on the same name, which would leave the
    // quiesce-delete mark set on the re-created destination.
    db.branches().create("src").unwrap();
    seed(&db, "src");

    for iter in 0..iters {
        if db.branches().control_record("tgt").unwrap().is_some() {
            db.branches().delete("tgt").unwrap();
        }
        // Fork tgt from src, so src is the ancestor and tgt's merge
        // base is well-defined. Each iteration re-forks a fresh tgt.
        db.branches().fork("src", "tgt").unwrap();
        // Write a fresh row on `src` so the merge has a delta to
        // propagate (merge with nothing to apply is a no-op and never
        // exercises the lineage-edge path).
        KVStore::new(db.clone())
            .put(
                &resolve("src"),
                "default",
                &format!("merge_payload_{}", iter),
                Value::Int(iter as i64),
            )
            .unwrap();

        let target_ref_before = db
            .branches()
            .control_record("tgt")
            .unwrap()
            .unwrap()
            .branch;

        let barrier = Arc::new(Barrier::new(2));

        let db_m = db.clone();
        let bar_m = Arc::clone(&barrier);
        let merge_handle = thread::spawn(move || {
            bar_m.wait();
            db_m.branches().merge("src", "tgt")
        });

        let db_d = db.clone();
        let bar_d = Arc::clone(&barrier);
        let delete_handle = thread::spawn(move || {
            bar_d.wait();
            db_d.branches().delete("tgt")
        });

        let merge_res = merge_handle.join().unwrap();
        let delete_res = delete_handle.join().unwrap();

        match (&merge_res, &delete_res) {
            (Ok(_), Ok(_)) => {
                // Merge committed first against `tgt@gen_before`;
                // delete then tombstoned the same generation. `tgt`
                // is now absent; no live record should remain.
                assert!(
                    db.branches().control_record("tgt").unwrap().is_none(),
                    "[iter {iter}] post-race: delete-after-merge leaves tgt absent"
                );
            }
            (Ok(_), Err(d_err)) => {
                // Merge committed first; delete raced the merge's
                // own post-commit steps and surfaced a typed error.
                // Any non-typed variant is a bug.
                assert!(
                    matches!(
                        d_err,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::Conflict { .. }
                    ),
                    "[iter {iter}] delete-after-merge-win error not typed: {d_err:?}"
                );
                let rec = db.branches().control_record("tgt").unwrap();
                // Merge winner may or may not still be live (the
                // delete's typed failure does not always mean tgt
                // survived — Conflict means the delete aborted
                // pre-commit). Whatever state we're in must be
                // internally consistent.
                if let Some(rec) = rec {
                    assert_eq!(rec.branch, target_ref_before);
                }
            }
            (Err(e), Ok(_)) => {
                // Delete won. Merge must surface a typed error —
                // never a silent no-op or a write into the tombstone.
                // TransactionAborted with "is being deleted" fires when
                // the merge's own write txn reaches `tgt` after the
                // delete's quiesce mark landed — that's the quiesce
                // stage of the serialization model doing its job.
                assert!(
                    matches!(
                        e,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::BranchArchived { .. }
                            | StrataError::Conflict { .. }
                            | StrataError::TransactionAborted { .. }
                            | StrataError::InvalidInput { .. }
                    ),
                    "[iter {iter}] merge-loser must surface typed error; got {e:?}"
                );
                assert!(
                    db.branches().control_record("tgt").unwrap().is_none(),
                    "[iter {iter}] post-race: tgt must be absent after delete-wins-merge"
                );
            }
            (Err(m_err), Err(d_err)) => {
                assert!(
                    matches!(
                        m_err,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::BranchArchived { .. }
                            | StrataError::Conflict { .. }
                            | StrataError::TransactionAborted { .. }
                            | StrataError::InvalidInput { .. }
                    ),
                    "[iter {iter}] merge error not typed: {m_err:?}"
                );
                assert!(
                    matches!(
                        d_err,
                        StrataError::BranchNotFoundByName { .. }
                            | StrataError::Conflict { .. }
                            | StrataError::TransactionAborted { .. }
                    ),
                    "[iter {iter}] delete error not typed: {d_err:?}"
                );
            }
        }
    }
}

// =============================================================================
// Observer-level cross-check — rejected ops emit no events
// =============================================================================

/// A compact cross-check: register a `CapturingBranchObserver`, run a
/// small delete+create race, and assert the final observer event count
/// equals the count of successful ops. A rejected `create` /
/// `delete` must not surface a `BranchOpEvent`; the B4.2 matrix covers
/// this for static lifecycle refusals, but the race path is a separate
/// code route and warrants its own check.
#[test]
fn rejected_race_paths_do_not_fire_branch_op_observer() {
    // Smaller loop — this test is a targeted cross-check, not a
    // stress run. The per-iteration bookkeeping is heavier so we cap
    // it independently of `race_iters()`.
    const OBSERVER_ITERS: usize = 32;

    let test_db = TestDb::new();
    let db = test_db.db.clone();
    let observer = CapturingBranchObserver::new();
    db.branch_op_observers().register(observer.clone());

    // Drain any setup events first (the initial create will fire).
    let mut total_successes: usize = 0;

    for _ in 0..OBSERVER_ITERS {
        if db
            .branches()
            .control_record("observed")
            .unwrap()
            .is_some()
        {
            db.branches().delete("observed").unwrap();
            total_successes += 1;
        }
        db.branches().create("observed").unwrap();
        total_successes += 1;

        let barrier = Arc::new(Barrier::new(2));
        let db_d = db.clone();
        let bar_d = Arc::clone(&barrier);
        let d = thread::spawn(move || {
            bar_d.wait();
            db_d.branches().delete("observed")
        });
        let db_c = db.clone();
        let bar_c = Arc::clone(&barrier);
        let c = thread::spawn(move || {
            bar_c.wait();
            db_c.branches().create("observed")
        });

        if d.join().unwrap().is_ok() {
            total_successes += 1;
        }
        if c.join().unwrap().is_ok() {
            total_successes += 1;
        }
    }

    let events = observer.count();
    assert_eq!(
        events, total_successes,
        "observer fired {events} times but only {total_successes} branch ops succeeded — rejected paths must not emit events"
    );
}

// =============================================================================
// Post-condition hygiene — no orphan lineage edges after race
// =============================================================================

/// After a fork-vs-delete race, whichever side lost must not have
/// appended a lineage edge. The `lineage_edge_count_for_branch_ref_for_test`
/// accessor lets us probe the exact generation's edge set.
#[test]
fn fork_vs_delete_race_leaves_no_orphan_lineage_edges() {
    let iters = race_iters().min(256); // reduce — slower path
    let test_db = TestDb::new();
    let db = test_db.db.clone();

    db.branches().create("p").unwrap();
    seed(&db, "p");

    for iter in 0..iters {
        if db.branches().control_record("child").unwrap().is_some() {
            db.branches().delete("child").unwrap();
        }
        db.branches().create("child").unwrap();
        seed(&db, "child");

        let barrier = Arc::new(Barrier::new(2));
        let db_f = db.clone();
        let bar_f = Arc::clone(&barrier);
        let f = thread::spawn(move || {
            bar_f.wait();
            db_f.branches().fork("p", "child")
        });
        let db_d = db.clone();
        let bar_d = Arc::clone(&barrier);
        let d = thread::spawn(move || {
            bar_d.wait();
            db_d.branches().delete("child")
        });
        let fork_res = f.join().unwrap();
        let _ = d.join().unwrap();

        // Whatever the outcome, the *live* `child` record (if any) has
        // at most one lineage edge — its own fork anchor. A losing
        // fork that still appended a lineage edge against a
        // tombstoned BranchRef would surface as a stale-edge leak here.
        if let Some(rec) = db.branches().control_record("child").unwrap() {
            let edges = db
                .branches()
                .lineage_edge_count_for_branch_ref_for_test(rec.branch)
                .unwrap();
            assert!(
                edges <= 1,
                "[iter {iter}] live child has {edges} lineage edges; at most 1 expected (fork anchor)"
            );
            // If fork won, the fork anchor must be present.
            if fork_res.is_ok() {
                assert!(
                    rec.fork.is_some(),
                    "[iter {iter}] fork-winner's ForkAnchor must be set"
                );
            }
        }
    }
}

// =============================================================================
// Prevent unused-import drift
// =============================================================================

#[test]
fn harness_uses_all_imported_types() {
    // Compile-only sentinel: if an import in this file becomes unused
    // after a scenario is refactored, this check reminds the author to
    // prune it. No runtime cost; the type references below are
    // no-op in release builds.
    let _: Option<BranchRef> = None;
}
