//! B6 — unified adversarial history suite.
//!
//! Proptest-driven state machine that exercises the full branching
//! stack against a compact 8-op alphabet over a 3-branch topology
//! (`main`, `feature`, `hotfix`). Per the branching adversarial
//! verification program
//! (`docs/design/branching/branching-adversarial-verification-program.md`,
//! lanes A–E), this consolidates per-epic ad-hoc proof gates into a
//! single branch-history regression lane.
//!
//! Scope: regression gate only. No new public API — the suite uses
//! the already-public `BranchService`, `RetentionReport`, and B5.4
//! `PrimitiveDegradationRegistry::mark` surfaces. Merge and revert
//! are deliberately absent (ancestry prediction over those ops is
//! larger than the B6 closeout warrants; merge-base correctness is
//! already pinned by `branching_merge_lineage_edges.rs`).
//!
//! ## 6 postconditions (see `assert_invariants`)
//!
//! 1. Live-branch visibility: for each name in the topology,
//!    `BranchService::control_record(name)?.is_some()` matches
//!    the model's `live` bit. (`control_record` is the public
//!    equivalent of `BranchControlStore::list_visible` for a single
//!    name — the store itself is `pub(crate)`.)
//! 2. KV read consistency: live branches return the expected `root`
//!    value; deleted branches read `None`.
//! 3. Fork-frontier preservation: unrewritten fork children keep the
//!    parent's snapshot value through subsequent parent rewrites
//!    (verified end-to-end by postcondition 2; this one asserts the
//!    model's own fork_frontier == value invariant so a model-logic
//!    regression trips the suite).
//! 4. Typed rejection surfaces: deleting a non-existent branch
//!    returns `StrataError::BranchNotFoundByName`.
//! 5. Orphan attribution on recreate: a recreated parent (generation
//!    at or above 1) with a live descendant must appear in
//!    `report.branches` with `shared_bytes == 0` — the old-lifecycle
//!    bytes belong in `orphan_storage`, not the recreated entry.
//! 6. Degraded-primitive isolation: `retention_report().degraded_primitives`
//!    obeys the B5.4 generation-equality filter (entries for the live
//!    generation appear; entries for stale generations do not) and
//!    never leaks to sibling branches.
//!
//! No `reclaim_status` postcondition beyond "stays `Allowed` on
//! healthy reopens" — the suite never injects `RecoveryHealth` faults.

#![cfg(not(miri))]

use crate::common::*;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError};
use std::collections::BTreeMap;
use std::sync::Arc;
use strata_core::contract::PrimitiveType;
use strata_core::value::Value;
use strata_core::{PrimitiveDegradedReason, StrataError};
use strata_engine::{BranchRef, PrimitiveDegradationRegistry, ReclaimStatus};
use strata_graph::GraphSubsystem;
use strata_vector::VectorSubsystem;

const BRANCH_NAMES: [&str; 3] = ["main", "feature", "hotfix"];

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

fn seed(db: &Arc<Database>, name: &str, key: &str, v: i64) {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", key, Value::Int(v))
        .expect("seed write succeeds");
}

fn flush_branch(db: &Arc<Database>, name: &str) {
    let id = resolve(name);
    db.storage().rotate_memtable(&id);
    db.storage()
        .flush_oldest_frozen(&id)
        .expect("flush succeeds");
}

fn write_branch_value_checked(
    db: &Arc<Database>,
    name: &str,
    value: i64,
    op: &Op,
    context: &str,
) -> Result<(), TestCaseError> {
    KVStore::new(db.clone())
        .put(&resolve(name), "default", "root", Value::Int(value))
        .map_err(|e| {
            TestCaseError::fail(format!(
                "{context} after {op:?}: kv write failed unexpectedly: {e:?}"
            ))
        })?;
    let id = resolve(name);
    db.storage().rotate_memtable(&id);
    db.storage().flush_oldest_frozen(&id).map_err(|e| {
        TestCaseError::fail(format!(
            "{context} after {op:?}: branch flush failed unexpectedly: {e:?}"
        ))
    })?;
    Ok(())
}

fn unexpected_engine_error(op: &Op, context: &str, err: &StrataError) -> TestCaseError {
    TestCaseError::fail(format!(
        "{context} after {op:?}: unexpected engine-side failure: {err:?}"
    ))
}

#[derive(Debug, Clone)]
struct BranchModel {
    live: bool,
    generation: u64,
    value: i64,
    /// Value captured at fork time for unrewritten fork children. Lets
    /// postcondition 3 verify that parent rewrites never bleed into
    /// the child's frontier.
    fork_frontier: Option<i64>,
    fork_parent: Option<String>,
}

#[derive(Debug, Clone)]
struct DegradationRecord {
    branch_ref: BranchRef,
    coll_name: String,
    reason: PrimitiveDegradedReason,
}

#[derive(Debug, Clone)]
struct ModelState {
    branches: BTreeMap<String, BranchModel>,
    /// `BranchRef` does not implement `Ord`, so degradations live in a
    /// flat `Vec` with `(branch_ref, coll_name)` as the dedupe key.
    degradations: Vec<DegradationRecord>,
    next_value: i64,
}

impl ModelState {
    fn new() -> Self {
        Self {
            branches: BTreeMap::new(),
            degradations: Vec::new(),
            next_value: 1,
        }
    }

    fn alloc_value(&mut self) -> i64 {
        let v = self.next_value;
        self.next_value += 1;
        v
    }

    fn live(&self, name: &str) -> bool {
        self.branches.get(name).map(|b| b.live).unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
enum Op {
    Create(String),
    Delete(String),
    Fork(String, String),
    Rewrite(String),
    Reopen,
    Materialize(String),
    Gc,
    InjectDegradation(String),
}

fn name_strategy() -> impl Strategy<Value = String> {
    (0usize..BRANCH_NAMES.len()).prop_map(|i| BRANCH_NAMES[i].to_string())
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        name_strategy().prop_map(Op::Create),
        name_strategy().prop_map(Op::Delete),
        (name_strategy(), name_strategy()).prop_map(|(a, b)| Op::Fork(a, b)),
        name_strategy().prop_map(Op::Rewrite),
        Just(Op::Reopen),
        name_strategy().prop_map(Op::Materialize),
        Just(Op::Gc),
        name_strategy().prop_map(Op::InjectDegradation),
    ]
}

/// Execute an op against the real database and advance the model in
/// lockstep. Returns `Ok(true)` when executed, `Ok(false)` when the
/// model-level guard skipped it (e.g. forking a missing parent),
/// `Err` only for unexpected engine-side failures.
fn apply_op(test_db: &mut TestDb, model: &mut ModelState, op: &Op) -> Result<bool, TestCaseError> {
    match op {
        Op::Create(name) => {
            if model.live(name) {
                return Ok(false);
            }
            test_db
                .db
                .branches()
                .create(name)
                .map_err(|e| unexpected_engine_error(op, "create", &e))?;
            let generation = model
                .branches
                .get(name)
                .map(|b| b.generation + 1)
                .unwrap_or(0);
            let value = model.alloc_value();
            write_branch_value_checked(&test_db.db, name, value, op, "create")?;
            model.branches.insert(
                name.clone(),
                BranchModel {
                    live: true,
                    generation,
                    value,
                    fork_frontier: None,
                    fork_parent: None,
                },
            );
            Ok(true)
        }
        Op::Delete(name) => {
            if !model.live(name) {
                return Ok(false);
            }
            test_db
                .db
                .branches()
                .delete(name)
                .map_err(|e| unexpected_engine_error(op, "delete", &e))?;
            if let Some(b) = model.branches.get_mut(name) {
                b.live = false;
                b.fork_frontier = None;
            }
            Ok(true)
        }
        Op::Fork(src, dst) => {
            // Guard rail: only fork into a destination that has never
            // had a lifecycle. Forking into a previously-deleted name
            // (same-name recreate via fork) is a narrow corner where
            // the engine's KV visibility through the fresh inherited
            // layer interacts with the tombstones of the old lifecycle
            // — the semantics there are not specified by B3 and not in
            // scope for the B6 regression gate. Recreate-via-fork is
            // separately covered by branching_recreate_state_machine.rs
            // for the narrower patterns that are specified.
            if src == dst || !model.live(src) || model.branches.contains_key(dst) {
                return Ok(false);
            }
            test_db
                .db
                .branches()
                .fork(src, dst)
                .map_err(|e| unexpected_engine_error(op, "fork", &e))?;
            let src_value = model.branches.get(src).map(|b| b.value).unwrap_or(0);
            model.branches.insert(
                dst.clone(),
                BranchModel {
                    live: true,
                    generation: 0,
                    value: src_value,
                    fork_frontier: Some(src_value),
                    fork_parent: Some(src.clone()),
                },
            );
            Ok(true)
        }
        Op::Rewrite(name) => {
            if !model.live(name) {
                return Ok(false);
            }
            let v = model.alloc_value();
            write_branch_value_checked(&test_db.db, name, v, op, "rewrite")?;
            if let Some(b) = model.branches.get_mut(name) {
                b.value = v;
                // Rewriting a fork child moves it past the fork frontier.
                b.fork_frontier = None;
            }
            Ok(true)
        }
        Op::Reopen => {
            test_db.reopen();
            // B5.4 — the PrimitiveDegradationRegistry is a per-`Database`
            // extension (in-memory DashMap); reopen constructs a fresh
            // Database so the registry starts empty. Degradations are
            // re-detected lazily on next read of the affected primitive.
            // Clear the model so postcondition 6 checks current-session
            // state, not pre-reopen history.
            model.degradations.clear();
            Ok(true)
        }
        Op::Materialize(name) => {
            if !model.live(name) {
                return Ok(false);
            }
            let id = resolve(name);
            if test_db.db.storage().inherited_layer_count(&id) == 0 {
                return Ok(false);
            }
            test_db
                .db
                .storage()
                .materialize_layer(&id, 0)
                .map_err(|e| {
                    TestCaseError::fail(format!(
                        "materialize after {op:?}: storage materialize failed unexpectedly: {e:?}"
                    ))
                })?;
            Ok(true)
        }
        Op::Gc => {
            // GC is only exercised when there is something potentially
            // reclaimable (a deleted lifecycle or a fork). Healthy
            // histories must permit GC with no RecoveryHealth faults.
            let has_deleted = model.branches.values().any(|b| !b.live);
            let has_fork = model
                .branches
                .values()
                .any(|b| b.fork_parent.is_some() && b.live);
            if !has_deleted && !has_fork {
                return Ok(false);
            }
            test_db.db.storage().gc_orphan_segments().map_err(|e| {
                TestCaseError::fail(format!(
                    "gc after {op:?}: healthy history GC failed unexpectedly: {e:?}"
                ))
            })?;
            Ok(true)
        }
        Op::InjectDegradation(name) => {
            if !model.live(name) {
                return Ok(false);
            }
            let Some(live_ref) = test_db.db.active_branch_ref(resolve(name)) else {
                return Ok(false);
            };
            let registry = test_db
                .db
                .extension::<PrimitiveDegradationRegistry>()
                .expect("primitive-degradation registry extension");
            let coll_name = format!("adv_{name}");
            registry.mark(
                live_ref,
                PrimitiveType::Vector,
                coll_name.clone(),
                PrimitiveDegradedReason::ConfigDecodeFailure,
                "adversarial-history injected degradation",
                Some(test_db.db.primitive_degraded_observers()),
            );
            let exists = model
                .degradations
                .iter()
                .any(|d| d.branch_ref == live_ref && d.coll_name == coll_name);
            if !exists {
                model.degradations.push(DegradationRecord {
                    branch_ref: live_ref,
                    coll_name,
                    reason: PrimitiveDegradedReason::ConfigDecodeFailure,
                });
            }
            Ok(true)
        }
    }
}

fn follower_adversarial_test_db() -> TestDb {
    let dir = tempfile::tempdir().expect("follower adversarial tempdir");
    let primary_spec = OpenSpec::primary(dir.path())
        .with_subsystem(GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem);
    let primary = Database::open_runtime(primary_spec).expect("primary open");
    primary.branches().create("main").expect("seed main branch");
    seed(&primary, "main", "root", 1);
    flush_branch(&primary, "main");
    drop(primary);

    let follower_spec = OpenSpec::follower(dir.path())
        .with_subsystem(GraphSubsystem)
        .with_subsystem(VectorSubsystem)
        .with_subsystem(SearchSubsystem);
    let follower = Database::open_runtime(follower_spec).expect("follower open");
    TestDb {
        db: follower,
        dir,
        branch_id: BranchId::new(),
    }
}

fn follower_seed_model() -> ModelState {
    let mut model = ModelState::new();
    model.branches.insert(
        "main".to_string(),
        BranchModel {
            live: true,
            generation: 0,
            value: 1,
            fork_frontier: None,
            fork_parent: None,
        },
    );
    model.next_value = 2;
    model
}

fn assert_invariants(
    test_db: &TestDb,
    model: &ModelState,
    step: usize,
    op: &Op,
) -> Result<(), TestCaseError> {
    let branches = test_db.db.branches();
    let kv = test_db.kv();

    // Postcondition 1 — live-branch visibility.
    for name in BRANCH_NAMES.iter() {
        let model_live = model.live(name);
        let control = branches
            .control_record(name)
            .expect("control_record read does not error on healthy db");
        prop_assert_eq!(
            control.is_some(),
            model_live,
            "step {} after {:?}: control_record liveness for {} disagrees with model",
            step,
            op,
            name
        );
    }

    // Postcondition 2 — KV read consistency.
    for (name, branch) in &model.branches {
        let observed = kv
            .get(&resolve(name), "default", "root")
            .expect("kv get on well-formed branch does not error");
        if branch.live {
            prop_assert_eq!(
                observed,
                Some(Value::Int(branch.value)),
                "step {} after {:?}: live branch {} must expose latest value",
                step,
                op,
                name
            );
        } else {
            prop_assert_eq!(
                observed,
                None,
                "step {} after {:?}: deleted branch {} must read through tombstoned storage",
                step,
                op,
                name
            );
        }
    }

    // Postcondition 3 — fork-frontier preservation. An unrewritten fork
    // child's model value must still match its captured frontier; the
    // KV check above covers the live-read side, this asserts the
    // model invariant explicitly so a fork-frontier bug does not hide
    // behind coincident value allocation.
    for (name, b) in &model.branches {
        if !b.live {
            continue;
        }
        if let Some(frontier) = b.fork_frontier {
            prop_assert_eq!(
                frontier,
                b.value,
                "step {} after {:?}: unrewritten fork {} must keep fork_frontier == current value",
                step,
                op,
                name
            );
        }
    }

    // Postcondition 4 — typed rejection for a branch name that has
    // never existed. Probes the engine's typed-error path so a
    // regression that swallows the error or returns the wrong shape
    // trips immediately.
    let missing = "ghost-branch-never-created";
    match branches.delete(missing) {
        Ok(()) => {
            return Err(TestCaseError::fail(format!(
                "step {step} after {op:?}: delete of {missing} must not succeed"
            )));
        }
        Err(e) => {
            prop_assert!(
                matches!(e, StrataError::BranchNotFoundByName { .. }),
                "step {step} after {op:?}: expected BranchNotFoundByName, got {e:?}"
            );
        }
    }

    // Postconditions 5 & 6 need the retention report. Skip the report
    // probe when no branch has been created yet — there is nothing
    // meaningful to attribute.
    if model.branches.is_empty() {
        return Ok(());
    }

    let arc_db = test_db.db.clone();
    let report = match arc_db.retention_report() {
        Ok(r) => r,
        Err(e) => {
            return Err(TestCaseError::fail(format!(
                "step {step} after {op:?}: retention_report refused ({e:?}); the suite never injects RecoveryHealth faults so the report must succeed"
            )));
        }
    };

    // Sanity: the suite never injects storage RecoveryHealth faults,
    // so reclaim stays Allowed. A flip here signals a real regression
    // in the reclaim-status derivation (driven by RecoveryHealth, not
    // primitive degradation — see retention_report.rs:281).
    prop_assert_eq!(
        report.reclaim_status.clone(),
        ReclaimStatus::Allowed,
        "step {} after {:?}: reclaim_status must stay Allowed; primitive degradation is not a RecoveryHealth fault",
        step,
        op
    );

    // Postcondition 5 — recreated-parent orphan attribution. A live
    // branch at generation >= 1 that still has a live descendant forked
    // from it must not claim shared bytes owned by the old (pre-delete)
    // lifecycle — those bytes live in orphan_storage keyed by BranchId.
    for (name, b) in &model.branches {
        if !b.live || b.generation == 0 {
            continue;
        }
        let has_live_descendant = model
            .branches
            .iter()
            .any(|(_, c)| c.live && c.fork_parent.as_deref() == Some(name.as_str()));
        if !has_live_descendant {
            continue;
        }
        // Every live branch must appear in report.branches (postcondition
        // 1 already verified liveness via control_record; the retention
        // report joins on the same control-record set). Missing entries
        // indicate a real attribution regression — don't let this check
        // silently skip.
        let entry = report
            .branches
            .iter()
            .find(|e| e.name == *name)
            .ok_or_else(|| {
                TestCaseError::fail(format!(
                    "step {step} after {op:?}: live recreated branch {name} missing from report.branches"
                ))
            })?;
        prop_assert_eq!(
            entry.shared_bytes,
            0,
            "step {} after {:?}: recreated {} must not inherit shared_bytes from old lifecycle",
            step,
            op,
            name
        );
    }

    // Postcondition 6 — degraded-primitive attribution. B5.4 filters
    // entries by generation-equality against the live control record;
    // a mismatched or sibling-attributed entry indicates a real bug.
    for DegradationRecord {
        branch_ref,
        coll_name,
        reason,
    } in &model.degradations
    {
        let Some(name) = BRANCH_NAMES
            .iter()
            .find(|n| resolve(n) == branch_ref.id)
            .map(|s| s.to_string())
        else {
            continue;
        };
        let live_matches = model.live(&name)
            && model
                .branches
                .get(&name)
                .map(|b| b.generation == branch_ref.generation)
                .unwrap_or(false);
        if live_matches {
            let found = report
                .degraded_primitives
                .iter()
                .find(|e| e.branch == *branch_ref && e.primitive_name == *coll_name);
            prop_assert!(
                found.is_some(),
                "step {step} after {op:?}: live degraded entry ({branch_ref:?}, {coll_name}) missing from retention_report().degraded_primitives"
            );
            let found = found.unwrap();
            prop_assert_eq!(
                found.reason,
                *reason,
                "step {} after {:?}: degraded primitive reason must round-trip",
                step,
                op
            );
            prop_assert_eq!(
                found.name.as_str(),
                name.as_str(),
                "step {} after {:?}: degraded entry must carry current live branch name",
                step,
                op
            );
        } else {
            // Enforce the B5.4 generation-equality filter in the negative
            // direction: when the recorded `branch_ref` does not match a
            // live control record (because the branch was deleted, or a
            // same-name recreate advanced the generation past the old
            // lifecycle), the entry must NOT appear in the report. A
            // leak here would surface old-lifecycle degradation against
            // the new live branch.
            let leaked = report
                .degraded_primitives
                .iter()
                .any(|e| e.branch == *branch_ref && e.primitive_name == *coll_name);
            prop_assert!(
                !leaked,
                "step {step} after {op:?}: stale degraded entry ({branch_ref:?}, {coll_name}) must not appear in report.degraded_primitives (B5.4 generation-equality filter)"
            );
        }
        for e in &report.degraded_primitives {
            if e.primitive_name == *coll_name && e.branch.id != branch_ref.id {
                return Err(TestCaseError::fail(format!(
                    "step {step} after {op:?}: degraded primitive {coll_name} leaked to sibling branch {:?}",
                    e.branch
                )));
            }
        }
    }

    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        max_local_rejects: 0,
        .. ProptestConfig::default()
    })]

    #[test]
    fn adversarial_history_preserves_branching_invariants(
        ops in prop::collection::vec(op_strategy(), 1..20)
    ) {
        let mut test_db = TestDb::new();
        let mut model = ModelState::new();

        // Seed `main` so the first ops have something to delete/fork
        // from, matching the convention used by every other branching
        // state-machine suite.
        test_db.db.branches().create("main").unwrap();
        seed(&test_db.db, "main", "root", 1);
        flush_branch(&test_db.db, "main");
        model.branches.insert(
            "main".to_string(),
            BranchModel {
                live: true,
                generation: 0,
                value: 1,
                fork_frontier: None,
                fork_parent: None,
            },
        );
        model.next_value = 2;

        assert_invariants(&test_db, &model, 0, &Op::Gc)?;

        for (step, op) in ops.into_iter().enumerate() {
            let _applied = apply_op(&mut test_db, &mut model, &op)?;
            assert_invariants(&test_db, &model, step + 1, &op)?;
        }
    }
}

#[test]
fn apply_op_create_surfaces_follower_engine_failure() {
    let mut test_db = follower_adversarial_test_db();
    let mut model = follower_seed_model();
    let err = apply_op(&mut test_db, &mut model, &Op::Create("feature".to_string()))
        .expect_err("follower create must surface as an unexpected engine failure");
    let msg = err.to_string();
    assert!(
        msg.contains("create after Create(\"feature\")"),
        "error should name the create op context, got: {msg}"
    );
    assert!(
        !model.live("feature"),
        "failed create must not mutate the model"
    );
    assert!(
        test_db
            .kv()
            .get(&resolve("feature"), "default", "root")
            .expect("feature kv read")
            .is_none(),
        "failed create must not leave a live follower-visible branch"
    );
}

#[test]
fn apply_op_delete_surfaces_follower_engine_failure() {
    let mut test_db = follower_adversarial_test_db();
    let mut model = follower_seed_model();
    let err = apply_op(&mut test_db, &mut model, &Op::Delete("main".to_string()))
        .expect_err("follower delete must surface as an unexpected engine failure");
    let msg = err.to_string();
    assert!(
        msg.contains("delete after Delete(\"main\")"),
        "error should name the delete op context, got: {msg}"
    );
    assert!(
        model.live("main"),
        "failed delete must not mutate the model"
    );
    assert!(
        matches!(
            test_db
                .kv()
                .get(&resolve("main"), "default", "root")
                .expect("main kv read"),
            Some(Value::Int(1))
        ),
        "failed delete must leave main readable on the follower"
    );
}

#[test]
fn apply_op_fork_surfaces_follower_engine_failure() {
    let mut test_db = follower_adversarial_test_db();
    let mut model = follower_seed_model();
    let err = apply_op(
        &mut test_db,
        &mut model,
        &Op::Fork("main".to_string(), "feature".to_string()),
    )
    .expect_err("follower fork must surface as an unexpected engine failure");
    let msg = err.to_string();
    assert!(
        msg.contains("fork after Fork(\"main\", \"feature\")"),
        "error should name the fork op context, got: {msg}"
    );
    assert!(
        !model.live("feature"),
        "failed fork must not mutate the model"
    );
    assert!(
        test_db
            .kv()
            .get(&resolve("feature"), "default", "root")
            .expect("feature kv read")
            .is_none(),
        "failed fork must not leave a follower-visible destination branch"
    );
}
