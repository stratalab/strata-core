//! Generated-history retention / rebuild lane.
//!
//! This suite is the heavy property/state-machine lane for retention and
//! rebuild behavior. It intentionally stays narrower than a full branching
//! model:
//!
//! - same-name recreate must not alias retention by branch name alone
//! - materialization may rewrite ownership, but not visible results
//! - reopen + GC must preserve manifest-reachable visibility
//! - generated histories must not let reclaim break a fork-frontier snapshot

#![cfg(not(miri))]

use crate::common::*;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError};
use std::sync::Arc;
use strata_core::value::Value;

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

#[derive(Debug, Clone, Copy)]
enum HistoryOp {
    RewriteMain,
    ForkFeature,
    DeleteMain,
    RecreateMain,
    MaterializeFeature,
    Reopen,
    Gc,
}

fn history_op_strategy() -> impl Strategy<Value = HistoryOp> {
    prop_oneof![
        Just(HistoryOp::RewriteMain),
        Just(HistoryOp::ForkFeature),
        Just(HistoryOp::DeleteMain),
        Just(HistoryOp::RecreateMain),
        Just(HistoryOp::MaterializeFeature),
        Just(HistoryOp::Reopen),
        Just(HistoryOp::Gc),
    ]
}

#[derive(Debug, Clone)]
struct ModelState {
    main_live: bool,
    main_generation: u64,
    main_value: i64,
    next_value: i64,
    feature_exists: bool,
    feature_snapshot: Option<i64>,
    feature_parent_generation: Option<u64>,
    feature_materialized: bool,
}

impl ModelState {
    fn new(initial_value: i64) -> Self {
        Self {
            main_live: true,
            main_generation: 0,
            main_value: initial_value,
            next_value: initial_value + 1,
            feature_exists: false,
            feature_snapshot: None,
            feature_parent_generation: None,
            feature_materialized: false,
        }
    }

    fn alloc_value(&mut self) -> i64 {
        let value = self.next_value;
        self.next_value += 1;
        value
    }
}

fn branch_entry<'a>(
    report: &'a strata_engine::RetentionReport,
    name: &str,
) -> Option<&'a strata_engine::BranchRetentionEntry> {
    report.branches.iter().find(|entry| entry.name == name)
}

fn orphan_bytes_for(report: &strata_engine::RetentionReport, branch_id: BranchId) -> u64 {
    report
        .orphan_storage
        .iter()
        .filter(|entry| entry.branch_id == branch_id)
        .map(|entry| entry.bytes)
        .sum()
}

fn assert_history_invariants(
    test_db: &mut TestDb,
    model: &ModelState,
    step: usize,
    op: HistoryOp,
) -> Result<(), TestCaseError> {
    let kv = test_db.kv();
    let main_value = kv
        .get(&resolve("main"), "default", "root")
        .expect("main read succeeds");
    if model.main_live {
        prop_assert_eq!(
            main_value,
            Some(Value::Int(model.main_value)),
            "step {} after {:?}: live main must expose the latest live value",
            step,
            op
        );
    } else {
        prop_assert_eq!(
            main_value,
            None,
            "step {} after {:?}: deleted main must not read through tombstoned storage",
            step,
            op
        );
    }

    if model.feature_exists {
        let feature_value = kv
            .get(&resolve("feature"), "default", "root")
            .expect("feature read succeeds");
        prop_assert_eq!(
            feature_value,
            Some(Value::Int(
                model.feature_snapshot.expect("feature snapshot recorded")
            )),
            "step {} after {:?}: feature must preserve its fork-frontier snapshot",
            step,
            op
        );
    }

    if !model.feature_exists {
        return Ok(());
    }

    let report = match test_db.db.retention_report() {
        Ok(report) => report,
        Err(err) => {
            let health = test_db.db.recovery_health();
            let snapshot = test_db.db.storage().retention_snapshot();
            return Err(TestCaseError::fail(format!(
                "retention-report assertions only run once a descendant exists: {err:?}; health={health:?}; snapshot={snapshot:?}"
            )));
        }
    };

    if model.feature_exists {
        let feature_entry = branch_entry(&report, "feature").expect("feature entry present");
        if model.feature_materialized {
            prop_assert_eq!(
                feature_entry.inherited_layer_bytes,
                0,
                "step {} after {:?}: materialized feature must not retain inherited-layer bytes",
                step,
                op
            );
        }

        let parent_generation = model
            .feature_parent_generation
            .expect("feature parent generation tracked");
        if !model.feature_materialized
            && model.main_live
            && parent_generation == model.main_generation
        {
            let main_entry = branch_entry(&report, "main").expect("live main entry present");
            let parent_pinned_bytes =
                main_entry.shared_bytes + orphan_bytes_for(&report, resolve("main"));
            prop_assert!(
                parent_pinned_bytes > 0,
                "step {step} after {op:?}: live parent bytes pinned by an unmaterialized child must stay attributable after reopen/compaction"
            );
            prop_assert!(
                feature_entry.inherited_layer_bytes > 0,
                "step {step} after {op:?}: unmaterialized child must report inherited-layer bytes"
            );
        }

        if !model.feature_materialized && !model.main_live {
            prop_assert!(
                report
                    .orphan_storage
                    .iter()
                    .any(|entry| entry.branch_id == resolve("main")),
                "step {step} after {op:?}: deleted parent bytes held by the child must surface as orphan storage"
            );
        }

        if !model.feature_materialized
            && model.main_live
            && parent_generation < model.main_generation
        {
            let main_entry = branch_entry(&report, "main").expect("recreated main entry present");
            prop_assert_eq!(
                main_entry.shared_bytes,
                0,
                "step {} after {:?}: recreated main must not inherit shared-byte attribution from the old lifecycle",
                step,
                op
            );
            prop_assert!(
                report
                    .orphan_storage
                    .iter()
                    .any(|entry| entry.branch_id == resolve("main")),
                "step {step} after {op:?}: old-generation retained bytes must stay orphan-attributed, not be folded into the recreated main"
            );
        }
    }

    Ok(())
}

#[test]
fn regression_retention_report_stays_available_after_reopen_materialize_rewrite_gc_history() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("main").unwrap();
    seed(&test_db.db, "main", "root", 1);
    flush_branch(&test_db.db, "main");

    let mut model = ModelState::new(1);
    assert_history_invariants(&mut test_db, &model, 0, HistoryOp::RewriteMain).unwrap();

    let ops = [
        HistoryOp::Reopen,
        HistoryOp::RewriteMain,
        HistoryOp::ForkFeature,
        HistoryOp::Reopen,
        HistoryOp::Reopen,
        HistoryOp::ForkFeature,
        HistoryOp::Reopen,
        HistoryOp::ForkFeature,
        HistoryOp::MaterializeFeature,
        HistoryOp::ForkFeature,
        HistoryOp::RewriteMain,
        HistoryOp::RewriteMain,
        HistoryOp::Gc,
    ];

    for (step, op) in ops.into_iter().enumerate() {
        match op {
            HistoryOp::RewriteMain => {
                if model.main_live {
                    let value = model.alloc_value();
                    seed(&test_db.db, "main", "root", value);
                    flush_branch(&test_db.db, "main");
                    model.main_value = value;
                }
            }
            HistoryOp::ForkFeature => {
                if model.main_live && !model.feature_exists {
                    test_db.db.branches().fork("main", "feature").unwrap();
                    model.feature_exists = true;
                    model.feature_snapshot = Some(model.main_value);
                    model.feature_parent_generation = Some(model.main_generation);
                    model.feature_materialized = false;
                }
            }
            HistoryOp::DeleteMain => {
                if model.main_live {
                    test_db.db.branches().delete("main").unwrap();
                    model.main_live = false;
                }
            }
            HistoryOp::RecreateMain => {
                if !model.main_live {
                    test_db.db.branches().create("main").unwrap();
                    model.main_generation += 1;
                    let value = model.alloc_value();
                    seed(&test_db.db, "main", "root", value);
                    flush_branch(&test_db.db, "main");
                    model.main_live = true;
                    model.main_value = value;
                }
            }
            HistoryOp::MaterializeFeature => {
                if model.feature_exists
                    && test_db
                        .db
                        .storage()
                        .inherited_layer_count(&resolve("feature"))
                        > 0
                {
                    test_db
                        .db
                        .storage()
                        .materialize_layer(&resolve("feature"), 0)
                        .expect("materialize layer 0 succeeds");
                }
                if model.feature_exists {
                    model.feature_materialized = test_db
                        .db
                        .storage()
                        .inherited_layer_count(&resolve("feature"))
                        == 0;
                }
            }
            HistoryOp::Reopen => test_db.reopen(),
            HistoryOp::Gc => {
                if model.feature_exists || !model.main_live {
                    test_db
                        .db
                        .storage()
                        .gc_orphan_segments()
                        .expect("healthy histories permit GC");
                }
            }
        }

        assert_history_invariants(&mut test_db, &model, step + 1, op).unwrap();
    }

    let report = test_db
        .db
        .retention_report()
        .expect("retention report should remain available after gc");
    let feature_entry = branch_entry(&report, "feature").expect("feature entry present");
    assert_eq!(
        feature_entry.inherited_layer_bytes, 0,
        "materialized feature must stay detached from inherited-layer attribution"
    );

    test_db
        .db
        .retention_report()
        .expect("repeated retention report reads should stay available");
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        max_local_rejects: 0,
        .. ProptestConfig::default()
    })]

    #[test]
    fn generated_histories_preserve_retention_and_rebuild_invariants(
        ops in prop::collection::vec(history_op_strategy(), 1..18)
    ) {
        let mut test_db = TestDb::new();
        test_db.db.branches().create("main").unwrap();
        seed(&test_db.db, "main", "root", 1);
        flush_branch(&test_db.db, "main");

        let mut model = ModelState::new(1);
        assert_history_invariants(&mut test_db, &model, 0, HistoryOp::RewriteMain)?;

        for (step, op) in ops.into_iter().enumerate() {
            match op {
                HistoryOp::RewriteMain => {
                    if model.main_live {
                        let value = model.alloc_value();
                        seed(&test_db.db, "main", "root", value);
                        flush_branch(&test_db.db, "main");
                        model.main_value = value;
                    }
                }
                HistoryOp::ForkFeature => {
                    if model.main_live && !model.feature_exists {
                        test_db.db.branches().fork("main", "feature").unwrap();
                        model.feature_exists = true;
                        model.feature_snapshot = Some(model.main_value);
                        model.feature_parent_generation = Some(model.main_generation);
                        model.feature_materialized = false;
                    }
                }
                HistoryOp::DeleteMain => {
                    if model.main_live {
                        test_db.db.branches().delete("main").unwrap();
                        model.main_live = false;
                    }
                }
                HistoryOp::RecreateMain => {
                    if !model.main_live {
                        test_db.db.branches().create("main").unwrap();
                        model.main_generation += 1;
                        let value = model.alloc_value();
                        seed(&test_db.db, "main", "root", value);
                        flush_branch(&test_db.db, "main");
                        model.main_live = true;
                        model.main_value = value;
                    }
                }
                HistoryOp::MaterializeFeature => {
                    if model.feature_exists && test_db.db.storage().inherited_layer_count(&resolve("feature")) > 0 {
                        test_db
                            .db
                            .storage()
                            .materialize_layer(&resolve("feature"), 0)
                            .expect("materialize layer 0 succeeds");
                    }
                    if model.feature_exists {
                        model.feature_materialized =
                            test_db.db.storage().inherited_layer_count(&resolve("feature")) == 0;
                    }
                }
                HistoryOp::Reopen => {
                    test_db.reopen();
                }
                HistoryOp::Gc => {
                    if model.feature_exists || !model.main_live {
                        test_db
                            .db
                            .storage()
                            .gc_orphan_segments()
                            .expect("healthy histories permit GC");
                    }
                }
            }

            assert_history_invariants(&mut test_db, &model, step + 1, op)?;
        }
    }
}
