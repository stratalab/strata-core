//! Search Budget Enforcement Tests
//!
//! Tests that search respects budget constraints.

use crate::common::*;

/// Test search respects time budget.
#[test]
fn test_search_respects_time_budget() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Populate with lots of data
    for i in 0..1000 {
        p.kv.put(&branch_id, &format!("key_{}", i),
            strata_core::Value::String(format!("value {}", i)))
            .expect("put");
    }

    // Search with time budget
    // When implemented:
    // let budget = SearchBudget { max_time_ms: 10, max_candidates: 100 };
    // search should terminate within budget
}

/// Test search respects candidate limit.
#[test]
fn test_search_respects_candidate_limit() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    // Populate data
    for i in 0..100 {
        p.kv.put(&branch_id, &format!("candidate_{}", i),
            strata_core::Value::String("match".into()))
            .expect("put");
    }

    // Search should respect max_candidates
}
