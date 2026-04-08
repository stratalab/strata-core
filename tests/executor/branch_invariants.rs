//! Deep Branch Invariant Tests
//!
//! These tests verify deeper invariants about branch behavior, not just API correctness.

use crate::common::*;
use strata_core::Value;
use strata_executor::{BranchId, Command, Output};

// ============================================================================
// Branch Isolation
// ============================================================================

/// Data in one branch must be completely invisible from another branch
#[test]
fn branch_data_is_isolated() {
    let executor = create_executor();

    // Create two branches
    let branch_a = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("isolation-branch-a".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    let branch_b = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("isolation-branch-b".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    // Write data to branch A
    executor
        .execute(Command::KvPut {
            branch: Some(branch_a.clone()),
            space: None,
            key: "secret".into(),
            value: Value::String("branch_a_secret".into()),
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: Some(branch_a.clone()),
            space: None,
            key: "state_key".into(),
            value: Value::Int(42),
        })
        .unwrap();

    // Branch B should NOT see branch A's data
    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_b.clone()),
            space: None,
            key: "secret".into(),
            as_of: None,
        })
        .unwrap();
    assert!(
        matches!(output, Output::MaybeVersioned(None) | Output::Maybe(None)),
        "Branch B should not see Branch A's KV data"
    );

    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_b.clone()),
            space: None,
            key: "state_key".into(),
            as_of: None,
        })
        .unwrap();
    assert!(
        matches!(output, Output::MaybeVersioned(None) | Output::Maybe(None)),
        "Branch B should not see Branch A's KV data (state_key)"
    );

    // Branch A should still see its own data
    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_a.clone()),
            space: None,
            key: "secret".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("branch_a_secret".into()));
        }
        _ => panic!("Branch A should see its own data"),
    }
}

// ============================================================================
// Delete Removes All Data
// ============================================================================

/// Deleting a branch should remove all its data (KV, State, Events)
#[test]
fn branch_delete_removes_all_data() {
    let executor = create_executor();

    let branch_id = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("delete-data-branch".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    // Add data to the branch
    executor
        .execute(Command::KvPut {
            branch: Some(branch_id.clone()),
            space: None,
            key: "key1".into(),
            value: Value::String("value1".into()),
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: Some(branch_id.clone()),
            space: None,
            key: "key2".into(),
            value: Value::Int(123),
        })
        .unwrap();

    // Verify data exists
    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_id.clone()),
            space: None,
            key: "key1".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
    ));

    // Delete the branch
    executor
        .execute(Command::BranchDelete {
            branch: branch_id.clone(),
        })
        .unwrap();

    // Branch should not exist
    let output = executor
        .execute(Command::BranchExists {
            branch: branch_id.clone(),
        })
        .unwrap();
    assert!(matches!(output, Output::Bool(false)));

    // Data should be gone - but we can't easily test this since the branch
    // doesn't exist anymore. Create a new branch with the same name and verify
    // data doesn't persist.
    executor
        .execute(Command::BranchCreate {
            branch_id: Some("delete-data-branch".into()),
            metadata: None,
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_id.clone()),
            space: None,
            key: "key1".into(),
            as_of: None,
        })
        .unwrap();
    assert!(
        matches!(output, Output::MaybeVersioned(None) | Output::Maybe(None)),
        "Data should not persist after branch deletion and recreation"
    );
}

/// Verify branch delete properly cleans up data (issue #781 fixed)
#[test]
fn branch_delete_cleans_up_data() {
    let executor = create_executor();

    let branch_id = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("delete-keeps-data".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    // Add data
    executor
        .execute(Command::KvPut {
            branch: Some(branch_id.clone()),
            space: None,
            key: "persistent_key".into(),
            value: Value::String("should_be_deleted".into()),
        })
        .unwrap();

    // Delete branch
    executor
        .execute(Command::BranchDelete {
            branch: branch_id.clone(),
        })
        .unwrap();

    // Recreate branch with same name
    executor
        .execute(Command::BranchCreate {
            branch_id: Some("delete-keeps-data".into()),
            metadata: None,
        })
        .unwrap();

    // Data should be gone after deletion
    let output = executor
        .execute(Command::KvGet {
            branch: Some(branch_id),
            space: None,
            key: "persistent_key".into(),
            as_of: None,
        })
        .unwrap();

    assert!(
        matches!(output, Output::MaybeVersioned(None) | Output::Maybe(None)),
        "Data should not persist after branch deletion (issue #781)"
    );
}

// ============================================================================
// Default Branch Behavior
// ============================================================================

/// Default branch always exists and can be used
#[test]
fn default_branch_always_works() {
    let executor = create_executor();

    // Write to default branch (branch: None)
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "default_key".into(),
            value: Value::String("default_value".into()),
        })
        .unwrap();

    // Read from default branch
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "default_key".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("default_value".into()));
        }
        _ => panic!("Default branch should work"),
    }

    // Explicit "default" branch should be equivalent
    let output = executor
        .execute(Command::KvGet {
            branch: Some(BranchId::from("default")),
            space: None,
            key: "default_key".into(),
            as_of: None,
        })
        .unwrap();
    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("default_value".into()));
        }
        _ => panic!("Explicit 'default' branch should work"),
    }
}

// ============================================================================
// Branch DAG: executor-path end-to-end
//
// This test proves that fork / merge operations going through the executor
// API (`Strata::branches().fork()`, `Strata::branches().merge()`) record DAG
// events. The mirror engine-direct tests live in
// `tests/integration/branching.rs::dag_*`. After PR "branch DAG: close
// engine bypass", both code paths must produce identical DAG state.
// ============================================================================

#[test]
fn dag_records_via_executor_api() {
    use strata_core::branch_dag::SYSTEM_BRANCH;
    use strata_executor::Strata;
    use strata_graph::types::{Direction, NodeData};
    use strata_graph::GraphStore;

    // fork_branch / merge_branches need a disk-backed database (storage
    // requires segment files). The executor's `Strata::open` registers the
    // branch DAG hook implementation as part of its recovery init.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dag_executor.strata");
    let mut db = Strata::open(path.to_str().unwrap()).unwrap();

    // Seed default branch with one write so the storage layer registers it
    // as forkable.
    db.kv_put("seed", strata_core::Value::Int(1)).unwrap();

    // Fork via executor API.
    db.branches().fork("default", "exec_fork").unwrap();

    // Diverge on the fork and merge back.
    db.set_branch("exec_fork").unwrap();
    db.kv_put("only_in_fork", strata_core::Value::Int(99))
        .unwrap();
    db.branches()
        .merge(
            "exec_fork",
            "default",
            strata_executor::MergeStrategy::LastWriterWins,
        )
        .unwrap();

    // Query the DAG via the GraphStore on the underlying Database. This is
    // exactly the same state the engine-direct integration test asserts on,
    // proving the executor path also funnels through the same hook.
    let inner = db.database();
    let graph = GraphStore::new(inner.clone());
    let system_id = strata_engine::primitives::branch::resolve_branch_name(SYSTEM_BRANCH);

    let read_node = |name: &str| -> Option<NodeData> {
        graph
            .get_node(system_id, "_graph_", "_branch_dag", name)
            .unwrap()
    };

    // Both branches present in the DAG.
    assert!(
        read_node("default").is_some(),
        "default branch missing from DAG (was seeded at db init)"
    );
    assert!(
        read_node("exec_fork").is_some(),
        "executor-forked branch missing from DAG"
    );

    // Fork event hangs off the parent (default) via `parent` edge — exactly
    // one because this is a fresh tempdir with one fork.
    let fork_neighbors = graph
        .neighbors(
            system_id,
            "_graph_",
            "_branch_dag",
            "default",
            Direction::Outgoing,
            Some("parent"),
        )
        .unwrap();
    assert_eq!(
        fork_neighbors.len(),
        1,
        "executor fork should produce exactly one fork event from default — \
         a value of 0 means the executor path bypassed the DAG hook"
    );
    let fork_event_id = &fork_neighbors[0].node_id;
    let fork_event = read_node(fork_event_id).expect("fork event missing");
    assert_eq!(fork_event.object_type.as_deref(), Some("fork"));
    let fork_props = fork_event.properties.expect("fork event has no properties");
    assert!(
        fork_props
            .get("fork_version")
            .and_then(|v| v.as_u64())
            .filter(|&v| v > 0)
            .is_some(),
        "fork_version must be recorded for executor-driven fork"
    );

    // Merge event hangs off the source (exec_fork) via `source` edge.
    let merge_neighbors = graph
        .neighbors(
            system_id,
            "_graph_",
            "_branch_dag",
            "exec_fork",
            Direction::Outgoing,
            Some("source"),
        )
        .unwrap();
    assert_eq!(
        merge_neighbors.len(),
        1,
        "executor merge did not produce exactly one merge event"
    );
    let merge_event_id = &merge_neighbors[0].node_id;
    let merge_event = read_node(merge_event_id).expect("merge event node missing");
    assert_eq!(merge_event.object_type.as_deref(), Some("merge"));
    let merge_props = merge_event
        .properties
        .expect("merge event has no properties");
    assert_eq!(
        merge_props.get("strategy").and_then(|v| v.as_str()),
        Some("last_writer_wins"),
        "merge strategy must be recorded as last_writer_wins for executor-driven merge"
    );
    assert!(
        merge_props
            .get("merge_version")
            .and_then(|v| v.as_u64())
            .filter(|&v| v > 0)
            .is_some(),
        "merge_version must be recorded for executor-driven merge"
    );
}
