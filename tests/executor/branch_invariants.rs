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
