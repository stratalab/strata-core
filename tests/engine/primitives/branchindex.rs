//! Branch service tests.
//!
//! Tests for branch lifecycle management.

use crate::common::*;
use strata_engine::BranchStatus;

// ============================================================================
// Basic CRUD
// ============================================================================

#[test]
fn create_branch() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    let result = branches.create("test_branch").unwrap();
    assert_eq!(result.name, "test_branch");
    // Initial status is Active
    assert_eq!(result.status, BranchStatus::Active);
}

#[test]
fn create_branch_duplicate_fails() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("test_branch").unwrap();

    let result = branches.create("test_branch");
    assert!(result.is_err());
}

#[test]
fn get_branch() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("test_branch").unwrap();

    let result = branches.info_versioned("test_branch").unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value.name, "test_branch");
}

#[test]
fn get_nonexistent_returns_none() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    let result = branches.info_versioned("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn exists_returns_correct_status() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    assert!(!branches.exists("test_branch").unwrap());

    branches.create("test_branch").unwrap();
    assert!(branches.exists("test_branch").unwrap());
}

#[test]
fn list_branches() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("branch_a").unwrap();
    branches.create("branch_b").unwrap();
    branches.create("branch_c").unwrap();

    let listed = branches.list().unwrap();
    assert_eq!(listed.len(), 3);
    assert!(listed.contains(&"branch_a".to_string()));
    assert!(listed.contains(&"branch_b".to_string()));
    assert!(listed.contains(&"branch_c".to_string()));
}

#[test]
fn count_branches() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    assert_eq!(branches.list().unwrap().len(), 0);

    branches.create("branch_a").unwrap();
    branches.create("branch_b").unwrap();

    assert_eq!(branches.list().unwrap().len(), 2);
}

#[test]
fn delete_branch() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("test_branch").unwrap();
    assert!(branches.exists("test_branch").unwrap());

    branches.delete("test_branch").unwrap();
    assert!(!branches.exists("test_branch").unwrap());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_branch_name() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    assert!(branches.create("").is_err());
}

#[test]
fn special_characters_in_name() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    let name = "branch/with:special@chars";
    branches.create(name).unwrap();
    assert!(branches.exists(name).unwrap());
}
