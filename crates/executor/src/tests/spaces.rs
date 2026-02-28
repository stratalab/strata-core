//! Space isolation and management tests.
//!
//! Tests that spaces provide independent namespaces within branches:
//! - Default space on open
//! - set_space validates names
//! - Data isolation across spaces (KV, State, Event, JSON)
//! - list_spaces / delete_space API
//! - new_handle starts on default space
//! - Backwards compat (commands without space field)

use std::collections::HashMap;

use crate::{Command, Error, Output, Strata, Value};

/// Create a test Strata instance (in-memory).
fn strata() -> Strata {
    Strata::cache().unwrap()
}

// =============================================================================
// Default space on open
// =============================================================================

#[test]
fn test_default_space_on_open() {
    let db = strata();
    assert_eq!(db.current_space(), "default");
}

#[test]
fn test_new_handle_starts_on_default_space() {
    let mut db = strata();
    db.set_space("alpha").unwrap();
    assert_eq!(db.current_space(), "alpha");

    let handle = db.new_handle().unwrap();
    assert_eq!(handle.current_space(), "default");
}

// =============================================================================
// set_space validation
// =============================================================================

#[test]
fn test_set_space_valid_names() {
    let mut db = strata();

    db.set_space("default").unwrap();
    db.set_space("my-space").unwrap();
    db.set_space("space_1").unwrap();
    db.set_space("a").unwrap();
    db.set_space("abc123").unwrap();
}

#[test]
fn test_set_space_rejects_empty() {
    let mut db = strata();
    let result = db.set_space("");
    assert!(matches!(result, Err(Error::InvalidInput { .. })));
}

#[test]
fn test_set_space_rejects_uppercase() {
    let mut db = strata();
    let result = db.set_space("MySpace");
    assert!(matches!(result, Err(Error::InvalidInput { .. })));
}

#[test]
fn test_set_space_rejects_system_prefix() {
    let mut db = strata();
    let result = db.set_space("_system_reserved");
    assert!(matches!(result, Err(Error::InvalidInput { .. })));
}

#[test]
fn test_set_space_rejects_too_long() {
    let mut db = strata();
    let long_name: String = "a".repeat(65);
    let result = db.set_space(&long_name);
    assert!(matches!(result, Err(Error::InvalidInput { .. })));
}

// =============================================================================
// KV isolation across spaces
// =============================================================================

#[test]
fn test_kv_isolation_across_spaces() {
    let mut db = strata();

    // Write to default space
    db.kv_put("key1", "default-value").unwrap();

    // Switch to another space
    db.set_space("alpha").unwrap();

    // key1 should not be visible in alpha
    let result = db.kv_get("key1").unwrap();
    assert_eq!(result, None);

    // Write same key with different value in alpha
    db.kv_put("key1", "alpha-value").unwrap();

    // Verify alpha sees its own value
    let v = db.kv_get("key1").unwrap().unwrap();
    assert_eq!(v, Value::String("alpha-value".into()));

    // Switch back to default, value should be preserved
    db.set_space("default").unwrap();
    let v = db.kv_get("key1").unwrap().unwrap();
    assert_eq!(v, Value::String("default-value".into()));
}

#[test]
fn test_kv_list_scoped_to_space() {
    let mut db = strata();

    // Write keys in default space
    db.kv_put("a", 1i64).unwrap();
    db.kv_put("b", 2i64).unwrap();

    // Switch to beta space
    db.set_space("beta").unwrap();
    db.kv_put("c", 3i64).unwrap();

    // List in beta should only show "c"
    let keys = db.kv_list(None).unwrap();
    assert_eq!(keys, vec!["c".to_string()]);

    // List in default should show "a" and "b"
    db.set_space("default").unwrap();
    let mut keys = db.kv_list(None).unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn test_kv_delete_scoped_to_space() {
    let mut db = strata();

    // Write to both spaces
    db.kv_put("shared", "default").unwrap();
    db.set_space("gamma").unwrap();
    db.kv_put("shared", "gamma").unwrap();

    // Delete in gamma
    let deleted = db.kv_delete("shared").unwrap();
    assert!(deleted);

    // Should be gone in gamma
    assert_eq!(db.kv_get("shared").unwrap(), None);

    // Should still exist in default
    db.set_space("default").unwrap();
    let v = db.kv_get("shared").unwrap().unwrap();
    assert_eq!(v, Value::String("default".into()));
}

// =============================================================================
// State isolation across spaces
// =============================================================================

#[test]
fn test_state_isolation_across_spaces() {
    let mut db = strata();

    // Set state cell in default space
    db.state_set("counter", 10i64).unwrap();

    // Switch to another space — cell should not be visible
    db.set_space("delta").unwrap();
    let result = db.state_get("counter").unwrap();
    assert_eq!(result, None);

    // Set same cell with different value in delta
    db.state_set("counter", 20i64).unwrap();

    // Verify delta sees its own value
    let v = db.state_get("counter").unwrap().unwrap();
    assert_eq!(v, Value::Int(20));

    // Default still has its original value
    db.set_space("default").unwrap();
    let v = db.state_get("counter").unwrap().unwrap();
    assert_eq!(v, Value::Int(10));
}

// =============================================================================
// Event isolation across spaces
// =============================================================================

#[test]
fn test_event_isolation_across_spaces() {
    let mut db = strata();

    // Append events in default space (payload must be a JSON object)
    let payload1 = Value::Object(Box::new(HashMap::from([(
        "page".into(),
        Value::String("page1".into()),
    )])));
    let payload2 = Value::Object(Box::new(HashMap::from([(
        "page".into(),
        Value::String("page2".into()),
    )])));
    db.event_append("click", payload1).unwrap();
    db.event_append("click", payload2).unwrap();

    let default_len = db.event_len().unwrap();
    assert_eq!(default_len, 2);

    // Switch to another space — event log should be independent
    db.set_space("epsilon").unwrap();
    let epsilon_len = db.event_len().unwrap();
    assert_eq!(epsilon_len, 0);

    // Append in epsilon
    let payload3 = Value::Object(Box::new(HashMap::from([(
        "page".into(),
        Value::String("page3".into()),
    )])));
    db.event_append("click", payload3).unwrap();
    assert_eq!(db.event_len().unwrap(), 1);

    // Default still has 2
    db.set_space("default").unwrap();
    assert_eq!(db.event_len().unwrap(), 2);
}

// =============================================================================
// JSON isolation across spaces
// =============================================================================

#[test]
fn test_json_isolation_across_spaces() {
    let mut db = strata();

    // Create a JSON document in default space
    db.json_set("doc1", "$", Value::String("default-doc".into()))
        .unwrap();

    // Switch to another space — doc should not be visible
    db.set_space("zeta").unwrap();
    let result = db.json_get("doc1", "$").unwrap();
    assert_eq!(result, None);

    // Create same-key document in zeta
    db.json_set("doc1", "$", Value::String("zeta-doc".into()))
        .unwrap();

    // Verify zeta sees its own value
    let v = db.json_get("doc1", "$").unwrap().unwrap();
    assert_eq!(v, Value::String("zeta-doc".into()));

    // Default still has its document
    db.set_space("default").unwrap();
    let v = db.json_get("doc1", "$").unwrap().unwrap();
    assert_eq!(v, Value::String("default-doc".into()));
}

// =============================================================================
// Space management commands via executor
// =============================================================================

#[test]
fn test_space_list_returns_default() {
    let db = strata();
    let spaces = db.list_spaces().unwrap();
    assert!(spaces.contains(&"default".to_string()));
}

#[test]
fn test_delete_default_space_rejected() {
    let db = strata();
    let result = db.delete_space("default");
    assert!(matches!(result, Err(Error::ConstraintViolation { .. })));
}

#[test]
fn test_delete_default_space_force_rejected() {
    let db = strata();
    let result = db.delete_space_force("default");
    assert!(matches!(result, Err(Error::ConstraintViolation { .. })));
}

#[test]
fn test_space_create_and_list() {
    let db = strata();

    // Create a space via executor
    let result = db.executor().execute(Command::SpaceCreate {
        branch: None,
        space: "alpha".to_string(),
    });
    assert!(matches!(result, Ok(Output::Unit)));

    // Verify it appears in list
    let spaces = db.list_spaces().unwrap();
    assert!(spaces.contains(&"default".to_string()));
    assert!(spaces.contains(&"alpha".to_string()));
}

#[test]
fn test_space_exists() {
    let db = strata();

    // Default always exists
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "default".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(true))));

    // Non-existent space
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "nonexistent".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(false))));

    // Create a space, then check it exists
    db.executor()
        .execute(Command::SpaceCreate {
            branch: None,
            space: "beta".to_string(),
        })
        .unwrap();
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "beta".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(true))));
}

#[test]
fn test_space_auto_register_on_write() {
    let db = strata();

    // Verify "auto-space" does not exist yet
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "auto-space".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(false))));

    // Write KV to "auto-space" — should auto-register
    db.executor()
        .execute(Command::KvPut {
            branch: None,
            space: Some("auto-space".to_string()),
            key: "key1".to_string(),
            value: Value::Int(42),
        })
        .unwrap();

    // Verify it now appears in list
    let spaces = db.list_spaces().unwrap();
    assert!(spaces.contains(&"auto-space".to_string()));

    // And exists check returns true
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "auto-space".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(true))));
}

#[test]
fn test_delete_empty_space() {
    let db = strata();

    // Create a space
    db.executor()
        .execute(Command::SpaceCreate {
            branch: None,
            space: "ephemeral".to_string(),
        })
        .unwrap();

    // Delete without force — should succeed because it's empty
    let result = db.delete_space("ephemeral");
    assert!(result.is_ok());

    // Verify it's gone
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "ephemeral".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(false))));
}

#[test]
fn test_delete_non_empty_space_rejected() {
    let mut db = strata();

    // Create a space and write data into it
    db.set_space("occupied").unwrap();
    db.kv_put("key1", "value1").unwrap();
    db.set_space("default").unwrap();

    // Delete without force — should be rejected
    let result = db.delete_space("occupied");
    assert!(matches!(result, Err(Error::ConstraintViolation { .. })));
}

#[test]
fn test_delete_non_empty_space_force() {
    let mut db = strata();

    // Create a space and write data into it
    db.set_space("to-delete").unwrap();
    db.kv_put("key1", "value1").unwrap();
    db.set_space("default").unwrap();

    // Force delete — should succeed
    let result = db.delete_space_force("to-delete");
    assert!(result.is_ok());

    // Space should be gone
    let result = db.executor().execute(Command::SpaceExists {
        branch: None,
        space: "to-delete".to_string(),
    });
    assert!(matches!(result, Ok(Output::Bool(false))));

    // Data should be gone too
    db.set_space("to-delete").unwrap();
    let v = db.kv_get("key1").unwrap();
    assert_eq!(v, None);
}

#[test]
fn test_space_create_validates_name() {
    let db = strata();

    // Empty name
    let result = db.executor().execute(Command::SpaceCreate {
        branch: None,
        space: "".to_string(),
    });
    assert!(result.is_err());

    // Uppercase
    let result = db.executor().execute(Command::SpaceCreate {
        branch: None,
        space: "MySpace".to_string(),
    });
    assert!(result.is_err());

    // System prefix
    let result = db.executor().execute(Command::SpaceCreate {
        branch: None,
        space: "_system_reserved".to_string(),
    });
    assert!(result.is_err());

    // Too long
    let result = db.executor().execute(Command::SpaceCreate {
        branch: None,
        space: "a".repeat(65),
    });
    assert!(result.is_err());
}

// =============================================================================
// Backwards compatibility
// =============================================================================

#[test]
fn test_command_without_space_uses_default() {
    let db = strata();

    // Issue a KvPut command with space: None (backwards compat)
    let result = db.executor().execute(Command::KvPut {
        branch: None,
        space: None,
        key: "compat-key".to_string(),
        value: Value::String("compat-value".into()),
    });
    assert!(matches!(result, Ok(Output::Version(_))));

    // Read it back — should be in default space
    let result = db.executor().execute(Command::KvGet {
        branch: None,
        space: Some("default".to_string()),
        key: "compat-key".to_string(),
        as_of: None,
    });
    match result {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            assert_eq!(vv.value, Value::String("compat-value".into()));
        }
        other => panic!("Expected MaybeVersioned, got {:?}", other),
    }
}

#[test]
fn test_explicit_space_in_command() {
    let db = strata();

    // Write to explicit space via command
    db.executor()
        .execute(Command::KvPut {
            branch: None,
            space: Some("explicit".to_string()),
            key: "key1".to_string(),
            value: Value::Int(42),
        })
        .unwrap();

    // Read from same explicit space
    let result = db.executor().execute(Command::KvGet {
        branch: None,
        space: Some("explicit".to_string()),
        key: "key1".to_string(),
        as_of: None,
    });
    match result {
        Ok(Output::MaybeVersioned(Some(vv))) => {
            assert_eq!(vv.value, Value::Int(42));
        }
        other => panic!("Expected MaybeVersioned, got {:?}", other),
    }

    // Not visible from default space
    let result = db.executor().execute(Command::KvGet {
        branch: None,
        space: None,
        key: "key1".to_string(),
        as_of: None,
    });
    match result {
        Ok(Output::MaybeVersioned(None)) => {}
        other => panic!("Expected MaybeVersioned(None), got {:?}", other),
    }
}

// =============================================================================
// Cross-space with branches
// =============================================================================

#[test]
fn test_spaces_independent_across_branches() {
    let mut db = strata();

    // Write in default branch, default space
    db.kv_put("key", "main-default").unwrap();

    // Write in default branch, alpha space
    db.set_space("alpha").unwrap();
    db.kv_put("key", "main-alpha").unwrap();

    // Create and switch to a new branch
    db.set_space("default").unwrap();
    db.create_branch("branch-2").unwrap();
    db.set_branch("branch-2").unwrap();

    // Write in branch-2, default space
    db.kv_put("key", "b2-default").unwrap();

    // Write in branch-2, alpha space
    db.set_space("alpha").unwrap();
    db.kv_put("key", "b2-alpha").unwrap();

    // Verify all four combinations are independent
    db.set_branch("default").unwrap();
    db.set_space("default").unwrap();
    assert_eq!(
        db.kv_get("key").unwrap().unwrap(),
        Value::String("main-default".into())
    );

    db.set_space("alpha").unwrap();
    assert_eq!(
        db.kv_get("key").unwrap().unwrap(),
        Value::String("main-alpha".into())
    );

    db.set_branch("branch-2").unwrap();
    db.set_space("default").unwrap();
    assert_eq!(
        db.kv_get("key").unwrap().unwrap(),
        Value::String("b2-default".into())
    );

    db.set_space("alpha").unwrap();
    assert_eq!(
        db.kv_get("key").unwrap().unwrap(),
        Value::String("b2-alpha".into())
    );
}
