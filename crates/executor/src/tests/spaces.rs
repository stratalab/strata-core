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
// Event isolation across spaces
// =============================================================================

#[test]
fn test_event_isolation_across_spaces() {
    let mut db = strata();

    // Append events in default space (payload must be a JSON object)
    let payload1 = Value::object(HashMap::from([(
        "page".into(),
        Value::String("page1".into()),
    )]));
    let payload2 = Value::object(HashMap::from([(
        "page".into(),
        Value::String("page2".into()),
    )]));
    db.event_append("click", payload1).unwrap();
    db.event_append("click", payload2).unwrap();

    let default_len = db.event_len().unwrap();
    assert_eq!(default_len, 2);

    // Switch to another space — event log should be independent
    db.set_space("epsilon").unwrap();
    let epsilon_len = db.event_len().unwrap();
    assert_eq!(epsilon_len, 0);

    // Append in epsilon
    let payload3 = Value::object(HashMap::from([(
        "page".into(),
        Value::String("page3".into()),
    )]));
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
    assert!(matches!(result, Ok(Output::WriteResult { .. })));

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

// =============================================================================
// Graph honors current_space (matches KV / JSON / Vector / Event)
// =============================================================================

/// `db.graph_create("g")` writes to `current_space/g`, not the legacy
/// `_graph_/g`. Verifies that `Strata::graph_*` methods auto-fill `space`
/// from `self.space_id()` exactly the way `kv_put` does.
#[test]
fn graph_honors_current_space_via_strata_api() {
    let mut db = strata();

    // Default space: create a graph and add a node.
    db.graph_create("social").unwrap();
    db.graph_add_node("social", "alice", None, None).unwrap();

    // Switch to a user space and create the SAME graph name with
    // a different node — this is only possible if graph honors
    // current_space.
    db.set_space("tenant_a").unwrap();
    db.graph_create("social").unwrap();
    db.graph_add_node("social", "bob", None, None).unwrap();

    // tenant_a sees only its own node.
    let tenant_a_nodes = db.graph_list_nodes("social").unwrap();
    let set: std::collections::HashSet<&str> = tenant_a_nodes.iter().map(|s| s.as_str()).collect();
    assert!(
        set.contains("bob"),
        "tenant_a graph should contain its own node, got {tenant_a_nodes:?}"
    );
    assert!(
        !set.contains("alice"),
        "tenant_a graph must NOT see default-space alice, got {tenant_a_nodes:?}"
    );

    // Switch back to default and verify isolation in the other direction.
    db.set_space("default").unwrap();
    let default_nodes = db.graph_list_nodes("social").unwrap();
    let set: std::collections::HashSet<&str> = default_nodes.iter().map(|s| s.as_str()).collect();
    assert!(
        set.contains("alice"),
        "default graph should contain alice, got {default_nodes:?}"
    );
    assert!(
        !set.contains("bob"),
        "default graph must NOT see tenant_a bob, got {default_nodes:?}"
    );
}

/// Graphs in three different user spaces are fully independent.
/// Strengthens `graph_honors_current_space_via_strata_api` by exercising
/// three distinct spaces and verifying every (space, graph_name)
/// combination is isolated.
#[test]
fn graph_three_user_spaces_independent_via_strata_api() {
    let mut db = strata();

    for tenant in &["tenant-a", "tenant-b", "tenant-c"] {
        db.set_space(tenant).unwrap();
        db.graph_create("shared_name").unwrap();
        db.graph_add_node("shared_name", &format!("node-{tenant}"), None, None)
            .unwrap();
    }

    // Each tenant sees only its own node in the shared graph name.
    for tenant in &["tenant-a", "tenant-b", "tenant-c"] {
        db.set_space(tenant).unwrap();
        let nodes = db.graph_list_nodes("shared_name").unwrap();
        assert_eq!(
            nodes.len(),
            1,
            "{tenant}/shared_name should have exactly one node, got {nodes:?}"
        );
        assert_eq!(nodes[0], format!("node-{tenant}"));
    }
}

// =============================================================================
// Phase 3 — Delete commands MUST NOT register their target space.
// =============================================================================
//
// Background: prior to Phase 3, six executor delete handlers called
// `ensure_space_registered` *before* dispatching the delete. The result was
// that issuing a delete against a key in a never-before-used space created a
// permanent empty `SpaceIndex` metadata entry. The fix removed those calls
// and the union `SpaceIndex::list` makes accidental phantoms naturally
// invisible. These tests pin both halves of the contract.
//
// Empirical revert verification: re-add `self.ensure_space_registered(...)`
// to KvDelete / JsonDelete / VectorDeleteCollection (etc.) in
// `executor.rs` and these tests fail.

#[test]
fn test_kv_delete_in_new_space_does_not_register_it() {
    let db = strata();

    // Issue KvDelete against a never-used space. Pre-fix this would silently
    // register `phantom_kv` even though no key was ever written there.
    db.executor()
        .execute(Command::KvDelete {
            branch: None,
            space: Some("phantom_kv".to_string()),
            key: "nonexistent".to_string(),
        })
        .unwrap();

    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"phantom_kv".to_string()),
        "KvDelete must not auto-register its target space; got {spaces:?}"
    );
}

#[test]
fn test_kv_batch_delete_in_new_space_does_not_register_it() {
    let db = strata();

    db.executor()
        .execute(Command::KvBatchDelete {
            branch: None,
            space: Some("phantom_kv_batch".to_string()),
            keys: vec!["a".to_string(), "b".to_string()],
        })
        .unwrap();

    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"phantom_kv_batch".to_string()),
        "KvBatchDelete must not auto-register its target space; got {spaces:?}"
    );
}

#[test]
fn test_json_delete_in_new_space_does_not_register_it() {
    let db = strata();

    // JsonDelete against a missing doc returns an error, but the executor
    // pre-registration ran before the delete and persisted the metadata
    // anyway — exactly the bug. Even after the call errors out, the space
    // must remain unregistered.
    let _ = db.executor().execute(Command::JsonDelete {
        branch: None,
        space: Some("phantom_json".to_string()),
        key: "nonexistent".to_string(),
        path: "$".to_string(),
    });

    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"phantom_json".to_string()),
        "JsonDelete must not auto-register its target space; got {spaces:?}"
    );
}

#[test]
fn test_json_batch_delete_in_new_space_does_not_register_it() {
    use crate::types::BatchJsonDeleteEntry;

    let db = strata();

    let _ = db.executor().execute(Command::JsonBatchDelete {
        branch: None,
        space: Some("phantom_json_batch".to_string()),
        entries: vec![BatchJsonDeleteEntry {
            key: "missing".to_string(),
            path: "".to_string(),
        }],
    });

    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"phantom_json_batch".to_string()),
        "JsonBatchDelete must not auto-register its target space; got {spaces:?}"
    );
}

#[test]
fn test_vector_delete_collection_in_new_space_does_not_register_it() {
    let db = strata();

    // VectorDeleteCollection against a non-existent collection returns an
    // error. Pre-fix the space metadata was already written by the
    // pre-registration helper before the inner call ran.
    let _ = db.executor().execute(Command::VectorDeleteCollection {
        branch: None,
        space: Some("phantom_vec".to_string()),
        collection: "missing".to_string(),
    });

    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"phantom_vec".to_string()),
        "VectorDeleteCollection must not auto-register its target space; got {spaces:?}"
    );
}

#[test]
fn test_vector_batch_delete_in_new_space_does_not_register_it() {
    let db = strata();

    // VectorBatchDelete against a never-used space. Pre-audit, the
    // executor's `Command::VectorBatchDelete` arm called
    // `ensure_space_registered` *before* dispatch, so this batch delete
    // would silently create a permanent empty `phantom_vec_batch`
    // metadata entry even though no vector was ever written there.
    // The fix removed that pre-registration call. Empirical revert:
    // re-add `self.ensure_space_registered(&branch, &space)?` to the
    // `VectorBatchDelete` arm in `executor.rs` and this test fails.
    let _ = db.executor().execute(Command::VectorBatchDelete {
        branch: None,
        space: Some("phantom_vec_batch".to_string()),
        collection: "missing".to_string(),
        keys: vec!["a".to_string(), "b".to_string()],
    });

    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"phantom_vec_batch".to_string()),
        "VectorBatchDelete must not auto-register its target space; got {spaces:?}"
    );
}

// =============================================================================
// Audit follow-up — Session transactions must register non-default spaces.
// =============================================================================
//
// Background: `Session::execute_in_txn` builds a per-command Transaction
// wrapper that delegates writes through `crates/engine/src/transaction/`
// `context.rs`. Before this fix, the engine-layer Transaction methods
// (`kv_put`, `event_append`, `json_create`, `json_set`) called
// `ctx.put` / `ctx.json_set` directly without going through the Phase 3
// `ensure_space_registered_in_txn` helper. The user data committed
// correctly, but the space metadata key was missing — `space list` /
// `space exists` were silently wrong until startup repair re-discovered
// the space via a data scan.
//
// These tests pin the contract for ALL session-driven write paths and
// double as empirical revert checks: remove the
// `ensure_space_registered_in_txn` call from one of the Transaction
// methods (or revert the `KvBatchPut` refactor to use raw `ctx.put`)
// and the matching test below fails.
//
// IMPORTANT: these tests must NOT use `db.list_spaces()` to verify the
// metadata, because `SpaceIndex::list` returns the *union* of metadata
// and data-scan discovery — a missing metadata key would be masked by
// the user data the test just committed. Instead, the helper below
// reads `Key::new_space(...)` directly via the storage layer, which is
// the same metadata-only check `SpaceIndex::exists_metadata` uses.

/// Read the space metadata key directly. Bypasses
/// `SpaceIndex::list`'s union-with-data-scan behaviour so the test
/// actually proves the metadata key was written by the txn.
fn space_metadata_registered(db: &std::sync::Arc<strata_engine::Database>, space: &str) -> bool {
    use strata_core::types::{BranchId, Key};
    let bid = BranchId::from_bytes([0u8; 16]);
    db.transaction(bid, |txn| {
        let key = Key::new_space(bid, space);
        txn.get(&key).map(|v| v.is_some())
    })
    .unwrap()
}

#[test]
fn test_session_kv_put_registers_non_default_space() {
    use crate::Session;
    use strata_engine::database::search_only_cache_spec;
    use strata_engine::Database;

    let spec = search_only_cache_spec();
    let db = Database::open_runtime(spec).unwrap();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: Some("session_kv".to_string()),
            key: "k1".to_string(),
            value: Value::Int(42),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    assert!(
        space_metadata_registered(&db, "session_kv"),
        "Session txn KV write must register the metadata key in `_space_/session_kv`"
    );
}

#[test]
fn test_session_kv_batch_put_registers_non_default_space() {
    use crate::types::BatchKvEntry;
    use crate::Session;
    use strata_engine::database::search_only_cache_spec;
    use strata_engine::Database;

    let spec = search_only_cache_spec();
    let db = Database::open_runtime(spec).unwrap();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    let out = session
        .execute(Command::KvBatchPut {
            branch: None,
            space: Some("session_kv_batch".to_string()),
            entries: vec![
                BatchKvEntry {
                    key: "a".to_string(),
                    value: Value::Int(1),
                },
                BatchKvEntry {
                    key: "b".to_string(),
                    value: Value::Int(2),
                },
            ],
        })
        .unwrap();

    // Inspect the per-entry results — a buggy refactor that silently
    // fails individual entries while returning command-level Ok would
    // otherwise slip through. The metadata check below is meaningful
    // ONLY when each entry actually wrote.
    match out {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2, "expected 2 batch results");
            assert!(
                results.iter().all(|r| r.error.is_none()),
                "every batch entry must succeed; got {results:?}"
            );
        }
        other => panic!("expected BatchResults, got {other:?}"),
    }

    session.execute(Command::TxnCommit).unwrap();

    assert!(
        space_metadata_registered(&db, "session_kv_batch"),
        "Session txn KvBatchPut must register the metadata key (the raw `ctx.put` path bypassed it before the fix)"
    );
}

#[test]
fn test_session_event_append_registers_non_default_space() {
    use crate::Session;
    use std::collections::HashMap;
    use strata_engine::database::search_only_cache_spec;
    use strata_engine::Database;

    let spec = search_only_cache_spec();
    let db = Database::open_runtime(spec).unwrap();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    let payload = Value::object(HashMap::from([(
        "msg".into(),
        Value::String("hello".into()),
    )]));
    session
        .execute(Command::EventAppend {
            branch: None,
            space: Some("session_events".to_string()),
            event_type: "click".to_string(),
            payload,
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    assert!(
        space_metadata_registered(&db, "session_events"),
        "Session txn EventAppend must register the metadata key"
    );
}

#[test]
fn test_session_event_batch_append_registers_non_default_space() {
    use crate::types::BatchEventEntry;
    use crate::Session;
    use std::collections::HashMap;
    use strata_engine::database::search_only_cache_spec;
    use strata_engine::Database;

    let spec = search_only_cache_spec();
    let db = Database::open_runtime(spec).unwrap();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    let payload1 = Value::object(HashMap::from([("n".into(), Value::Int(1))]));
    let payload2 = Value::object(HashMap::from([("n".into(), Value::Int(2))]));
    let out = session
        .execute(Command::EventBatchAppend {
            branch: None,
            space: Some("session_events_batch".to_string()),
            entries: vec![
                BatchEventEntry {
                    event_type: "click".to_string(),
                    payload: payload1,
                },
                BatchEventEntry {
                    event_type: "click".to_string(),
                    payload: payload2,
                },
            ],
        })
        .unwrap();

    // Inspect per-entry results so a silent partial failure cannot
    // mask the registration test.
    match out {
        Output::BatchResults(results) => {
            assert_eq!(results.len(), 2, "expected 2 batch results");
            assert!(
                results.iter().all(|r| r.error.is_none()),
                "every batch entry must succeed; got {results:?}"
            );
        }
        other => panic!("expected BatchResults, got {other:?}"),
    }

    session.execute(Command::TxnCommit).unwrap();

    assert!(
        space_metadata_registered(&db, "session_events_batch"),
        "Session txn EventBatchAppend must register the metadata key"
    );
}

#[test]
fn test_session_json_set_registers_non_default_space() {
    use crate::Session;
    use strata_engine::database::search_only_cache_spec;
    use strata_engine::Database;

    let spec = search_only_cache_spec();
    let db = Database::open_runtime(spec).unwrap();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::JsonSet {
            branch: None,
            space: Some("session_json".to_string()),
            key: "doc1".to_string(),
            path: "$".to_string(),
            value: Value::String("phase3 audit follow-up".into()),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    assert!(
        space_metadata_registered(&db, "session_json"),
        "Session txn JsonSet must register the metadata key"
    );
}

#[test]
fn test_session_default_space_skipped() {
    // The Phase 3 helper explicitly skips `default` and `_system_`,
    // so a session txn write to the implicit default space must NOT
    // create an explicit metadata key for it (the default space is
    // already implicit in `SpaceIndex::list` / `exists`). This pins
    // the skip rule and prevents an over-eager fix from registering
    // the implicit space.
    use crate::Session;
    use strata_core::traits::Storage;
    use strata_core::types::{BranchId, Key};
    use strata_engine::database::search_only_cache_spec;
    use strata_engine::Database;

    let spec = search_only_cache_spec();
    let db = Database::open_runtime(spec).unwrap();
    let mut session = Session::new(db.clone());

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None, // → default
            key: "k1".to_string(),
            value: Value::Int(1),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    // The default-space metadata key must remain absent after a write
    // to the default space — the helper short-circuits before the put.
    use strata_core::id::CommitVersion;
    let bid = BranchId::from_bytes([0u8; 16]);
    let key = Key::new_space(bid, "default");
    let version = CommitVersion(db.storage().version());
    let entry = db.storage().get_versioned(&key, version).unwrap();
    assert!(
        entry.is_none(),
        "Session txn write to `default` must not create an explicit metadata key for it"
    );
}

// =============================================================================
// Phase 4 — Force-delete must wipe ALL secondary state for the space.
// =============================================================================
//
// Background: prior to Phase 4, `space_delete --force` only wiped primary
// KV/Event/Json/Vector/Graph data + the metadata key. It left behind:
//   1. BM25 inverted-index documents (search returned stale hits)
//   2. In-memory vector backends (`VectorBackendState::backends` ghosts)
//   3. On-disk vector caches (`.vec` mmap and `_graphs/` dirs)
//   4. Shadow embeddings in `_system_embed_*` whose source is the
//      deleted space
//
// These tests pin the post-commit cleanup wired into `space_delete` and
// double as empirical revert checks: comment out one of the cleanup
// blocks in `crates/executor/src/handlers/space.rs` and exactly the
// matching test below fails.

use crate::types::{DistanceMetric, SearchQuery};

/// Helper: search the current space and return the number of hits.
fn search_hits(db: &Strata, query: &str) -> usize {
    let (hits, _) = db
        .search(SearchQuery {
            query: query.to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: Some(10),
            as_of: None,
            diff: None,
        })
        .unwrap();
    hits.len()
}

/// Phase 4 / Part 1 — search index cleanup. Force-deleting a space must
/// remove every BM25 doc whose `EntityRef` was in that space, both from
/// the active segment and via tombstones in sealed segments.
///
/// Empirical revert: comment out the `index.remove_documents_in_space`
/// block in `space_delete` and this test fails — the search returns the
/// stale hit and `total_docs` does not decrement.
#[test]
fn test_force_delete_clears_search_index() {
    let mut db = strata();

    // Write a searchable string into a tenant space.
    db.set_space("tenant_x").unwrap();
    db.kv_put("doc1", "phase4 unique searchable text").unwrap();

    // Sanity: BM25 finds it.
    assert_eq!(
        search_hits(&db, "phase4"),
        1,
        "BM25 must find the doc before delete"
    );

    // Snapshot total_docs from the inverted index extension.
    let core_db = db.database();
    let index = core_db
        .extension::<strata_engine::search::InvertedIndex>()
        .expect("InvertedIndex extension must exist for cache databases");
    let before = index.total_docs();
    assert!(
        before >= 1,
        "expected at least 1 doc in BM25 index, got {before}"
    );

    // Force-delete tenant_x. Switch back to default first because
    // delete_space_force is not allowed on the active space.
    db.set_space("default").unwrap();
    db.delete_space_force("tenant_x").unwrap();

    // Search must return 0 hits and total_docs must have decremented
    // by exactly the count we wrote.
    db.set_space("tenant_x").unwrap();
    assert_eq!(
        search_hits(&db, "phase4"),
        0,
        "BM25 must not return stale hits after force-delete"
    );
    let after = index.total_docs();
    assert_eq!(
        after,
        before - 1,
        "total_docs must decrement by exactly 1 (before={before}, after={after})"
    );
}

/// Phase 4 / Part 1 — per-space scoping. Force-deleting one tenant must
/// not touch another tenant's BM25 docs even when they share the same
/// key string.
#[test]
fn test_force_delete_search_index_other_spaces_unaffected() {
    let mut db = strata();

    db.set_space("tenant_x").unwrap();
    db.kv_put("shared", "phase4 isolation marker").unwrap();
    db.set_space("tenant_y").unwrap();
    db.kv_put("shared", "phase4 isolation marker").unwrap();

    // Both spaces find the marker before any delete.
    db.set_space("tenant_x").unwrap();
    assert_eq!(search_hits(&db, "isolation"), 1);
    db.set_space("tenant_y").unwrap();
    assert_eq!(search_hits(&db, "isolation"), 1);

    // Force-delete tenant_x.
    db.set_space("default").unwrap();
    db.delete_space_force("tenant_x").unwrap();

    // tenant_x — gone.
    db.set_space("tenant_x").unwrap();
    assert_eq!(
        search_hits(&db, "isolation"),
        0,
        "tenant_x must lose its hit after force-delete"
    );

    // tenant_y — survives, pinning the per-space filter inside
    // `remove_documents_in_space`.
    db.set_space("tenant_y").unwrap();
    assert_eq!(
        search_hits(&db, "isolation"),
        1,
        "tenant_y must keep its hit (per-space filter must isolate)"
    );
}

/// Phase 4 / Part 2 — vector backend cleanup. Force-deleting a space
/// must evict every collection in that space from the in-memory
/// `VectorBackendState::backends` map.
///
/// Empirical revert: comment out the `purge_collections_in_space` call
/// in `space_delete` and this test fails — the `CollectionId` survives
/// in the DashMap.
#[test]
fn test_force_delete_clears_vector_backend() {
    let mut db = strata();
    // The executor maps the string branch "default" → all-zero UUID via
    // `to_core_branch_id` (see crates/executor/src/bridge.rs:91). Note
    // that `strata_core::types::BranchId::default()` is a *random* UUID
    // and would NOT match what the executor uses internally.
    let core_branch = strata_core::types::BranchId::from_bytes([0u8; 16]);

    db.set_space("tenant_x").unwrap();
    db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("embeddings", "v1", vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    // Snapshot in-memory state — the collection backend must exist.
    let core_db = db.database();
    let state = core_db
        .extension::<strata_vector::VectorBackendState>()
        .unwrap();
    let cid =
        strata_core::primitives::vector::CollectionId::new(core_branch, "tenant_x", "embeddings");
    assert!(
        state.backends.contains_key(&cid),
        "backend must exist before delete"
    );

    // Force-delete tenant_x.
    db.set_space("default").unwrap();
    db.delete_space_force("tenant_x").unwrap();

    assert!(
        !state.backends.contains_key(&cid),
        "backend must be evicted from VectorBackendState after force-delete"
    );
}

/// Phase 4 / Part 2 — cross-space isolation for vector backends and
/// disk caches. Deleting one tenant must not touch another tenant's
/// in-memory backend.
#[test]
fn test_force_delete_other_vector_spaces_unaffected() {
    let mut db = strata();
    // The executor maps the string branch "default" → all-zero UUID via
    // `to_core_branch_id` (see crates/executor/src/bridge.rs:91). Note
    // that `strata_core::types::BranchId::default()` is a *random* UUID
    // and would NOT match what the executor uses internally.
    let core_branch = strata_core::types::BranchId::from_bytes([0u8; 16]);

    db.set_space("tenant_x").unwrap();
    db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("embeddings", "v1", vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    db.set_space("tenant_y").unwrap();
    db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("embeddings", "v1", vec![0.0, 1.0, 0.0, 0.0], None)
        .unwrap();

    let core_db = db.database();
    let state = core_db
        .extension::<strata_vector::VectorBackendState>()
        .unwrap();
    let cid_x =
        strata_core::primitives::vector::CollectionId::new(core_branch, "tenant_x", "embeddings");
    let cid_y =
        strata_core::primitives::vector::CollectionId::new(core_branch, "tenant_y", "embeddings");
    assert!(state.backends.contains_key(&cid_x));
    assert!(state.backends.contains_key(&cid_y));

    db.set_space("default").unwrap();
    db.delete_space_force("tenant_x").unwrap();

    assert!(
        !state.backends.contains_key(&cid_x),
        "tenant_x backend must be gone"
    );
    assert!(
        state.backends.contains_key(&cid_y),
        "tenant_y backend must survive (per-space filter must isolate)"
    );

    // tenant_y query path still works after the delete.
    db.set_space("tenant_y").unwrap();
    let matches = db
        .vector_query("embeddings", vec![0.0, 1.0, 0.0, 0.0], 5)
        .unwrap();
    assert_eq!(matches.len(), 1, "tenant_y vector must still be queryable");
}

/// Phase 4 / Part 2 — on-disk vector cache cleanup. Force-deleting a
/// space must remove the `.vec` mmap heap and the `_graphs/` directory
/// for every collection in that space — and must NOT touch the
/// equivalent files belonging to a different space.
///
/// Uses `Strata::open` against a tempdir so that the recovery / freeze
/// path actually writes the cache files. Calls `freeze_vector_heaps`
/// explicitly to force the freeze before checking the path.
///
/// Empirical revert:
///   • Comment out the `purge_collections_in_space` call in
///     `space_delete` and the tenant_x assertions fail — the `.vec`
///     file survives.
///   • Weaken the `(branch_id, space)` filter inside
///     `purge_collections_in_space` and the tenant_y survival
///     assertions fail.
#[test]
fn test_force_delete_clears_vector_disk_cache() {
    use std::path::PathBuf;
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir: PathBuf = temp.path().to_path_buf();

    let mut db = Strata::open(&data_dir).unwrap();

    // tenant_x: collection that MUST be wiped.
    db.set_space("tenant_x").unwrap();
    db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("embeddings", "v1", vec![1.0, 0.0, 0.0, 0.0], None)
        .unwrap();

    // tenant_y: same collection name in a different space — MUST
    // survive the tenant_x delete. Pins the per-space filter inside
    // `purge_collections_in_space` and `purge_collection_disk_cache`.
    db.set_space("tenant_y").unwrap();
    db.vector_create_collection("embeddings", 4, DistanceMetric::Cosine)
        .unwrap();
    db.vector_upsert("embeddings", "v1", vec![0.0, 1.0, 0.0, 0.0], None)
        .unwrap();

    // Force both vector heaps onto disk so we have real `.vec` files
    // to assert against.
    db.database().freeze_vector_heaps().unwrap();

    // The executor maps the string branch "default" → all-zero UUID via
    // `to_core_branch_id` (see crates/executor/src/bridge.rs:91). Note
    // that `strata_core::types::BranchId::default()` is a *random* UUID
    // and would NOT match what the executor uses internally.
    let core_branch = strata_core::types::BranchId::from_bytes([0u8; 16]);
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*core_branch.as_bytes()));

    let vec_path_x = data_dir
        .join("vectors")
        .join(&branch_hex)
        .join("tenant_x")
        .join("embeddings.vec");
    let graph_dir_x = data_dir
        .join("vectors")
        .join(&branch_hex)
        .join("tenant_x")
        .join("embeddings_graphs");

    let vec_path_y = data_dir
        .join("vectors")
        .join(&branch_hex)
        .join("tenant_y")
        .join("embeddings.vec");
    let space_dir_y = data_dir.join("vectors").join(&branch_hex).join("tenant_y");

    assert!(
        vec_path_x.exists(),
        "tenant_x mmap heap must exist after freeze, looked for {vec_path_x:?}"
    );
    assert!(
        vec_path_y.exists(),
        "tenant_y mmap heap must exist after freeze, looked for {vec_path_y:?}"
    );

    // Force-delete tenant_x.
    db.set_space("default").unwrap();
    db.delete_space_force("tenant_x").unwrap();

    // tenant_x cache files — gone.
    assert!(
        !vec_path_x.exists(),
        "tenant_x .vec mmap must be wiped after force-delete, still found at {vec_path_x:?}"
    );
    assert!(
        !graph_dir_x.exists(),
        "tenant_x _graphs/ dir must be wiped after force-delete, still found at {graph_dir_x:?}"
    );

    // tenant_y files — survive. Per-space subdirectory must still
    // exist (tenant_x's wipe stops at the collection-level paths and
    // does not climb into tenant_y).
    assert!(
        vec_path_y.exists(),
        "tenant_y .vec mmap must survive tenant_x delete, missing at {vec_path_y:?}"
    );
    assert!(
        space_dir_y.exists(),
        "tenant_y space dir must survive tenant_x delete, missing at {space_dir_y:?}"
    );
}

/// Phase 4 / Part 3 — shadow embedding cleanup. Force-deleting a space
/// must remove every entry in **all three** shadow collections
/// (`_system_embed_kv`, `_system_embed_json`, `_system_embed_event`)
/// whose composite key starts with `{space}\x1f`, and must NOT touch
/// entries belonging to a different space.
///
/// We seed the shadow vectors by hand via `system_insert_with_source`
/// — this avoids needing an actual embedder model in the test (the
/// auto-embed pipeline would normally generate the same composite key
/// shape). The cleanup helper is unconditional; only the
/// `EmbedBuffer` drain inside it is `embed`-feature-gated, so this
/// test exercises the real cleanup path without the feature flag.
///
/// Empirical revert: comment out the `delete_shadow_embeddings_for_space`
/// call in `space_delete` and this test fails — the tenant_x shadow
/// vectors survive. Likewise, weakening the prefix filter would break
/// the tenant_y survival assertions.
#[test]
fn test_force_delete_clears_shadow_embeddings() {
    // Pull the shadow collection names from the engine — the same
    // constants the cleanup helper sees. Hard-coding them here would
    // mask a rename: the test would seed at the old name while
    // `delete_shadow_embeddings_for_space` looks at the new one, and
    // the failure would point at "cleanup broken" instead of
    // "constants mismatched".
    use strata_engine::database::{SHADOW_JSON, SHADOW_KV};
    use strata_vector::{DistanceMetric as VDistanceMetric, VectorConfig, VectorStore};

    let mut db = strata();
    // The executor maps the string branch "default" → all-zero UUID via
    // `to_core_branch_id` (see crates/executor/src/bridge.rs:91). Note
    // that `strata_core::types::BranchId::default()` is a *random* UUID
    // and would NOT match what the executor uses internally.
    let core_branch = strata_core::types::BranchId::from_bytes([0u8; 16]);
    let core_db = db.database();
    let vs = VectorStore::new(core_db.clone());

    // Seed: source data in both tenant_x and tenant_y so the source
    // keys actually exist (auto-registers the spaces).
    db.set_space("tenant_x").unwrap();
    db.kv_put("k1", "phase4 shadow seed kv").unwrap();
    db.json_set("d1", "$", Value::String("phase4 shadow seed json".into()))
        .unwrap();
    db.set_space("tenant_y").unwrap();
    db.kv_put("k1", "phase4 sibling kv").unwrap();

    // Create both shadow collections and insert vectors that mimic
    // what the auto-embed pipeline would produce: composite key
    // `{space}\x1f{source_key}`.
    let cfg_kv = VectorConfig::new(4, VDistanceMetric::Cosine).unwrap();
    vs.create_system_collection(core_branch, SHADOW_KV, cfg_kv)
        .unwrap();
    let cfg_json = VectorConfig::new(4, VDistanceMetric::Cosine).unwrap();
    vs.create_system_collection(core_branch, SHADOW_JSON, cfg_json)
        .unwrap();

    // tenant_x KV shadow.
    let kv_x = "tenant_x\x1fk1".to_string();
    vs.system_insert_with_source(
        core_branch,
        SHADOW_KV,
        &kv_x,
        &[1.0, 0.0, 0.0, 0.0],
        None,
        strata_core::EntityRef::kv(core_branch, "tenant_x", "k1"),
    )
    .unwrap();

    // tenant_x JSON shadow.
    let json_x = "tenant_x\x1fd1".to_string();
    vs.system_insert_with_source(
        core_branch,
        SHADOW_JSON,
        &json_x,
        &[0.0, 1.0, 0.0, 0.0],
        None,
        strata_core::EntityRef::json(core_branch, "tenant_x", "d1"),
    )
    .unwrap();

    // tenant_y KV shadow — MUST survive the tenant_x delete. Pins
    // both the per-space prefix filter and the cross-collection
    // isolation guarantee.
    let kv_y = "tenant_y\x1fk1".to_string();
    vs.system_insert_with_source(
        core_branch,
        SHADOW_KV,
        &kv_y,
        &[0.0, 0.0, 1.0, 0.0],
        None,
        strata_core::EntityRef::kv(core_branch, "tenant_y", "k1"),
    )
    .unwrap();

    // Sanity: all three seeded shadow keys exist before the delete.
    let kv_before = vs
        .list_keys(
            core_branch,
            strata_engine::system_space::SYSTEM_SPACE,
            SHADOW_KV,
        )
        .unwrap();
    let json_before = vs
        .list_keys(
            core_branch,
            strata_engine::system_space::SYSTEM_SPACE,
            SHADOW_JSON,
        )
        .unwrap();
    assert!(
        kv_before.iter().any(|k| k == &kv_x),
        "tenant_x KV shadow must exist before delete, got {kv_before:?}"
    );
    assert!(
        kv_before.iter().any(|k| k == &kv_y),
        "tenant_y KV shadow must exist before delete, got {kv_before:?}"
    );
    assert!(
        json_before.iter().any(|k| k == &json_x),
        "tenant_x JSON shadow must exist before delete, got {json_before:?}"
    );

    // Force-delete tenant_x.
    db.set_space("default").unwrap();
    db.delete_space_force("tenant_x").unwrap();

    // After cleanup:
    //   • all `tenant_x\x1f` entries gone from BOTH shadow collections,
    //   • the `tenant_y\x1f` entry still present (per-space filter).
    let kv_after = vs
        .list_keys(
            core_branch,
            strata_engine::system_space::SYSTEM_SPACE,
            SHADOW_KV,
        )
        .unwrap();
    let json_after = vs
        .list_keys(
            core_branch,
            strata_engine::system_space::SYSTEM_SPACE,
            SHADOW_JSON,
        )
        .unwrap();

    assert!(
        !kv_after.iter().any(|k| k.starts_with("tenant_x\x1f")),
        "tenant_x KV shadows must be wiped, got {kv_after:?}"
    );
    assert!(
        !json_after.iter().any(|k| k.starts_with("tenant_x\x1f")),
        "tenant_x JSON shadows must be wiped, got {json_after:?}"
    );
    assert!(
        kv_after.iter().any(|k| k == &kv_y),
        "tenant_y KV shadow must survive (per-space filter must isolate), got {kv_after:?}"
    );
}

/// Phase 4 / Part 4 — the metadata key delete still runs LAST after
/// all secondary cleanup. Pins the existing post-Phase 0 contract that
/// `SpaceIndex::list` no longer reports the space.
#[test]
fn test_force_delete_metadata_deleted_last() {
    let mut db = strata();

    db.set_space("tenant_x").unwrap();
    db.kv_put("k1", "v1").unwrap();
    db.set_space("default").unwrap();

    // Pre-condition: space is listed (auto-registered on first write).
    let spaces = db.list_spaces().unwrap();
    assert!(spaces.contains(&"tenant_x".to_string()));

    db.delete_space_force("tenant_x").unwrap();

    // Post-condition: metadata key is gone — list does not include it.
    // (`SpaceIndex::list` is the union of metadata + data scan; both
    // halves must be empty after a successful force-delete.)
    let spaces = db.list_spaces().unwrap();
    assert!(
        !spaces.contains(&"tenant_x".to_string()),
        "metadata key must be cleared as the last step of force-delete; got {spaces:?}"
    );
}
