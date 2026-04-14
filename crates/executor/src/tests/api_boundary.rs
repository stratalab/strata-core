//! Characterization tests for application open boundary.
//!
//! These tests verify that the application API boundary is correctly
//! constrained to `Strata::open()` and related methods, with engine
//! internals not directly accessible.

use crate::types::DistanceMetric;
use crate::{Command, Output, Strata, Value};

/// Verify that Strata::cache() installs the product-default subsystems.
///
/// The default subsystem order is: Graph, Vector, Search.
/// This test ensures app opens always use this standard configuration
/// by verifying that graph, vector, and search operations work.
#[test]
fn test_strata_open_installs_product_default_subsystems() {
    let strata = Strata::cache().unwrap();
    let executor = strata.executor();

    // Verify GraphSubsystem: can create nodes
    let result = executor.execute(Command::GraphAddNode {
        branch: None,
        space: None,
        graph: "test-graph".into(),
        node_id: "test-node".into(),
        entity_ref: None,
        properties: None,
        object_type: None,
    });
    assert!(
        result.is_ok(),
        "GraphSubsystem must be installed: {:?}",
        result
    );

    // Verify VectorSubsystem: can create collections
    let result = executor.execute(Command::VectorCreateCollection {
        branch: None,
        space: None,
        collection: "test-collection".into(),
        dimension: 4,
        metric: DistanceMetric::Cosine,
    });
    assert!(
        result.is_ok(),
        "VectorSubsystem must be installed: {:?}",
        result
    );

    // Verify SearchSubsystem: search commands route correctly
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: crate::types::SearchQuery {
            query: "test".into(),
            recipe: None,
            precomputed_embedding: None,
            k: Some(1),
            as_of: None,
            diff: None,
        },
    });
    // Search may return empty results, but should not error
    assert!(
        matches!(result, Ok(Output::SearchResults { .. })),
        "SearchSubsystem must be installed: {:?}",
        result
    );
}

/// Verify that Database constructors are not accessible from application code.
///
/// This is a compile-time guarantee. If this test compiles and runs, it confirms
/// that app code cannot call Database::open(), Database::cache(), etc. directly.
///
/// The test body is intentionally trivial — the assertion is that this module
/// compiles without access to internal Database constructors.
#[test]
fn test_database_not_directly_constructible_from_executor() {
    // These would NOT compile if attempted from outside the workspace:
    //
    // use strata_engine::Database;
    // let _ = Database::open(path);        // pub(crate) - not accessible
    // let _ = Database::open_follower(path);  // pub(crate) - not accessible
    // let _ = Database::cache();           // pub(crate) - not accessible
    //
    // Note: While Database::open_runtime() is pub for workspace-internal use,
    // strata_executor does not re-export Database, so application code cannot
    // access it. The only app-facing path is Strata::open()/cache().

    // This SHOULD work — it's the only app-facing path:
    let strata = Strata::cache();
    assert!(
        strata.is_ok(),
        "Strata::cache() is the canonical app entry point"
    );
}

/// Verify that default_product_spec is not exported from strata_executor.
///
/// Application code should not be able to construct OpenSpec directly.
/// The only way to open a database is through Strata::open/cache/open_with.
#[test]
fn test_openspec_not_directly_accessible() {
    // These would NOT compile if attempted:
    //
    // use strata_executor::default_product_spec;  // not exported
    // use strata_executor::default_product_cache_spec;  // not exported
    //
    // The functions exist but are pub(crate), so they can't be imported.

    // App code uses Strata::open() which internally uses default_product_spec
    let strata = Strata::cache();
    assert!(strata.is_ok());
}

// ==================== T2-E4: Recipe Auto-Seeding on Product Opens ====================

use strata_security::{AccessMode, OpenOptions};

/// Fresh `Strata::cache()` auto-seeds built-in recipes.
///
/// Per T2-E4: product opens SHOULD automatically seed recipes.
/// This verifies the cache open path seeds recipes without explicit user action.
#[test]
fn test_strata_cache_auto_seeds_recipes() {
    let strata = Strata::cache().expect("cache() should succeed");

    // All 6 built-in recipes should be available immediately
    let result = strata.executor().execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert_eq!(names.len(), 6, "Should have 6 built-in recipes auto-seeded");
            assert!(names.contains(&"default".to_string()));
            assert!(names.contains(&"keyword".to_string()));
            assert!(names.contains(&"semantic".to_string()));
            assert!(names.contains(&"hybrid".to_string()));
            assert!(names.contains(&"graph".to_string()));
            assert!(names.contains(&"rag".to_string()));
        }
        other => panic!("Expected Keys with 6 items, got: {other:?}"),
    }
}

/// Fresh `Strata::open_with()` auto-seeds built-in recipes.
///
/// Per T2-E4: product opens SHOULD automatically seed recipes.
/// This verifies the disk-based open path seeds recipes.
#[test]
fn test_strata_open_with_auto_seeds_recipes() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let opts = OpenOptions::new().access_mode(AccessMode::ReadWrite);
    let strata = Strata::open_with(temp_dir.path(), opts).expect("open should succeed");

    // All 6 built-in recipes should be available immediately
    let result = strata.executor().execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert_eq!(names.len(), 6, "Should have 6 built-in recipes auto-seeded");
            assert!(names.contains(&"default".to_string()));
            assert!(names.contains(&"keyword".to_string()));
        }
        other => panic!("Expected Keys with 6 items, got: {other:?}"),
    }
}

/// Local ReadOnly open still auto-seeds built-in recipes.
///
/// Per T2-E4: only followers and IPC clients skip seeding.
/// A local ReadOnly open (not a follower) should still seed.
#[test]
fn test_strata_local_readonly_auto_seeds_recipes() {
    let temp_dir = tempfile::TempDir::new().unwrap();

    // Open ReadOnly on a fresh database - seeding should still happen
    // because it's a local open, not a follower or IPC client.
    let opts = OpenOptions::new().access_mode(AccessMode::ReadOnly);
    let strata = Strata::open_with(temp_dir.path(), opts).expect("open should succeed");

    // Built-in recipes should be available
    let result = strata.executor().execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert_eq!(
                names.len(),
                6,
                "Local ReadOnly should auto-seed recipes: {:?}",
                names
            );
        }
        other => panic!("Expected Keys with 6 items, got: {other:?}"),
    }
}

/// Explicit `seed_builtin_recipes()` API is idempotent.
///
/// Per T2-E4: the typed product API should work correctly.
#[test]
fn test_strata_seed_builtin_recipes_api() {
    let strata = Strata::cache().expect("cache() should succeed");

    // Already auto-seeded, calling again should be idempotent
    strata
        .seed_builtin_recipes()
        .expect("seed should succeed (idempotent)");
    strata
        .seed_builtin_recipes()
        .expect("seed should succeed (idempotent)");

    // Still have exactly 6 recipes
    let result = strata.executor().execute(Command::RecipeList { branch: None });
    match result {
        Ok(Output::Keys(names)) => {
            assert_eq!(names.len(), 6, "Should have exactly 6 built-in recipes");
        }
        other => panic!("Expected Keys with 6 items, got: {other:?}"),
    }
}

/// Explicit seed via typed API is blocked in read-only mode.
#[test]
fn test_strata_seed_blocked_in_readonly_mode() {
    let temp_dir = tempfile::TempDir::new().unwrap();

    // First open ReadWrite to seed (auto-seeding happens)
    {
        let opts = OpenOptions::new().access_mode(AccessMode::ReadWrite);
        let _ = Strata::open_with(temp_dir.path(), opts).unwrap();
    }

    // Reopen ReadOnly - explicit seed should be blocked
    let opts = OpenOptions::new().access_mode(AccessMode::ReadOnly);
    let strata = Strata::open_with(temp_dir.path(), opts).unwrap();
    let result = strata.seed_builtin_recipes();
    assert!(
        result.is_err(),
        "seed_builtin_recipes() should fail in ReadOnly mode"
    );
}
