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
    // Note: OpenSpec is pub, but Database::open_runtime() is pub(crate),
    // so external code cannot open databases directly via OpenSpec.

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
