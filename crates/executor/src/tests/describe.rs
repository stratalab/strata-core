//! Integration tests for the Describe command.

use crate::types::{BranchId, DistanceMetric};
use crate::{Command, Executor, Output};
use strata_engine::database::search_only_cache_spec;
use strata_engine::Database;
use strata_security::AccessMode;

/// Create a test executor with an in-memory database.
fn create_test_executor() -> Executor {
    let db = Database::open_runtime(search_only_cache_spec()).unwrap();
    Executor::new(db)
}

// =============================================================================
// Empty database
// =============================================================================

#[test]
fn describe_empty_database() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert!(!d.version.is_empty());
            assert!(!d.follower);
            assert_eq!(d.branch, "default");
            // "default" should always be in the branches list
            assert!(d.branches.contains(&"default".to_string()));
            // spaces should include "default"
            assert!(d.spaces.contains(&"default".to_string()));
            // All primitive counts should be zero
            assert_eq!(d.primitives.kv.count, 0);
            assert_eq!(d.primitives.json.count, 0);
            assert_eq!(d.primitives.events.count, 0);
            assert!(d.primitives.vector.collections.is_empty());
            assert!(d.primitives.graph.graphs.is_empty());
            // Default config values
            assert_eq!(d.config.provider, "local");
            assert!(!d.config.auto_embed);
            assert_eq!(d.config.embed_model, "miniLM");
            assert_eq!(d.config.durability, "standard");
            assert!(d.config.default_model.is_none());
            // Capabilities
            assert!(!d.capabilities.vector_query);
            assert!(!d.capabilities.generation);
            assert!(!d.capabilities.auto_embed);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// is_write() correctness
// =============================================================================

#[test]
fn describe_is_not_a_write() {
    let cmd = Command::Describe { branch: None };
    assert!(!cmd.is_write(), "Describe should be read-only");
}

// =============================================================================
// Read-only mode
// =============================================================================

#[test]
fn describe_works_in_read_only_mode() {
    let db = Database::open_runtime(search_only_cache_spec()).unwrap();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let result = executor.execute(Command::Describe {
        branch: Some(BranchId::default()),
    });
    assert!(result.is_ok(), "Describe should work in read-only mode");
    assert!(matches!(result.unwrap(), Output::Described(_)));
}

// =============================================================================
// KV counts
// =============================================================================

#[test]
fn describe_counts_kv_keys() {
    let executor = create_test_executor();

    for i in 0..5 {
        executor
            .execute(Command::KvPut {
                branch: Some(BranchId::default()),
                space: None,
                key: format!("key{}", i),
                value: strata_core::Value::Int(i),
            })
            .unwrap();
    }

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.primitives.kv.count, 5);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// JSON document counts
// =============================================================================

#[test]
fn describe_counts_json_docs() {
    let executor = create_test_executor();

    for i in 0..3 {
        executor
            .execute(Command::JsonSet {
                branch: Some(BranchId::default()),
                space: None,
                key: format!("doc{}", i),
                path: "$".into(),
                value: strata_core::Value::object(
                    [("id".to_string(), strata_core::Value::Int(i))]
                        .into_iter()
                        .collect(),
                ),
            })
            .unwrap();
    }

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.primitives.json.count, 3);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Event counts
// =============================================================================

#[test]
fn describe_counts_events() {
    let executor = create_test_executor();

    for i in 0..4 {
        executor
            .execute(Command::EventAppend {
                branch: Some(BranchId::default()),
                space: None,
                event_type: "test".into(),
                payload: strata_core::Value::object(
                    [("n".to_string(), strata_core::Value::Int(i))]
                        .into_iter()
                        .collect(),
                ),
            })
            .unwrap();
    }

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.primitives.events.count, 4);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Vector collections
// =============================================================================

#[test]
fn describe_lists_vector_collections() {
    let executor = create_test_executor();

    executor
        .execute(Command::VectorCreateCollection {
            branch: Some(BranchId::default()),
            space: None,
            collection: "embeddings".into(),
            dimension: 128,
            metric: DistanceMetric::Cosine,
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.primitives.vector.collections.len(), 1);
            let coll = &d.primitives.vector.collections[0];
            assert_eq!(coll.name, "embeddings");
            assert_eq!(coll.dimension, 128);
            assert_eq!(coll.metric, DistanceMetric::Cosine);
            assert_eq!(coll.count, 0);
            assert!(d.capabilities.vector_query);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

#[test]
fn describe_vector_query_false_without_collections() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert!(!d.capabilities.vector_query);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Graph data (nodes, edges, ontology)
// =============================================================================

#[test]
fn describe_lists_graphs_with_edges() {
    let executor = create_test_executor();

    executor
        .execute(Command::GraphCreate {
            branch: Some(BranchId::default()),
            space: None,
            graph: "social".into(),
            cascade_policy: None,
        })
        .unwrap();

    // Add two nodes
    for node in ["alice", "bob"] {
        executor
            .execute(Command::GraphAddNode {
                branch: Some(BranchId::default()),
                space: None,
                graph: "social".into(),
                node_id: node.into(),
                entity_ref: None,
                properties: None,
                object_type: None,
            })
            .unwrap();
    }

    // Add an edge
    executor
        .execute(Command::GraphAddEdge {
            branch: Some(BranchId::default()),
            space: None,
            graph: "social".into(),
            src: "alice".into(),
            dst: "bob".into(),
            edge_type: "follows".into(),
            weight: None,
            properties: None,
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.primitives.graph.graphs.len(), 1);
            let g = &d.primitives.graph.graphs[0];
            assert_eq!(g.name, "social");
            assert_eq!(g.nodes, 2);
            assert_eq!(g.edges, 1);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

#[test]
fn describe_includes_graph_ontology_types() {
    let executor = create_test_executor();

    executor
        .execute(Command::GraphCreate {
            branch: Some(BranchId::default()),
            space: None,
            graph: "medical".into(),
            cascade_policy: None,
        })
        .unwrap();

    // Define object type
    executor
        .execute(Command::GraphDefineObjectType {
            branch: Some(BranchId::default()),
            space: None,
            graph: "medical".into(),
            definition: strata_core::Value::object(
                [
                    (
                        "name".to_string(),
                        strata_core::Value::String("Patient".to_string()),
                    ),
                    (
                        "properties".to_string(),
                        strata_core::Value::object(std::collections::HashMap::new()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    // Define link type (requires source + target object types)
    executor
        .execute(Command::GraphDefineLinkType {
            branch: Some(BranchId::default()),
            space: None,
            graph: "medical".into(),
            definition: strata_core::Value::object(
                [
                    (
                        "name".to_string(),
                        strata_core::Value::String("prescribed".to_string()),
                    ),
                    (
                        "source".to_string(),
                        strata_core::Value::String("Patient".to_string()),
                    ),
                    (
                        "target".to_string(),
                        strata_core::Value::String("Patient".to_string()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            let g = &d.primitives.graph.graphs[0];
            assert_eq!(g.name, "medical");
            assert!(
                g.object_types.contains(&"Patient".to_string()),
                "Expected Patient in object_types: {:?}",
                g.object_types
            );
            assert!(
                g.link_types.contains(&"prescribed".to_string()),
                "Expected prescribed in link_types: {:?}",
                g.link_types
            );
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Config reflects changes
// =============================================================================

#[test]
fn describe_config_reflects_changes() {
    let executor = create_test_executor();

    executor
        .execute(Command::ConfigureSet {
            key: "provider".into(),
            value: "anthropic".into(),
        })
        .unwrap();

    executor
        .execute(Command::ConfigureSet {
            key: "default_model".into(),
            value: "claude-sonnet-4-20250514".into(),
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.config.provider, "anthropic");
            assert_eq!(
                d.config.default_model,
                Some("claude-sonnet-4-20250514".to_string())
            );
            // With a default_model set, generation capability should be true
            assert!(d.capabilities.generation);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

#[test]
fn describe_generation_false_without_model() {
    let executor = create_test_executor();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert!(d.config.default_model.is_none());
            assert!(!d.capabilities.generation);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Non-default space: counts are only from default space
// =============================================================================

#[test]
fn describe_only_counts_default_space() {
    let executor = create_test_executor();

    // Create a non-default space and put data there
    executor
        .execute(Command::SpaceCreate {
            branch: Some(BranchId::default()),
            space: "archive".into(),
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::default()),
            space: Some("archive".into()),
            key: "archived-key".into(),
            value: strata_core::Value::Int(99),
        })
        .unwrap();

    // Put one key in default space
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::default()),
            space: None,
            key: "default-key".into(),
            value: strata_core::Value::Int(1),
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            // Should only count the key in default space
            assert_eq!(d.primitives.kv.count, 1);
            // Both spaces should be listed
            assert!(d.spaces.contains(&"default".to_string()));
            assert!(d.spaces.contains(&"archive".to_string()));
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Default branch resolution
// =============================================================================

#[test]
fn describe_with_none_branch_uses_default() {
    let executor = create_test_executor();

    let mut cmd = Command::Describe { branch: None };
    cmd.resolve_defaults();

    let result = executor.execute(cmd).unwrap();
    match result {
        Output::Described(d) => {
            assert_eq!(d.branch, "default");
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Non-default branch
// =============================================================================

#[test]
fn describe_on_named_branch() {
    let executor = create_test_executor();

    // Create a branch
    executor
        .execute(Command::BranchCreate {
            branch_id: Some("experiment".into()),
            metadata: None,
        })
        .unwrap();

    // Put data on that branch
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::from("experiment")),
            space: None,
            key: "branch-key".into(),
            value: strata_core::Value::Int(42),
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::from("experiment")),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.branch, "experiment");
            assert_eq!(d.primitives.kv.count, 1);
            // The branches list should contain both
            assert!(d.branches.contains(&"default".to_string()));
            assert!(d.branches.contains(&"experiment".to_string()));
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}

// =============================================================================
// Serialization round-trip with data
// =============================================================================

#[test]
fn describe_result_serializes_to_json() {
    let executor = create_test_executor();

    // Add some data so we're testing serialization of non-empty values
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::default()),
            space: None,
            key: "hello".into(),
            value: strata_core::Value::String("world".into()),
        })
        .unwrap();

    executor
        .execute(Command::VectorCreateCollection {
            branch: Some(BranchId::default()),
            space: None,
            collection: "test_col".into(),
            dimension: 384,
            metric: DistanceMetric::Euclidean,
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    // Serialize and deserialize
    let json = serde_json::to_string_pretty(&result).unwrap();
    let restored: Output = serde_json::from_str(&json).unwrap();
    assert_eq!(result, restored, "Round-trip failed");

    // Verify JSON shape contains expected top-level fields
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    let inner = &val["Described"];
    assert!(inner["version"].is_string());
    assert!(inner["path"].is_string());
    assert!(inner["branch"].is_string());
    assert!(inner["branches"].is_array());
    assert!(inner["spaces"].is_array());
    assert!(inner["follower"].is_boolean());
    assert!(inner["primitives"].is_object());
    assert!(inner["config"].is_object());
    assert!(inner["capabilities"].is_object());
    // Check nested structure
    assert_eq!(inner["primitives"]["kv"]["count"], 1);
    assert_eq!(
        inner["primitives"]["vector"]["collections"][0]["name"],
        "test_col"
    );
    assert_eq!(
        inner["primitives"]["vector"]["collections"][0]["metric"],
        "euclidean"
    );
}

// =============================================================================
// Mixed data: all primitives populated
// =============================================================================

#[test]
fn describe_with_all_primitives() {
    let executor = create_test_executor();

    // KV
    executor
        .execute(Command::KvPut {
            branch: Some(BranchId::default()),
            space: None,
            key: "k1".into(),
            value: strata_core::Value::Int(1),
        })
        .unwrap();

    // JSON
    executor
        .execute(Command::JsonSet {
            branch: Some(BranchId::default()),
            space: None,
            key: "doc1".into(),
            path: "$".into(),
            value: strata_core::Value::object(
                [("x".to_string(), strata_core::Value::Int(1))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();

    // Event
    executor
        .execute(Command::EventAppend {
            branch: Some(BranchId::default()),
            space: None,
            event_type: "click".into(),
            payload: strata_core::Value::object(
                [("n".to_string(), strata_core::Value::Int(1))]
                    .into_iter()
                    .collect(),
            ),
        })
        .unwrap();

    // Vector
    executor
        .execute(Command::VectorCreateCollection {
            branch: Some(BranchId::default()),
            space: None,
            collection: "emb".into(),
            dimension: 4,
            metric: DistanceMetric::DotProduct,
        })
        .unwrap();

    // Graph
    executor
        .execute(Command::GraphCreate {
            branch: Some(BranchId::default()),
            space: None,
            graph: "g1".into(),
            cascade_policy: None,
        })
        .unwrap();
    executor
        .execute(Command::GraphAddNode {
            branch: Some(BranchId::default()),
            space: None,
            graph: "g1".into(),
            node_id: "n1".into(),
            entity_ref: None,
            properties: None,
            object_type: None,
        })
        .unwrap();

    let result = executor
        .execute(Command::Describe {
            branch: Some(BranchId::default()),
        })
        .unwrap();

    match result {
        Output::Described(d) => {
            assert_eq!(d.primitives.kv.count, 1);
            assert_eq!(d.primitives.json.count, 1);
            assert_eq!(d.primitives.events.count, 1);
            assert_eq!(d.primitives.vector.collections.len(), 1);
            assert_eq!(
                d.primitives.vector.collections[0].metric,
                DistanceMetric::DotProduct
            );
            assert_eq!(d.primitives.graph.graphs.len(), 1);
            assert_eq!(d.primitives.graph.graphs[0].nodes, 1);
        }
        other => panic!("Expected Described, got {:?}", other),
    }
}
