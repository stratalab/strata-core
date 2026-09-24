//! Graph core conformance tests.

#![allow(clippy::too_many_lines)]

mod common;

use serde_json::json;
use strata_core::CommitVersion;
use strata_engine::{
    BranchName, Database, EngineErrorClass, GraphAnalyticsBudget, GraphBatchOperation,
    GraphBatchWrite, GraphBindingPrimitive, GraphBindingTarget, GraphDirection, GraphEdgeData,
    GraphEdgeType, GraphEntityBinding, GraphName, GraphNodeData, GraphNodeId, GraphProperties,
    GraphService,
};

use common::{branch, open_cache_database, open_durable_database, space};

#[test]
fn graph_lifecycle_node_edge_and_binding_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_lifecycle_node_edge_and_binding);
}

#[test]
fn graph_batch_write_is_atomic_and_positional() {
    run_database_modes(exercise_graph_batch_write);
}

#[test]
fn graph_temporal_reads_track_updates_and_deletes() {
    run_database_modes(exercise_graph_temporal_reads);
}

#[test]
fn graph_lifecycle_pagination_and_recreate_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_lifecycle_pagination_and_recreate);
}

#[test]
fn graph_dense_edges_self_loop_and_neighbor_pages_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_dense_edges_self_loop_and_neighbor_pages);
}

#[test]
fn graph_binding_lookup_pages_and_isolation_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_binding_lookup_pages_and_isolation);
}

#[test]
fn graph_rejects_cross_branch_relationship_bindings_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_cross_branch_binding_rejection);
}

#[test]
fn graph_commit_counts_exclude_derived_rows_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_commit_counts_exclude_derived_rows);
}

#[test]
fn graph_batch_ordering_and_failure_regressions_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_graph_batch_ordering_and_failure_regressions);
}

#[test]
fn graph_reads_take_shared_ref_so_one_handle_serves_concurrent_readers() {
    run_database_modes(exercise_graph_reads_take_shared_ref);
}

#[test]
fn graph_list_edges_streams_full_edge_data_with_a_cursor() {
    run_database_modes(exercise_graph_list_edges);
}

#[test]
fn graph_delete_guards_a_populated_graph_without_force() {
    run_database_modes(exercise_graph_delete_force_guard);
}

/// #3122: deleting a graph destroys every node and edge in it, so a populated
/// graph refuses deletion without force (matching space delete) while an empty
/// one deletes freely.
fn exercise_graph_delete_force_guard(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");

    // An empty graph deletes without force — no data is at risk.
    graph
        .create_graph(graph_name("empty"))
        .expect("empty graph create succeeds");
    assert!(graph
        .delete_graph(&graph_name("empty"), false)
        .expect("an empty graph deletes without force")
        .deleted());

    // A populated graph refuses deletion without force...
    graph
        .create_graph(graph_name("deps"))
        .expect("deps graph create succeeds");
    graph
        .upsert_node(&graph_name("deps"), node("a"), node_data(json!({}), None))
        .expect("node upsert succeeds");
    let refused = graph
        .delete_graph(&graph_name("deps"), false)
        .expect_err("a populated graph refuses deletion without force");
    assert_eq!(refused.class(), EngineErrorClass::Conflict);
    assert_eq!(refused.code(), "failed_precondition.engine.graph_not_empty");
    // ...and the refusal is zero-mutation — the node is still there.
    assert!(graph
        .get_node(&graph_name("deps"), &node("a"))
        .expect("read succeeds")
        .is_some());

    // With force, it deletes.
    assert!(graph
        .delete_graph(&graph_name("deps"), true)
        .expect("force deletes a populated graph")
        .deleted());
}

#[test]
fn graph_branch_and_space_isolation_match_other_primitives() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let parent_binding = binding(GraphBindingPrimitive::Json, "docs", "parent-bound");
    let child_binding = binding(GraphBindingPrimitive::Json, "docs", "child-bound");
    {
        let mut graph = graph_service(&mut database, "default", "default");
        graph
            .create_graph(graph_name("deps"))
            .expect("graph create succeeds");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("shared"),
                node_data(json!({"branch": "default"}), None),
            )
            .expect("node upsert succeeds");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("bound"),
                node_data(
                    json!({"branch": "default", "bound": true}),
                    Some(parent_binding.clone()),
                ),
            )
            .expect("bound node upsert succeeds");
    }

    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch fork succeeds");

    {
        let mut feature = graph_service(&mut database, "feature", "default");
        let inherited = feature
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("read inherited node succeeds")
            .expect("inherited node exists");
        assert_eq!(
            inherited
                .data()
                .properties()
                .expect("properties")
                .as_inner(),
            &json!({"branch": "default"})
        );
        assert_eq!(
            feature
                .bindings_for_entity(parent_binding.target(), None, 10)
                .expect("feature inherited binding lookup succeeds")
                .bindings()
                .len(),
            1
        );
        feature
            .upsert_node(
                &graph_name("deps"),
                node("shared"),
                node_data(json!({"branch": "feature"}), None),
            )
            .expect("feature node upsert succeeds");
        feature
            .upsert_node(
                &graph_name("deps"),
                node("bound"),
                node_data(
                    json!({"branch": "feature", "bound": true}),
                    Some(child_binding.clone()),
                ),
            )
            .expect("feature binding replacement succeeds");
        assert!(feature
            .bindings_for_entity(parent_binding.target(), None, 10)
            .expect("feature old binding lookup succeeds")
            .bindings()
            .is_empty());
        assert_eq!(
            feature
                .bindings_for_entity(child_binding.target(), None, 10)
                .expect("feature new binding lookup succeeds")
                .bindings()
                .len(),
            1
        );
    }

    {
        let parent = graph_service(&mut database, "default", "default");
        let node = parent
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("parent read succeeds")
            .expect("parent node exists");
        assert_eq!(
            node.data().properties().expect("properties").as_inner(),
            &json!({"branch": "default"})
        );
        assert_eq!(
            parent
                .bindings_for_entity(parent_binding.target(), None, 10)
                .expect("parent binding lookup succeeds")
                .bindings()
                .len(),
            1
        );
        assert!(parent
            .bindings_for_entity(child_binding.target(), None, 10)
            .expect("parent child-binding lookup succeeds")
            .bindings()
            .is_empty());
    }

    {
        let mut feature = graph_service(&mut database, "feature", "default");
        let feature_node = feature
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("feature read succeeds")
            .expect("feature node exists");
        assert_eq!(
            feature_node
                .data()
                .properties()
                .expect("properties")
                .as_inner(),
            &json!({"branch": "feature"})
        );
        assert!(feature
            .delete_node(&graph_name("deps"), &node("shared"))
            .expect("feature node delete succeeds")
            .deleted());
        assert!(feature
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("feature node read succeeds")
            .is_none());
        feature
            .upsert_node(
                &graph_name("deps"),
                node("feature-only"),
                node_data(json!({"branch": "feature"}), None),
            )
            .expect("feature-only node upsert succeeds");
    }

    {
        let parent = graph_service(&mut database, "default", "default");
        let node = parent
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("parent read succeeds")
            .expect("parent node still exists");
        assert_eq!(
            node.data().properties().expect("properties").as_inner(),
            &json!({"branch": "default"})
        );
    }

    {
        let mut feature = graph_service(&mut database, "feature", "default");
        feature
            .create_graph(graph_name("feature-only"))
            .expect("feature graph create succeeds");
        assert!(feature
            .delete_graph(&graph_name("deps"), true)
            .expect("feature graph delete succeeds")
            .deleted());
        assert!(feature
            .graph_info(&graph_name("deps"))
            .expect("feature graph info succeeds")
            .is_none());
        assert!(feature
            .bindings_for_entity(child_binding.target(), None, 10)
            .expect("feature child binding lookup after graph delete succeeds")
            .bindings()
            .is_empty());
        assert!(feature
            .bindings_for_entity(parent_binding.target(), None, 10)
            .expect("feature parent binding lookup after graph delete succeeds")
            .bindings()
            .is_empty());
    }

    {
        let parent = graph_service(&mut database, "default", "default");
        assert!(parent
            .graph_info(&graph_name("deps"))
            .expect("parent graph info succeeds")
            .is_some());
        assert!(parent
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("parent inherited node read succeeds")
            .is_some());
        assert!(parent
            .get_node(&graph_name("deps"), &node("feature-only"))
            .expect("parent feature-only node read succeeds")
            .is_none());
        assert!(parent
            .graph_info(&graph_name("feature-only"))
            .expect("parent feature-only graph read succeeds")
            .is_none());
        assert_eq!(
            parent
                .bindings_for_entity(parent_binding.target(), None, 10)
                .expect("parent binding lookup after child graph delete succeeds")
                .bindings()
                .len(),
            1
        );
        assert!(parent
            .bindings_for_entity(child_binding.target(), None, 10)
            .expect("parent child-binding lookup after child graph delete succeeds")
            .bindings()
            .is_empty());
    }

    {
        let mut other_space = graph_service(&mut database, "default", "other");
        other_space
            .create_graph(graph_name("deps"))
            .expect("other-space graph create succeeds");
        assert!(other_space
            .get_node(&graph_name("deps"), &node("shared"))
            .expect("other-space read succeeds")
            .is_none());
    }
}

#[test]
fn graph_durable_reopen_preserves_core_indexes() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        let mut graph = graph_service(&mut database, "default", "default");
        graph
            .create_graph(graph_name("deps"))
            .expect("graph create succeeds");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("doc"),
                node_data(
                    json!({"kind": "doc"}),
                    Some(binding(GraphBindingPrimitive::Json, "docs", "doc-1")),
                ),
            )
            .expect("doc node upsert succeeds");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("chunk"),
                node_data(json!({"kind": "chunk"}), None),
            )
            .expect("chunk node upsert succeeds");
        graph
            .upsert_edge(
                &graph_name("deps"),
                node("doc"),
                edge_type("contains"),
                node("chunk"),
                edge_data(0.5, json!({"rank": 1})),
            )
            .expect("edge upsert succeeds");
        let first_page = graph
            .list_nodes(&graph_name("deps"), None, None, 1)
            .expect("node list page succeeds");
        assert_eq!(first_page.nodes().len(), 1);
        assert!(first_page.has_more());
        assert_eq!(
            graph
                .list_nodes(&graph_name("deps"), None, first_page.cursor(), 10)
                .expect("second node list page succeeds")
                .nodes()
                .len(),
            1
        );
        drop(graph);
        database.close().expect("close succeeds");
    }

    let mut reopened = open_durable_database(tempdir.path()).expect("durable reopen succeeds");
    let graph = graph_service(&mut reopened, "default", "default");
    assert!(graph
        .graph_info(&graph_name("deps"))
        .expect("graph info succeeds")
        .is_some());
    let first_page = graph
        .list_nodes(&graph_name("deps"), None, None, 1)
        .expect("reopened node list page succeeds");
    assert_eq!(first_page.nodes().len(), 1);
    assert!(first_page.has_more());
    assert_eq!(
        graph
            .list_nodes(&graph_name("deps"), None, first_page.cursor(), 10)
            .expect("reopened second node list page succeeds")
            .nodes()
            .len(),
        1
    );
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk")
        )
        .expect("edge read succeeds")
        .is_some());
    assert_eq!(
        graph
            .neighbors(
                &graph_name("deps"),
                &node("doc"),
                GraphDirection::Outgoing,
                None,
                None,
                10
            )
            .expect("neighbors succeed")
            .neighbors()
            .len(),
        1
    );
    assert_eq!(
        graph
            .bindings_for_entity(
                &target(GraphBindingPrimitive::Json, "docs", "doc-1"),
                None,
                10
            )
            .expect("binding lookup succeeds")
            .bindings()
            .len(),
        1
    );
}

#[test]
fn graph_durable_delete_reopen_and_recreate_drops_stale_indexes() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let doc_binding = binding(GraphBindingPrimitive::Json, "docs", "doc-1");

    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        let mut graph = graph_service(&mut database, "default", "default");
        graph
            .create_graph(graph_name("deps"))
            .expect("graph create succeeds");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("doc"),
                node_data(json!({"kind": "doc"}), Some(doc_binding.clone())),
            )
            .expect("doc node upsert succeeds");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("chunk"),
                node_data(json!({"kind": "chunk"}), None),
            )
            .expect("chunk node upsert succeeds");
        graph
            .upsert_edge(
                &graph_name("deps"),
                node("doc"),
                edge_type("contains"),
                node("chunk"),
                edge_data(1.0, json!({"rank": 1})),
            )
            .expect("edge upsert succeeds");
        assert!(graph
            .delete_graph(&graph_name("deps"), true)
            .expect("graph delete succeeds")
            .deleted());
        assert!(graph
            .bindings_for_entity(doc_binding.target(), None, 10)
            .expect("binding lookup after delete succeeds")
            .bindings()
            .is_empty());
        drop(graph);
        database.close().expect("close succeeds");
    }

    {
        let mut database = open_durable_database(tempdir.path()).expect("durable reopen succeeds");
        let mut graph = graph_service(&mut database, "default", "default");
        assert!(graph
            .graph_info(&graph_name("deps"))
            .expect("graph info after reopen succeeds")
            .is_none());
        assert!(graph
            .bindings_for_entity(doc_binding.target(), None, 10)
            .expect("binding lookup after reopen succeeds")
            .bindings()
            .is_empty());

        let (recreated, _) = graph
            .create_graph(graph_name("deps"))
            .expect("graph recreate succeeds");
        assert_eq!(recreated.node_count(), 0);
        assert_eq!(recreated.edge_count(), 0);
        assert!(graph
            .list_nodes(&graph_name("deps"), None, None, 10)
            .expect("recreated node list succeeds")
            .nodes()
            .is_empty());
        assert!(graph
            .get_node(&graph_name("deps"), &node("doc"))
            .expect("recreated stale node read succeeds")
            .is_none());
        assert!(graph
            .get_edge(
                &graph_name("deps"),
                &node("doc"),
                &edge_type("contains"),
                &node("chunk"),
            )
            .expect("recreated stale edge read succeeds")
            .is_none());
        assert!(graph
            .bindings_for_entity(doc_binding.target(), None, 10)
            .expect("recreated binding lookup succeeds")
            .bindings()
            .is_empty());
        drop(graph);
        database.close().expect("close succeeds");
    }

    let mut database =
        open_durable_database(tempdir.path()).expect("durable second reopen succeeds");
    let graph = graph_service(&mut database, "default", "default");
    assert!(graph
        .graph_info(&graph_name("deps"))
        .expect("recreated graph info succeeds")
        .is_some());
    assert!(graph
        .list_nodes(&graph_name("deps"), None, None, 10)
        .expect("second reopen node list succeeds")
        .nodes()
        .is_empty());
    assert!(graph
        .bindings_for_entity(doc_binding.target(), None, 10)
        .expect("second reopen binding lookup succeeds")
        .bindings()
        .is_empty());
}

fn exercise_graph_lifecycle_node_edge_and_binding(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    assert!(graph
        .list_graphs(None, 10)
        .expect("list succeeds")
        .graphs()
        .is_empty());

    let (info, _) = graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");
    assert_eq!(info.name().as_str(), "deps");
    assert_eq!(info.node_count(), 0);
    assert_eq!(info.edge_count(), 0);
    assert_eq!(
        graph.list_graphs(None, 10).expect("list succeeds").graphs()[0].as_str(),
        "deps"
    );
    assert_eq!(
        graph
            .create_graph(graph_name("deps"))
            .expect_err("duplicate create fails")
            .class(),
        EngineErrorClass::Conflict
    );

    let original_binding = binding(GraphBindingPrimitive::Json, "docs", "doc-1");
    let replacement_binding = binding(GraphBindingPrimitive::Json, "docs", "doc-2");
    let created = graph
        .upsert_node(
            &graph_name("deps"),
            node("doc"),
            node_data(json!({"kind": "doc"}), Some(original_binding.clone())),
        )
        .expect("node upsert succeeds");
    assert!(created.created());
    let updated = graph
        .upsert_node(
            &graph_name("deps"),
            node("doc"),
            node_data(
                json!({"kind": "updated"}),
                Some(replacement_binding.clone()),
            ),
        )
        .expect("node update succeeds");
    assert!(!updated.created());
    graph
        .upsert_node(
            &graph_name("deps"),
            node("chunk-a"),
            node_data(json!({"kind": "chunk"}), None),
        )
        .expect("chunk node upsert succeeds");
    graph
        .upsert_node(
            &graph_name("deps"),
            node("chunk-b"),
            node_data(json!({"kind": "chunk"}), None),
        )
        .expect("chunk node upsert succeeds");

    let doc_node = graph
        .get_node(&graph_name("deps"), &node("doc"))
        .expect("node get succeeds")
        .expect("node exists");
    assert_eq!(
        doc_node.data().properties().expect("properties").as_inner(),
        &json!({"kind": "updated"})
    );
    assert!(doc_node.data().binding().is_some());

    let page = graph
        .list_nodes(&graph_name("deps"), Some(&node("chunk")), None, 1)
        .expect("node list succeeds");
    assert_eq!(page.nodes().len(), 1);
    assert!(page.has_more());
    assert_eq!(page.nodes()[0].node_id().as_str(), "chunk-a");

    let edge_created = graph
        .upsert_edge(
            &graph_name("deps"),
            node("doc"),
            edge_type("contains"),
            node("chunk-a"),
            edge_data(1.0, json!({"rank": 1})),
        )
        .expect("edge upsert succeeds");
    assert!(edge_created.created());
    let edge_updated = graph
        .upsert_edge(
            &graph_name("deps"),
            node("doc"),
            edge_type("contains"),
            node("chunk-a"),
            edge_data(2.0, json!({"rank": 2})),
        )
        .expect("edge update succeeds");
    assert!(!edge_updated.created());
    graph
        .upsert_edge(
            &graph_name("deps"),
            node("chunk-b"),
            edge_type("references"),
            node("doc"),
            edge_data(1.0, json!({})),
        )
        .expect("incoming edge upsert succeeds");

    let edge = graph
        .get_edge(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk-a"),
        )
        .expect("edge get succeeds")
        .expect("edge exists");
    assert_float_eq(edge.data().weight(), 2.0);
    assert_eq!(
        edge.data().properties().expect("properties").as_inner(),
        &json!({"rank": 2})
    );

    let outgoing = graph
        .neighbors(
            &graph_name("deps"),
            &node("doc"),
            GraphDirection::Outgoing,
            None,
            None,
            10,
        )
        .expect("outgoing neighbors succeed");
    assert_eq!(outgoing.neighbors().len(), 1);
    assert_eq!(outgoing.neighbors()[0].node().node_id().as_str(), "chunk-a");

    let incoming = graph
        .neighbors(
            &graph_name("deps"),
            &node("doc"),
            GraphDirection::Incoming,
            None,
            None,
            10,
        )
        .expect("incoming neighbors succeed");
    assert_eq!(incoming.neighbors().len(), 1);
    assert_eq!(incoming.neighbors()[0].node().node_id().as_str(), "chunk-b");

    let filtered = graph
        .neighbors(
            &graph_name("deps"),
            &node("doc"),
            GraphDirection::Both,
            Some(&edge_type("contains")),
            None,
            10,
        )
        .expect("filtered neighbors succeed");
    assert_eq!(filtered.neighbors().len(), 1);

    assert!(graph
        .bindings_for_entity(original_binding.target(), None, 10)
        .expect("old binding lookup succeeds")
        .bindings()
        .is_empty());
    let binding_page = graph
        .bindings_for_entity(replacement_binding.target(), None, 10)
        .expect("binding lookup succeeds");
    assert_eq!(binding_page.bindings().len(), 1);
    assert_eq!(binding_page.bindings()[0].node_id().as_str(), "doc");

    let missing_endpoint = graph
        .upsert_edge(
            &graph_name("deps"),
            node("missing"),
            edge_type("contains"),
            node("doc"),
            edge_data(1.0, json!({})),
        )
        .expect_err("missing endpoint rejected");
    assert_eq!(missing_endpoint.class(), EngineErrorClass::InvalidInput);

    assert!(graph
        .delete_edge(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk-a"),
        )
        .expect("edge delete succeeds")
        .deleted());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk-a"),
        )
        .expect("edge read succeeds")
        .is_none());

    assert!(graph
        .delete_node(&graph_name("deps"), &node("doc"))
        .expect("node delete succeeds")
        .deleted());
    assert!(graph
        .bindings_for_entity(replacement_binding.target(), None, 10)
        .expect("binding lookup succeeds")
        .bindings()
        .is_empty());
    assert!(graph
        .neighbors(
            &graph_name("deps"),
            &node("chunk-b"),
            GraphDirection::Outgoing,
            None,
            None,
            10,
        )
        .expect("neighbor lookup succeeds")
        .neighbors()
        .is_empty());

    assert!(graph
        .delete_graph(&graph_name("deps"), true)
        .expect("graph delete succeeds")
        .deleted());
    assert!(graph
        .graph_info(&graph_name("deps"))
        .expect("graph info succeeds")
        .is_none());
}

fn exercise_graph_batch_write(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");

    let empty = graph
        .batch_write(&graph_name("deps"), &GraphBatchWrite::new(Vec::new()))
        .expect("empty batch succeeds");
    assert!(empty.results().is_empty());
    assert!(empty.commit().is_none());

    let batch = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("doc"),
            data: node_data(json!({"kind": "doc"}), None),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("chunk"),
            data: node_data(json!({"kind": "chunk"}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("doc"),
            edge_type: edge_type("contains"),
            dst: node("chunk"),
            data: edge_data(1.0, json!({})),
        },
    ]);
    let outcome = graph
        .batch_write(&graph_name("deps"), &batch)
        .expect("batch succeeds");
    assert_eq!(outcome.results().len(), 3);
    assert_eq!(outcome.results()[0].created_flag(), Some(true));
    assert_eq!(outcome.results()[2].created_flag(), Some(true));
    assert!(outcome.commit().is_some());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk")
        )
        .expect("edge read succeeds")
        .is_some());

    let bound = binding(GraphBindingPrimitive::Json, "docs", "batch-doc");
    let create_then_delete_bound = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("bound"),
            data: node_data(json!({"kind": "bound"}), Some(bound.clone())),
        },
        GraphBatchOperation::DeleteNode {
            node_id: node("bound"),
        },
    ]);
    let outcome = graph
        .batch_write(&graph_name("deps"), &create_then_delete_bound)
        .expect("create/delete bound node batch succeeds");
    assert_eq!(outcome.results()[0].created_flag(), Some(true));
    assert_eq!(outcome.results()[1].deleted_flag(), Some(true));
    assert!(graph
        .get_node(&graph_name("deps"), &node("bound"))
        .expect("bound node read succeeds")
        .is_none());
    assert!(graph
        .bindings_for_entity(bound.target(), None, 10)
        .expect("binding lookup succeeds")
        .bindings()
        .is_empty());

    let invalid = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("orphan"),
            data: node_data(json!({"kind": "orphan"}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("missing"),
            edge_type: edge_type("bad"),
            dst: node("orphan"),
            data: edge_data(1.0, json!({})),
        },
    ]);
    let error = graph
        .batch_write(&graph_name("deps"), &invalid)
        .expect_err("invalid batch fails");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.graph_edge_endpoint");
    // A genuinely-missing endpoint (never upserted, here or later) gets the
    // plain error, not the batch-ordering hint (#3192).
    assert!(!error.to_string().contains("same batch"));
    assert!(graph
        .get_node(&graph_name("deps"), &node("orphan"))
        .expect("orphan read succeeds")
        .is_none());

    // #3192: an UpsertEdge whose endpoints are upserted LATER in the same batch
    // is refused (order is semantic), and the error names the ordering rule
    // rather than reporting a bare missing endpoint.
    let out_of_order = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertEdge {
            src: node("late_a"),
            edge_type: edge_type("links"),
            dst: node("late_b"),
            data: edge_data(1.0, json!({})),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("late_a"),
            data: node_data(json!({"kind": "n"}), None),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("late_b"),
            data: node_data(json!({"kind": "n"}), None),
        },
    ]);
    let ordering = graph
        .batch_write(&graph_name("deps"), &out_of_order)
        .expect_err("an edge before its endpoints is refused");
    assert_eq!(
        ordering.code(),
        "invalid_argument.engine.graph_edge_endpoint"
    );
    assert!(
        ordering.to_string().contains("same batch"),
        "the error names the batch ordering rule (#3192): {ordering}"
    );
    assert!(
        graph
            .get_node(&graph_name("deps"), &node("late_a"))
            .expect("read succeeds")
            .is_none(),
        "the refused batch persists nothing"
    );

    // #3192: the ordering rule is symmetric across endpoints — an edge whose
    // src exists but whose DST is upserted later in the batch is refused with
    // the ordering hint too, not a bare missing endpoint. (Without this the dst
    // half of the check is never the deciding term and its logic goes untested.)
    let dst_upserted_later = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("present_src"),
            data: node_data(json!({"kind": "n"}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("present_src"),
            edge_type: edge_type("links"),
            dst: node("late_dst"),
            data: edge_data(1.0, json!({})),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("late_dst"),
            data: node_data(json!({"kind": "n"}), None),
        },
    ]);
    let dst_ordering = graph
        .batch_write(&graph_name("deps"), &dst_upserted_later)
        .expect_err("an edge before its dst endpoint is refused");
    assert_eq!(
        dst_ordering.code(),
        "invalid_argument.engine.graph_edge_endpoint"
    );
    assert!(
        dst_ordering.to_string().contains("same batch"),
        "a dst upserted later names the ordering rule (#3192): {dst_ordering}"
    );

    // #3192: conversely, a src that exists paired with a DST that is never
    // upserted (here or later) is a genuine missing endpoint, not an ordering
    // problem — the dst half must be a real conjunction, not "dst is absent".
    let dst_never_upserted = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("present_src2"),
            data: node_data(json!({"kind": "n"}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("present_src2"),
            edge_type: edge_type("bad"),
            dst: node("never_dst"),
            data: edge_data(1.0, json!({})),
        },
    ]);
    let dst_missing = graph
        .batch_write(&graph_name("deps"), &dst_never_upserted)
        .expect_err("an edge to a never-upserted dst fails");
    assert_eq!(
        dst_missing.code(),
        "invalid_argument.engine.graph_edge_endpoint"
    );
    assert!(
        !dst_missing.to_string().contains("same batch"),
        "a never-upserted dst is a plain missing endpoint, not an ordering error (#3192): {dst_missing}"
    );

    // #3192: symmetric on the src side — an edge whose dst exists but whose SRC
    // is upserted later names the ordering rule too, so the src half of the
    // check is the deciding term here (out_of_order has both endpoints late, so
    // either half alone could carry the verdict there).
    let src_upserted_later = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("present_dst"),
            data: node_data(json!({"kind": "n"}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("late_src"),
            edge_type: edge_type("links"),
            dst: node("present_dst"),
            data: edge_data(1.0, json!({})),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("late_src"),
            data: node_data(json!({"kind": "n"}), None),
        },
    ]);
    let src_ordering = graph
        .batch_write(&graph_name("deps"), &src_upserted_later)
        .expect_err("an edge before its src endpoint is refused");
    assert_eq!(
        src_ordering.code(),
        "invalid_argument.engine.graph_edge_endpoint"
    );
    assert!(
        src_ordering.to_string().contains("same batch"),
        "a src upserted later names the ordering rule (#3192): {src_ordering}"
    );

    // The same operations with nodes first succeed.
    let in_order = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("late_a"),
            data: node_data(json!({"kind": "n"}), None),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("late_b"),
            data: node_data(json!({"kind": "n"}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("late_a"),
            edge_type: edge_type("links"),
            dst: node("late_b"),
            data: edge_data(1.0, json!({})),
        },
    ]);
    graph
        .batch_write(&graph_name("deps"), &in_order)
        .expect("nodes before edges succeed in one batch");

    let deletes = GraphBatchWrite::new(vec![
        GraphBatchOperation::DeleteEdge {
            src: node("doc"),
            edge_type: edge_type("contains"),
            dst: node("chunk"),
        },
        GraphBatchOperation::DeleteNode {
            node_id: node("chunk"),
        },
    ]);
    let outcome = graph
        .batch_write(&graph_name("deps"), &deletes)
        .expect("delete batch succeeds");
    assert_eq!(outcome.results()[0].deleted_flag(), Some(true));
    assert_eq!(outcome.results()[1].deleted_flag(), Some(true));
    assert!(graph
        .get_node(&graph_name("deps"), &node("chunk"))
        .expect("chunk read succeeds")
        .is_none());
}

fn exercise_graph_temporal_reads(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");

    let old_binding = binding(GraphBindingPrimitive::Json, "docs", "doc-old");
    let node_create = graph
        .upsert_node(
            &graph_name("deps"),
            node("doc"),
            node_data(json!({"kind": "doc", "rev": 1}), Some(old_binding.clone())),
        )
        .expect("node create succeeds");
    graph
        .upsert_node(
            &graph_name("deps"),
            node("chunk"),
            node_data(json!({"kind": "chunk"}), None),
        )
        .expect("chunk create succeeds");
    let edge_create = graph
        .upsert_edge(
            &graph_name("deps"),
            node("doc"),
            edge_type("contains"),
            node("chunk"),
            edge_data(1.0, json!({"rank": 1})),
        )
        .expect("edge create succeeds");

    let new_binding = binding(GraphBindingPrimitive::Json, "docs", "doc-new");
    let node_update = graph
        .upsert_node(
            &graph_name("deps"),
            node("doc"),
            node_data(json!({"kind": "doc", "rev": 2}), Some(new_binding.clone())),
        )
        .expect("node update succeeds");
    let edge_update = graph
        .upsert_edge(
            &graph_name("deps"),
            node("doc"),
            edge_type("contains"),
            node("chunk"),
            edge_data(2.0, json!({"rank": 2})),
        )
        .expect("edge update succeeds");

    assert_eq!(
        graph
            .get_node_at_version(
                &graph_name("deps"),
                &node("doc"),
                node_create.commit().version(),
            )
            .expect("historical node by version succeeds")
            .expect("historical node exists")
            .data()
            .properties()
            .expect("properties")
            .as_inner(),
        &json!({"kind": "doc", "rev": 1})
    );
    assert_eq!(
        graph
            .get_node_at(
                &graph_name("deps"),
                &node("doc"),
                node_create.commit().timestamp(),
            )
            .expect("historical node by timestamp succeeds")
            .expect("historical node exists")
            .data()
            .properties()
            .expect("properties")
            .as_inner(),
        &json!({"kind": "doc", "rev": 1})
    );
    assert_eq!(
        graph
            .get_node(&graph_name("deps"), &node("doc"))
            .expect("latest node succeeds")
            .expect("latest node exists")
            .data()
            .properties()
            .expect("properties")
            .as_inner(),
        &json!({"kind": "doc", "rev": 2})
    );

    let historical_weight = graph
        .get_edge_at_version(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk"),
            edge_create.commit().version(),
        )
        .expect("historical edge succeeds")
        .expect("historical edge exists")
        .data()
        .weight();
    assert_float_eq(historical_weight, 1.0);
    let neighbor_weight = graph
        .neighbors_at_version(
            &graph_name("deps"),
            &node("doc"),
            GraphDirection::Outgoing,
            Some(&edge_type("contains")),
            None,
            10,
            edge_update.commit().version(),
        )
        .expect("historical neighbors succeed")
        .neighbors()[0]
        .edge()
        .data()
        .weight();
    assert_float_eq(neighbor_weight, 2.0);

    assert_eq!(
        graph
            .bindings_for_entity_at_version(
                old_binding.target(),
                None,
                10,
                node_create.commit().version(),
            )
            .expect("historical binding succeeds")
            .bindings()
            .len(),
        1
    );
    assert!(graph
        .bindings_for_entity(old_binding.target(), None, 10)
        .expect("latest old binding succeeds")
        .bindings()
        .is_empty());
    assert_eq!(
        graph
            .bindings_for_entity_at_version(
                new_binding.target(),
                None,
                10,
                node_update.commit().version(),
            )
            .expect("new binding succeeds")
            .bindings()
            .len(),
        1
    );

    let info = graph
        .graph_info_at_version(&graph_name("deps"), edge_update.commit().version())
        .expect("historical graph info succeeds")
        .expect("graph exists");
    assert_eq!(info.node_count(), 2);
    assert_eq!(info.edge_count(), 1);
    assert_eq!(
        graph
            .list_nodes_at_version(
                &graph_name("deps"),
                None,
                None,
                10,
                edge_update.commit().version()
            )
            .expect("historical node list succeeds")
            .nodes()
            .len(),
        2
    );

    graph
        .delete_node(&graph_name("deps"), &node("doc"))
        .expect("node delete succeeds");
    assert!(graph
        .get_node(&graph_name("deps"), &node("doc"))
        .expect("latest node succeeds")
        .is_none());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk")
        )
        .expect("latest edge succeeds")
        .is_none());
    assert!(graph
        .get_edge_at_version(
            &graph_name("deps"),
            &node("doc"),
            &edge_type("contains"),
            &node("chunk"),
            edge_update.commit().version(),
        )
        .expect("historical edge after delete succeeds")
        .is_some());
}

fn exercise_graph_lifecycle_pagination_and_recreate(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    assert!(!graph
        .delete_graph(&graph_name("missing"), true)
        .expect("missing graph delete succeeds")
        .deleted());

    for name in ["alpha", "beta", "gamma"] {
        graph
            .create_graph(graph_name(name))
            .expect("graph create succeeds");
    }

    let first_page = graph
        .list_graphs(None, 2)
        .expect("first graph page succeeds");
    assert_eq!(
        first_page
            .graphs()
            .iter()
            .map(GraphName::as_str)
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert!(first_page.has_more());
    let second_page = graph
        .list_graphs(first_page.cursor(), 2)
        .expect("second graph page succeeds");
    assert_eq!(
        second_page
            .graphs()
            .iter()
            .map(GraphName::as_str)
            .collect::<Vec<_>>(),
        vec!["gamma"]
    );
    assert!(!second_page.has_more());

    assert!(graph
        .delete_graph(&graph_name("alpha"), true)
        .expect("empty graph delete succeeds")
        .deleted());
    assert!(graph
        .graph_info(&graph_name("alpha"))
        .expect("deleted graph info succeeds")
        .is_none());

    let beta_binding = binding(GraphBindingPrimitive::Json, "docs", "beta-a");
    graph
        .upsert_node(
            &graph_name("beta"),
            node("a"),
            node_data(json!({"kind": "doc"}), Some(beta_binding.clone())),
        )
        .expect("node upsert succeeds");
    graph
        .upsert_edge(
            &graph_name("beta"),
            node("a"),
            edge_type("self"),
            node("a"),
            edge_data(1.0, json!({"loop": true})),
        )
        .expect("self edge upsert succeeds");
    assert!(graph
        .delete_graph(&graph_name("beta"), true)
        .expect("non-empty graph delete succeeds")
        .deleted());
    assert!(graph
        .graph_info(&graph_name("beta"))
        .expect("deleted graph info succeeds")
        .is_none());
    assert!(graph
        .bindings_for_entity(beta_binding.target(), None, 10)
        .expect("binding lookup succeeds")
        .bindings()
        .is_empty());

    let (recreated, _) = graph
        .create_graph(graph_name("beta"))
        .expect("graph recreate succeeds");
    assert_eq!(recreated.node_count(), 0);
    assert_eq!(recreated.edge_count(), 0);
    assert!(graph
        .list_nodes(&graph_name("beta"), None, None, 10)
        .expect("recreated graph node list succeeds")
        .nodes()
        .is_empty());
}

fn exercise_graph_dense_edges_self_loop_and_neighbor_pages(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");
    for node_id in ["a", "b", "c", "d"] {
        graph
            .upsert_node(&graph_name("deps"), node(node_id), GraphNodeData::default())
            .expect("node upsert succeeds");
    }

    for (src, edge, dst, weight) in [
        ("a", "follows", "b", 1.0),
        ("a", "mentions", "b", 2.0),
        ("a", "follows", "c", 3.0),
        ("d", "follows", "b", 4.0),
        ("a", "self", "a", 5.0),
    ] {
        graph
            .upsert_edge(
                &graph_name("deps"),
                node(src),
                edge_type(edge),
                node(dst),
                edge_data(weight, json!({"weight": weight})),
            )
            .expect("edge upsert succeeds");
    }

    for (src, edge, dst, weight) in [
        ("a", "follows", "b", 1.0),
        ("a", "mentions", "b", 2.0),
        ("a", "follows", "c", 3.0),
        ("d", "follows", "b", 4.0),
        ("a", "self", "a", 5.0),
    ] {
        let stored = graph
            .get_edge(
                &graph_name("deps"),
                &node(src),
                &edge_type(edge),
                &node(dst),
            )
            .expect("edge get succeeds")
            .expect("edge exists");
        assert_float_eq(stored.data().weight(), weight);
    }

    let first_outgoing = graph
        .neighbors(
            &graph_name("deps"),
            &node("a"),
            GraphDirection::Outgoing,
            None,
            None,
            2,
        )
        .expect("outgoing neighbor page succeeds");
    assert_eq!(first_outgoing.neighbors().len(), 2);
    assert!(first_outgoing.has_more());
    let second_outgoing = graph
        .neighbors(
            &graph_name("deps"),
            &node("a"),
            GraphDirection::Outgoing,
            None,
            first_outgoing.cursor(),
            10,
        )
        .expect("second outgoing neighbor page succeeds");
    assert_eq!(second_outgoing.neighbors().len(), 2);

    let incoming_b = graph
        .neighbors(
            &graph_name("deps"),
            &node("b"),
            GraphDirection::Incoming,
            None,
            None,
            10,
        )
        .expect("incoming neighbors succeed");
    assert_eq!(incoming_b.neighbors().len(), 3);

    let self_edge = edge_type("self");
    let self_hits = graph
        .neighbors(
            &graph_name("deps"),
            &node("a"),
            GraphDirection::Both,
            Some(&self_edge),
            None,
            10,
        )
        .expect("self-loop neighbors succeed");
    assert_eq!(self_hits.neighbors().len(), 2);
    assert!(self_hits
        .neighbors()
        .iter()
        .all(|neighbor| neighbor.node().node_id().as_str() == "a"));
    assert!(graph
        .neighbors(
            &graph_name("deps"),
            &node("missing"),
            GraphDirection::Both,
            None,
            None,
            10,
        )
        .expect("missing node neighbors succeed")
        .neighbors()
        .is_empty());
    assert!(!graph
        .delete_edge(
            &graph_name("deps"),
            &node("b"),
            &edge_type("missing"),
            &node("a"),
        )
        .expect("missing edge delete succeeds")
        .deleted());

    assert!(graph
        .delete_node(&graph_name("deps"), &node("a"))
        .expect("delete node succeeds")
        .deleted());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("a"),
            &edge_type("follows"),
            &node("b")
        )
        .expect("deleted outgoing edge read succeeds")
        .is_none());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("a"),
            &edge_type("self"),
            &node("a")
        )
        .expect("deleted self edge read succeeds")
        .is_none());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("d"),
            &edge_type("follows"),
            &node("b")
        )
        .expect("surviving edge read succeeds")
        .is_some());
    assert_eq!(
        graph
            .neighbors(
                &graph_name("deps"),
                &node("b"),
                GraphDirection::Incoming,
                None,
                None,
                10,
            )
            .expect("remaining incoming neighbors succeed")
            .neighbors()
            .len(),
        1
    );
}

fn exercise_graph_binding_lookup_pages_and_isolation(mut database: Database) {
    let shared = binding(GraphBindingPrimitive::Json, "docs", "shared");
    {
        let mut graph = graph_service(&mut database, "default", "default");
        for name in ["deps", "refs"] {
            graph
                .create_graph(graph_name(name))
                .expect("graph create succeeds");
            graph
                .upsert_node(
                    &graph_name(name),
                    node("doc"),
                    node_data(json!({"graph": name}), Some(shared.clone())),
                )
                .expect("bound node upsert succeeds");
        }
        let first_page = graph
            .bindings_for_entity(shared.target(), None, 1)
            .expect("first binding page succeeds");
        assert_eq!(first_page.bindings().len(), 1);
        assert!(first_page.has_more());
        let second_page = graph
            .bindings_for_entity(shared.target(), first_page.cursor(), 10)
            .expect("second binding page succeeds");
        assert_eq!(second_page.bindings().len(), 1);
    }

    {
        let mut other = graph_service(&mut database, "default", "other");
        other
            .create_graph(graph_name("deps"))
            .expect("other-space graph create succeeds");
        other
            .upsert_node(
                &graph_name("deps"),
                node("doc"),
                node_data(json!({"space": "other"}), Some(shared.clone())),
            )
            .expect("other-space bound node upsert succeeds");
        assert_eq!(
            other
                .bindings_for_entity(shared.target(), None, 10)
                .expect("other-space binding lookup succeeds")
                .bindings()
                .len(),
            1
        );
    }

    {
        let mut graph = graph_service(&mut database, "default", "default");
        assert_eq!(
            graph
                .bindings_for_entity(shared.target(), None, 10)
                .expect("default binding lookup succeeds")
                .bindings()
                .len(),
            2
        );
        let updated = binding(GraphBindingPrimitive::Json, "docs", "updated");
        graph
            .upsert_node(
                &graph_name("deps"),
                node("doc"),
                node_data(
                    json!({"graph": "deps", "updated": true}),
                    Some(updated.clone()),
                ),
            )
            .expect("binding replacement succeeds");
        assert_eq!(
            graph
                .bindings_for_entity(shared.target(), None, 10)
                .expect("old binding lookup succeeds")
                .bindings()
                .len(),
            1
        );
        assert_eq!(
            graph
                .bindings_for_entity(updated.target(), None, 10)
                .expect("new binding lookup succeeds")
                .bindings()
                .len(),
            1
        );
    }
}

fn exercise_graph_batch_ordering_and_failure_regressions(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");
    for node_id in ["a", "b"] {
        graph
            .upsert_node(
                &graph_name("deps"),
                node(node_id),
                node_data(json!({"node": node_id}), None),
            )
            .expect("node upsert succeeds");
    }
    graph
        .upsert_edge(
            &graph_name("deps"),
            node("a"),
            edge_type("links"),
            node("b"),
            edge_data(1.0, json!({"rev": 1})),
        )
        .expect("edge upsert succeeds");

    let ordered = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("a"),
            data: node_data(json!({"rev": 1}), None),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("a"),
            data: node_data(json!({"rev": 2}), None),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("a"),
            edge_type: edge_type("links"),
            dst: node("b"),
            data: edge_data(2.0, json!({"rev": 2})),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("a"),
            edge_type: edge_type("links"),
            dst: node("b"),
            data: edge_data(3.0, json!({"rev": 3})),
        },
        GraphBatchOperation::DeleteEdge {
            src: node("b"),
            edge_type: edge_type("missing"),
            dst: node("a"),
        },
    ]);
    let outcome = graph
        .batch_write(&graph_name("deps"), &ordered)
        .expect("ordered batch succeeds");
    assert_eq!(outcome.results().len(), 5);
    assert_eq!(outcome.results()[0].created_flag(), Some(false));
    assert_eq!(outcome.results()[1].created_flag(), Some(false));
    assert_eq!(outcome.results()[2].created_flag(), Some(false));
    assert_eq!(outcome.results()[3].created_flag(), Some(false));
    assert_eq!(outcome.results()[4].deleted_flag(), Some(false));
    assert_eq!(
        graph
            .get_node(&graph_name("deps"), &node("a"))
            .expect("node read succeeds")
            .expect("node exists")
            .data()
            .properties()
            .expect("properties")
            .as_inner(),
        &json!({"rev": 2})
    );
    let edge_weight = graph
        .get_edge(
            &graph_name("deps"),
            &node("a"),
            &edge_type("links"),
            &node("b"),
        )
        .expect("edge read succeeds")
        .expect("edge exists")
        .data()
        .weight();
    assert_float_eq(edge_weight, 3.0);

    let invalid = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("planned-a"),
            data: GraphNodeData::default(),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node("planned-b"),
            data: GraphNodeData::default(),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("planned-a"),
            edge_type: edge_type("links"),
            dst: node("planned-b"),
            data: GraphEdgeData::default(),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("planned-a"),
            edge_type: edge_type("bad"),
            dst: node("missing"),
            data: GraphEdgeData::default(),
        },
    ]);
    assert_eq!(
        graph
            .batch_write(&graph_name("deps"), &invalid)
            .expect_err("invalid batch rejected")
            .class(),
        EngineErrorClass::InvalidInput
    );
    assert!(graph
        .get_node(&graph_name("deps"), &node("planned-a"))
        .expect("planned-a read succeeds")
        .is_none());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("planned-a"),
            &edge_type("links"),
            &node("planned-b"),
        )
        .expect("planned edge read succeeds")
        .is_none());

    let delete_incident = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("c"),
            data: GraphNodeData::default(),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("b"),
            edge_type: edge_type("links"),
            dst: node("c"),
            data: GraphEdgeData::default(),
        },
        GraphBatchOperation::DeleteNode { node_id: node("b") },
    ]);
    let outcome = graph
        .batch_write(&graph_name("deps"), &delete_incident)
        .expect("incident delete batch succeeds");
    assert_eq!(outcome.results()[2].deleted_flag(), Some(true));
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("a"),
            &edge_type("links"),
            &node("b")
        )
        .expect("pre-existing incident edge read succeeds")
        .is_none());
    assert!(graph
        .get_edge(
            &graph_name("deps"),
            &node("b"),
            &edge_type("links"),
            &node("c")
        )
        .expect("batch-created incident edge read succeeds")
        .is_none());
}

fn outgoing_dsts(
    graph: &mut GraphService<'_>,
    name: &GraphName,
    source: &GraphNodeId,
    version: CommitVersion,
) -> Vec<String> {
    let mut ids: Vec<String> = graph
        .neighbors_at_version(
            name,
            source,
            GraphDirection::Outgoing,
            None,
            None,
            16,
            version,
        )
        .expect("historical neighbor read must not corrupt")
        .neighbors()
        .iter()
        .map(|neighbor| neighbor.node().node_id().as_str().to_owned())
        .collect();
    ids.sort();
    ids
}

/// Historical neighbor and edge reads never surface a dangling endpoint or a
/// spurious corruption: an edge is visible exactly when both of its endpoints
/// are, and the delete cascade keeps that invariant across every version.
// Short node names (a/b/c) and their per-edge version markers are intentionally terse.
#[allow(clippy::similar_names)]
#[test]
fn graph_historical_edge_reads_never_dangle_or_corrupt() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut graph = database
        .graph(branch("default"), space("default"))
        .expect("graph service opens");
    let name = graph_name("deps");
    let rel = edge_type("rel");
    graph.create_graph(name.clone()).expect("create graph");

    graph
        .upsert_node(&name, node("a"), node_data(json!({}), None))
        .expect("node a");
    let b_create = graph
        .upsert_node(&name, node("b"), node_data(json!({}), None))
        .expect("node b");
    let v_no_edges = b_create.commit().version();

    let ab = graph
        .upsert_edge(
            &name,
            node("a"),
            rel.clone(),
            node("b"),
            edge_data(1.0, json!({})),
        )
        .expect("edge a->b");
    let ab_commit = ab.commit();
    let v_ab = ab_commit.version();
    let ts_ab = ab_commit.timestamp();

    graph
        .upsert_node(&name, node("c"), node_data(json!({}), None))
        .expect("node c");
    let ac = graph
        .upsert_edge(
            &name,
            node("a"),
            rel.clone(),
            node("c"),
            edge_data(1.0, json!({})),
        )
        .expect("edge a->c");
    let v_ac = ac.commit().version();

    let delete = graph
        .delete_node(&name, &node("b"))
        .expect("delete b cascades the a->b edge");
    let v_del = delete.commit().expect("delete commits").version();

    // Outgoing neighbors of `a` track exactly the edges whose endpoints are both
    // visible at each version; the deleted endpoint never dangles.
    assert!(outgoing_dsts(&mut graph, &name, &node("a"), v_no_edges).is_empty());
    assert_eq!(
        outgoing_dsts(&mut graph, &name, &node("a"), v_ab),
        vec!["b".to_owned()]
    );
    assert_eq!(
        outgoing_dsts(&mut graph, &name, &node("a"), v_ac),
        vec!["b".to_owned(), "c".to_owned()]
    );
    assert_eq!(
        outgoing_dsts(&mut graph, &name, &node("a"), v_del),
        vec!["c".to_owned()]
    );

    // A historical edge read resolves cleanly across the endpoint's lifetime.
    assert!(graph
        .get_edge_at_version(&name, &node("a"), &rel, &node("b"), v_no_edges)
        .expect("edge before creation")
        .is_none());
    assert!(graph
        .get_edge_at_version(&name, &node("a"), &rel, &node("b"), v_ab)
        .expect("edge present")
        .is_some());
    assert!(graph
        .get_edge_at_version(&name, &node("a"), &rel, &node("b"), v_del)
        .expect("edge after cascade")
        .is_none());

    // The deleted endpoint is visible before the delete and invisible at it.
    assert!(graph
        .get_node_at_version(&name, &node("b"), v_ab)
        .expect("node b before")
        .is_some());
    assert!(graph
        .get_node_at_version(&name, &node("b"), v_del)
        .expect("node b after")
        .is_none());

    // No version across the whole history raises a dangling-endpoint
    // corruption. A version before the graph existed legitimately returns
    // not-found; what must never happen is a corruption-class error.
    for raw in 1..=v_del.as_u64() {
        if let Err(error) = graph.neighbors_at_version(
            &name,
            &node("a"),
            GraphDirection::Outgoing,
            None,
            None,
            16,
            CommitVersion::new(raw),
        ) {
            assert_ne!(
                error.class(),
                EngineErrorClass::Corruption,
                "neighbors at version {raw} corrupted: {}",
                error.code()
            );
        }
    }

    // A timestamp-based historical read agrees with the version-based one.
    let mut at_ts: Vec<String> = graph
        .neighbors_at(
            &name,
            &node("a"),
            GraphDirection::Outgoing,
            None,
            None,
            16,
            ts_ab,
        )
        .expect("timestamp neighbor read")
        .neighbors()
        .iter()
        .map(|neighbor| neighbor.node().node_id().as_str().to_owned())
        .collect();
    at_ts.sort();
    assert_eq!(at_ts, vec!["b".to_owned()]);
}

/// Conformance test 9 (entity-ref-and-relationship-layer-contract Branch Scope
/// rule 4 / Binding Decision 6) and CLAUDE.md Hard Rule 18: a relationship
/// binding whose target names a different branch must be rejected, on both the
/// single-node and batch write paths, while same-branch and current-branch
/// (`None`) targets are accepted.
fn exercise_graph_cross_branch_binding_rejection(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");

    let cross_branch = GraphEntityBinding::new(
        GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            Some(BranchName::new("other").expect("valid branch name")),
            space("docs"),
            "doc-1",
        )
        .expect("valid binding target"),
    );

    // Single-node path rejects with the structured unsupported code.
    let error = graph
        .upsert_node(
            &graph_name("deps"),
            node("cross"),
            node_data(json!({"kind": "doc"}), Some(cross_branch.clone())),
        )
        .expect_err("cross-branch binding rejected");
    assert_eq!(
        error.code(),
        "unsupported.engine.graph_binding_cross_branch"
    );

    // Batch path rejects the same way (whole batch fails atomically).
    let batch_error = graph
        .batch_write(
            &graph_name("deps"),
            &GraphBatchWrite::new(vec![GraphBatchOperation::UpsertNode {
                node_id: node("cross-batch"),
                data: node_data(json!({"kind": "doc"}), Some(cross_branch)),
            }]),
        )
        .expect_err("cross-branch binding rejected in batch");
    assert_eq!(
        batch_error.code(),
        "unsupported.engine.graph_binding_cross_branch"
    );

    // An explicit same-branch target and an implicit (None) target are accepted.
    let same_branch = GraphEntityBinding::new(
        GraphBindingTarget::new(
            GraphBindingPrimitive::Json,
            Some(BranchName::new("default").expect("valid branch name")),
            space("docs"),
            "doc-1",
        )
        .expect("valid binding target"),
    );
    graph
        .upsert_node(
            &graph_name("deps"),
            node("same"),
            node_data(json!({"kind": "doc"}), Some(same_branch)),
        )
        .expect("same-branch binding accepted");
    graph
        .upsert_node(
            &graph_name("deps"),
            node("implicit"),
            node_data(
                json!({"kind": "doc"}),
                Some(binding(GraphBindingPrimitive::Json, "docs", "doc-2")),
            ),
        )
        .expect("current-branch (None) binding accepted");
}

/// U28: user-facing commit counts must reflect authored rows only. One edge
/// upsert emits a forward + reverse row, and a node re-bind emits a binding
/// index row; those derived rows must not inflate put/delete counts.
fn exercise_graph_commit_counts_exclude_derived_rows(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    graph
        .create_graph(graph_name("deps"))
        .expect("graph create succeeds");
    for id in ["a", "b"] {
        graph
            .upsert_node(&graph_name("deps"), node(id), node_data(json!({}), None))
            .expect("node upsert succeeds");
    }

    // One edge writes a forward row + a derived reverse row; count is 1.
    let edge = graph
        .upsert_edge(
            &graph_name("deps"),
            node("a"),
            edge_type("depends_on"),
            node("b"),
            edge_data(1.0, json!({})),
        )
        .expect("edge upsert succeeds");
    assert_eq!(edge.commit().put_count(), 1);

    // A node with a binding writes a node row + a derived binding-index row;
    // count is 1.
    let bound = graph
        .upsert_node(
            &graph_name("deps"),
            node("c"),
            node_data(
                json!({}),
                Some(binding(GraphBindingPrimitive::Json, "docs", "doc-1")),
            ),
        )
        .expect("bound node upsert succeeds");
    assert_eq!(bound.commit().put_count(), 1);

    // Removing the edge deletes forward + derived reverse rows; count is 1.
    let removed = graph
        .delete_edge(
            &graph_name("deps"),
            &node("a"),
            &edge_type("depends_on"),
            &node("b"),
        )
        .expect("edge delete succeeds");
    let commit = removed.commit().expect("delete commits");
    assert_eq!(commit.delete_count(), 1);
}

/// #3459: graph read methods take `&self`, so ONE `GraphService` handle serves
/// several readers at once — a snapshot from one read stays live while another
/// read runs on the SAME shared `&service`. Under the old `&mut self` receivers
/// this could not compile; `reads_through_shared_ref` is the compile-time guard
/// (it can only reach the reads through `&GraphService`), and holding `index`
/// across the second read exercises the concurrent-reader guarantee at runtime.
fn exercise_graph_reads_take_shared_ref(mut database: Database) {
    {
        let mut graph = graph_service(&mut database, "default", "default");
        graph
            .create_graph(graph_name("roads"))
            .expect("graph create succeeds");
        for id in ["a", "b", "c"] {
            graph
                .upsert_node(&graph_name("roads"), node(id), node_data(json!({}), None))
                .expect("node upsert succeeds");
        }
        graph
            .upsert_edge(
                &graph_name("roads"),
                node("a"),
                edge_type("to"),
                node("b"),
                edge_data(1.0, json!({})),
            )
            .expect("edge a->b upsert succeeds");
        graph
            .upsert_edge(
                &graph_name("roads"),
                node("a"),
                edge_type("to"),
                node("c"),
                edge_data(1.0, json!({})),
            )
            .expect("edge a->c upsert succeeds");
    }

    // A single, NON-mut handle: every call below is `&self`.
    let service = graph_service(&mut database, "default", "default");
    let index = service
        .adjacency_index(&graph_name("roads"), &GraphAnalyticsBudget::default())
        .expect("adjacency index builds from a shared handle");
    // Second read while `index` is still alive — two immutable borrows of the
    // same `service` coexist, which `&mut self` reads would forbid.
    let out_neighbors = reads_through_shared_ref(&service, &graph_name("roads"));
    assert_eq!(out_neighbors, 2, "a has two out-neighbors (b and c)");
    // `index` remains valid here, held across the second read.
    assert_eq!(
        index.node_count(),
        3,
        "the snapshot survived the second read"
    );
}

/// Compile-time guard for #3459: each read below is reached through a shared
/// `&GraphService`. If any reverts to `&mut self`, this stops compiling.
fn reads_through_shared_ref(service: &GraphService<'_>, name: &GraphName) -> usize {
    service
        .graph_info(name)
        .expect("graph_info reads via &self");
    service
        .list_nodes(name, None, None, 10)
        .expect("list_nodes reads via &self");
    service
        .neighbors(name, &node("a"), GraphDirection::Outgoing, None, None, 10)
        .expect("neighbors reads via &self")
        .neighbors()
        .len()
}

/// #3457: `list_edges` pages a graph's edges WITH their full data (weight plus
/// properties like street names/lengths), ordered by `(src, type, dst)`, so a
/// caller that imported labeled edges reads them back without per-node
/// `neighbors` round-trips or re-joining labels from an external fixture.
fn exercise_graph_list_edges(mut database: Database) {
    let (version, timestamp) = {
        let mut graph = graph_service(&mut database, "default", "default");
        graph
            .create_graph(graph_name("streets"))
            .expect("graph create succeeds");
        for id in ["a", "b", "c"] {
            graph
                .upsert_node(
                    &graph_name("streets"),
                    node(id),
                    node_data(json!({"x": 1}), None),
                )
                .expect("node upsert succeeds");
        }
        graph
            .upsert_edge(
                &graph_name("streets"),
                node("a"),
                edge_type("road"),
                node("b"),
                edge_data(120.0, json!({"name": "Main St"})),
            )
            .expect("edge a->b upsert succeeds");
        graph
            .upsert_edge(
                &graph_name("streets"),
                node("a"),
                edge_type("road"),
                node("c"),
                edge_data(80.0, json!({"name": "Oak Ave"})),
            )
            .expect("edge a->c upsert succeeds");
        let last = graph
            .upsert_edge(
                &graph_name("streets"),
                node("b"),
                edge_type("road"),
                node("c"),
                edge_data(50.0, json!({"name": "2nd St"})),
            )
            .expect("edge b->c upsert succeeds");
        (last.commit().version(), last.commit().timestamp())
    };

    let service = graph_service(&mut database, "default", "default");

    // Full page: every edge, ordered by (src, type, dst), with data attached.
    let all = service
        .list_edges(&graph_name("streets"), None, 10)
        .expect("list_edges succeeds");
    assert_eq!(all.edges().len(), 3);
    assert!(!all.has_more());
    assert!(all.cursor().is_none());
    let identity: Vec<(&str, &str)> = all
        .edges()
        .iter()
        .map(|edge| (edge.src().as_str(), edge.dst().as_str()))
        .collect();
    assert_eq!(identity, vec![("a", "b"), ("a", "c"), ("b", "c")]);
    // #3457's point: the imported property survives on the listed edge.
    let first = &all.edges()[0];
    assert!((first.data().weight() - 120.0).abs() < f64::EPSILON);
    assert_eq!(
        first
            .data()
            .properties()
            .expect("edge properties")
            .as_inner(),
        &json!({"name": "Main St"})
    );

    // A page holding exactly `limit` edges is the LAST page, not a spuriously
    // "more" one (guards the has_more boundary).
    let exact = service
        .list_edges(&graph_name("streets"), None, 3)
        .expect("exact-limit page");
    assert_eq!(exact.edges().len(), 3);
    assert!(!exact.has_more());
    assert!(exact.cursor().is_none());

    // Cursor pagination: a page of two, then resume with no overlap.
    let page1 = service
        .list_edges(&graph_name("streets"), None, 2)
        .expect("first page");
    assert_eq!(page1.edges().len(), 2);
    assert!(page1.has_more());
    assert_eq!(page1.edges().last().expect("edge").src().as_str(), "a");
    let cursor = page1.cursor().expect("cursor").to_owned();
    let page2 = service
        .list_edges(&graph_name("streets"), Some(&cursor), 2)
        .expect("second page");
    assert_eq!(page2.edges().len(), 1);
    assert!(!page2.has_more());
    assert_eq!(page2.edges()[0].src().as_str(), "b");
    assert_eq!(page2.edges()[0].dst().as_str(), "c");

    // limit 0 -> an empty page, no cursor.
    let empty = service
        .list_edges(&graph_name("streets"), None, 0)
        .expect("limit-0 page");
    assert!(empty.edges().is_empty());
    assert!(!empty.has_more());
    assert!(empty.cursor().is_none());

    // Time-travel variants read the edges visible at the commit that wrote them.
    let at_version = service
        .list_edges_at_version(&graph_name("streets"), None, 10, version)
        .expect("list_edges_at_version succeeds");
    assert_eq!(at_version.edges().len(), 3);
    let at_time = service
        .list_edges_at(&graph_name("streets"), None, 10, timestamp)
        .expect("list_edges_at succeeds");
    assert_eq!(at_time.edges().len(), 3);

    // A missing graph is a not-found error, not a silent empty page.
    let missing = service
        .list_edges(&graph_name("absent"), None, 10)
        .expect_err("missing graph is rejected");
    assert_eq!(missing.class(), EngineErrorClass::NotFound);
    assert_eq!(missing.code(), "not_found.engine.graph");
}

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn graph_service<'a>(
    database: &'a mut Database,
    branch_name: &str,
    space_name: &str,
) -> strata_engine::GraphService<'a> {
    database
        .graph(branch(branch_name), space(space_name))
        .expect("graph service opens")
}

fn graph_name(value: &str) -> GraphName {
    GraphName::new(value).expect("valid graph")
}

fn node(value: &str) -> GraphNodeId {
    GraphNodeId::new(value).expect("valid node")
}

fn edge_type(value: &str) -> GraphEdgeType {
    GraphEdgeType::new(value).expect("valid edge type")
}

fn props(value: serde_json::Value) -> GraphProperties {
    GraphProperties::new(value).expect("valid properties")
}

fn node_data(value: serde_json::Value, binding: Option<GraphEntityBinding>) -> GraphNodeData {
    GraphNodeData::new(Some(props(value)), binding)
}

fn edge_data(weight: f64, value: serde_json::Value) -> GraphEdgeData {
    GraphEdgeData::new(weight, Some(props(value))).expect("valid edge data")
}

fn target(primitive: GraphBindingPrimitive, space_name: &str, key: &str) -> GraphBindingTarget {
    GraphBindingTarget::new(primitive, None, space(space_name), key).expect("valid binding target")
}

fn binding(primitive: GraphBindingPrimitive, space_name: &str, key: &str) -> GraphEntityBinding {
    GraphEntityBinding::new(target(primitive, space_name, key))
}

fn assert_float_eq(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < f64::EPSILON,
        "expected {expected}, got {actual}"
    );
}
