//! Executor graph command behavior tests.

#![allow(clippy::result_large_err, clippy::too_many_lines)]

use serde_json::json;
use strata_engine::{CacheOpenOptions, Database, DurableLocalOpenOptions};
use strata_executor::{
    Command, CommitReceipt, Executor, ExecutorError, ExecutorErrorClass, GraphBatchItemResult,
    GraphBatchOperation, GraphBindingHit, GraphBindingPrimitive, GraphBindingTarget,
    GraphDirection, GraphEdgeData, GraphEntityBinding, GraphNeighborHit, GraphNodeData,
    MutationEffect, MutationEffectKind, Output, DEFAULT_BRANCH,
};
use tempfile::TempDir;

#[test]
fn graph_batch_operation_rejects_unknown_keys_instead_of_dropping_data() {
    // A misspelled or extraneous key on a batch operation must be rejected, not
    // silently dropped: dropping it applies the operation without the intended
    // payload (or with a default) and returns no error, so the caller believes
    // a field took effect that never did.
    let error = serde_json::from_str::<Command>(
        r#"{"type":"graph_batch_write","graph":"g","operations":[{"type":"delete_node","node_id":"a","cascade":true}]}"#,
    )
    .expect_err("an unknown operation key is rejected");
    assert!(
        error.to_string().contains("cascade"),
        "the rejection names the unknown key: {error}"
    );

    // A misspelled payload key on an upsert is rejected too.
    let upsert_error = serde_json::from_str::<Command>(
        r#"{"type":"graph_batch_write","graph":"g","operations":[{"type":"upsert_node","node_id":"a","dat":{}}]}"#,
    )
    .expect_err("an unknown upsert key is rejected");
    assert!(
        upsert_error.to_string().contains("dat"),
        "the rejection names the unknown key: {upsert_error}"
    );

    // Correctly-shaped operations still deserialize.
    serde_json::from_str::<Command>(
        r#"{"type":"graph_batch_write","graph":"g","operations":[{"type":"delete_node","node_id":"a"}]}"#,
    )
    .expect("a valid batch operation deserializes");
}

#[test]
fn graph_core_command_suite_runs_in_cache_and_durable_modes() {
    run_graph_modes(run_graph_core_command_suite);
}

#[test]
fn durable_executor_reopens_graph_core_rows() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");

    {
        let mut executor = Executor::open_durable_local(&path).expect("durable executor opens");
        create_graph(&mut executor, "deps");
        add_node(
            &mut executor,
            "deps",
            "node-a",
            json!({"kind": "root"}),
            Some(binding("doc-a")),
        );
        add_node(
            &mut executor,
            "deps",
            "node-b",
            json!({"kind": "child"}),
            None,
        );
        add_edge(
            &mut executor,
            "deps",
            "node-a",
            "depends_on",
            "node-b",
            Some(2.5),
        );
        executor.close().expect("durable executor closes");
    }

    let mut reopened = Executor::open_durable_local(&path).expect("durable executor reopens");
    let meta = get_meta(&mut reopened, "deps").expect("graph metadata persists");
    assert_eq!(meta.node_count(), 2);
    assert_eq!(meta.edge_count(), 1);
    let node = get_node(&mut reopened, "deps", "node-a").expect("node persists");
    assert_eq!(node.binding().expect("binding").target().key(), "doc-a");
    let edge =
        get_edge(&mut reopened, "deps", "node-a", "depends_on", "node-b").expect("edge persists");
    assert_float_eq(edge.weight(), 2.5);
    assert_eq!(
        neighbor_nodes(
            &mut reopened,
            "deps",
            "node-a",
            GraphDirection::Outgoing,
            Some("depends_on".to_owned()),
        ),
        vec!["node-b".to_owned()]
    );
    assert_eq!(
        binding_nodes(&mut reopened, target("doc-a")),
        vec!["node-a".to_owned()]
    );
    assert!(delete_graph(&mut reopened, "deps"));
    reopened.close().expect("reopened executor closes");
}

#[test]
fn graph_lifecycle_and_pagination_run_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_lifecycle_and_pagination);
}

#[test]
fn graph_node_crud_and_list_edges_run_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_node_crud_and_list_edges);
}

#[test]
fn graph_edge_neighbor_and_self_loop_edges_run_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_edge_neighbor_and_self_loop_edges);
}

#[test]
fn graph_binding_lookup_update_and_delete_run_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_binding_lookup_update_and_delete);
}

#[test]
fn graph_batch_success_atomicity_and_delete_edges_run_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_batch_success_atomicity_and_delete_edges);
}

#[test]
fn graph_error_mapping_runs_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_error_mapping);
}

#[test]
fn graph_executor_inherits_configured_database_default_branch_in_cache_and_durable_modes() {
    let options = CacheOpenOptions::new()
        .with_default_branch("main")
        .expect("valid branch");
    let database = Database::open_cache(options)
        .expect("cache database opens")
        .into_database();
    let mut executor = Executor::from_database(database);
    assert_graph_executor_inherits_configured_database_default_branch(&mut executor);

    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");
    let options = DurableLocalOpenOptions::new()
        .with_default_branch("main")
        .expect("valid branch");
    let database = Database::open_local(&path, options)
        .expect("durable database opens")
        .into_database();
    let mut executor = Executor::from_database(database);
    assert_graph_executor_inherits_configured_database_default_branch(&mut executor);
    executor.close().expect("durable executor closes");
}

fn assert_graph_executor_inherits_configured_database_default_branch(executor: &mut Executor) {
    assert_eq!(executor.default_branch(), "main");
    create_graph(executor, "deps");
    add_node(executor, "deps", "node-a", json!({"branch": "main"}), None);
    assert!(get_node(executor, "deps", "node-a").is_some());

    let error = executor
        .execute(Command::GraphGetNode {
            branch: Some(DEFAULT_BRANCH.to_owned()),
            space: None,
            graph: "deps".to_owned(),
            node_id: "node-a".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("literal default branch is absent");
    assert_eq!(error.class(), strata_executor::ExecutorErrorClass::NotFound);
}

#[test]
fn graph_branch_and_space_are_isolated_in_cache_and_durable_modes() {
    run_graph_modes(assert_graph_branch_and_space_are_isolated);
}

fn assert_graph_branch_and_space_are_isolated(executor: &mut Executor) {
    create_graph(executor, "deps");
    add_node(
        executor,
        "deps",
        "source-node",
        json!({"branch": "source"}),
        None,
    );
    add_node(
        executor,
        "deps",
        "source-peer",
        json!({"branch": "source"}),
        None,
    );
    graph_add_edge_in(
        executor,
        None,
        None,
        "deps",
        "source-node",
        "source_link",
        "source-peer",
        Some(json!({"branch": "source"})),
    );
    executor
        .execute(Command::BranchForkCurrent {
            source: DEFAULT_BRANCH.to_owned(),
            branch: "feature".to_owned(),
        })
        .expect("branch fork succeeds");
    graph_add_node_in(
        executor,
        Some("feature"),
        None,
        "deps",
        "feature-node",
        json!({"branch": "feature"}),
    );
    assert!(graph_get_node_in(executor, None, None, "deps", "feature-node").is_none());
    assert!(graph_get_node_in(executor, Some("feature"), None, "deps", "feature-node").is_some());
    assert!(graph_get_edge_in(
        executor,
        Some("feature"),
        None,
        "deps",
        "source-node",
        "source_link",
        "source-peer",
    )
    .is_some());

    graph_add_edge_in(
        executor,
        Some("feature"),
        None,
        "deps",
        "feature-node",
        "feature_link",
        "source-peer",
        Some(json!({"branch": "feature"})),
    );
    assert!(graph_get_edge_in(
        executor,
        None,
        None,
        "deps",
        "feature-node",
        "feature_link",
        "source-peer",
    )
    .is_none());
    assert!(neighbor_node_ids_in(
        executor,
        None,
        None,
        "deps",
        "feature-node",
        GraphDirection::Outgoing,
        Some("feature_link"),
    )
    .is_empty());

    graph_remove_node_in(executor, Some("feature"), None, "deps", "source-node");
    assert!(graph_get_node_in(executor, None, None, "deps", "source-node").is_some());
    assert!(graph_get_node_in(executor, Some("feature"), None, "deps", "source-node").is_none());
    assert!(graph_get_edge_in(
        executor,
        None,
        None,
        "deps",
        "source-node",
        "source_link",
        "source-peer",
    )
    .is_some());
    assert!(graph_get_edge_in(
        executor,
        Some("feature"),
        None,
        "deps",
        "source-node",
        "source_link",
        "source-peer",
    )
    .is_none());

    create_graph(executor, "spaces");
    graph_create_in(executor, None, Some("tenant-b"), "spaces");
    graph_add_node_in(
        executor,
        None,
        None,
        "spaces",
        "default-node",
        json!({"space": "default"}),
    );
    graph_add_node_in(
        executor,
        None,
        Some("tenant-b"),
        "spaces",
        "tenant-node",
        json!({"space": "tenant-b"}),
    );
    graph_add_node_in(
        executor,
        None,
        None,
        "spaces",
        "shared-a",
        json!({"space": "default"}),
    );
    graph_add_node_in(
        executor,
        None,
        None,
        "spaces",
        "shared-b",
        json!({"space": "default"}),
    );
    graph_add_node_in(
        executor,
        None,
        Some("tenant-b"),
        "spaces",
        "shared-a",
        json!({"space": "tenant-b"}),
    );
    graph_add_node_in(
        executor,
        None,
        Some("tenant-b"),
        "spaces",
        "shared-b",
        json!({"space": "tenant-b"}),
    );
    graph_add_edge_in(
        executor,
        None,
        None,
        "spaces",
        "shared-a",
        "same",
        "shared-b",
        Some(json!({"space": "default"})),
    );
    graph_add_edge_in(
        executor,
        None,
        Some("tenant-b"),
        "spaces",
        "shared-a",
        "same",
        "shared-b",
        Some(json!({"space": "tenant-b"})),
    );
    graph_add_node_with_binding_in(
        executor,
        None,
        None,
        "spaces",
        "default-bound",
        json!({"space": "default"}),
        binding("shared-doc"),
    );
    graph_add_node_with_binding_in(
        executor,
        None,
        Some("tenant-b"),
        "spaces",
        "tenant-bound",
        json!({"space": "tenant-b"}),
        binding("shared-doc"),
    );
    assert_eq!(
        graph_node_ids_in(executor, None, None, "spaces"),
        vec![
            "default-bound".to_owned(),
            "default-node".to_owned(),
            "shared-a".to_owned(),
            "shared-b".to_owned(),
        ]
    );
    assert_eq!(
        graph_node_ids_in(executor, None, Some("tenant-b"), "spaces"),
        vec![
            "shared-a".to_owned(),
            "shared-b".to_owned(),
            "tenant-bound".to_owned(),
            "tenant-node".to_owned(),
        ]
    );
    assert_eq!(
        graph_get_edge_in(executor, None, None, "spaces", "shared-a", "same", "shared-b",)
            .expect("default edge exists")
            .properties(),
        Some(&json!({"space": "default"}))
    );
    assert_eq!(
        graph_get_edge_in(
            executor,
            None,
            Some("tenant-b"),
            "spaces",
            "shared-a",
            "same",
            "shared-b",
        )
        .expect("tenant edge exists")
        .properties(),
        Some(&json!({"space": "tenant-b"}))
    );
    assert_eq!(
        binding_nodes_in(executor, None, None, target("shared-doc")),
        vec!["default-bound".to_owned()]
    );
    assert_eq!(
        binding_nodes_in(executor, None, Some("tenant-b"), target("shared-doc")),
        vec!["tenant-bound".to_owned()]
    );
}

fn run_graph_modes(mut exercise: impl FnMut(&mut Executor)) {
    let mut cache = Executor::open_cache().expect("cache executor opens");
    exercise(&mut cache);

    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");
    let mut durable = Executor::open_durable_local(&path).expect("durable executor opens");
    exercise(&mut durable);
}

fn assert_graph_lifecycle_and_pagination(executor: &mut Executor) {
    let empty_page = graph_name_page(executor, None, Some(10));
    assert!(empty_page.items.is_empty());
    assert!(!empty_page.has_more);
    assert!(empty_page.cursor.is_none());

    for graph in ["graph-a", "graph-b", "graph-c"] {
        let created = create_graph(executor, graph);
        assert_eq!(created.graph(), graph);
        assert_eq!(created.node_count(), 0);
        assert_eq!(created.edge_count(), 0);
    }
    let duplicate = executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: "graph-a".to_owned(),
        })
        .expect_err("duplicate graph create fails");
    assert_eq!(duplicate.class(), ExecutorErrorClass::Conflict);

    let first_page = graph_name_page(executor, None, Some(2));
    assert_eq!(
        first_page.items,
        vec!["graph-a".to_owned(), "graph-b".to_owned()]
    );
    assert!(first_page.has_more);
    assert_eq!(first_page.cursor, Some("graph-b".to_owned()));
    let second_page = graph_name_page(executor, first_page.cursor, Some(2));
    assert_eq!(second_page.items, vec!["graph-c".to_owned()]);
    assert!(!second_page.has_more);
    assert!(second_page.cursor.is_none());

    let zero_page = graph_name_page(executor, None, Some(0));
    assert!(zero_page.items.is_empty());
    assert!(!zero_page.has_more);
    assert!(zero_page.cursor.is_none());

    assert!(delete_graph(executor, "graph-b"));
    assert_meta_absent(executor, "graph-b");
    assert!(!delete_graph(executor, "graph-b"));
    let recreated = create_graph(executor, "graph-b");
    assert_eq!(recreated.node_count(), 0);
    assert_eq!(recreated.edge_count(), 0);
    assert!(node_page(executor, "graph-b", None, None, Some(10))
        .items
        .is_empty());
}

fn assert_graph_node_crud_and_list_edges(executor: &mut Executor) {
    create_graph(executor, "nodes");

    let created = graph_add_node_output(executor, "nodes", "plain", None, None);
    assert!(created);
    let plain = get_node(executor, "nodes", "plain").expect("plain node exists");
    assert!(plain.properties().is_none());
    assert!(plain.binding().is_none());

    let updated = graph_add_node_output(
        executor,
        "nodes",
        "plain",
        Some(json!({"nested": {"ok": true}, "list": [1, 2]})),
        None,
    );
    assert!(!updated);
    assert_eq!(
        get_node(executor, "nodes", "plain")
            .expect("updated node exists")
            .properties(),
        Some(&json!({"nested": {"ok": true}, "list": [1, 2]}))
    );
    assert!(get_node(executor, "nodes", "missing").is_none());

    for node_id in ["a-1", "a-2", "b-1"] {
        graph_add_node_output(
            executor,
            "nodes",
            node_id,
            Some(json!({"node": node_id})),
            None,
        );
    }
    let first_page = node_page(executor, "nodes", None, None, Some(2));
    assert_eq!(first_page.items, vec!["a-1".to_owned(), "a-2".to_owned()]);
    assert!(first_page.has_more);
    assert_eq!(first_page.cursor, Some("a-2".to_owned()));
    let second_page = node_page(executor, "nodes", None, first_page.cursor, Some(5));
    assert_eq!(
        second_page.items,
        vec!["b-1".to_owned(), "plain".to_owned()]
    );
    assert!(!second_page.has_more);

    assert_eq!(
        node_page(executor, "nodes", Some("a-".to_owned()), None, Some(10)).items,
        vec!["a-1".to_owned(), "a-2".to_owned()]
    );
    assert!(remove_node(executor, "nodes", "a-1"));
    assert_eq!(
        node_page(executor, "nodes", Some("a-".to_owned()), None, Some(10)).items,
        vec!["a-2".to_owned()]
    );

    let missing_graph = executor
        .execute(Command::GraphListNodes {
            branch: None,
            space: None,
            graph: "missing-graph".to_owned(),
            prefix: None,
            cursor: None,
            limit: Some(10),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("missing graph list fails");
    assert_eq!(missing_graph.class(), ExecutorErrorClass::NotFound);

    graph_add_node_output(
        executor,
        "nodes",
        "bound",
        Some(json!({"bound": true})),
        Some(binding("bound-doc")),
    );
    assert_eq!(
        binding_nodes(executor, target("bound-doc")),
        vec!["bound".to_owned()]
    );
    assert!(remove_node(executor, "nodes", "bound"));
    assert!(binding_nodes(executor, target("bound-doc")).is_empty());
}

fn assert_graph_edge_neighbor_and_self_loop_edges(executor: &mut Executor) {
    create_graph(executor, "edges");
    for node_id in ["a", "b", "c", "d", "loop"] {
        graph_add_node_output(
            executor,
            "edges",
            node_id,
            Some(json!({"id": node_id})),
            None,
        );
    }

    assert!(graph_add_edge_output(
        executor,
        "edges",
        "a",
        "likes",
        "b",
        Some(1.0),
        Some(json!({"rev": 1})),
    ));
    assert!(!graph_add_edge_output(
        executor,
        "edges",
        "a",
        "likes",
        "b",
        Some(2.0),
        Some(json!({"rev": 2})),
    ));
    let edge = get_edge(executor, "edges", "a", "likes", "b").expect("edge exists");
    assert_float_eq(edge.weight(), 2.0);
    assert_eq!(edge.properties(), Some(&json!({"rev": 2})));
    assert!(get_edge(executor, "edges", "a", "missing", "b").is_none());

    assert!(graph_add_edge_output(
        executor,
        "edges",
        "a",
        "owns",
        "b",
        Some(3.0),
        Some(json!({"type": "second"})),
    ));
    assert!(get_edge(executor, "edges", "a", "owns", "b").is_some());

    graph_add_edge_output(executor, "edges", "c", "likes", "a", None, None);
    graph_add_edge_output(executor, "edges", "a", "blocks", "d", None, None);
    assert_eq!(
        neighbor_node_ids(
            executor,
            "edges",
            "a",
            GraphDirection::Outgoing,
            Some("likes"),
            None,
            Some(10)
        ),
        vec!["b".to_owned()]
    );
    assert_eq!(
        neighbor_node_ids(
            executor,
            "edges",
            "a",
            GraphDirection::Incoming,
            Some("likes"),
            None,
            Some(10)
        ),
        vec!["c".to_owned()]
    );
    let both = neighbor_page(
        executor,
        "edges",
        "a",
        GraphDirection::Both,
        Some("likes"),
        None,
        Some(10),
    );
    let mut both_hits = both
        .hits
        .iter()
        .map(|hit| (hit.node_id.clone(), hit.direction))
        .collect::<Vec<_>>();
    both_hits.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        both_hits,
        vec![
            ("b".to_owned(), GraphDirection::Outgoing),
            ("c".to_owned(), GraphDirection::Incoming),
        ]
    );

    let paged = neighbor_page(
        executor,
        "edges",
        "a",
        GraphDirection::Outgoing,
        None,
        None,
        Some(1),
    );
    assert_eq!(paged.hits.len(), 1);
    assert!(paged.has_more);
    assert!(paged.cursor.is_some());
    let next = neighbor_page(
        executor,
        "edges",
        "a",
        GraphDirection::Outgoing,
        None,
        paged.cursor,
        Some(10),
    );
    assert!(!next.hits.is_empty());
    assert!(!next.has_more);
    let mut paged_neighbors = paged
        .hits
        .iter()
        .chain(next.hits.iter())
        .map(|hit| {
            assert_eq!(hit.direction, GraphDirection::Outgoing);
            (hit.node_id.clone(), hit.edge_type.clone())
        })
        .collect::<Vec<_>>();
    paged_neighbors.sort();
    assert_eq!(
        paged_neighbors,
        vec![
            ("b".to_owned(), "likes".to_owned()),
            ("b".to_owned(), "owns".to_owned()),
            ("d".to_owned(), "blocks".to_owned()),
        ]
    );

    assert!(remove_edge(executor, "edges", "a", "likes", "b"));
    assert!(!remove_edge(executor, "edges", "a", "likes", "b"));
    assert!(get_edge(executor, "edges", "a", "likes", "b").is_none());
    assert!(neighbor_node_ids(
        executor,
        "edges",
        "missing-node",
        GraphDirection::Outgoing,
        None,
        None,
        Some(10),
    )
    .is_empty());

    graph_add_edge_output(executor, "edges", "loop", "self", "loop", None, None);
    assert_eq!(
        neighbor_node_ids(
            executor,
            "edges",
            "loop",
            GraphDirection::Outgoing,
            None,
            None,
            Some(10),
        ),
        vec!["loop".to_owned()]
    );
    assert_eq!(
        neighbor_node_ids(
            executor,
            "edges",
            "loop",
            GraphDirection::Incoming,
            None,
            None,
            Some(10),
        ),
        vec!["loop".to_owned()]
    );
    let self_loop_both = neighbor_page(
        executor,
        "edges",
        "loop",
        GraphDirection::Both,
        None,
        None,
        Some(10),
    );
    let self_loop_directions = self_loop_both
        .hits
        .iter()
        .map(|hit| hit.direction)
        .collect::<Vec<_>>();
    assert_eq!(self_loop_directions.len(), 2);
    assert!(self_loop_directions.contains(&GraphDirection::Incoming));
    assert!(self_loop_directions.contains(&GraphDirection::Outgoing));
    assert!(remove_edge(executor, "edges", "loop", "self", "loop"));
    assert!(neighbor_node_ids(
        executor,
        "edges",
        "loop",
        GraphDirection::Both,
        None,
        None,
        Some(10),
    )
    .is_empty());
}

fn assert_graph_binding_lookup_update_and_delete(executor: &mut Executor) {
    create_graph(executor, "bindings");
    graph_add_node_output(
        executor,
        "bindings",
        "node-a",
        Some(json!({"bound": "a"})),
        Some(binding("doc-a")),
    );
    graph_add_node_output(
        executor,
        "bindings",
        "node-b",
        Some(json!({"bound": "b"})),
        Some(binding("doc-a")),
    );
    assert_eq!(
        binding_page(executor, target("doc-a"), None, Some(10)).items,
        vec!["node-a".to_owned(), "node-b".to_owned()]
    );
    graph_add_node_output(
        executor,
        "bindings",
        "node-a",
        Some(json!({"bound": "moved"})),
        Some(binding("doc-b")),
    );
    assert_eq!(
        binding_page(executor, target("doc-a"), None, Some(10)).items,
        vec!["node-b".to_owned()]
    );
    assert_eq!(
        binding_page(executor, target("doc-b"), None, Some(10)).items,
        vec!["node-a".to_owned()]
    );

    assert!(remove_node(executor, "bindings", "node-b"));
    assert!(binding_page(executor, target("doc-a"), None, Some(10))
        .items
        .is_empty());
    assert!(delete_graph(executor, "bindings"));
    assert!(binding_page(executor, target("doc-b"), None, Some(10))
        .items
        .is_empty());
}

fn assert_graph_batch_success_atomicity_and_delete_edges(executor: &mut Executor) {
    create_graph(executor, "batch");
    let empty =
        graph_batch_write_output(executor, "batch", Vec::new()).expect("empty batch succeeds");
    assert!(empty.results.is_empty());
    assert!(!empty.effect.applied());
    assert!(empty.effect.matched());
    assert_eq!(empty.effect.kind(), MutationEffectKind::Unchanged);
    assert!(empty.version.is_none());
    assert!(empty.timestamp.is_none());

    let created = graph_batch_write_output(
        executor,
        "batch",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "a".to_owned(),
                data: GraphNodeData::new(Some(json!({"rev": 1})), None),
            },
            GraphBatchOperation::UpsertNode {
                node_id: "b".to_owned(),
                data: GraphNodeData::new(Some(json!({"rev": 1})), None),
            },
            GraphBatchOperation::UpsertEdge {
                src: "b".to_owned(),
                edge_type: "back".to_owned(),
                dst: "a".to_owned(),
                data: GraphEdgeData::new(Some(1.5), Some(json!({"rev": 1}))),
            },
            GraphBatchOperation::UpsertEdge {
                src: "a".to_owned(),
                edge_type: "links".to_owned(),
                dst: "b".to_owned(),
                data: GraphEdgeData::new(Some(1.0), Some(json!({"rev": 1}))),
            },
        ],
    )
    .expect("create batch succeeds");
    assert_eq!(created.results.len(), 4);
    assert!(created
        .results
        .iter()
        .all(|result| result.created() == Some(true)));
    assert_eq!(created.effect.affected_count(), 4);
    assert_eq!(created.effect.kind(), MutationEffectKind::Created);
    assert!(!created.effect.matched());
    assert!(created
        .results
        .iter()
        .all(|result| result.effect() == Some(&MutationEffect::created())));
    assert!(get_edge(executor, "batch", "a", "links", "b").is_some());
    assert!(get_edge(executor, "batch", "b", "back", "a").is_some());

    let updated = graph_batch_write_output(
        executor,
        "batch",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "a".to_owned(),
                data: GraphNodeData::new(Some(json!({"rev": 2})), None),
            },
            GraphBatchOperation::UpsertEdge {
                src: "a".to_owned(),
                edge_type: "links".to_owned(),
                dst: "b".to_owned(),
                data: GraphEdgeData::new(Some(2.0), Some(json!({"rev": 2}))),
            },
        ],
    )
    .expect("update batch succeeds");
    assert_eq!(updated.results[0].created(), Some(false));
    assert_eq!(updated.results[1].created(), Some(false));
    assert_eq!(updated.effect.affected_count(), 2);
    assert_eq!(updated.effect.kind(), MutationEffectKind::Updated);
    assert!(updated.effect.matched());
    assert!(updated
        .results
        .iter()
        .all(|result| result.effect() == Some(&MutationEffect::updated())));

    let invalid_endpoint = graph_batch_write_output(
        executor,
        "batch",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "planned".to_owned(),
                data: GraphNodeData::default(),
            },
            GraphBatchOperation::UpsertEdge {
                src: "planned".to_owned(),
                edge_type: "bad".to_owned(),
                dst: "missing".to_owned(),
                data: GraphEdgeData::default(),
            },
        ],
    )
    .expect_err("invalid endpoint batch fails");
    assert_eq!(invalid_endpoint.class(), ExecutorErrorClass::InvalidInput);
    assert!(get_node(executor, "batch", "planned").is_none());

    let invalid_node = graph_batch_write_output(
        executor,
        "batch",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "planned-node".to_owned(),
                data: GraphNodeData::default(),
            },
            GraphBatchOperation::DeleteNode {
                node_id: String::new(),
            },
        ],
    )
    .expect_err("invalid node batch fails before commit");
    assert_eq!(invalid_node.class(), ExecutorErrorClass::InvalidInput);
    assert!(get_node(executor, "batch", "planned-node").is_none());

    let invalid_edge_type = graph_batch_write_output(
        executor,
        "batch",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "planned-edge-type".to_owned(),
                data: GraphNodeData::default(),
            },
            GraphBatchOperation::DeleteEdge {
                src: "a".to_owned(),
                edge_type: String::new(),
                dst: "b".to_owned(),
            },
        ],
    )
    .expect_err("invalid edge-type batch fails before commit");
    assert_eq!(invalid_edge_type.class(), ExecutorErrorClass::InvalidInput);
    assert!(get_node(executor, "batch", "planned-edge-type").is_none());

    let deletes = graph_batch_write_output(
        executor,
        "batch",
        vec![
            GraphBatchOperation::DeleteEdge {
                src: "a".to_owned(),
                edge_type: "links".to_owned(),
                dst: "b".to_owned(),
            },
            GraphBatchOperation::DeleteNode {
                node_id: "a".to_owned(),
            },
            GraphBatchOperation::DeleteEdge {
                src: "a".to_owned(),
                edge_type: "links".to_owned(),
                dst: "b".to_owned(),
            },
        ],
    )
    .expect("delete batch succeeds");
    assert_eq!(deletes.results[0].deleted(), Some(true));
    assert_eq!(deletes.results[1].deleted(), Some(true));
    assert_eq!(deletes.results[2].deleted(), Some(false));
    assert_eq!(deletes.effect.affected_count(), 2);
    assert_eq!(deletes.effect.kind(), MutationEffectKind::Deleted);
    assert!(deletes.effect.matched());
    assert_eq!(
        deletes.results[0].effect(),
        Some(&MutationEffect::deleted())
    );
    assert_eq!(
        deletes.results[1].effect(),
        Some(&MutationEffect::deleted())
    );
    assert_eq!(
        deletes.results[2].effect(),
        Some(&MutationEffect::not_found())
    );
    assert!(deletes.results[2]
        .commit()
        .map(CommitReceipt::version)
        .is_none());
    assert!(get_edge(executor, "batch", "a", "links", "b").is_none());
    assert!(get_edge(executor, "batch", "b", "back", "a").is_none());
    assert!(neighbor_node_ids(
        executor,
        "batch",
        "b",
        GraphDirection::Outgoing,
        Some("back"),
        None,
        Some(10),
    )
    .is_empty());
    assert!(get_node(executor, "batch", "a").is_none());
}

fn assert_graph_error_mapping(executor: &mut Executor) {
    let invalid_graph = executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: String::new(),
        })
        .expect_err("invalid graph name fails");
    assert_eq!(invalid_graph.class(), ExecutorErrorClass::InvalidInput);

    create_graph(executor, "errors");
    let invalid_node = executor
        .execute(Command::GraphGetNode {
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            node_id: String::new(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("invalid node id fails");
    assert_eq!(invalid_node.class(), ExecutorErrorClass::InvalidInput);

    let invalid_edge_type = executor
        .execute(Command::GraphGetEdge {
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            src: "a".to_owned(),
            edge_type: String::new(),
            dst: "b".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("invalid edge type fails");
    assert_eq!(invalid_edge_type.class(), ExecutorErrorClass::InvalidInput);

    let invalid_direction = serde_json::from_value::<Command>(json!({
        "type": "graph_neighbors",
        "graph": "errors",
        "node_id": "a",
        "direction": "sideways"
    }));
    assert!(invalid_direction.is_err());

    let bad_node_props = executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            node_id: "bad-node-props".to_owned(),
            properties: Some(json!("not-object")),
            binding: None,
        })
        .expect_err("non-object node properties fail");
    assert_eq!(bad_node_props.class(), ExecutorErrorClass::InvalidInput);

    graph_add_node_output(executor, "errors", "a", Some(json!({"ok": true})), None);
    graph_add_node_output(executor, "errors", "b", Some(json!({"ok": true})), None);
    let bad_edge_props = executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            src: "a".to_owned(),
            edge_type: "bad_props".to_owned(),
            dst: "b".to_owned(),
            weight: Some(1.0),
            properties: Some(json!(["not-object"])),
        })
        .expect_err("non-object edge properties fail");
    assert_eq!(bad_edge_props.class(), ExecutorErrorClass::InvalidInput);

    let non_finite_weight = executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            src: "a".to_owned(),
            edge_type: "nan_weight".to_owned(),
            dst: "b".to_owned(),
            weight: Some(f64::NAN),
            properties: None,
        })
        .expect_err("non-finite weight fails");
    assert_eq!(non_finite_weight.class(), ExecutorErrorClass::InvalidInput);

    let bad_binding = executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            node_id: "bad-binding".to_owned(),
            properties: Some(json!({"ok": true})),
            binding: Some(GraphEntityBinding::new(GraphBindingTarget::new(
                GraphBindingPrimitive::Json,
                None,
                "_system_",
                "doc",
            ))),
        })
        .expect_err("malformed binding fails");
    assert_eq!(bad_binding.class(), ExecutorErrorClass::InvalidInput);

    let missing_read = executor
        .execute(Command::GraphGetNode {
            branch: None,
            space: None,
            graph: "missing".to_owned(),
            node_id: "a".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("missing graph read fails");
    assert_eq!(missing_read.class(), ExecutorErrorClass::NotFound);
    let missing_meta = executor
        .execute(Command::GraphGetMeta {
            branch: None,
            space: None,
            graph: "missing".to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("graph meta on a missing graph errors like its siblings");
    assert_eq!(missing_meta.class(), ExecutorErrorClass::NotFound);
    assert_eq!(missing_meta.code(), "not_found.engine.graph");

    let missing_write = executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: None,
            graph: "missing".to_owned(),
            node_id: "a".to_owned(),
            properties: Some(json!({"ok": true})),
            binding: None,
        })
        .expect_err("missing graph write fails");
    assert_eq!(missing_write.class(), ExecutorErrorClass::NotFound);

    let missing_endpoint = executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: "errors".to_owned(),
            src: "a".to_owned(),
            edge_type: "missing_endpoint".to_owned(),
            dst: "missing-node".to_owned(),
            weight: Some(1.0),
            properties: None,
        })
        .expect_err("missing edge endpoint fails");
    assert_eq!(missing_endpoint.class(), ExecutorErrorClass::InvalidInput);
}

#[allow(clippy::too_many_lines)]
fn run_graph_core_command_suite(executor: &mut Executor) {
    assert_eq!(graph_names(executor, None, Some(5)), Vec::<String>::new());

    let created = create_graph(executor, "deps");
    assert_eq!(created.graph(), "deps");
    assert_eq!(created.node_count(), 0);
    assert_eq!(created.edge_count(), 0);

    add_node(
        executor,
        "deps",
        "node-a",
        json!({"kind": "root"}),
        Some(binding("doc-a")),
    );
    add_node(executor, "deps", "node-b", json!({"kind": "child"}), None);
    add_edge(
        executor,
        "deps",
        "node-a",
        "depends_on",
        "node-b",
        Some(2.5),
    );
    let meta_after_writes = get_meta(executor, "deps").expect("metadata exists");
    assert!(meta_after_writes.updated_version() > meta_after_writes.created_version());
    assert!(meta_after_writes.updated_timestamp() >= meta_after_writes.created_timestamp());

    assert_eq!(
        graph_names(executor, None, Some(5)),
        vec!["deps".to_owned()]
    );
    assert!(get_meta(executor, "deps").is_some());
    assert_eq!(
        get_node(executor, "deps", "node-a")
            .expect("node exists")
            .properties(),
        Some(&json!({"kind": "root"}))
    );
    assert_eq!(
        node_ids(executor, "deps", Some("node-".to_owned()), None, Some(5)),
        vec!["node-a".to_owned(), "node-b".to_owned()]
    );
    assert_float_eq(
        get_edge(executor, "deps", "node-a", "depends_on", "node-b")
            .expect("edge exists")
            .weight(),
        2.5,
    );
    assert_eq!(
        neighbor_nodes(
            executor,
            "deps",
            "node-a",
            GraphDirection::Outgoing,
            Some("depends_on".to_owned()),
        ),
        vec!["node-b".to_owned()]
    );
    assert_eq!(
        neighbor_nodes(executor, "deps", "node-b", GraphDirection::Incoming, None),
        vec!["node-a".to_owned()]
    );
    assert_eq!(
        binding_nodes(executor, target("doc-a")),
        vec!["node-a".to_owned()]
    );

    let batch = graph_batch_write(
        executor,
        "deps",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "node-c".to_owned(),
                data: GraphNodeData::new(Some(json!({"kind": "batch"})), None),
            },
            GraphBatchOperation::UpsertEdge {
                src: "node-b".to_owned(),
                edge_type: "relates_to".to_owned(),
                dst: "node-c".to_owned(),
                data: GraphEdgeData::new(Some(1.0), Some(json!({"batch": true}))),
            },
            GraphBatchOperation::DeleteEdge {
                src: "node-a".to_owned(),
                edge_type: "depends_on".to_owned(),
                dst: "node-b".to_owned(),
            },
        ],
    );
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].created(), Some(true));
    assert_eq!(batch[1].created(), Some(true));
    assert_eq!(batch[2].deleted(), Some(true));
    assert!(batch[2].commit().map(CommitReceipt::version).is_some());
    assert!(get_edge(executor, "deps", "node-a", "depends_on", "node-b").is_none());
    assert_eq!(
        neighbor_nodes(executor, "deps", "node-b", GraphDirection::Outgoing, None),
        vec!["node-c".to_owned()]
    );

    let mixed_no_op_batch = graph_batch_write(
        executor,
        "deps",
        vec![
            GraphBatchOperation::UpsertNode {
                node_id: "node-d".to_owned(),
                data: GraphNodeData::new(Some(json!({"kind": "applied"})), None),
            },
            GraphBatchOperation::DeleteEdge {
                src: "node-a".to_owned(),
                edge_type: "missing_edge_type".to_owned(),
                dst: "node-b".to_owned(),
            },
        ],
    );
    assert_eq!(mixed_no_op_batch[0].created(), Some(true));
    assert!(mixed_no_op_batch[0]
        .commit()
        .map(CommitReceipt::version)
        .is_some());
    assert_eq!(mixed_no_op_batch[1].deleted(), Some(false));
    assert!(mixed_no_op_batch[1]
        .commit()
        .map(CommitReceipt::version)
        .is_none());
    assert!(mixed_no_op_batch[1]
        .commit()
        .map(CommitReceipt::timestamp)
        .is_none());

    assert!(remove_node(executor, "deps", "node-c"));
    assert!(get_node(executor, "deps", "node-c").is_none());
    assert!(!remove_edge(
        executor,
        "deps",
        "node-b",
        "relates_to",
        "node-c"
    ));
    assert!(delete_graph(executor, "deps"));
    assert_meta_absent(executor, "deps");
    assert!(!delete_graph(executor, "deps"));
}

#[derive(Debug, PartialEq)]
struct StringPage {
    items: Vec<String>,
    has_more: bool,
    cursor: Option<String>,
}

#[derive(Debug, PartialEq)]
struct NeighborSummary {
    node_id: String,
    direction: GraphDirection,
    edge_type: String,
}

#[derive(Debug, PartialEq)]
struct NeighborSummaryPage {
    hits: Vec<NeighborSummary>,
    has_more: bool,
    cursor: Option<String>,
}

#[derive(Debug, PartialEq)]
struct GraphBatchOutput {
    results: Vec<strata_executor::BatchItem<GraphBatchItemResult>>,
    effect: MutationEffect,
    version: Option<u64>,
    timestamp: Option<u64>,
}

#[test]
fn graph_sample_returns_total_and_deterministic_nodes() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_graph(&mut executor, "social");
    for index in 0..8 {
        add_node(
            &mut executor,
            "social",
            &format!("n{index}"),
            json!({}),
            None,
        );
    }

    let sample = |executor: &mut Executor| {
        let Output::GraphSampleResult {
            total_count, items, ..
        } = executor
            .execute(Command::GraphSample {
                branch: None,
                space: None,
                graph: "social".to_owned(),
                count: Some(3),
            })
            .expect("graph sample succeeds")
        else {
            panic!("unexpected graph sample output");
        };
        (
            total_count,
            items
                .iter()
                .map(|node| node.node_id().to_owned())
                .collect::<Vec<_>>(),
        )
    };

    let (total_count, ids) = sample(&mut executor);
    assert_eq!(total_count, 8, "total is the full live node count");
    assert_eq!(ids.len(), 3);
    // The stride sample is deterministic across identical reads.
    assert_eq!(sample(&mut executor).1, ids);
}

fn create_graph(executor: &mut Executor, graph: &str) -> strata_executor::GraphInfoData {
    match executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: graph.to_owned(),
        })
        .expect("graph create succeeds")
    {
        Output::GraphCreateResult { info, .. } => info,
        output => panic!("unexpected graph create output: {output:?}"),
    }
}

fn graph_name_page(
    executor: &mut Executor,
    cursor: Option<String>,
    limit: Option<u64>,
) -> StringPage {
    match executor
        .execute(Command::GraphList {
            branch: None,
            space: None,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph list succeeds")
    {
        Output::GraphNamePage {
            items: graphs,
            page,
        } => StringPage {
            items: graphs,
            has_more: page.has_more(),
            cursor: page.cursor().cloned(),
        },
        output => panic!("unexpected graph list output: {output:?}"),
    }
}

fn graph_create_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
) -> strata_executor::GraphInfoData {
    match executor
        .execute(Command::GraphCreate {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
        })
        .expect("graph create succeeds")
    {
        Output::GraphCreateResult { info, .. } => info,
        output => panic!("unexpected graph create output: {output:?}"),
    }
}

fn delete_graph(executor: &mut Executor, graph: &str) -> bool {
    // Route through the facade `graph_delete` (with force) so its wiring is
    // exercised, not just the raw Command (#3122).
    match executor
        .graph_delete(graph, true)
        .expect("graph delete succeeds")
    {
        Output::GraphDeleteResult { effect, .. } => effect.applied(),
        output => panic!("unexpected graph delete output: {output:?}"),
    }
}

fn graph_names(executor: &mut Executor, cursor: Option<String>, limit: Option<u64>) -> Vec<String> {
    match executor
        .execute(Command::GraphList {
            branch: None,
            space: None,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph list succeeds")
    {
        Output::GraphNamePage { items: graphs, .. } => graphs,
        output => panic!("unexpected graph list output: {output:?}"),
    }
}

fn assert_meta_absent(executor: &mut Executor, graph: &str) {
    let error = executor
        .execute(Command::GraphGetMeta {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("graph meta on a missing graph errors like its siblings");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.graph");
}

fn get_meta(executor: &mut Executor, graph: &str) -> Option<strata_executor::GraphInfoData> {
    match executor
        .execute(Command::GraphGetMeta {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph metadata succeeds")
    {
        Output::GraphInfoResult(info) => info,
        output => panic!("unexpected graph metadata output: {output:?}"),
    }
}

fn add_node(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    properties: serde_json::Value,
    binding: Option<GraphEntityBinding>,
) {
    match executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            properties: Some(properties),
            binding,
        })
        .expect("graph node add succeeds")
    {
        Output::GraphNodeWriteResult { effect, .. } => {
            assert!(effect.applied());
        }
        output => panic!("unexpected graph node add output: {output:?}"),
    }
}

fn graph_add_node_output(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    properties: Option<serde_json::Value>,
    binding: Option<GraphEntityBinding>,
) -> bool {
    match executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            properties,
            binding,
        })
        .expect("graph node add succeeds")
    {
        Output::GraphNodeWriteResult { effect, .. } => {
            let created = effect.kind() == MutationEffectKind::Created;
            assert_eq!(
                effect,
                if created {
                    MutationEffect::created()
                } else {
                    MutationEffect::updated()
                }
            );
            created
        }
        output => panic!("unexpected graph node add output: {output:?}"),
    }
}

fn graph_add_node_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    node_id: &str,
    properties: serde_json::Value,
) {
    match executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            properties: Some(properties),
            binding: None,
        })
        .expect("graph node add succeeds")
    {
        Output::GraphNodeWriteResult { .. } => {}
        output => panic!("unexpected graph node add output: {output:?}"),
    }
}

fn graph_add_node_with_binding_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    node_id: &str,
    properties: serde_json::Value,
    binding: GraphEntityBinding,
) {
    match executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            properties: Some(properties),
            binding: Some(binding),
        })
        .expect("graph node add succeeds")
    {
        Output::GraphNodeWriteResult { .. } => {}
        output => panic!("unexpected graph node add output: {output:?}"),
    }
}

#[test]
fn graph_get_node_as_of_reads_historical_state() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_graph(&mut executor, "deps");
    let t1 = add_node_capturing_timestamp(&mut executor, "deps", "n", json!({"v": 1}));
    let _t2 = add_node_capturing_timestamp(&mut executor, "deps", "n", json!({"v": 2}));

    // Latest read sees the newest value.
    assert_eq!(
        get_node(&mut executor, "deps", "n")
            .expect("latest node exists")
            .properties(),
        Some(&json!({"v": 2}))
    );

    // ENGINE-1: reading as_of the first commit returns the historical value,
    // proving the GraphGetNode command routes as_of into the engine's
    // get_node_at (graph time travel is reachable from the command surface).
    assert_eq!(
        get_node_as_of(&mut executor, "deps", "n", t1)
            .expect("historical node exists")
            .properties(),
        Some(&json!({"v": 1}))
    );
}

fn add_node_capturing_timestamp(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    properties: serde_json::Value,
) -> u64 {
    match executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            properties: Some(properties),
            binding: None,
        })
        .expect("graph node add succeeds")
    {
        Output::GraphNodeWriteResult { commit, .. } => commit.timestamp(),
        output => panic!("unexpected graph node add output: {output:?}"),
    }
}

fn get_node_as_of(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    as_of: u64,
) -> Option<strata_executor::GraphNodeDataOutput> {
    match executor
        .execute(Command::GraphGetNode {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("graph node get succeeds")
    {
        Output::GraphNodeResult(node) => node.into_option(),
        output => panic!("unexpected graph node get output: {output:?}"),
    }
}

fn get_node(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
) -> Option<strata_executor::GraphNodeDataOutput> {
    match executor
        .execute(Command::GraphGetNode {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph node get succeeds")
    {
        Output::GraphNodeResult(node) => node.into_option(),
        output => panic!("unexpected graph node get output: {output:?}"),
    }
}

fn graph_get_node_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    node_id: &str,
) -> Option<strata_executor::GraphNodeDataOutput> {
    match executor
        .execute(Command::GraphGetNode {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph node get succeeds")
    {
        Output::GraphNodeResult(node) => node.into_option(),
        output => panic!("unexpected graph node get output: {output:?}"),
    }
}

fn remove_node(executor: &mut Executor, graph: &str, node_id: &str) -> bool {
    match executor
        .execute(Command::GraphRemoveNode {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
        })
        .expect("graph node remove succeeds")
    {
        Output::GraphDeleteResult { effect, .. } => effect.applied(),
        output => panic!("unexpected graph node remove output: {output:?}"),
    }
}

fn graph_remove_node_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    node_id: &str,
) -> bool {
    match executor
        .execute(Command::GraphRemoveNode {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
        })
        .expect("graph node remove succeeds")
    {
        Output::GraphDeleteResult { effect, .. } => effect.applied(),
        output => panic!("unexpected graph node remove output: {output:?}"),
    }
}

fn node_ids(
    executor: &mut Executor,
    graph: &str,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u64>,
) -> Vec<String> {
    match executor
        .execute(Command::GraphListNodes {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            prefix,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph node list succeeds")
    {
        Output::GraphNodePage { items: nodes, .. } => {
            nodes.iter().map(|node| node.node_id().to_owned()).collect()
        }
        output => panic!("unexpected graph node list output: {output:?}"),
    }
}

fn node_page(
    executor: &mut Executor,
    graph: &str,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u64>,
) -> StringPage {
    match executor
        .execute(Command::GraphListNodes {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            prefix,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph node list succeeds")
    {
        Output::GraphNodePage { items: nodes, page } => StringPage {
            items: nodes.iter().map(|node| node.node_id().to_owned()).collect(),
            has_more: page.has_more(),
            cursor: page.cursor().cloned(),
        },
        output => panic!("unexpected graph node list output: {output:?}"),
    }
}

fn graph_node_ids_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
) -> Vec<String> {
    match executor
        .execute(Command::GraphListNodes {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            prefix: None,
            cursor: None,
            limit: Some(10),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph node list succeeds")
    {
        Output::GraphNodePage { items: nodes, .. } => {
            nodes.iter().map(|node| node.node_id().to_owned()).collect()
        }
        output => panic!("unexpected graph node list output: {output:?}"),
    }
}

fn add_edge(
    executor: &mut Executor,
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
    weight: Option<f64>,
) {
    match executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            src: src.to_owned(),
            edge_type: edge_type.to_owned(),
            dst: dst.to_owned(),
            weight,
            properties: Some(json!({"kind": "edge"})),
        })
        .expect("graph edge add succeeds")
    {
        Output::GraphEdgeWriteResult { effect, .. } => {
            assert!(effect.applied());
        }
        output => panic!("unexpected graph edge add output: {output:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn graph_add_edge_output(
    executor: &mut Executor,
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
    weight: Option<f64>,
    properties: Option<serde_json::Value>,
) -> bool {
    match executor
        .execute(Command::GraphAddEdge {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            src: src.to_owned(),
            edge_type: edge_type.to_owned(),
            dst: dst.to_owned(),
            weight,
            properties,
        })
        .expect("graph edge add succeeds")
    {
        Output::GraphEdgeWriteResult { effect, .. } => {
            let created = effect.kind() == MutationEffectKind::Created;
            assert_eq!(
                effect,
                if created {
                    MutationEffect::created()
                } else {
                    MutationEffect::updated()
                }
            );
            created
        }
        output => panic!("unexpected graph edge add output: {output:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn graph_add_edge_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
    properties: Option<serde_json::Value>,
) {
    match executor
        .execute(Command::GraphAddEdge {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            src: src.to_owned(),
            edge_type: edge_type.to_owned(),
            dst: dst.to_owned(),
            weight: None,
            properties,
        })
        .expect("graph edge add succeeds")
    {
        Output::GraphEdgeWriteResult { .. } => {}
        output => panic!("unexpected graph edge add output: {output:?}"),
    }
}

fn get_edge(
    executor: &mut Executor,
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
) -> Option<strata_executor::GraphEdgeDataOutput> {
    match executor
        .execute(Command::GraphGetEdge {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            src: src.to_owned(),
            edge_type: edge_type.to_owned(),
            dst: dst.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph edge get succeeds")
    {
        Output::GraphEdgeResult(edge) => edge.into_option(),
        output => panic!("unexpected graph edge get output: {output:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn graph_get_edge_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
) -> Option<strata_executor::GraphEdgeDataOutput> {
    match executor
        .execute(Command::GraphGetEdge {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            src: src.to_owned(),
            edge_type: edge_type.to_owned(),
            dst: dst.to_owned(),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph edge get succeeds")
    {
        Output::GraphEdgeResult(edge) => edge.into_option(),
        output => panic!("unexpected graph edge get output: {output:?}"),
    }
}

fn neighbor_node_ids(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    direction: GraphDirection,
    edge_type: Option<&str>,
    cursor: Option<String>,
    limit: Option<u64>,
) -> Vec<String> {
    neighbor_page(
        executor, graph, node_id, direction, edge_type, cursor, limit,
    )
    .hits
    .into_iter()
    .map(|hit| hit.node_id)
    .collect()
}

#[allow(clippy::too_many_arguments)]
fn neighbor_node_ids_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    graph: &str,
    node_id: &str,
    direction: GraphDirection,
    edge_type: Option<&str>,
) -> Vec<String> {
    match executor
        .execute(Command::GraphNeighbors {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            direction,
            edge_type: edge_type.map(str::to_owned),
            cursor: None,
            limit: Some(10),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph neighbors succeeds")
    {
        Output::GraphNeighborPage {
            items: neighbors, ..
        } => neighbors
            .iter()
            .map(|hit| hit.node_id().to_owned())
            .collect(),
        output => panic!("unexpected graph neighbors output: {output:?}"),
    }
}

fn neighbor_page(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    direction: GraphDirection,
    edge_type: Option<&str>,
    cursor: Option<String>,
    limit: Option<u64>,
) -> NeighborSummaryPage {
    match executor
        .execute(Command::GraphNeighbors {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            direction,
            edge_type: edge_type.map(str::to_owned),
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph neighbors succeeds")
    {
        Output::GraphNeighborPage {
            items: neighbors,
            page,
        } => NeighborSummaryPage {
            hits: neighbors.iter().map(neighbor_summary).collect(),
            has_more: page.has_more(),
            cursor: page.cursor().cloned(),
        },
        output => panic!("unexpected graph neighbors output: {output:?}"),
    }
}

fn neighbor_summary(hit: &GraphNeighborHit) -> NeighborSummary {
    NeighborSummary {
        node_id: hit.node_id().to_owned(),
        direction: hit.direction(),
        edge_type: hit.edge_type().to_owned(),
    }
}

fn remove_edge(
    executor: &mut Executor,
    graph: &str,
    src: &str,
    edge_type: &str,
    dst: &str,
) -> bool {
    match executor
        .execute(Command::GraphRemoveEdge {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            src: src.to_owned(),
            edge_type: edge_type.to_owned(),
            dst: dst.to_owned(),
        })
        .expect("graph edge remove succeeds")
    {
        Output::GraphDeleteResult { effect, .. } => effect.applied(),
        output => panic!("unexpected graph edge remove output: {output:?}"),
    }
}

fn binding_page(
    executor: &mut Executor,
    target: GraphBindingTarget,
    cursor: Option<String>,
    limit: Option<u64>,
) -> StringPage {
    match executor
        .execute(Command::GraphBindingsForEntity {
            branch: None,
            space: None,
            target,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph binding lookup succeeds")
    {
        Output::GraphBindingPage {
            items: bindings,
            page,
        } => StringPage {
            items: bindings
                .iter()
                .map(GraphBindingHit::node_id)
                .map(str::to_owned)
                .collect(),
            has_more: page.has_more(),
            cursor: page.cursor().cloned(),
        },
        output => panic!("unexpected graph binding output: {output:?}"),
    }
}

fn neighbor_nodes(
    executor: &mut Executor,
    graph: &str,
    node_id: &str,
    direction: GraphDirection,
    edge_type: Option<String>,
) -> Vec<String> {
    match executor
        .execute(Command::GraphNeighbors {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            node_id: node_id.to_owned(),
            direction,
            edge_type,
            cursor: None,
            limit: Some(10),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph neighbors succeeds")
    {
        Output::GraphNeighborPage {
            items: neighbors, ..
        } => neighbors
            .iter()
            .map(|hit| hit.node_id().to_owned())
            .collect(),
        output => panic!("unexpected graph neighbors output: {output:?}"),
    }
}

fn binding_nodes(executor: &mut Executor, target: GraphBindingTarget) -> Vec<String> {
    match executor
        .execute(Command::GraphBindingsForEntity {
            branch: None,
            space: None,
            target,
            cursor: None,
            limit: Some(10),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph binding lookup succeeds")
    {
        Output::GraphBindingPage {
            items: bindings, ..
        } => bindings
            .iter()
            .map(|binding| binding.node_id().to_owned())
            .collect(),
        output => panic!("unexpected graph binding output: {output:?}"),
    }
}

fn binding_nodes_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    target: GraphBindingTarget,
) -> Vec<String> {
    match executor
        .execute(Command::GraphBindingsForEntity {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            target,
            cursor: None,
            limit: Some(10),
            as_of: None,
            as_of_time: None,
        })
        .expect("graph binding lookup succeeds")
    {
        Output::GraphBindingPage {
            items: bindings, ..
        } => bindings
            .iter()
            .map(|binding| binding.node_id().to_owned())
            .collect(),
        output => panic!("unexpected graph binding output: {output:?}"),
    }
}

fn graph_batch_write(
    executor: &mut Executor,
    graph: &str,
    operations: Vec<GraphBatchOperation>,
) -> Vec<strata_executor::BatchItem<strata_executor::GraphBatchItemResult>> {
    match executor
        .execute(Command::GraphBatchWrite {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            operations,
        })
        .expect("graph batch write succeeds")
    {
        Output::GraphBatchWriteResult { batch, .. } => batch.into_items(),
        output => panic!("unexpected graph batch output: {output:?}"),
    }
}

fn graph_batch_write_output(
    executor: &mut Executor,
    graph: &str,
    operations: Vec<GraphBatchOperation>,
) -> Result<GraphBatchOutput, ExecutorError> {
    executor
        .execute(Command::GraphBatchWrite {
            branch: None,
            space: None,
            graph: graph.to_owned(),
            operations,
        })
        .map(|output| match output {
            Output::GraphBatchWriteResult { batch, .. } => {
                let effect = graph_batch_effect(batch.items().iter());
                let version = batch.commit().map(CommitReceipt::version);
                let timestamp = batch.commit().map(CommitReceipt::timestamp);
                GraphBatchOutput {
                    results: batch.into_items(),
                    effect,
                    version,
                    timestamp,
                }
            }
            output => panic!("unexpected graph batch output: {output:?}"),
        })
}

fn graph_batch_effect<'a>(
    items: impl IntoIterator<Item = &'a strata_executor::BatchItem<GraphBatchItemResult>>,
) -> MutationEffect {
    let items = items.into_iter().collect::<Vec<_>>();
    let mut affected_count = 0_u64;
    let mut aggregate_kind = None;
    let mut mixed_kind = false;
    let mut matched = false;
    for item in &items {
        let Some(item_effect) = item.effect() else {
            continue;
        };
        if !item_effect.applied() {
            continue;
        }
        affected_count = affected_count.saturating_add(1);
        matched |= item_effect.matched();
        match aggregate_kind {
            None => aggregate_kind = Some(item_effect.kind()),
            Some(kind) if kind == item_effect.kind() => {}
            Some(_) => mixed_kind = true,
        }
    }
    if affected_count == 0 {
        if items.is_empty() {
            return MutationEffect::new(false, MutationEffectKind::Unchanged, true, 0);
        }
        MutationEffect::not_found()
    } else {
        let kind = if mixed_kind {
            MutationEffectKind::Updated
        } else {
            aggregate_kind.expect("applied graph batch has an aggregate effect kind")
        };
        MutationEffect::new(true, kind, matched, affected_count)
    }
}

fn target(key: &str) -> GraphBindingTarget {
    GraphBindingTarget::new(GraphBindingPrimitive::Json, None, "docs", key)
}

fn binding(key: &str) -> GraphEntityBinding {
    GraphEntityBinding::new(target(key))
}

fn assert_float_eq(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < f64::EPSILON,
        "expected {expected}, got {actual}"
    );
}
