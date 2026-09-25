//! GI3 exit gate: chunked bulk ingest — upsert semantics with derived
//! index maintenance, node-then-edge ordering, endpoint validation,
//! frozen-ontology enforcement, and mid-stream refusals leaving no
//! partial state.

#![allow(clippy::too_many_lines)]

mod common;

use strata_engine::{
    Database, GraphAnalyticsBudget, GraphBfsOptions, GraphBindingPrimitive, GraphBindingTarget,
    GraphDeletePolicy, GraphEdgeData, GraphEdgeType, GraphEntityBinding, GraphName, GraphNodeData,
    GraphNodeId, ProductSpace,
};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn graph_name(value: &str) -> GraphName {
    GraphName::new(value).expect("valid graph")
}

fn node_id(value: &str) -> GraphNodeId {
    GraphNodeId::new(value).expect("valid node id")
}

fn edge_type(value: &str) -> GraphEdgeType {
    GraphEdgeType::new(value).expect("valid edge type")
}

fn nodes(ids: &[&str]) -> Vec<(GraphNodeId, GraphNodeData)> {
    ids.iter()
        .map(|id| (node_id(id), GraphNodeData::default()))
        .collect()
}

fn edge(
    src: &str,
    kind: &str,
    dst: &str,
    weight: f64,
) -> (GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData) {
    (
        node_id(src),
        edge_type(kind),
        node_id(dst),
        GraphEdgeData::new(weight, None).expect("edge data"),
    )
}

#[test]
fn bulk_insert_ingests_in_cache_and_durable_modes() {
    run_database_modes(exercise_bulk_insert);
}

// Takes `Database` by value on purpose: the `fn(Database)` harness contract
// hands over ownership so the database is DROPPED — and therefore closed —
// when the exercise ends. Clippy cannot see that Drop is the point (#3126:
// the body only needs `&Database` now that services borrow shared).
#[allow(clippy::needless_pass_by_value)]
fn exercise_bulk_insert(database: Database) {
    let mut graph = database
        .graph(branch("default"), space("default"))
        .expect("graph service opens");
    let name = graph_name("bulk");
    graph.create_graph(name.clone()).expect("graph created");

    // Empty input commits nothing.
    let outcome = graph
        .bulk_insert(&name, &[], &[], None)
        .expect("empty ingest");
    assert_eq!(outcome.nodes_inserted(), 0);
    assert_eq!(outcome.edges_inserted(), 0);
    assert_eq!(outcome.commits(), 0);
    assert!(outcome.last_commit().is_none());

    // Chunk size 1 forces one commit per item: 4 nodes + 3 edges = 7.
    let outcome = graph
        .bulk_insert(
            &name,
            &nodes(&["a", "b", "c", "lone"]),
            &[
                edge("a", "e", "b", 1.0),
                edge("b", "e", "c", 2.0),
                edge("a", "f", "c", 3.0),
            ],
            Some(1),
        )
        .expect("chunked ingest");
    assert_eq!(outcome.nodes_inserted(), 4);
    assert_eq!(outcome.edges_inserted(), 3);
    assert_eq!(outcome.commits(), 7);
    assert!(outcome.last_commit().is_some());

    // The ingested graph is fully queryable: traversal and analytics
    // see the same rows any other write path would produce.
    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    assert_eq!(index.node_count(), 4);
    assert_eq!(index.edge_count(), 3);
    let bfs = index
        .bfs(&node_id("a"), &GraphBfsOptions::default())
        .expect("bfs runs");
    assert_eq!(bfs.visited().len(), 3);

    // Edges may lead with endpoints arriving in the same call.
    let outcome = graph
        .bulk_insert(&name, &nodes(&["d"]), &[edge("d", "e", "a", 1.0)], None)
        .expect("same-call endpoint ingest");
    assert_eq!(outcome.commits(), 2, "one node chunk, one edge chunk");
    assert!(graph
        .get_edge(&name, &node_id("d"), &edge_type("e"), &node_id("a"))
        .expect("read")
        .is_some());

    // Re-ingesting is an upsert: same rows, updated weight.
    graph
        .bulk_insert(&name, &nodes(&["a"]), &[edge("a", "e", "b", 9.0)], None)
        .expect("upsert ingest");
    let updated = graph
        .get_edge(&name, &node_id("a"), &edge_type("e"), &node_id("b"))
        .expect("read")
        .expect("edge visible");
    assert!((updated.data().weight() - 9.0).abs() < f64::EPSILON);
}

#[test]
fn bulk_chunks_respect_the_storage_commit_budget_in_cache_and_durable_modes() {
    run_database_modes(exercise_storage_commit_budget);
}

/// Regression (found ingesting the ASOIAF dataset): every edge writes a
/// forward and a reverse row, so an unchunked commit of a few thousand
/// edges exceeds the storage per-commit mutation budget. The default
/// and the clamp must keep every chunk inside one commit.
// Takes `Database` by value on purpose: the `fn(Database)` harness contract
// hands over ownership so the database is DROPPED — and therefore closed —
// when the exercise ends. Clippy cannot see that Drop is the point (#3126:
// the body only needs `&Database` now that services borrow shared).
#[allow(clippy::needless_pass_by_value)]
fn exercise_storage_commit_budget(database: Database) {
    let mut graph = database
        .graph(branch("default"), space("default"))
        .expect("graph service opens");
    let name = graph_name("wide");
    graph.create_graph(name.clone()).expect("graph created");

    let ids: Vec<String> = (0..2_501).map(|index| format!("n{index}")).collect();
    let node_items: Vec<(GraphNodeId, GraphNodeData)> = ids
        .iter()
        .map(|id| (node_id(id), GraphNodeData::default()))
        .collect();
    // 2,500 edges = 5,000 row mutations: over the 4,096 budget if they
    // ever landed in one commit.
    let edge_items: Vec<(GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData)> =
        ids[1..].iter().map(|id| edge("n0", "e", id, 1.0)).collect();

    // Default chunking succeeds and spreads the work over many commits.
    let outcome = graph
        .bulk_insert(&name, &node_items, &edge_items, None)
        .expect("default chunking ingests");
    assert_eq!(outcome.edges_inserted(), 2_500);
    assert!(outcome.commits() > 2, "default chunking splits commits");

    // An absurd explicit chunk size clamps instead of building a commit
    // the storage layer refuses.
    let outcome = graph
        .bulk_insert(&name, &node_items, &edge_items, Some(250_000))
        .expect("clamped chunking ingests");
    assert!(outcome.commits() >= 4, "oversized chunk request clamps");
}

#[test]
fn bulk_insert_refuses_before_committing_in_cache_and_durable_modes() {
    run_database_modes(exercise_bulk_refusals);
}

// Takes `Database` by value on purpose: the `fn(Database)` harness contract
// hands over ownership so the database is DROPPED — and therefore closed —
// when the exercise ends. Clippy cannot see that Drop is the point (#3126:
// the body only needs `&Database` now that services borrow shared).
#[allow(clippy::needless_pass_by_value)]
fn exercise_bulk_refusals(database: Database) {
    let mut graph = database
        .graph(branch("default"), space("default"))
        .expect("graph service opens");
    let name = graph_name("bulk");
    graph.create_graph(name.clone()).expect("graph created");

    // Missing graph refuses.
    let error = graph
        .bulk_insert(&graph_name("ghost"), &nodes(&["a"]), &[], None)
        .expect_err("missing graph");
    assert_eq!(error.code(), "not_found.engine.graph");

    // An edge naming an endpoint that neither exists nor arrives in the
    // call refuses up front — no chunk commits, no partial state.
    let error = graph
        .bulk_insert(
            &name,
            &nodes(&["a", "b"]),
            &[edge("a", "e", "ghost", 1.0)],
            None,
        )
        .expect_err("missing endpoint");
    assert_eq!(error.code(), "invalid_argument.engine.graph_edge_endpoint");
    assert!(
        graph
            .get_node(&name, &node_id("a"))
            .expect("read")
            .is_none(),
        "validation failure commits nothing"
    );

    // Frozen-ontology enforcement applies to bulk exactly like single
    // upserts: an undeclared object type refuses before any commit.
    graph
        .define_object_type(
            &name,
            strata_engine::GraphObjectTypeDef::new(
                strata_engine::GraphTypeName::new("Document").expect("type name"),
                std::iter::empty::<(String, strata_engine::GraphPropertyDef)>(),
            )
            .expect("object type"),
        )
        .expect("type defined");
    graph.freeze_ontology(&name).expect("ontology frozen");
    let typed = vec![(
        node_id("typed"),
        GraphNodeData::default()
            .with_object_type(strata_engine::GraphTypeName::new("Undeclared").expect("type name")),
    )];
    let error = graph
        .bulk_insert(&name, &typed, &[], None)
        .expect_err("undeclared type");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_node_type"
    );
    assert!(
        graph
            .get_node(&name, &node_id("typed"))
            .expect("read")
            .is_none(),
        "ontology refusal commits nothing"
    );

    // Bindings ingested in bulk feed the same reverse map the delete
    // policies consume.
    let target = GraphBindingTarget::new(
        GraphBindingPrimitive::Kv,
        None,
        ProductSpace::new("default").expect("space"),
        "doc-bulk",
    )
    .expect("target");
    graph
        .bulk_insert(
            &name,
            &[(
                node_id("bound"),
                GraphNodeData::new(None, Some(GraphEntityBinding::new(target.clone()))),
            )],
            &[],
            None,
        )
        .expect("bound ingest");
    let outcome = graph
        .apply_binding_delete_policy(&target, GraphDeletePolicy::Cascade)
        .expect("cascade applies");
    assert_eq!(outcome.nodes_affected(), 1);
}

/// The items-per-chunk size clamps to 800 (#3214).
///
/// A caller's chunk request is documented to clamp at 800 so one chunk fits
/// one storage commit. That number is only observable through the resulting
/// commit count: with an over-max chunk request, 800 nodes still fit one
/// commit while 801 need two — which is exactly what an 800-item cap means,
/// and an off-by-one in the cap would move the boundary. Cache mode alone,
/// since the clamp is mode-independent arithmetic on the request.
#[test]
fn bulk_insert_clamps_chunk_size_to_800() {
    let bulk_nodes = |count: usize| -> Vec<(GraphNodeId, GraphNodeData)> {
        (0..count)
            .map(|index| (node_id(&format!("n{index}")), GraphNodeData::default()))
            .collect()
    };

    let database = open_cache_database().expect("cache open succeeds");
    let mut graph = database
        .graph(branch("default"), space("default"))
        .expect("graph service opens");
    let name = graph_name("clamp");
    graph.create_graph(name.clone()).expect("graph created");

    // A chunk far above the cap: 800 nodes fit one commit; 801 spill to two.
    let at = graph
        .bulk_insert(&name, &bulk_nodes(800), &[], Some(100_000))
        .expect("800-node ingest");
    assert_eq!(at.nodes_inserted(), 800);
    assert_eq!(
        at.commits(),
        1,
        "800 nodes fit one chunk when the request clamps to 800"
    );

    let over = graph
        .bulk_insert(&name, &bulk_nodes(801), &[], Some(100_000))
        .expect("801-node ingest");
    assert_eq!(over.nodes_inserted(), 801);
    assert_eq!(
        over.commits(),
        2,
        "801 nodes need a second chunk, so the cap is exactly 800"
    );
}

#[test]
fn bulk_replace_retires_the_old_binding_and_type_index_rows_in_cache_and_durable_modes() {
    run_database_modes(exercise_bulk_replace_indexes);
}

/// A bulk re-import that changes a node's binding or object type must
/// retire the derived rows of the old ones, exactly as `upsert_node` does:
/// the old target must no longer list the node, and the old type must no
/// longer index it — a stale index row would surface a ghost.
// Takes `Database` by value on purpose: the `fn(Database)` harness contract
// hands over ownership so the database is DROPPED — and therefore closed —
// when the exercise ends (#3126).
#[allow(clippy::needless_pass_by_value)]
fn exercise_bulk_replace_indexes(database: Database) {
    let mut graph = database
        .graph(branch("default"), space("default"))
        .expect("graph service opens");
    let name = graph_name("catalog");
    graph.create_graph(name.clone()).expect("graph created");
    let place = strata_engine::GraphTypeName::new("place").expect("type name");
    let shop = strata_engine::GraphTypeName::new("shop").expect("type name");
    for kind in [&place, &shop] {
        graph
            .define_object_type(
                &name,
                strata_engine::GraphObjectTypeDef::new(kind.clone(), std::iter::empty())
                    .expect("object type"),
            )
            .expect("type defined");
    }
    let target = |key: &str| {
        GraphBindingTarget::new(
            strata_engine::GraphBindingPrimitive::Kv,
            None,
            space("docs"),
            key,
        )
        .expect("binding target")
    };
    let bound_typed = |key: &str, kind: &strata_engine::GraphTypeName| {
        GraphNodeData::new(None, Some(GraphEntityBinding::new(target(key))))
            .with_object_type(kind.clone())
    };
    let bound_to = |graph: &strata_engine::GraphService<'_>, key: &str| -> Vec<String> {
        graph
            .bindings_for_entity(&target(key), None, 10)
            .expect("bindings read")
            .bindings()
            .iter()
            .map(|binding| binding.node_id().as_str().to_owned())
            .collect()
    };
    let typed_as = |graph: &strata_engine::GraphService<'_>,
                    kind: &strata_engine::GraphTypeName|
     -> Vec<String> {
        graph
            .nodes_by_type(&name, kind, None, 10)
            .expect("typed listing")
            .nodes()
            .iter()
            .map(|node| node.node_id().as_str().to_owned())
            .collect()
    };

    graph
        .bulk_insert(
            &name,
            &[(node_id("a"), bound_typed("doc-1", &place))],
            &[],
            None,
        )
        .expect("first import");
    assert_eq!(bound_to(&graph, "doc-1"), ["a"]);
    assert_eq!(typed_as(&graph, &place), ["a"]);

    // Re-import `a` bound elsewhere and typed differently.
    graph
        .bulk_insert(
            &name,
            &[(node_id("a"), bound_typed("doc-2", &shop))],
            &[],
            None,
        )
        .expect("re-import with a new binding and type");
    assert!(
        bound_to(&graph, "doc-1").is_empty(),
        "the old binding is retired"
    );
    assert_eq!(bound_to(&graph, "doc-2"), ["a"]);
    assert!(
        typed_as(&graph, &place).is_empty(),
        "the old type index row is retired"
    );
    assert_eq!(typed_as(&graph, &shop), ["a"]);

    // Re-import `a` with neither: both derived rows go.
    graph
        .bulk_insert(
            &name,
            &[(node_id("a"), GraphNodeData::default())],
            &[],
            None,
        )
        .expect("re-import plain");
    assert!(bound_to(&graph, "doc-2").is_empty());
    assert!(typed_as(&graph, &shop).is_empty());
    assert!(graph
        .get_node(&name, &node_id("a"))
        .expect("read")
        .is_some());
}
