//! #3477: a graph that `bulk_insert` built in bounded chunks must be
//! deletable through the public API, whatever its size — deletion may span
//! several commits, disappears atomically at the first, and can be resumed.

mod common;

use strata_engine::{
    Database, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId,
    GraphObjectTypeDef, GraphService, GraphTypeName,
};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn graph_service(database: &mut Database) -> GraphService<'_> {
    database
        .graph(branch("default"), space("probe"))
        .expect("graph service opens")
}

fn name(value: &str) -> GraphName {
    GraphName::new(value).expect("valid graph")
}

fn node(index: usize) -> GraphNodeId {
    GraphNodeId::new(format!("n:{index}")).expect("valid node id")
}

/// The issue's reproducer: a ring of `count` nodes and `count` edges,
/// imported in chunks of 512.
fn import_ring(graph: &mut GraphService<'_>, graph_name: &GraphName, count: usize) {
    graph
        .create_graph(graph_name.clone())
        .expect("graph created");
    let nodes: Vec<_> = (0..count)
        .map(|index| (node(index), GraphNodeData::default()))
        .collect();
    let next = GraphEdgeType::new("next").expect("edge type");
    let edges: Vec<_> = (0..count)
        .map(|index| {
            (
                node(index),
                next.clone(),
                node((index + 1) % count),
                GraphEdgeData::default(),
            )
        })
        .collect();
    graph
        .bulk_insert(graph_name, &nodes, &edges, Some(512))
        .expect("bulk import succeeds in chunks");
}

#[test]
fn a_graph_larger_than_one_commit_can_still_be_deleted_in_cache_and_durable_modes() {
    run_database_modes(exercise_large_delete);
}

fn exercise_large_delete(mut database: Database) {
    let places = name("places");
    let mut graph = graph_service(&mut database);
    // 3,000 nodes + 3,000 edges = 9,000 rows once reverse edges count, far
    // past the 4,096-mutation commit limit that one-shot deletion hit.
    import_ring(&mut graph, &places, 3_000);
    let before = graph
        .graph_info(&places)
        .expect("info reads")
        .expect("graph exists");
    assert_eq!((before.node_count(), before.edge_count()), (3_000, 3_000));

    let outcome = graph
        .delete_graph(&places, true)
        .expect("a graph that could be imported can be deleted");
    assert!(outcome.deleted());
    let last = outcome.commit().expect("deletion commits");
    assert!(last.version() > before.updated_version());
    // The acknowledgement covers the whole deletion, not its last commit:
    // every node and forward edge plus the graph's own row.
    assert_eq!(last.delete_count(), 3_000 + 3_000 + 1);
    assert_eq!(last.put_count(), 0);

    // Gone: every read refuses, the listing omits it, and a fresh graph of
    // the same name starts empty — no row of the old one survives.
    assert!(graph.graph_info(&places).expect("reads").is_none());
    assert_eq!(
        graph
            .get_node(&places, &node(0))
            .expect_err("no graph")
            .code(),
        "not_found.engine.graph"
    );
    assert!(graph
        .list_graphs(None, 10)
        .expect("lists")
        .graphs()
        .is_empty());
    let (again, _) = graph.create_graph(places.clone()).expect("recreated");
    assert_eq!((again.node_count(), again.edge_count()), (0, 0));
    assert!(graph
        .list_nodes(&places, None, None, 10)
        .expect("lists nodes")
        .nodes()
        .is_empty());
    assert!(graph
        .list_edges(&places, None, 10)
        .expect("lists edges")
        .edges()
        .is_empty());
    // Re-adding two old endpoints without edges must surface nothing from
    // either adjacency direction: forward and reverse rows both went.
    for index in [0, 1] {
        graph
            .upsert_node(&places, node(index), GraphNodeData::default())
            .expect("node re-added");
    }
    for (index, direction) in [
        (0, strata_engine::GraphDirection::Outgoing),
        (1, strata_engine::GraphDirection::Incoming),
        (0, strata_engine::GraphDirection::Both),
        (1, strata_engine::GraphDirection::Both),
    ] {
        assert!(
            graph
                .neighbors(&places, &node(index), direction, None, None, 10)
                .expect("neighbors in the new graph")
                .neighbors()
                .is_empty(),
            "n:{index} {direction:?}: no adjacency row of the old graph survives"
        );
    }
}

#[test]
fn a_small_graph_still_deletes_in_one_commit_in_cache_and_durable_modes() {
    run_database_modes(exercise_small_delete);
}

/// A graph that fits one commit deletes exactly as it always did: one
/// commit, whose counts name every node and edge and the graph's own row.
/// (The exact chunk boundary is pinned in-crate, where the chunk size is
/// visible.)
fn exercise_small_delete(mut database: Database) {
    let mut graph = graph_service(&mut database);
    let g = name("small");
    import_ring(&mut graph, &g, 3);
    let before = graph
        .graph_info(&g)
        .expect("info reads")
        .expect("exists")
        .updated_version();
    let outcome = graph.delete_graph(&g, true).expect("deleted");
    let last = outcome.commit().expect("deletion commits");
    assert_eq!(last.version().as_u64() - before.as_u64(), 1, "one commit");
    assert_eq!(last.delete_count(), 3 + 3 + 1);
    assert!(graph.graph_info(&g).expect("reads").is_none());
}

#[test]
fn deleting_a_graph_takes_its_type_index_rows_too_in_cache_and_durable_modes() {
    run_database_modes(exercise_typed_delete);
}

/// Every derived row class goes with the graph — here the node-type index,
/// which no listing of a recreated graph may still see.
fn exercise_typed_delete(mut database: Database) {
    let mut graph = graph_service(&mut database);
    let g = name("typed");
    let place = GraphTypeName::new("place").expect("type name");
    let define = |graph: &mut GraphService<'_>| {
        graph
            .define_object_type(
                &g,
                GraphObjectTypeDef::new(place.clone(), std::iter::empty()).expect("object type"),
            )
            .expect("type defined");
    };
    graph.create_graph(g.clone()).expect("graph created");
    define(&mut graph);
    let typed: Vec<_> = (0..5)
        .map(|index| {
            (
                node(index),
                GraphNodeData::default().with_object_type(place.clone()),
            )
        })
        .collect();
    graph
        .bulk_insert(&g, &typed, &[], None)
        .expect("typed nodes");
    assert_eq!(
        graph
            .nodes_by_type(&g, &place, None, 10)
            .expect("typed listing")
            .nodes()
            .len(),
        5
    );

    assert!(graph.delete_graph(&g, true).expect("deleted").deleted());
    graph.create_graph(g.clone()).expect("recreated");
    define(&mut graph);
    assert!(
        graph
            .nodes_by_type(&g, &place, None, 10)
            .expect("typed listing of the new graph")
            .nodes()
            .is_empty(),
        "no type-index row of the old graph survives"
    );
    assert!(graph
        .list_nodes(&g, None, None, 10)
        .expect("lists")
        .nodes()
        .is_empty());
}
