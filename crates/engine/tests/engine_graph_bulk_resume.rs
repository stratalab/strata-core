//! #3464: a `bulk_insert` that spans more than one commit carries a durable
//! watermark between its first and last commits, so a caller can tell a
//! finished import from one cut short — and finishing is a re-run away.

mod common;

use strata_engine::{
    Database, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId, GraphService,
};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn graph_service(database: &mut Database) -> GraphService<'_> {
    database
        .graph(branch("default"), space("city"))
        .expect("graph service opens")
}

fn node(index: usize) -> GraphNodeId {
    GraphNodeId::new(format!("n:{index}")).expect("valid node id")
}

type Edge = (GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData);

/// A ring of `count` nodes and edges, the shape a city import has.
fn ring(count: usize) -> (Vec<(GraphNodeId, GraphNodeData)>, Vec<Edge>) {
    let street = GraphEdgeType::new("street").expect("edge type");
    let nodes = (0..count)
        .map(|index| (node(index), GraphNodeData::default()))
        .collect();
    let edges = (0..count)
        .map(|index| {
            (
                node(index),
                street.clone(),
                node((index + 1) % count),
                GraphEdgeData::default(),
            )
        })
        .collect();
    (nodes, edges)
}

#[test]
fn a_finished_import_is_never_pending_whatever_its_commit_count_in_cache_and_durable_modes() {
    run_database_modes(exercise_finished_imports);
}

fn exercise_finished_imports(mut database: Database) {
    let mut graph = graph_service(&mut database);
    // One commit (nodes only, one chunk): never marked, not even inside.
    let one = GraphName::new("one").expect("graph");
    graph.create_graph(one.clone()).expect("created");
    let (nodes, _) = ring(3);
    let outcome = graph
        .bulk_insert(&one, &nodes, &[], None)
        .expect("imported");
    assert_eq!(outcome.commits(), 1);
    let info = graph.graph_info(&one).expect("reads").expect("exists");
    assert!(!info.import_pending());
    assert_eq!(info.node_count(), 3);

    // Many commits: the watermark was set and cleared inside one call.
    let many = GraphName::new("many").expect("graph");
    graph.create_graph(many.clone()).expect("created");
    let (nodes, edges) = ring(50);
    let outcome = graph
        .bulk_insert(&many, &nodes, &edges, Some(8))
        .expect("imported");
    assert!(outcome.commits() > 2, "{} commits", outcome.commits());
    let info = graph.graph_info(&many).expect("reads").expect("exists");
    assert!(!info.import_pending(), "a finished import leaves no mark");
    assert_eq!((info.node_count(), info.edge_count()), (50, 50));
    assert_eq!(
        info.updated_version(),
        outcome.last_commit().expect("commits").version()
    );

    // Every version inside the import shows it pending; before and after, not.
    let last = outcome.last_commit().expect("commits").version().as_u64();
    let first = last + 1 - outcome.commits();
    for version in first..last {
        let then = graph
            .graph_info_at_version(&many, strata_core::CommitVersion::new(version))
            .expect("historical")
            .expect("existed");
        assert!(
            then.import_pending(),
            "version {version} is inside the import"
        );
    }
    assert!(!graph
        .graph_info_at_version(&many, strata_core::CommitVersion::new(first - 1))
        .expect("historical")
        .expect("existed")
        .import_pending());
}
