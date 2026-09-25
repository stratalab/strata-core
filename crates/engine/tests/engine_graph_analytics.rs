//! GA1/GA2 exit gates: adjacency snapshots built from storage at a
//! consistent read — correctness (isolated nodes, self-loops, weights,
//! edge types), temporal snapshots, determinism, budget refusals by
//! error code — and the exact algorithms computed over those snapshots.

mod common;

use std::collections::HashMap;

use strata_engine::{
    Database, GraphAdjacencyEdge, GraphAnalyticsBudget, GraphBfsOptions, GraphCdlpOptions,
    GraphDirection, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId,
    GraphPageRankOptions,
};

use common::{branch, open_cache_database, open_durable_database, space};

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

fn node_id(value: &str) -> GraphNodeId {
    GraphNodeId::new(value).expect("valid node id")
}

fn edge_type(value: &str) -> GraphEdgeType {
    GraphEdgeType::new(value).expect("valid edge type")
}

fn edge_data(weight: f64) -> GraphEdgeData {
    GraphEdgeData::new(weight, None).expect("edge data")
}

/// deps: a -links(1.0)-> b, a -cites(2.5)-> c, b -links(0.5)-> b (self
/// loop), plus isolated node `lone`.
fn seed_graph(graph: &mut strata_engine::GraphService<'_>, name: &GraphName) {
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["a", "b", "c", "lone"] {
        graph
            .upsert_node(name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    graph
        .upsert_edge(
            name,
            node_id("a"),
            edge_type("links"),
            node_id("b"),
            edge_data(1.0),
        )
        .expect("edge");
    graph
        .upsert_edge(
            name,
            node_id("a"),
            edge_type("cites"),
            node_id("c"),
            edge_data(2.5),
        )
        .expect("edge");
    graph
        .upsert_edge(
            name,
            node_id("b"),
            edge_type("links"),
            node_id("b"),
            edge_data(0.5),
        )
        .expect("self loop");
}

/// #3465: a count weight survives the stored record exactly and sums to an
/// exact distance, so a graph weighted in meters needs no parallel
/// property — through the real storage path in both modes.
#[test]
fn count_weights_survive_storage_and_sum_exactly_in_cache_and_durable_modes() {
    run_database_modes(exercise_count_weights);
}

fn exercise_count_weights(mut database: Database) {
    const HALF: u64 = 4_503_599_627_370_496; // 2^52
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("meters");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["a", "b", "c"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    // a -> b -> c: the two counts sum to the last exact count, 2^53 - 1.
    for (src, dst, count) in [("a", "b", HALF), ("b", "c", HALF - 1)] {
        graph
            .upsert_edge(
                &name,
                node_id(src),
                edge_type("road"),
                node_id(dst),
                GraphEdgeData::from_count(count, None).expect("count in range"),
            )
            .expect("edge");
    }

    // Read back through the stored record: the count, not a rounded float.
    let stored = graph
        .get_edge(&name, &node_id("a"), &edge_type("road"), &node_id("b"))
        .expect("edge read")
        .expect("edge present");
    assert_eq!(stored.data().weight_count(), Some(HALF));
    assert_eq!(
        stored.data(),
        &GraphEdgeData::from_count(HALF, None).expect("count in range")
    );

    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let a = index.node_index(&node_id("a")).expect("a indexed");
    let c = index.node_index(&node_id("c")).expect("c indexed");
    assert_eq!(index.outgoing(a)[0].weight_count(), Some(HALF));
    let sssp = index
        .sssp(&node_id("a"), GraphDirection::Outgoing)
        .expect("sssp runs");
    assert_eq!(
        sssp.distance_count(c),
        Some(GraphEdgeData::MAX_COUNT),
        "2^52 + (2^52 - 1) is exact"
    );
}

#[test]
fn adjacency_snapshot_reflects_visible_state_in_cache_and_durable_modes() {
    run_database_modes(exercise_snapshot_correctness);
}

fn exercise_snapshot_correctness(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");

    let error = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect_err("missing graph refuses");
    assert_eq!(error.code(), "not_found.engine.graph");

    seed_graph(&mut graph, &name);
    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");

    assert_eq!(index.graph().as_str(), "deps");
    assert_eq!(index.node_count(), 4, "isolated node included");
    assert_eq!(index.edge_count(), 3, "self loop counts once");
    let ids: Vec<&str> = index.node_ids().iter().map(GraphNodeId::as_str).collect();
    assert_eq!(ids, vec!["a", "b", "c", "lone"], "ascending id order");

    let a = index.node_index(&node_id("a")).expect("a indexed");
    let b = index.node_index(&node_id("b")).expect("b indexed");
    let c = index.node_index(&node_id("c")).expect("c indexed");
    let lone = index.node_index(&node_id("lone")).expect("lone indexed");

    // a's outgoing edges, sorted by (edge type, neighbor): cites < links.
    let out = index.outgoing(a);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].neighbor(), c);
    assert_eq!(
        index
            .edge_type_name(out[0].edge_type())
            .expect("type")
            .as_str(),
        "cites"
    );
    assert!((out[0].weight() - 2.5).abs() < f64::EPSILON);
    assert_eq!(out[1].neighbor(), b);
    assert!((out[1].weight() - 1.0).abs() < f64::EPSILON);

    // b: one incoming from a plus the self loop in both directions.
    assert_eq!(index.outgoing(b).len(), 1);
    assert_eq!(index.outgoing(b)[0].neighbor(), b);
    let incoming: Vec<usize> = index
        .incoming(b)
        .iter()
        .map(GraphAdjacencyEdge::neighbor)
        .collect();
    assert_eq!(incoming, vec![a, b]);

    // Isolated node has no edges; the index stays queryable at the edges.
    assert!(index.outgoing(lone).is_empty());
    assert!(index.incoming(lone).is_empty());
    assert!(index.outgoing(999).is_empty(), "out of range is empty");
    assert_eq!(index.node_index(&node_id("ghost")), None);

    // Determinism: a second build over the same state is equal.
    let again = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index rebuilds");
    assert_eq!(again, index);
}

#[test]
fn adjacency_snapshots_are_temporal_in_cache_and_durable_modes() {
    run_database_modes(exercise_temporal_snapshots);
}

fn exercise_temporal_snapshots(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    seed_graph(&mut graph, &name);

    let before = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let seeded_version = graph
        .graph_info(&name)
        .expect("info reads")
        .expect("graph visible")
        .updated_version();

    // Mutate: delete the self loop, add a node and an edge.
    graph
        .delete_edge(&name, &node_id("b"), &edge_type("links"), &node_id("b"))
        .expect("delete edge");
    graph
        .upsert_node(&name, node_id("d"), GraphNodeData::default())
        .expect("node");
    graph
        .upsert_edge(
            &name,
            node_id("d"),
            edge_type("links"),
            node_id("a"),
            edge_data(4.0),
        )
        .expect("edge");

    let latest = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    assert_eq!(latest.node_count(), 5);
    assert_eq!(latest.edge_count(), 3);
    let b = latest.node_index(&node_id("b")).expect("b indexed");
    assert!(latest.outgoing(b).is_empty(), "self loop gone");

    // The historical snapshot equals the one built before the mutations.
    let historical = graph
        .adjacency_index_at_version(&name, &GraphAnalyticsBudget::default(), seeded_version)
        .expect("historical index builds");
    assert_eq!(historical, before, "time-traveled snapshot matches");
}

#[test]
fn adjacency_budget_refusals_surface_by_code_in_cache_and_durable_modes() {
    run_database_modes(exercise_budget_refusals);
}

fn exercise_budget_refusals(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    seed_graph(&mut graph, &name);

    let error = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::new(2, 100))
        .expect_err("node budget refuses");
    assert_eq!(
        error.code(),
        "resource_exhausted.engine.graph_analytics_budget"
    );

    let error = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::new(100, 2))
        .expect_err("edge budget refuses");
    assert_eq!(
        error.code(),
        "resource_exhausted.engine.graph_analytics_budget"
    );

    // A budget that fits succeeds.
    assert!(graph
        .adjacency_index(&name, &GraphAnalyticsBudget::new(4, 3))
        .is_ok());
}

#[test]
fn exact_algorithms_run_over_snapshots_in_cache_and_durable_modes() {
    run_database_modes(exercise_exact_algorithms);
}

/// net: a -1-> b -2-> c, a -10-> c, c -1-> d, plus isolated node `lone`.
fn exercise_exact_algorithms(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("net");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["a", "b", "c", "d", "lone"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    for (src, dst, weight) in [
        ("a", "b", 1.0),
        ("b", "c", 2.0),
        ("a", "c", 10.0),
        ("c", "d", 1.0),
    ] {
        graph
            .upsert_edge(
                &name,
                node_id(src),
                edge_type("e"),
                node_id(dst),
                edge_data(weight),
            )
            .expect("edge");
    }

    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");

    // Components: {a, b, c, d} joined regardless of direction, lone apart.
    let wcc = index.wcc();
    assert_eq!(wcc.component_count(), 2);
    let connected = wcc.component(at("a")).expect("labeled");
    for id in ["b", "c", "d"] {
        assert_eq!(wcc.component(at(id)), Some(connected));
    }
    assert_ne!(wcc.component(at("lone")), Some(connected));

    // Clustering: a and b close the a-b-c triangle; c has one closed
    // pair of three; d and lone have fewer than two neighbors.
    let lcc = index.lcc();
    assert!((lcc.coefficient(at("a")).expect("scored") - 1.0).abs() < 1e-10);
    assert!((lcc.coefficient(at("b")).expect("scored") - 1.0).abs() < 1e-10);
    assert!((lcc.coefficient(at("c")).expect("scored") - 1.0 / 3.0).abs() < 1e-10);
    assert!(lcc.coefficient(at("d")).expect("scored").abs() < 1e-10);
    assert!(lcc.coefficient(at("lone")).expect("scored").abs() < 1e-10);

    // Shortest paths from a: the two-hop route to c beats the direct
    // edge; lone stays unreachable; a missing source refuses by code.
    let sssp = index
        .sssp(&node_id("a"), GraphDirection::Outgoing)
        .expect("sssp runs");
    assert_eq!(sssp.distance(at("a")), Some(0.0));
    assert_eq!(sssp.distance(at("b")), Some(1.0));
    assert_eq!(sssp.distance(at("c")), Some(3.0));
    assert_eq!(sssp.distance(at("d")), Some(4.0));
    assert_eq!(sssp.distance(at("lone")), None);
    assert_eq!(sssp.reachable_count(), 4);
    let error = index
        .sssp(&node_id("ghost"), GraphDirection::Outgoing)
        .expect_err("missing source refuses");
    assert_eq!(error.code(), "not_found.engine.graph_node");

    let seeded_version = graph
        .graph_info(&name)
        .expect("info reads")
        .expect("graph visible")
        .updated_version();

    // Mutate: cut c-d and bridge d to lone with a negative weight.
    graph
        .delete_edge(&name, &node_id("c"), &edge_type("e"), &node_id("d"))
        .expect("delete edge");
    graph
        .upsert_edge(
            &name,
            node_id("d"),
            edge_type("e"),
            node_id("lone"),
            edge_data(-2.0),
        )
        .expect("edge");

    // Latest state: the partition regrouped, and the negative weight
    // makes shortest paths refuse by code.
    let latest = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let latest_wcc = latest.wcc();
    assert_eq!(latest_wcc.component_count(), 2);
    let d = latest.node_index(&node_id("d")).expect("d indexed");
    let lone = latest.node_index(&node_id("lone")).expect("lone indexed");
    let a = latest.node_index(&node_id("a")).expect("a indexed");
    assert_eq!(latest_wcc.component(d), latest_wcc.component(lone));
    assert_ne!(latest_wcc.component(d), latest_wcc.component(a));
    let error = latest
        .sssp(&node_id("a"), GraphDirection::Outgoing)
        .expect_err("negative weight refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_negative_weight"
    );

    // The historical snapshot still answers with the seeded state.
    let historical = graph
        .adjacency_index_at_version(&name, &GraphAnalyticsBudget::default(), seeded_version)
        .expect("historical index builds");
    assert_eq!(historical.wcc(), wcc);
    assert_eq!(
        historical
            .sssp(&node_id("a"), GraphDirection::Outgoing)
            .expect("historical sssp runs"),
        sssp
    );
}

#[test]
fn iterative_algorithms_run_over_snapshots_in_cache_and_durable_modes() {
    run_database_modes(exercise_iterative_algorithms);
}

/// web: bidirectional triangles {a, b, c} and {x, y, z} with bridge c -> x.
fn exercise_iterative_algorithms(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("web");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["a", "b", "c", "x", "y", "z"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    let mut link = |src: &str, dst: &str| {
        graph
            .upsert_edge(
                &name,
                node_id(src),
                edge_type("e"),
                node_id(dst),
                edge_data(1.0),
            )
            .expect("edge");
    };
    for (left, right) in [
        ("a", "b"),
        ("b", "c"),
        ("a", "c"),
        ("x", "y"),
        ("y", "z"),
        ("x", "z"),
    ] {
        link(left, right);
        link(right, left);
    }
    link("c", "x");

    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");

    // Communities settle within each triangle.
    let cdlp = index.cdlp(&GraphCdlpOptions::default());
    assert_eq!(cdlp.label(at("a")), cdlp.label(at("b")));
    assert_eq!(cdlp.label(at("a")), cdlp.label(at("c")));
    assert_eq!(cdlp.label(at("x")), cdlp.label(at("y")));
    assert_eq!(cdlp.label(at("x")), cdlp.label(at("z")));

    // PageRank conserves mass, and the bridge makes x the top node.
    let pagerank = index.pagerank(&GraphPageRankOptions::default());
    let sum: f64 = pagerank.ranks().iter().sum();
    assert!((sum - 1.0).abs() < 1e-6, "mass leaked: {sum}");
    let x_rank = pagerank.rank(at("x")).expect("ranked");
    for id in ["a", "b", "c", "y", "z"] {
        assert!(x_rank > pagerank.rank(at(id)).expect("ranked"));
    }

    // Personalized PageRank concentrates mass on the seed's side, and
    // an empty personalization refuses by code.
    let seeded = index
        .personalized_pagerank(
            &GraphPageRankOptions::default(),
            &HashMap::from([(node_id("a"), 1.0)]),
        )
        .expect("ppr runs");
    assert!(seeded.rank(at("a")).expect("ranked") > seeded.rank(at("z")).expect("ranked"));
    let error = index
        .personalized_pagerank(&GraphPageRankOptions::default(), &HashMap::new())
        .expect_err("empty personalization refuses");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.graph_personalization"
    );

    let seeded_version = graph
        .graph_info(&name)
        .expect("info reads")
        .expect("graph visible")
        .updated_version();

    // Cut the bridge: the latest ranking changes, the historical
    // snapshot reproduces every pre-mutation result exactly.
    graph
        .delete_edge(&name, &node_id("c"), &edge_type("e"), &node_id("x"))
        .expect("delete edge");
    let latest = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    assert_ne!(latest.pagerank(&GraphPageRankOptions::default()), pagerank);
    let historical = graph
        .adjacency_index_at_version(&name, &GraphAnalyticsBudget::default(), seeded_version)
        .expect("historical index builds");
    assert_eq!(
        historical.pagerank(&GraphPageRankOptions::default()),
        pagerank
    );
    assert_eq!(historical.cdlp(&GraphCdlpOptions::default()), cdlp);
}

#[test]
fn cdlp_converges_a_star_to_a_single_community_instead_of_oscillating() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("star");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["h", "a", "b", "c"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    // Directed star h -> {a, b, c}. The default `Both` direction makes the
    // adjacency symmetric, i.e. an undirected star.
    for leaf in ["a", "b", "c"] {
        graph
            .upsert_edge(
                &name,
                node_id("h"),
                edge_type("e"),
                node_id(leaf),
                edge_data(1.0),
            )
            .expect("edge");
    }

    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");

    // #3024: propagation is SYNCHRONOUS (the LDBC definition), and sync
    // label propagation oscillates with period 2 on a star — the iteration
    // bound defines the deterministic answer, identically on every
    // conformant platform. At the default (even) bound the leaves share one
    // label and the hub holds its own. The old asynchronous sweep converged
    // the star to a single label, but its results depended on the internal
    // sweep order — untenable as a user-facing contract.
    let cdlp = index.cdlp(&GraphCdlpOptions::default());
    assert_eq!(cdlp.label(at("a")), Some(at("a")), "even-bound leaf state");
    assert_eq!(cdlp.label(at("b")), Some(at("a")), "leaves share one label");
    assert_eq!(cdlp.label(at("c")), Some(at("a")), "leaves share one label");
    assert_eq!(
        cdlp.label(at("h")),
        Some(at("h")),
        "the hub alternates against the leaves and holds its own label at the bound"
    );
}

#[test]
fn cdlp_follows_the_majority_label_and_settles_over_multiple_sweeps() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("majority");
    graph.create_graph(name.clone()).expect("graph created");
    // Node ids are index-sorted, so names fix the initial labels: the "b"
    // triangle takes the lowest labels, the "y" triangle higher ones, "zz" is
    // the bridge (highest), and "aa" is a pendant on the bridge (lowest).
    for id in ["aa", "b0", "b1", "b2", "y0", "y1", "y2", "zz"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    let mut link = |src: &str, dst: &str| {
        graph
            .upsert_edge(
                &name,
                node_id(src),
                edge_type("e"),
                node_id(dst),
                edge_data(1.0),
            )
            .expect("edge");
    };
    // Two triangles (each collapses to one community) ...
    for (a, b) in [("b0", "b1"), ("b1", "b2"), ("b2", "b0")] {
        link(a, b);
    }
    for (a, b) in [("y0", "y1"), ("y1", "y2"), ("y2", "y0")] {
        link(a, b);
    }
    // ... bridged by `zz`, which has three edges into the higher `y` community
    // and only one into the lower `b` community, plus a pendant `aa`.
    for dst in ["y0", "y1", "y2", "b0", "aa"] {
        link("zz", dst);
    }

    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");
    let cdlp = index.cdlp(&GraphCdlpOptions::default());

    // #3024 (synchronous propagation): the triangle held together by a
    // strict internal majority stays one community — a run that only
    // tracked the minimum neighbor label would merge everything — while the
    // bridge-and-pendant appendage oscillates with period 2, so the (even)
    // default iteration bound defines its deterministic state: the bridge
    // sits with the y-majority side and the two triangles stay distinct.
    assert_eq!(
        cdlp.label(at("b0")),
        cdlp.label(at("b1")),
        "the majority-held triangle stays one community"
    );
    assert_eq!(cdlp.label(at("b1")), cdlp.label(at("b2")));
    assert_ne!(
        cdlp.label(at("b0")),
        cdlp.label(at("y0")),
        "the two triangles stay distinct communities"
    );
    assert_eq!(
        cdlp.label(at("zz")),
        cdlp.label(at("y0")),
        "the bridge sits with its majority side at the bound"
    );
    assert_ne!(
        cdlp.label(at("zz")),
        cdlp.label(at("b0")),
        "the bridge does not join the single-vote minority"
    );
}

#[test]
fn traversal_runs_over_snapshots_in_cache_and_durable_modes() {
    run_database_modes(exercise_traversal);
}

/// paths: a -e-> b, b -e-> c, b -f-> d, plus isolated node `lone`.
fn exercise_traversal(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("paths");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["a", "b", "c", "d", "lone"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    for (src, kind, dst) in [("a", "e", "b"), ("b", "e", "c"), ("b", "f", "d")] {
        graph
            .upsert_edge(
                &name,
                node_id(src),
                edge_type(kind),
                node_id(dst),
                edge_data(1.0),
            )
            .expect("edge");
    }

    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");

    // Breadth-first from a: level order, depths, isolated node untouched.
    let bfs = index
        .bfs(&node_id("a"), &GraphBfsOptions::default())
        .expect("bfs runs");
    assert_eq!(bfs.visited(), &[at("a"), at("b"), at("c"), at("d")]);
    assert_eq!(bfs.depth(at("a")), Some(0));
    assert_eq!(bfs.depth(at("b")), Some(1));
    assert_eq!(bfs.depth(at("c")), Some(2));
    assert_eq!(bfs.depth(at("d")), Some(2));
    assert_eq!(bfs.depth(at("lone")), None);
    assert_eq!(bfs.edges().len(), 3, "tree edges only");

    // The edge-type restriction applies at every hop.
    let filtered = index
        .bfs(
            &node_id("a"),
            &GraphBfsOptions::new(
                10,
                None,
                Some(vec![edge_type("e")]),
                GraphDirection::Outgoing,
            ),
        )
        .expect("bfs runs");
    assert_eq!(filtered.visited(), &[at("a"), at("b"), at("c")]);

    // A start node outside the snapshot refuses by code.
    let error = index
        .bfs(&node_id("ghost"), &GraphBfsOptions::default())
        .expect_err("missing start refuses");
    assert_eq!(error.code(), "not_found.engine.graph_node");

    // Degree per direction.
    assert_eq!(index.degree(at("b"), GraphDirection::Outgoing), 2);
    assert_eq!(index.degree(at("b"), GraphDirection::Incoming), 1);
    assert_eq!(index.degree(at("b"), GraphDirection::Both), 3);
    assert_eq!(index.degree(at("lone"), GraphDirection::Both), 0);

    // Subgraph keeps only edges among the selected nodes.
    let subgraph = index.subgraph(&[node_id("a"), node_id("b"), node_id("d")]);
    assert_eq!(subgraph.node_count(), 3);
    assert_eq!(subgraph.edge_count(), 2, "a->b and b->d survive");

    let seeded_version = graph
        .graph_info(&name)
        .expect("info reads")
        .expect("graph visible")
        .updated_version();

    // Cut b -f-> d: the latest traversal shrinks, the historical
    // snapshot reproduces the original walk exactly.
    graph
        .delete_edge(&name, &node_id("b"), &edge_type("f"), &node_id("d"))
        .expect("delete edge");
    let latest = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let latest_bfs = latest
        .bfs(&node_id("a"), &GraphBfsOptions::default())
        .expect("bfs runs");
    assert_eq!(latest_bfs.visited().len(), 3);
    let historical = graph
        .adjacency_index_at_version(&name, &GraphAnalyticsBudget::default(), seeded_version)
        .expect("historical index builds");
    assert_eq!(
        historical
            .bfs(&node_id("a"), &GraphBfsOptions::default())
            .expect("historical bfs runs"),
        bfs
    );
    assert_eq!(
        historical.subgraph(&[node_id("a"), node_id("b"), node_id("d")]),
        subgraph
    );
}

#[test]
fn bfs_reports_truncation_and_drops_edges_to_unvisited_nodes() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("star");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["h", "a", "b", "c"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    for leaf in ["a", "b", "c"] {
        graph
            .upsert_edge(
                &name,
                node_id("h"),
                edge_type("e"),
                node_id(leaf),
                edge_data(1.0),
            )
            .expect("edge");
    }
    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");

    // A node cap of 1 visits only the hub. Its three tree edges were discovered
    // but the leaves were never visited: the result must report truncation and
    // must not emit edges that reference an unvisited node.
    let capped = index
        .bfs(
            &node_id("h"),
            &GraphBfsOptions::new(100, Some(1), None, GraphDirection::Both),
        )
        .expect("bfs runs");
    assert_eq!(capped.visited(), &[at("h")]);
    assert!(
        capped.truncated(),
        "a node cap that stops mid-traversal is truncated"
    );
    assert!(
        capped.edges().is_empty(),
        "no edge may reference a node that was not visited: {:?}",
        capped.edges()
    );

    // The uncapped traversal completes and is not truncated.
    let full = index
        .bfs(
            &node_id("h"),
            &GraphBfsOptions::new(100, None, None, GraphDirection::Both),
        )
        .expect("bfs runs");
    assert_eq!(full.visited().len(), 4);
    assert!(!full.truncated(), "a complete traversal is not truncated");
    assert_eq!(full.edges().len(), 3);

    // A depth cap that stops before every reachable node is also truncated,
    // and emits no edge to a node it never expanded to.
    let shallow = index
        .bfs(
            &node_id("h"),
            &GraphBfsOptions::new(0, None, None, GraphDirection::Both),
        )
        .expect("bfs runs");
    assert_eq!(
        shallow.visited(),
        &[at("h")],
        "depth 0 visits only the start"
    );
    assert!(
        shallow.truncated(),
        "a depth cap with unreached neighbors is truncated"
    );
    assert!(shallow.edges().is_empty());
}

#[test]
fn bfs_at_depth_cap_ignores_neighbors_behind_filtered_out_edges() {
    // h -[keep]-> a -[skip]-> b. A traversal restricted to `keep` reaches only
    // {h, a}; `b` is reachable solely through the excluded `skip` edge. At the
    // depth cap on `a`, its one unseen neighbor `b` sits behind a filtered-out
    // edge, so the restricted traversal is complete and must NOT be truncated.
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("filtered");
    graph.create_graph(name.clone()).expect("graph created");
    for id in ["h", "a", "b"] {
        graph
            .upsert_node(&name, node_id(id), GraphNodeData::default())
            .expect("node");
    }
    graph
        .upsert_edge(
            &name,
            node_id("h"),
            edge_type("keep"),
            node_id("a"),
            edge_data(1.0),
        )
        .expect("edge");
    graph
        .upsert_edge(
            &name,
            node_id("a"),
            edge_type("skip"),
            node_id("b"),
            edge_data(1.0),
        )
        .expect("edge");
    let index = graph
        .adjacency_index(&name, &GraphAnalyticsBudget::default())
        .expect("index builds");
    let at = |id: &str| index.node_index(&node_id(id)).expect("node indexed");

    let filtered = index
        .bfs(
            &node_id("h"),
            &GraphBfsOptions::new(
                1,
                None,
                Some(vec![edge_type("keep")]),
                GraphDirection::Outgoing,
            ),
        )
        .expect("bfs runs");
    assert_eq!(filtered.visited(), &[at("h"), at("a")]);
    assert!(
        !filtered.truncated(),
        "a node behind a filtered-out edge is not reachable, so the restricted \
         traversal is complete: {filtered:?}"
    );
}
