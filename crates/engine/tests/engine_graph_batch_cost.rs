//! #3472: a graph batch costs what it touches, not the graph. `batch_write`
//! used to load every visible node and edge before looking at its first
//! operation, so a one-edge closure over a city graph materialized the city.
//! Now each operation reads the rows it needs — endpoints and the edge by
//! point read, a deleted node's adjacency by its own two prefixes — and the
//! batch's earlier operations by overlay. These tests pin the scan verbs'
//! row count per batch shape against the testkit counter; the overlay's
//! semantics are pinned without the testkit in `engine_graph_batch_overlay.rs`.

#![cfg(feature = "testkit")]

mod common;

use strata_engine::{
    Database, GraphBatchOperation, GraphBatchWrite, GraphEdgeData, GraphEdgeType, GraphName,
    GraphNodeData, GraphNodeId, GraphService,
};

use common::{branch, open_cache_database, space};

const NODE_COUNT: usize = 1_000;
/// Hub edges reach the first half of the chain: the hub's out-degree and a
/// chain node's in-degree are both known exactly.
const HUB_TARGETS: usize = 500;

fn graph_name() -> GraphName {
    GraphName::new("city").expect("graph name")
}

fn node(id: &str) -> GraphNodeId {
    GraphNodeId::new(id).expect("node id")
}

fn chain_node(index: usize) -> GraphNodeId {
    node(&format!("n{index:04}"))
}

fn road() -> GraphEdgeType {
    GraphEdgeType::new("road").expect("edge type")
}

fn open_graph(db: &Database) -> GraphService<'_> {
    db.graph(branch("default"), space("default"))
        .expect("graph opens")
}

/// A chain `n0000 -> n0001 -> … -> n0999` plus a hub with an edge to each of
/// the first `HUB_TARGETS` chain nodes: every chain node past the head has
/// one incoming chain edge, one outgoing chain edge (except the tail), and
/// the first half has one incoming hub edge besides.
fn seed_city(graph: &mut GraphService<'_>) {
    graph.create_graph(graph_name()).expect("graph create");
    let mut nodes: Vec<(GraphNodeId, GraphNodeData)> = (0..NODE_COUNT)
        .map(|index| (chain_node(index), GraphNodeData::default()))
        .collect();
    nodes.push((node("hub"), GraphNodeData::default()));
    let mut edges: Vec<(GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData)> = (1..NODE_COUNT)
        .map(|index| {
            (
                chain_node(index - 1),
                road(),
                chain_node(index),
                GraphEdgeData::default(),
            )
        })
        .collect();
    edges.extend((0..HUB_TARGETS).map(|index| {
        (
            node("hub"),
            road(),
            chain_node(index),
            GraphEdgeData::default(),
        )
    }));
    graph
        .bulk_insert(&graph_name(), &nodes, &edges, None)
        .expect("city ingest");
}

fn upsert_edge(src: GraphNodeId, dst: GraphNodeId) -> GraphBatchOperation {
    GraphBatchOperation::UpsertEdge {
        src,
        edge_type: road(),
        dst,
        data: GraphEdgeData::new(2.0, None).expect("edge data"),
    }
}

fn delete_edge(src: GraphNodeId, dst: GraphNodeId) -> GraphBatchOperation {
    GraphBatchOperation::DeleteEdge {
        src,
        edge_type: road(),
        dst,
    }
}

/// Runs one batch and returns the rows the scan verbs handed the engine
/// while it ran.
fn scanned_by(db: &Database, graph: &mut GraphService<'_>, batch: Vec<GraphBatchOperation>) -> u64 {
    db.reset_scanned_rows_for_test();
    let outcome = graph
        .batch_write(&graph_name(), &GraphBatchWrite::new(batch))
        .expect("batch succeeds");
    assert!(outcome.commit().is_some(), "the batch wrote something");
    db.scanned_rows_for_test()
}

/// A one-edge update and an eight-edge closure read their endpoints and
/// edges by point read: no scan verb runs at all.
#[test]
fn edge_batches_scan_nothing() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_city(&mut graph);

    let one_edge = vec![upsert_edge(chain_node(10), chain_node(11))];
    assert_eq!(
        scanned_by(&db, &mut graph, one_edge),
        0,
        "a one-edge update scanned rows"
    );

    let closure: Vec<GraphBatchOperation> = (20..28)
        .map(|index| delete_edge(chain_node(index), chain_node(index + 1)))
        .collect();
    assert_eq!(
        scanned_by(&db, &mut graph, closure),
        0,
        "an eight-edge closure scanned rows"
    );

    let reopen: Vec<GraphBatchOperation> = (20..28)
        .map(|index| upsert_edge(chain_node(index), chain_node(index + 1)))
        .collect();
    assert_eq!(
        scanned_by(&db, &mut graph, reopen),
        0,
        "an eight-edge reopen scanned rows"
    );
}

/// A node upsert reads the node by point read, whether it is new or a
/// replacement.
#[test]
fn node_batches_scan_nothing() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_city(&mut graph);

    let replace = vec![GraphBatchOperation::UpsertNode {
        node_id: chain_node(500),
        data: GraphNodeData::default(),
    }];
    assert_eq!(scanned_by(&db, &mut graph, replace), 0);

    let create = vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("new"),
            data: GraphNodeData::default(),
        },
        upsert_edge(node("new"), chain_node(0)),
    ];
    assert_eq!(scanned_by(&db, &mut graph, create), 0);
}

/// Deleting a node reads exactly its adjacency — one row per incident edge,
/// from the node's own outgoing and incoming prefixes — however large the
/// graph is.
#[test]
fn delete_node_batch_scans_the_node_degree() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_city(&mut graph);

    // n0100: in from n0099 and the hub, out to n0101.
    let batch = vec![GraphBatchOperation::DeleteNode {
        node_id: chain_node(100),
    }];
    assert_eq!(scanned_by(&db, &mut graph, batch), 3);

    // n0800: in from n0799 only (past the hub's reach), out to n0801.
    let batch = vec![GraphBatchOperation::DeleteNode {
        node_id: chain_node(800),
    }];
    assert_eq!(scanned_by(&db, &mut graph, batch), 2);

    // The hub: HUB_TARGETS outgoing rows and nothing incoming. The edge to
    // n0100 went with n0100, but its tombstone is still a row of the hub's
    // adjacency that the scan hands over — a cost, not an edge.
    let batch = vec![GraphBatchOperation::DeleteNode {
        node_id: node("hub"),
    }];
    let expected = u64::try_from(HUB_TARGETS).expect("fits");
    assert_eq!(scanned_by(&db, &mut graph, batch), expected);
    let info = graph
        .graph_info(&graph_name())
        .expect("info reads")
        .expect("graph exists");
    let live_edges = u64::try_from(NODE_COUNT - 1 + HUB_TARGETS).expect("fits");
    // Three deletes: n0100 took 3 edges, n0800 took 2, the hub its 499 left.
    assert_eq!(info.edge_count(), live_edges - 3 - 2 - (expected - 1));
}

/// The single-node delete reads the same adjacency the batch does.
#[test]
fn single_delete_node_scans_the_node_degree() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_city(&mut graph);

    db.reset_scanned_rows_for_test();
    let outcome = graph
        .delete_node(&graph_name(), &chain_node(200))
        .expect("delete succeeds");
    assert!(outcome.deleted());
    assert_eq!(db.scanned_rows_for_test(), 3);
}

/// Deleting a node with no incident edges reads neither adjacency prefix, so
/// the batch scans nothing however large the graph is.
#[test]
fn delete_node_with_no_incident_edges_scans_nothing() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_city(&mut graph);
    graph
        .upsert_node(&graph_name(), node("island"), GraphNodeData::default())
        .expect("isolated node written");

    let batch = vec![GraphBatchOperation::DeleteNode {
        node_id: node("island"),
    }];
    assert_eq!(
        scanned_by(&db, &mut graph, batch),
        0,
        "deleting an isolated node scanned rows"
    );
    assert!(graph
        .get_node(&graph_name(), &node("island"))
        .expect("read succeeds")
        .is_none());
}
