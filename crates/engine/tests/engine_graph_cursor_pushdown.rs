//! Graph cursor pushdown: a page costs page-sized work (#3458, #3473, #3489).
//!
//! `list_nodes`, `nodes_by_type` and `neighbors` are cursor-paginated, and the
//! cursor is a real position (the last id of the page), so the next page can
//! be *sought*, not sliced out of a whole-prefix scan. These tests pin that
//! the rows a page pulls through the persistence scan verbs are bounded by the
//! page, not the graph, and that a neighbor page hydrates only its own hits.
//! Both need the testkit's scan counter and read-corruption injector. The
//! contract those reads keep while seeking — order, cursor, `has_more`,
//! tombstones, legs — is pinned in `engine_graph_cursor_contract.rs`, which
//! needs no testkit and so is compiled in every lane.

#![cfg(feature = "testkit")]

mod common;

use strata_engine::testkit::RowCorruption;
use strata_engine::{
    Database, EngineErrorClass, GraphDirection, GraphEdgeData, GraphEdgeType, GraphName,
    GraphNodeData, GraphNodeId, GraphService, GraphTypeName,
};

use common::{assert_status, branch, open_cache_database, space};

/// A malformed value whose leading byte is not any decoder's format version.
const BAD_RECORD_BYTES: [u8; 2] = [0xFF, 0x00];

/// The scan rows one page may pull: its `limit` rows, the one-row lookahead
/// that decides `has_more`, and one probe seek that finds the id-length bucket
/// the page lives in. A first page keeps that probe row as its first row and
/// so costs `limit + 1`; a cursor page discards it and costs `limit + 2`. A
/// whole-prefix scan pulls every row of the graph. A page can never cost
/// fewer than `limit` rows either — it returns that many.
fn assert_page_sized(scanned: u64, limit: usize, what: &str) {
    let limit = u64::try_from(limit).expect("limit fits u64");
    assert!(
        (limit..=limit + 2).contains(&scanned),
        "{what} scanned {scanned} rows for a page of {limit}; a page costs between its own rows and two more"
    );
}

fn graph_name() -> GraphName {
    GraphName::new("g").expect("graph name")
}

fn node(id: &str) -> GraphNodeId {
    GraphNodeId::new(id).expect("node id")
}

fn edge_type(name: &str) -> GraphEdgeType {
    GraphEdgeType::new(name).expect("edge type")
}

fn type_name(name: &str) -> GraphTypeName {
    GraphTypeName::new(name).expect("type name")
}

/// Uniform-length ids so every row lives in one id-length bucket.
fn ids(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}{index:04}"))
        .collect()
}

fn open_graph(db: &Database) -> GraphService<'_> {
    let mut graph = db
        .graph(branch("default"), space("default"))
        .expect("graph opens");
    graph.create_graph(graph_name()).expect("graph create");
    graph
}

fn seed_nodes(graph: &mut GraphService<'_>, ids: &[String]) {
    let nodes: Vec<(GraphNodeId, GraphNodeData)> = ids
        .iter()
        .map(|id| (node(id), GraphNodeData::default()))
        .collect();
    graph
        .bulk_insert(&graph_name(), &nodes, &[], None)
        .expect("nodes ingest");
}

fn seed_typed_nodes(graph: &mut GraphService<'_>, ids: &[String], object_type: &str) {
    for id in ids {
        graph
            .upsert_node(
                &graph_name(),
                node(id),
                GraphNodeData::new(None, None).with_object_type(type_name(object_type)),
            )
            .expect("typed node");
    }
}

/// `hub -[kind]-> every id`.
fn seed_hub(graph: &mut GraphService<'_>, hub: &str, kind: &str, ids: &[String]) {
    let mut nodes: Vec<(GraphNodeId, GraphNodeData)> = vec![(node(hub), GraphNodeData::default())];
    nodes.extend(ids.iter().map(|id| (node(id), GraphNodeData::default())));
    let edges: Vec<(GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData)> = ids
        .iter()
        .map(|id| {
            (
                node(hub),
                edge_type(kind),
                node(id),
                GraphEdgeData::new(1.0, None).expect("edge data"),
            )
        })
        .collect();
    graph
        .bulk_insert(&graph_name(), &nodes, &edges, None)
        .expect("hub ingest");
}

/// A `list_nodes` page pulls a bounded number of rows through the scan verbs
/// wherever it sits in the graph — first, middle, or last.
#[test]
fn list_nodes_page_scans_page_sized_work() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    let all = ids("n", 600);
    seed_nodes(&mut graph, &all);
    let limit = 20usize;

    for (cursor, expected_first) in [
        (None, "n0000"),
        (Some("n0299"), "n0300"),
        (Some("n0579"), "n0580"),
    ] {
        db.reset_scanned_rows_for_test();
        let page = graph
            .list_nodes(&graph_name(), None, cursor.map(node).as_ref(), limit)
            .expect("page");
        let scanned = db.scanned_rows_for_test();
        assert_eq!(page.nodes().len(), limit);
        assert_eq!(page.nodes()[0].node_id().as_str(), expected_first);
        assert_page_sized(scanned, limit, &format!("the page after {cursor:?}"));
    }
}

/// A `nodes_by_type` page is bounded the same way, and never touches rows of
/// another type.
#[test]
fn nodes_by_type_page_scans_page_sized_work() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_typed_nodes(&mut graph, &ids("p", 600), "Place");
    seed_typed_nodes(&mut graph, &ids("o", 50), "Other");
    let limit = 20usize;

    for (cursor, expected_first) in [
        (None, "p0000"),
        (Some("p0299"), "p0300"),
        (Some("p0579"), "p0580"),
    ] {
        db.reset_scanned_rows_for_test();
        let page = graph
            .nodes_by_type(
                &graph_name(),
                &type_name("Place"),
                cursor.map(node).as_ref(),
                limit,
            )
            .expect("page");
        let scanned = db.scanned_rows_for_test();
        assert_eq!(page.nodes().len(), limit);
        assert_eq!(page.nodes()[0].node_id().as_str(), expected_first);
        assert_page_sized(scanned, limit, &format!("the page after {cursor:?}"));
    }
}

/// A `neighbors` page over a high-degree hub pulls a bounded number of edge
/// rows, wherever it sits in the adjacency.
#[test]
fn neighbors_page_scans_page_sized_work() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    let targets = ids("a", 600);
    seed_hub(&mut graph, "hub", "on_street", &targets);
    let limit = 20usize;

    let mut cursor: Option<String> = None;
    for expected_first in ["a0000", "a0020", "a0040"] {
        db.reset_scanned_rows_for_test();
        let page = graph
            .neighbors(
                &graph_name(),
                &node("hub"),
                GraphDirection::Outgoing,
                Some(&edge_type("on_street")),
                cursor.as_deref(),
                limit,
            )
            .expect("page");
        let scanned = db.scanned_rows_for_test();
        assert_eq!(page.neighbors().len(), limit);
        assert_eq!(
            page.neighbors()[0].node().node_id().as_str(),
            expected_first
        );
        assert_page_sized(scanned, limit, &format!("the page after {cursor:?}"));
        cursor = page.cursor().map(str::to_owned);
        assert!(page.has_more());
    }
}

/// A neighbor page hydrates only its own hits: the point read that would
/// fetch the first node past the page is never issued. Proven with an armed
/// corruption on that read — a page that hydrated the whole hub trips it.
#[test]
fn neighbors_page_hydrates_only_its_page() {
    let limit = 10usize;
    // Reads a page issues before hydrating: the graph row and the hub's own
    // row. Arming past `limit` plus that prelude, with margin, means a
    // page-bounded read never reaches the corrupted read while a whole-hub
    // hydration (50 node reads) always does.
    let arm_after = limit + 5;

    let mut db = open_cache_database().expect("cache database opens");
    {
        let mut graph = open_graph(&db);
        seed_hub(&mut graph, "hub", "on_street", &ids("a", 50));
    }

    db.inject_read_corruption_after_for_test(
        arm_after,
        RowCorruption::SetValue(BAD_RECORD_BYTES.to_vec()),
    );
    let page = db
        .graph(branch("default"), space("default"))
        .expect("graph opens")
        .neighbors(
            &graph_name(),
            &node("hub"),
            GraphDirection::Outgoing,
            None,
            None,
            limit,
        )
        .expect("a page-bounded read never hydrates past its page");
    assert_eq!(page.neighbors().len(), limit);

    // Direction control: the same arming is live — a read that must hydrate
    // the whole hub reaches the corrupted row.
    db.inject_read_corruption_after_for_test(
        arm_after,
        RowCorruption::SetValue(BAD_RECORD_BYTES.to_vec()),
    );
    let error = db
        .graph(branch("default"), space("default"))
        .expect("graph opens")
        .neighbors(
            &graph_name(),
            &node("hub"),
            GraphDirection::Outgoing,
            None,
            None,
            50,
        )
        .expect_err("hydrating the whole hub reaches the corrupted read");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.graph_node_record",
        false,
    );
}
