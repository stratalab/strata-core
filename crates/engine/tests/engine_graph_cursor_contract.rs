//! The contract graph cursor pagination keeps while seeking (#3458, #3473,
//! #3489): node-id string order over length-major keys, an exclusive cursor
//! that is a position, `has_more` that speaks for the prefix, pages that fill
//! past tombstones, `Both` as incoming-then-outgoing, and a cursor that must
//! be one this listing produced. None of this needs the testkit, so it is
//! compiled in every lane — including the mutation gate.

mod common;

use strata_engine::{
    Database, GraphDirection, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId,
    GraphService, GraphTypeName,
};

use common::{branch, open_cache_database, space};

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

fn edge(
    src: &str,
    kind: &str,
    dst: &str,
) -> (GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData) {
    (
        node(src),
        edge_type(kind),
        node(dst),
        GraphEdgeData::new(1.0, None).expect("edge data"),
    )
}

/// Every endpoint named by `edges`, then the edges.
fn seed_edges(graph: &mut GraphService<'_>, edges: &[(&str, &str, &str)]) {
    let mut nodes: Vec<(GraphNodeId, GraphNodeData)> = Vec::new();
    for (src, _, dst) in edges {
        for id in [src, dst] {
            if !nodes.iter().any(|(existing, _)| existing.as_str() == *id) {
                nodes.push((node(id), GraphNodeData::default()));
            }
        }
    }
    let edges: Vec<_> = edges
        .iter()
        .map(|(src, kind, dst)| edge(src, kind, dst))
        .collect();
    graph
        .bulk_insert(&graph_name(), &nodes, &edges, None)
        .expect("edges ingest");
}

/// `hub -[kind]-> every id`.
fn seed_hub(graph: &mut GraphService<'_>, hub: &str, kind: &str, ids: &[String]) {
    let edges: Vec<(&str, &str, &str)> = ids.iter().map(|id| (hub, kind, id.as_str())).collect();
    seed_edges(graph, &edges);
}

fn node_ids(page: &strata_engine::GraphNodePage) -> Vec<String> {
    page.nodes()
        .iter()
        .map(|node| node.node_id().as_str().to_owned())
        .collect()
}

fn neighbor_ids(page: &strata_engine::GraphNeighborPage) -> Vec<String> {
    page.neighbors()
        .iter()
        .map(|hit| hit.node().node_id().as_str().to_owned())
        .collect()
}

/// Row keys order ids length-first; the page orders them as strings. Mixed
/// lengths are where a naive seek would reorder — the guard for all three.
#[test]
fn mixed_length_ids_page_in_string_order() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    let mixed: Vec<String> = ["z", "aa", "b", "aaa", "ab"]
        .iter()
        .map(|id| (*id).to_owned())
        .collect();
    // The hub ingest upserts its targets untyped, so type them afterwards.
    seed_hub(&mut graph, "hub", "knows", &mixed);
    seed_typed_nodes(&mut graph, &mixed, "Place");
    let ordered = ["aa", "aaa", "ab", "b", "z"];

    let page = graph
        .list_nodes(&graph_name(), None, None, 10)
        .expect("nodes");
    // The hub itself lists too; it sorts between "b" and "z".
    assert_eq!(node_ids(&page), ["aa", "aaa", "ab", "b", "hub", "z"]);

    let page = graph
        .nodes_by_type(&graph_name(), &type_name("Place"), None, 10)
        .expect("typed nodes");
    assert_eq!(node_ids(&page), ordered);

    let page = graph
        .neighbors(
            &graph_name(),
            &node("hub"),
            GraphDirection::Outgoing,
            None,
            None,
            10,
        )
        .expect("neighbors");
    assert_eq!(neighbor_ids(&page), ordered);

    // Walking by cursor keeps that order across page boundaries.
    let first = graph
        .nodes_by_type(&graph_name(), &type_name("Place"), None, 2)
        .expect("first");
    assert_eq!(node_ids(&first), ["aa", "aaa"]);
    assert_eq!(first.cursor().map(GraphNodeId::as_str), Some("aaa"));
    let second = graph
        .nodes_by_type(&graph_name(), &type_name("Place"), first.cursor(), 2)
        .expect("second");
    assert_eq!(node_ids(&second), ["ab", "b"]);
    let third = graph
        .nodes_by_type(&graph_name(), &type_name("Place"), second.cursor(), 2)
        .expect("third");
    assert_eq!(node_ids(&third), ["z"]);
    assert!(!third.has_more());
}

/// `has_more` speaks for the prefix, not the scan: rows beyond the prefix
/// must not make the last matching page claim more.
#[test]
fn list_nodes_prefix_page_reports_has_more_only_within_prefix() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    let mut all = ids("a", 6);
    all.extend(ids("b", 6));
    seed_nodes(&mut graph, &all);

    let first = graph
        .list_nodes(&graph_name(), Some(&node("a")), None, 3)
        .expect("first");
    assert_eq!(node_ids(&first), ["a0000", "a0001", "a0002"]);
    assert!(first.has_more());
    let second = graph
        .list_nodes(&graph_name(), Some(&node("a")), first.cursor(), 3)
        .expect("second");
    assert_eq!(node_ids(&second), ["a0003", "a0004", "a0005"]);
    assert!(
        !second.has_more(),
        "b* rows beyond the prefix are not `more`"
    );
    assert!(second.cursor().is_none());
}

/// An id equal to the prefix starts with it: the bucket of exactly the
/// prefix's length is searched, not skipped.
#[test]
fn list_nodes_prefix_matches_an_id_equal_to_it() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_nodes(
        &mut graph,
        &["a".to_owned(), "ab".to_owned(), "b".to_owned()],
    );
    let page = graph
        .list_nodes(&graph_name(), Some(&node("a")), None, 10)
        .expect("prefixed page");
    assert_eq!(node_ids(&page), ["a", "ab"]);
}

/// A page that holds exactly `limit` rows with nothing after it is the last
/// page: `has_more` is false and there is no cursor — for all three reads.
#[test]
fn a_full_last_page_reports_no_more() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    let five = ids("p", 5);
    seed_hub(&mut graph, "hub", "knows", &five);
    seed_typed_nodes(&mut graph, &five, "Place");

    let page = graph
        .nodes_by_type(&graph_name(), &type_name("Place"), None, 5)
        .expect("typed page");
    assert_eq!(page.nodes().len(), 5);
    assert!(!page.has_more());
    assert!(page.cursor().is_none());

    let page = graph
        .list_nodes(&graph_name(), Some(&node("p")), None, 5)
        .expect("node page");
    assert_eq!(page.nodes().len(), 5);
    assert!(!page.has_more());

    let page = graph
        .neighbors(
            &graph_name(),
            &node("hub"),
            GraphDirection::Outgoing,
            None,
            None,
            5,
        )
        .expect("neighbor page");
    assert_eq!(page.neighbors().len(), 5);
    assert!(!page.has_more());
    assert!(page.cursor().is_none());
}

/// Tombstones are scanned but not returned, so a page must keep seeking until
/// it is full — and a read at the version before the deletes still sees every
/// row. The re-seek is where an `as_of` page under-fills.
#[test]
fn tombstone_heavy_walk_fills_pages_with_live_rows() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    let all = ids("n", 300);
    seed_nodes(&mut graph, &all);
    let before_deletes = graph
        .upsert_node(&graph_name(), node("n0000"), GraphNodeData::default())
        .expect("rewrite")
        .commit()
        .version();
    for id in all.iter().step_by(2) {
        graph.delete_node(&graph_name(), &node(id)).expect("delete");
    }
    let live: Vec<String> = all.iter().skip(1).step_by(2).cloned().collect();

    let walk = |at: Option<strata_core::CommitVersion>| {
        let mut seen = Vec::new();
        let mut cursor: Option<GraphNodeId> = None;
        loop {
            let page = match at {
                Some(version) => graph
                    .list_nodes_at_version(&graph_name(), None, cursor.as_ref(), 20, version)
                    .expect("page at version"),
                None => graph
                    .list_nodes(&graph_name(), None, cursor.as_ref(), 20)
                    .expect("page"),
            };
            if page.has_more() {
                assert_eq!(page.nodes().len(), 20, "a non-final page is full");
            }
            seen.extend(node_ids(&page));
            match page.cursor() {
                Some(next) => cursor = Some(next.clone()),
                None => break,
            }
        }
        seen
    };
    assert_eq!(
        walk(None),
        live,
        "live walk returns every survivor once, in order"
    );
    assert_eq!(
        walk(Some(before_deletes)),
        all,
        "the same walk at the pre-delete version returns every row"
    );
}

/// A neighbor cursor is a position this listing produced; anything else is
/// refused rather than seeked from garbage.
#[test]
fn neighbors_rejects_a_cursor_it_did_not_produce() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_hub(&mut graph, "hub", "knows", &ids("a", 3));

    let error = graph
        .neighbors(
            &graph_name(),
            &node("hub"),
            GraphDirection::Outgoing,
            None,
            Some("not-a-cursor"),
            10,
        )
        .expect_err("a cursor this listing never produced is refused");
    assert_eq!(error.code(), "invalid_argument.engine.graph_cursor");
}

/// With an edge-type filter, the cursor positions the walk only when it names
/// that type: a cursor from an earlier type starts the type fresh, one from a
/// later type means nothing is left.
#[test]
fn neighbors_filtered_walk_positions_by_the_cursors_type() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_edges(
        &mut graph,
        &[
            ("hub", "a", "a1"),
            ("hub", "a", "a2"),
            ("hub", "b", "b1"),
            ("hub", "b", "b2"),
        ],
    );
    let page = |filter: &str, cursor: Option<&str>, limit: usize| {
        graph
            .neighbors(
                &graph_name(),
                &node("hub"),
                GraphDirection::Outgoing,
                Some(&edge_type(filter)),
                cursor,
                limit,
            )
            .expect("filtered page")
    };

    // The cursor names the filtered type: the walk resumes after it.
    let first = page("b", None, 1);
    assert_eq!(neighbor_ids(&first), ["b1"]);
    assert!(first.has_more());
    let second = page("b", first.cursor(), 1);
    assert_eq!(neighbor_ids(&second), ["b2"]);
    assert!(!second.has_more());

    // The cursor names an earlier type: the filtered type starts fresh.
    let in_a = graph
        .neighbors(
            &graph_name(),
            &node("hub"),
            GraphDirection::Outgoing,
            None,
            None,
            1,
        )
        .expect("unfiltered first hit");
    assert_eq!(neighbor_ids(&in_a), ["a1"]);
    let from_a = page("b", in_a.cursor(), 10);
    assert_eq!(neighbor_ids(&from_a), ["b1", "b2"]);

    // The cursor names a later type: nothing is left.
    let past = page("a", first.cursor(), 10);
    assert!(past.neighbors().is_empty());
    assert!(!past.has_more());
}

/// Unfiltered, a cursor's own type may still hold neighbors past it; the walk
/// continues that type before moving to the next.
#[test]
fn neighbors_unfiltered_walk_continues_the_cursors_own_type() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_edges(
        &mut graph,
        &[
            ("hub", "a", "a1"),
            ("hub", "a", "a2"),
            ("hub", "b", "b1"),
            ("hub", "b", "b2"),
        ],
    );
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = graph
            .neighbors(
                &graph_name(),
                &node("hub"),
                GraphDirection::Outgoing,
                None,
                cursor.as_deref(),
                1,
            )
            .expect("page");
        seen.extend(neighbor_ids(&page));
        match page.cursor() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(seen, ["a1", "a2", "b1", "b2"]);
}

/// `Both` is incoming then outgoing: a walk whose page boundary falls between
/// the legs continues into the outgoing leg from its start, a cursor from the
/// outgoing leg never re-reads the incoming one, and every hit appears exactly
/// once — live, and at a version before a later edge landed.
#[test]
fn both_direction_walk_crosses_legs_and_holds_at_version() {
    let db = open_cache_database().expect("cache database opens");
    let mut graph = open_graph(&db);
    seed_edges(
        &mut graph,
        &[
            ("i1", "a", "hub"),
            ("i2", "b", "hub"),
            ("i3", "c", "hub"),
            ("hub", "a", "o1"),
            ("hub", "b", "o2"),
            ("hub", "c", "o3"),
        ],
    );
    let before_later_edge = graph
        .upsert_node(&graph_name(), node("hub"), GraphNodeData::default())
        .expect("rewrite")
        .commit()
        .version();
    // A fourth outgoing type, added later: visible live, not at the version.
    graph
        .upsert_edge(
            &graph_name(),
            node("hub"),
            edge_type("d"),
            node("o1"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect("later edge");

    let walk = |at: Option<strata_core::CommitVersion>| {
        let mut pages: Vec<Vec<(char, String)>> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = match at {
                Some(version) => graph
                    .neighbors_at_version(
                        &graph_name(),
                        &node("hub"),
                        GraphDirection::Both,
                        None,
                        cursor.as_deref(),
                        2,
                        version,
                    )
                    .expect("page at version"),
                None => graph
                    .neighbors(
                        &graph_name(),
                        &node("hub"),
                        GraphDirection::Both,
                        None,
                        cursor.as_deref(),
                        2,
                    )
                    .expect("page"),
            };
            pages.push(
                page.neighbors()
                    .iter()
                    .map(|hit| {
                        let leg = match hit.direction() {
                            GraphDirection::Incoming => 'i',
                            _ => 'o',
                        };
                        (leg, hit.node().node_id().as_str().to_owned())
                    })
                    .collect(),
            );
            match page.cursor() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }
        pages
    };
    let hit = |leg: char, id: &str| (leg, id.to_owned());

    // Live: the second page straddles the legs; the third starts from an
    // outgoing cursor and must not revisit the incoming leg.
    assert_eq!(
        walk(None),
        vec![
            vec![hit('i', "i1"), hit('i', "i2")],
            vec![hit('i', "i3"), hit('o', "o1")],
            vec![hit('o', "o2"), hit('o', "o3")],
            vec![hit('o', "o1")],
        ]
    );
    // At the earlier version the later `d` edge is absent and the walk ends
    // on a full page.
    assert_eq!(
        walk(Some(before_later_edge)),
        vec![
            vec![hit('i', "i1"), hit('i', "i2")],
            vec![hit('i', "i3"), hit('o', "o1")],
            vec![hit('o', "o2"), hit('o', "o3")],
        ]
    );
}
