//! #3474: `graph_info` answers from the graph's metadata row, which every
//! node/edge-changing commit rewrites with the live counts — one point read,
//! whose `updated_version` is a per-graph revision token. These tests hold
//! that row to the truth a full listing gives after every kind of write, at
//! every earlier version, across a fork and on recreate. The row written
//! before counts were kept is covered in-crate (`service.rs`), where the
//! default-feature mutation lane can see it.

mod common;

use std::collections::BTreeMap;

use strata_core::CommitVersion;
use strata_engine::{
    Database, GraphBatchOperation, GraphBatchWrite, GraphBindingPrimitive, GraphBindingTarget,
    GraphDeletePolicy, GraphEdgeData, GraphEdgeType, GraphEntityBinding, GraphInfo, GraphName,
    GraphNodeData, GraphNodeId, GraphObjectTypeDef, GraphPropertyDef, GraphService, GraphTypeName,
};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn graph_service<'a>(database: &'a mut Database, branch_name: &str) -> GraphService<'a> {
    database
        .graph(branch(branch_name), space("default"))
        .expect("graph service opens")
}

fn name(value: &str) -> GraphName {
    GraphName::new(value).expect("valid graph")
}

fn node(value: &str) -> GraphNodeId {
    GraphNodeId::new(value).expect("valid node id")
}

fn kind(value: &str) -> GraphEdgeType {
    GraphEdgeType::new(value).expect("valid edge type")
}

fn plain() -> GraphNodeData {
    GraphNodeData::default()
}

fn edge() -> GraphEdgeData {
    GraphEdgeData::default()
}

fn bound_to(key: &str) -> GraphNodeData {
    GraphNodeData::new(
        None,
        Some(GraphEntityBinding::new(
            GraphBindingTarget::new(GraphBindingPrimitive::Kv, None, space("docs"), key)
                .expect("valid binding target"),
        )),
    )
}

/// Counts live nodes and forward edges by listing every page — the reading
/// the maintained row must agree with.
fn listed_counts(graph: &GraphService<'_>, graph_name: &GraphName) -> (u64, u64) {
    let mut nodes = 0_u64;
    let mut cursor: Option<GraphNodeId> = None;
    loop {
        let page = graph
            .list_nodes(graph_name, None, cursor.as_ref(), 100)
            .expect("node page");
        nodes += u64::try_from(page.nodes().len()).expect("fits");
        match page.cursor() {
            Some(next) => cursor = Some(next.clone()),
            None => break,
        }
    }
    let mut edges = 0_u64;
    let mut cursor: Option<String> = None;
    loop {
        let page = graph
            .list_edges(graph_name, cursor.as_deref(), 100)
            .expect("edge page");
        edges += u64::try_from(page.edges().len()).expect("fits");
        match page.cursor() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    (nodes, edges)
}

/// The row after a write: counts equal to the listing, `updated` equal to the
/// last commit that changed a node or edge, `created` never moving.
fn expect_info(
    graph: &GraphService<'_>,
    graph_name: &GraphName,
    created: CommitVersion,
    updated: CommitVersion,
    context: &str,
) -> GraphInfo {
    let info = graph
        .graph_info(graph_name)
        .expect("info reads")
        .unwrap_or_else(|| panic!("{context}: graph exists"));
    let (nodes, edges) = listed_counts(graph, graph_name);
    assert_eq!(info.node_count(), nodes, "{context}: node count vs listing");
    assert_eq!(info.edge_count(), edges, "{context}: edge count vs listing");
    assert_eq!(info.created_version(), created, "{context}: create commit");
    assert_eq!(info.updated_version(), updated, "{context}: update commit");
    info
}

#[test]
fn graph_info_tracks_every_kind_of_write_in_cache_and_durable_modes() {
    run_database_modes(exercise_every_write);
}

#[allow(clippy::too_many_lines)]
fn exercise_every_write(mut database: Database) {
    let g = name("roads");
    let mut graph = graph_service(&mut database, "default");
    // Every recorded `(version, info)` is replayed with `as_of` at the end.
    let mut history: BTreeMap<CommitVersion, GraphInfo> = BTreeMap::new();

    let (info, create) = graph.create_graph(g.clone()).expect("graph created");
    let created = create.version();
    assert_eq!((info.node_count(), info.edge_count()), (0, 0));
    assert_eq!(info.updated_version(), created);
    history.insert(
        created,
        expect_info(&graph, &g, created, created, "created"),
    );

    // Nodes: three new, one replace (bumps the revision, not the count).
    let mut latest = created;
    for id in ["a", "b", "c"] {
        let write = graph.upsert_node(&g, node(id), plain()).expect("node");
        assert!(write.created());
        assert!(
            write.commit().version() > latest,
            "the revision only moves forward"
        );
        latest = write.commit().version();
        history.insert(latest, expect_info(&graph, &g, created, latest, id));
    }
    let replace = graph.upsert_node(&g, node("a"), plain()).expect("replace");
    assert!(!replace.created());
    latest = replace.commit().version();
    let info = expect_info(&graph, &g, created, latest, "replace a");
    assert_eq!(info.node_count(), 3);
    history.insert(latest, info);

    // Edges: two new, a self-loop, a replace.
    for (src, dst) in [("a", "b"), ("b", "c"), ("c", "c"), ("a", "b")] {
        let write = graph
            .upsert_edge(&g, node(src), kind("road"), node(dst), edge())
            .expect("edge");
        latest = write.commit().version();
        history.insert(
            latest,
            expect_info(&graph, &g, created, latest, &format!("edge {src}->{dst}")),
        );
    }
    assert_eq!(history[&latest].edge_count(), 3, "a replace adds no edge");

    // Deletes: an edge, a missing edge (no commit, no change), a node with
    // its incident self-loop.
    let removed = graph
        .delete_edge(&g, &node("b"), &kind("road"), &node("c"))
        .expect("delete edge");
    latest = removed.commit().expect("a live edge commits").version();
    history.insert(
        latest,
        expect_info(&graph, &g, created, latest, "delete b->c"),
    );
    let missing = graph
        .delete_edge(&g, &node("b"), &kind("road"), &node("zzz"))
        .expect("missing edge is fine");
    assert!(missing.commit().is_none());
    expect_info(&graph, &g, created, latest, "missing delete leaves the row");
    let removed = graph.delete_node(&g, &node("c")).expect("delete node");
    latest = removed.commit().expect("a live node commits").version();
    let info = expect_info(&graph, &g, created, latest, "delete c");
    assert_eq!((info.node_count(), info.edge_count()), (2, 1));
    history.insert(latest, info);

    // A batch: create, wire, delete an edge explicitly, then delete its
    // endpoint (taking the other incident edge, not the one already gone),
    // create-then-delete inside the batch, delete a missing node.
    let batch = graph
        .batch_write(
            &g,
            &GraphBatchWrite::new(vec![
                GraphBatchOperation::UpsertNode {
                    node_id: node("d"),
                    data: plain(),
                },
                GraphBatchOperation::UpsertEdge {
                    src: node("d"),
                    edge_type: kind("road"),
                    dst: node("a"),
                    data: edge(),
                },
                GraphBatchOperation::DeleteEdge {
                    src: node("a"),
                    edge_type: kind("road"),
                    dst: node("b"),
                },
                GraphBatchOperation::DeleteNode { node_id: node("a") },
                GraphBatchOperation::UpsertNode {
                    node_id: node("e"),
                    data: plain(),
                },
                GraphBatchOperation::DeleteNode { node_id: node("e") },
                GraphBatchOperation::DeleteNode {
                    node_id: node("zzz"),
                },
            ]),
        )
        .expect("batch");
    latest = batch.commit().expect("the batch commits").version();
    let info = expect_info(&graph, &g, created, latest, "batch");
    assert_eq!(
        (info.node_count(), info.edge_count()),
        (2, 0),
        "b and d remain"
    );
    history.insert(latest, info);
    let idle = graph
        .batch_write(
            &g,
            &GraphBatchWrite::new(vec![GraphBatchOperation::DeleteNode {
                node_id: node("zzz"),
            }]),
        )
        .expect("no-op batch");
    assert!(idle.commit().is_none());
    expect_info(&graph, &g, created, latest, "a no-op batch leaves the row");

    // Bulk insert in chunks of two, with a replace and duplicates: nodes f,
    // b (replace), f (again); edges f->b, f->b (again), d->b.
    let bulk = graph
        .bulk_insert(
            &g,
            &[
                (node("f"), plain()),
                (node("b"), plain()),
                (node("f"), plain()),
            ],
            &[
                (node("f"), kind("road"), node("b"), edge()),
                (node("f"), kind("road"), node("b"), edge()),
                (node("d"), kind("road"), node("b"), edge()),
            ],
            Some(2),
        )
        .expect("bulk insert");
    assert!(bulk.commits() >= 2, "chunked into several commits");
    latest = bulk.last_commit().expect("bulk commits").version();
    let info = expect_info(&graph, &g, created, latest, "bulk");
    assert_eq!((info.node_count(), info.edge_count()), (3, 2));
    history.insert(latest, info);

    // A batch that only deletes an edge: exactly one fewer.
    let unlink = graph
        .batch_write(
            &g,
            &GraphBatchWrite::new(vec![GraphBatchOperation::DeleteEdge {
                src: node("d"),
                edge_type: kind("road"),
                dst: node("b"),
            }]),
        )
        .expect("edge-only batch");
    latest = unlink.commit().expect("the batch commits").version();
    let info = expect_info(&graph, &g, created, latest, "batch delete edge");
    assert_eq!((info.node_count(), info.edge_count()), (3, 1));
    history.insert(latest, info);

    // Ontology writes change the schema, not the graph's rows: the revision
    // stays where the last node/edge commit left it.
    let schema = graph
        .define_object_type(
            &g,
            GraphObjectTypeDef::new(
                GraphTypeName::new("place").expect("type"),
                [(
                    "title".to_owned(),
                    GraphPropertyDef::new(Some("string".to_owned()), false).expect("property"),
                )],
            )
            .expect("object type"),
        )
        .expect("ontology write");
    assert!(schema.commit().version() > latest);
    history.insert(
        schema.commit().version(),
        expect_info(&graph, &g, created, latest, "ontology"),
    );

    // Every earlier version answers as it did then.
    for (version, expected) in &history {
        let then = graph
            .graph_info_at_version(&g, *version)
            .expect("historical info")
            .unwrap_or_else(|| panic!("graph existed at {version:?}"));
        assert_eq!(&then, expected, "as_of {version:?}");
    }

    // Recreate: the old rows are gone and the create commit moves; history
    // before the delete is untouched.
    graph.delete_graph(&g, true).expect("forced delete");
    assert!(graph.graph_info(&g).expect("reads").is_none());
    let (again, recreate) = graph.create_graph(g.clone()).expect("recreated");
    assert_eq!((again.node_count(), again.edge_count()), (0, 0));
    assert_eq!(again.created_version(), recreate.version());
    expect_info(
        &graph,
        &g,
        recreate.version(),
        recreate.version(),
        "recreated",
    );
    let first = history.keys().next().expect("history");
    assert_eq!(
        graph
            .graph_info_at_version(&g, *first)
            .expect("historical")
            .expect("existed"),
        history[first]
    );
}

#[test]
fn graph_info_follows_binding_policies_across_graphs_in_cache_and_durable_modes() {
    run_database_modes(exercise_binding_policies);
}

fn exercise_binding_policies(mut database: Database) {
    let mut graph = graph_service(&mut database, "default");
    let roads = name("roads");
    let places = name("places");
    let (_, roads_create) = graph.create_graph(roads.clone()).expect("roads");
    let (_, places_create) = graph.create_graph(places.clone()).expect("places");
    // Two graphs each bind a node to the same document; roads also links it.
    for (g, id) in [(&roads, "r1"), (&places, "p1")] {
        graph
            .upsert_node(g, node(id), bound_to("doc-1"))
            .expect("bound node");
        graph.upsert_node(g, node("other"), plain()).expect("node");
    }
    graph
        .upsert_edge(&roads, node("other"), kind("road"), node("r1"), edge())
        .expect("edge into the bound node");
    let before_roads = graph.graph_info(&roads).expect("reads").expect("exists");
    let before_places = graph.graph_info(&places).expect("reads").expect("exists");
    assert_eq!(
        (before_roads.node_count(), before_roads.edge_count()),
        (2, 1)
    );
    assert_eq!(
        (before_places.node_count(), before_places.edge_count()),
        (2, 0)
    );

    let target = GraphBindingTarget::new(GraphBindingPrimitive::Kv, None, space("docs"), "doc-1")
        .expect("target");
    // Detach rewrites the bound node rows: counts hold, both revisions move.
    let detach = graph
        .apply_binding_delete_policy(&target, GraphDeletePolicy::Detach)
        .expect("detach");
    let detached = detach.commit().expect("detach commits").version();
    let roads_info = expect_info(
        &graph,
        &roads,
        roads_create.version(),
        detached,
        "detach roads",
    );
    let places_info = expect_info(
        &graph,
        &places,
        places_create.version(),
        detached,
        "detach places",
    );
    assert_eq!((roads_info.node_count(), roads_info.edge_count()), (2, 1));
    assert_eq!((places_info.node_count(), places_info.edge_count()), (2, 0));

    // Re-bind, then cascade: the bound nodes and roads' incident edge go.
    for (g, id) in [(&roads, "r1"), (&places, "p1")] {
        graph
            .upsert_node(g, node(id), bound_to("doc-1"))
            .expect("re-bound node");
    }
    let cascade = graph
        .apply_binding_delete_policy(&target, GraphDeletePolicy::Cascade)
        .expect("cascade");
    let cascaded = cascade.commit().expect("cascade commits").version();
    let roads_info = expect_info(
        &graph,
        &roads,
        roads_create.version(),
        cascaded,
        "cascade roads",
    );
    let places_info = expect_info(
        &graph,
        &places,
        places_create.version(),
        cascaded,
        "cascade places",
    );
    assert_eq!((roads_info.node_count(), roads_info.edge_count()), (1, 0));
    assert_eq!((places_info.node_count(), places_info.edge_count()), (1, 0));

    // A policy that touches nothing commits nothing and moves no row.
    let nothing = graph
        .apply_binding_delete_policy(&target, GraphDeletePolicy::Cascade)
        .expect("nothing bound");
    assert!(nothing.commit().is_none());
    expect_info(
        &graph,
        &roads,
        roads_create.version(),
        cascaded,
        "idle roads",
    );
}

#[test]
fn graph_info_is_inherited_by_a_fork_and_then_diverges_in_cache_and_durable_modes() {
    run_database_modes(exercise_fork);
}

fn exercise_fork(mut database: Database) {
    let g = name("roads");
    let (created, before) = {
        let mut graph = graph_service(&mut database, "default");
        let (_, create) = graph.create_graph(g.clone()).expect("graph");
        for id in ["a", "b"] {
            graph.upsert_node(&g, node(id), plain()).expect("node");
        }
        let write = graph
            .upsert_edge(&g, node("a"), kind("road"), node("b"), edge())
            .expect("edge");
        (
            create.version(),
            expect_info(
                &graph,
                &g,
                create.version(),
                write.commit().version(),
                "parent",
            ),
        )
    };
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("fork");

    let child_updated = {
        let mut feature = graph_service(&mut database, "feature");
        assert_eq!(
            feature.graph_info(&g).expect("reads").expect("inherited"),
            before,
            "the child inherits the row as it stood at the fork"
        );
        let write = feature
            .upsert_node(&g, node("c"), plain())
            .expect("child node");
        let info = expect_info(&feature, &g, created, write.commit().version(), "child");
        assert_eq!(info.node_count(), 3);
        write.commit().version()
    };
    let parent = graph_service(&mut database, "default");
    assert_eq!(
        parent.graph_info(&g).expect("reads").expect("still there"),
        before,
        "the parent's row is untouched by the child's write"
    );
    assert!(child_updated > before.updated_version());
}

#[test]
fn graph_info_reports_the_create_and_update_commit_timestamps_in_cache_and_durable_modes() {
    run_database_modes(exercise_commit_timestamps);
}

/// The version assertions elsewhere in this file pin `created`/`updated`
/// versions; the `as_of` replay only proves a historical read matches the live
/// read. Neither holds the maintained row's timestamps to the actual commit
/// clock, so this test pins both `created_timestamp` and `updated_timestamp`
/// against the `CommitOutcome` each write returns: the create point's timestamp
/// survives every rewrite, and `updated_timestamp` tracks the last write's.
fn exercise_commit_timestamps(mut database: Database) {
    let g = name("roads");
    let mut graph = graph_service(&mut database, "default");

    // Create: both coordinates are the create commit — version and timestamp.
    let (info, create) = graph.create_graph(g.clone()).expect("graph created");
    assert_eq!(info.created_version(), create.version());
    assert_eq!(info.created_timestamp(), create.timestamp());
    assert_eq!(info.updated_version(), create.version());
    assert_eq!(info.updated_timestamp(), create.timestamp());

    // First node: `created` holds at the create commit (version and timestamp);
    // `updated` moves to this write's commit, timestamp included.
    let first = graph
        .upsert_node(&g, node("a"), plain())
        .expect("first node");
    let after_first = graph.graph_info(&g).expect("reads").expect("exists");
    assert_eq!(after_first.created_version(), create.version());
    assert_eq!(after_first.created_timestamp(), create.timestamp());
    assert_eq!(after_first.updated_version(), first.commit().version());
    assert_eq!(after_first.updated_timestamp(), first.commit().timestamp());

    // Second node: `updated` moves again to this write's commit; `created` never
    // budges from the create commit — the create point, its timestamp with it,
    // survives every rewrite.
    let second = graph
        .upsert_node(&g, node("b"), plain())
        .expect("second node");
    let after_second = graph.graph_info(&g).expect("reads").expect("exists");
    assert_eq!(after_second.created_version(), create.version());
    assert_eq!(after_second.created_timestamp(), create.timestamp());
    assert_eq!(after_second.updated_version(), second.commit().version());
    assert_eq!(
        after_second.updated_timestamp(),
        second.commit().timestamp()
    );
}
