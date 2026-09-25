//! #3472: `batch_write` stages a batch over the nodes and edges it touches,
//! not over a copy of the graph. Each operation reads what it needs from
//! storage the first time and then sees the batch's own earlier operations —
//! so the sequential semantics, the endpoint and ontology checks, the
//! derived-index maintenance, the delete cascades and the maintained counts
//! all have to come out of that overlay exactly as they came out of the
//! whole-graph maps. These tests pin every overlay edge: a node deleted
//! earlier in the batch is a missing endpoint, a cascade includes the edges
//! the batch wrote and excludes the ones it deleted, a self-loop is one edge,
//! a tombstoned edge is not an edge, and a repeated operation reports its
//! second application honestly. The scan cost itself is measured in
//! `engine_graph_batch_cost.rs` (testkit).

mod common;

use strata_engine::{
    Database, EngineErrorClass, GraphBatchOpOutcome, GraphBatchOperation, GraphBatchWrite,
    GraphBatchWriteOutcome, GraphBindingPrimitive, GraphBindingTarget, GraphDeletePolicy,
    GraphDirection, GraphEdgeData, GraphEdgeType, GraphEntityBinding, GraphName, GraphNodeData,
    GraphNodeId, GraphService, GraphTypeName,
};

use common::{branch, open_cache_database, open_durable_database, space};

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn graph_service(database: &mut Database) -> GraphService<'_> {
    database
        .graph(branch("default"), space("default"))
        .expect("graph service opens")
}

fn name() -> GraphName {
    GraphName::new("roads").expect("valid graph")
}

fn node(value: &str) -> GraphNodeId {
    GraphNodeId::new(value).expect("valid node id")
}

fn kind() -> GraphEdgeType {
    GraphEdgeType::new("road").expect("valid edge type")
}

fn upsert_node(id: &str) -> GraphBatchOperation {
    GraphBatchOperation::UpsertNode {
        node_id: node(id),
        data: GraphNodeData::default(),
    }
}

fn delete_node(id: &str) -> GraphBatchOperation {
    GraphBatchOperation::DeleteNode { node_id: node(id) }
}

fn upsert_edge(src: &str, dst: &str) -> GraphBatchOperation {
    GraphBatchOperation::UpsertEdge {
        src: node(src),
        edge_type: kind(),
        dst: node(dst),
        data: GraphEdgeData::default(),
    }
}

fn delete_edge(src: &str, dst: &str) -> GraphBatchOperation {
    GraphBatchOperation::DeleteEdge {
        src: node(src),
        edge_type: kind(),
        dst: node(dst),
    }
}

/// Creates the graph with the given nodes and `road` edges, one write each.
fn seed(graph: &mut GraphService<'_>, nodes: &[&str], edges: &[(&str, &str)]) {
    graph.create_graph(name()).expect("graph created");
    for id in nodes {
        graph
            .upsert_node(&name(), node(id), GraphNodeData::default())
            .expect("node written");
    }
    for (src, dst) in edges {
        graph
            .upsert_edge(
                &name(),
                node(src),
                kind(),
                node(dst),
                GraphEdgeData::default(),
            )
            .expect("edge written");
    }
}

fn has_edge(graph: &GraphService<'_>, src: &str, dst: &str) -> bool {
    graph
        .get_edge(&name(), &node(src), &kind(), &node(dst))
        .expect("edge read")
        .is_some()
}

fn has_node(graph: &GraphService<'_>, id: &str) -> bool {
    graph
        .get_node(&name(), &node(id))
        .expect("node read")
        .is_some()
}

/// Every node the edges of `id` in `direction` reach, in id order.
fn neighbor_ids(graph: &GraphService<'_>, id: &str, direction: GraphDirection) -> Vec<String> {
    let mut ids: Vec<String> = graph
        .neighbors(&name(), &node(id), direction, None, None, 64)
        .expect("neighbors read")
        .neighbors()
        .iter()
        .map(|hit| hit.node().node_id().as_str().to_owned())
        .collect();
    ids.sort();
    ids
}

/// The maintained counts (#3474) and the listed rows must both say `nodes`
/// live nodes and `edges` live edges: the first proves the batch's deltas,
/// the second proves its row mutations.
fn expect_counts(graph: &GraphService<'_>, nodes: u64, edges: u64, context: &str) {
    let info = graph
        .graph_info(&name())
        .expect("info reads")
        .expect("graph exists");
    assert_eq!(info.node_count(), nodes, "{context}: maintained node count");
    assert_eq!(info.edge_count(), edges, "{context}: maintained edge count");
    let listed_nodes = graph
        .list_nodes(&name(), None, None, 1_000)
        .expect("nodes listed")
        .nodes()
        .len();
    let listed_edges = graph
        .list_edges(&name(), None, 1_000)
        .expect("edges listed")
        .edges()
        .len();
    assert_eq!(
        u64::try_from(listed_nodes).expect("fits"),
        nodes,
        "{context}: listed nodes"
    );
    assert_eq!(
        u64::try_from(listed_edges).expect("fits"),
        edges,
        "{context}: listed edges"
    );
}

fn created_flags(outcome: &GraphBatchWriteOutcome) -> Vec<Option<bool>> {
    outcome
        .results()
        .iter()
        .map(GraphBatchOpOutcome::created_flag)
        .collect()
}

fn deleted_flags(outcome: &GraphBatchWriteOutcome) -> Vec<Option<bool>> {
    outcome
        .results()
        .iter()
        .map(GraphBatchOpOutcome::deleted_flag)
        .collect()
}

#[test]
fn batch_delete_node_cascades_every_stored_incident_edge_once() {
    run_database_modes(exercise_stored_cascade);
}

/// A deleted node takes its incoming edges, its outgoing edges and its
/// self-loop with it — the self-loop counted once — and nothing else.
fn exercise_stored_cascade(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(
        &mut graph,
        &["p", "x", "q", "r"],
        &[("p", "x"), ("x", "q"), ("x", "x"), ("p", "r"), ("q", "r")],
    );
    expect_counts(&graph, 4, 5, "seeded");

    let outcome = graph
        .batch_write(&name(), &GraphBatchWrite::new(vec![delete_node("x")]))
        .expect("delete batch succeeds");
    assert_eq!(deleted_flags(&outcome), vec![Some(true)]);

    assert!(!has_node(&graph, "x"));
    assert!(!has_edge(&graph, "p", "x"), "incoming edge cascaded");
    assert!(!has_edge(&graph, "x", "q"), "outgoing edge cascaded");
    assert!(!has_edge(&graph, "x", "x"), "self-loop cascaded");
    assert!(has_edge(&graph, "p", "r"), "an edge of another node stays");
    assert!(has_edge(&graph, "q", "r"), "an edge of another node stays");
    assert_eq!(neighbor_ids(&graph, "p", GraphDirection::Outgoing), ["r"]);
    assert_eq!(
        neighbor_ids(&graph, "q", GraphDirection::Incoming),
        Vec::<String>::new()
    );
    expect_counts(&graph, 3, 2, "after the cascade");
}

#[test]
fn batch_delete_node_scopes_the_cascade_to_the_node_id_exactly() {
    run_database_modes(exercise_exact_id_scope);
}

/// The cascade reads the deleted node's own adjacency, not every node whose
/// id starts with the same text: deleting `x` leaves `xy`'s edges alone.
fn exercise_exact_id_scope(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(
        &mut graph,
        &["x", "xy", "q"],
        &[("x", "q"), ("xy", "q"), ("q", "xy"), ("xy", "x")],
    );

    let outcome = graph
        .batch_write(&name(), &GraphBatchWrite::new(vec![delete_node("x")]))
        .expect("delete batch succeeds");
    assert_eq!(deleted_flags(&outcome), vec![Some(true)]);

    assert!(!has_edge(&graph, "x", "q"));
    assert!(!has_edge(&graph, "xy", "x"), "the edge into x cascaded");
    assert!(has_edge(&graph, "xy", "q"), "xy's outgoing edge stays");
    assert!(has_edge(&graph, "q", "xy"), "xy's incoming edge stays");
    expect_counts(&graph, 2, 2, "after deleting x");
}

#[test]
fn batch_delete_node_ignores_tombstoned_incident_edges() {
    run_database_modes(exercise_tombstoned_incident);
}

/// An incident edge deleted before the batch is a tombstone in the node's
/// adjacency; the cascade neither counts it nor deletes it again.
fn exercise_tombstoned_incident(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["x", "q", "r"], &[("x", "q"), ("r", "x")]);
    graph
        .delete_edge(&name(), &node("x"), &kind(), &node("q"))
        .expect("edge deleted ahead of the batch");
    graph
        .delete_edge(&name(), &node("r"), &kind(), &node("x"))
        .expect("edge deleted ahead of the batch");
    graph
        .upsert_edge(
            &name(),
            node("x"),
            kind(),
            node("r"),
            GraphEdgeData::default(),
        )
        .expect("one live edge remains");
    expect_counts(&graph, 3, 1, "one live edge, two tombstones");

    let outcome = graph
        .batch_write(&name(), &GraphBatchWrite::new(vec![delete_node("x")]))
        .expect("the cascade counts the live edge only");
    assert_eq!(deleted_flags(&outcome), vec![Some(true)]);
    assert!(!has_edge(&graph, "x", "r"));
    expect_counts(&graph, 2, 0, "after the cascade");
}

#[test]
fn batch_delete_node_cascades_the_edges_the_batch_itself_wrote() {
    run_database_modes(exercise_batch_local_cascade);
}

/// Edges upserted earlier in the same batch are incident edges too — into
/// the node and out of it — while a batch-local edge between other nodes is
/// left alone.
fn exercise_batch_local_cascade(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["a", "b"], &[("a", "b")]);

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![
                upsert_node("c"),
                upsert_edge("c", "a"),
                upsert_edge("a", "c"),
                upsert_edge("b", "c"),
                delete_node("a"),
            ]),
        )
        .expect("batch succeeds");
    assert_eq!(
        created_flags(&outcome),
        vec![Some(true), Some(true), Some(true), Some(true), None]
    );
    assert_eq!(deleted_flags(&outcome)[4], Some(true));

    assert!(!has_node(&graph, "a"));
    assert!(
        !has_edge(&graph, "c", "a"),
        "batch-written incoming edge cascaded"
    );
    assert!(
        !has_edge(&graph, "a", "c"),
        "batch-written outgoing edge cascaded"
    );
    assert!(!has_edge(&graph, "a", "b"), "stored outgoing edge cascaded");
    assert!(
        has_edge(&graph, "b", "c"),
        "a batch-written edge elsewhere stays"
    );
    assert_eq!(neighbor_ids(&graph, "c", GraphDirection::Both), ["b"]);
    expect_counts(&graph, 2, 1, "after the cascade");
}

#[test]
fn batch_delete_node_does_not_recount_an_edge_the_batch_already_deleted() {
    run_database_modes(exercise_batch_deleted_incident);
}

/// An incident edge the batch deleted earlier is still a stored row when the
/// cascade reads the adjacency; the batch's own deletion wins, so the edge is
/// removed once and counted once.
fn exercise_batch_deleted_incident(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["a", "b", "c"], &[("a", "b"), ("c", "a")]);

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![
                delete_edge("a", "b"),
                delete_edge("c", "a"),
                delete_node("a"),
            ]),
        )
        .expect("the cascade defers to the batch's own deletions");
    assert_eq!(
        deleted_flags(&outcome),
        vec![Some(true), Some(true), Some(true)]
    );
    assert!(!has_edge(&graph, "a", "b"));
    assert!(!has_edge(&graph, "c", "a"));
    expect_counts(&graph, 2, 0, "after the batch");
}

#[test]
fn batch_edge_on_a_node_the_batch_deleted_is_a_missing_endpoint() {
    run_database_modes(exercise_deleted_endpoint);
}

/// The overlay is sequential: an endpoint deleted earlier in the batch is
/// missing for a later edge, whichever end it is, and the refusal leaves the
/// graph untouched.
fn exercise_deleted_endpoint(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["a", "b"], &[("a", "b")]);

    for batch in [
        vec![delete_node("a"), upsert_edge("a", "b")],
        vec![delete_node("b"), upsert_edge("a", "b")],
    ] {
        let error = graph
            .batch_write(&name(), &GraphBatchWrite::new(batch))
            .expect_err("an edge on a node deleted earlier in the batch is refused");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.graph_edge_endpoint");
        assert!(has_node(&graph, "a"), "the refused batch wrote nothing");
        assert!(has_node(&graph, "b"), "the refused batch wrote nothing");
        assert!(
            has_edge(&graph, "a", "b"),
            "the refused batch wrote nothing"
        );
        expect_counts(&graph, 2, 1, "after the refusal");
    }
}

#[test]
fn batch_delete_then_recreate_reports_the_edge_as_created_again() {
    run_database_modes(exercise_delete_then_recreate);
}

/// Deleting a node cascades its edge away inside the batch, so recreating
/// the node and the edge later in the same batch creates both — and the
/// counts end where they started.
fn exercise_delete_then_recreate(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["a", "b"], &[("a", "b")]);

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![
                delete_node("a"),
                upsert_node("a"),
                upsert_edge("a", "b"),
            ]),
        )
        .expect("batch succeeds");
    assert_eq!(deleted_flags(&outcome)[0], Some(true));
    assert_eq!(created_flags(&outcome)[1..], [Some(true), Some(true)]);
    assert!(has_edge(&graph, "a", "b"));
    assert_eq!(neighbor_ids(&graph, "b", GraphDirection::Incoming), ["a"]);
    expect_counts(&graph, 2, 1, "after delete and recreate");
}

#[test]
fn batch_repeated_operations_report_their_second_application_honestly() {
    run_database_modes(exercise_repeated_operations);
}

/// The second of two identical operations sees the first: a node or edge is
/// created once and deleted once, and the counts move once.
fn exercise_repeated_operations(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["keep"], &[]);

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![
                upsert_node("a"),
                upsert_node("a"),
                upsert_node("b"),
                upsert_edge("a", "b"),
                upsert_edge("a", "b"),
            ]),
        )
        .expect("batch succeeds");
    assert_eq!(
        created_flags(&outcome),
        vec![Some(true), Some(false), Some(true), Some(true), Some(false)]
    );
    expect_counts(&graph, 3, 1, "after the creating batch");

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![
                delete_edge("a", "b"),
                delete_edge("a", "b"),
                delete_node("a"),
                delete_node("a"),
            ]),
        )
        .expect("batch succeeds");
    assert_eq!(
        deleted_flags(&outcome),
        vec![Some(true), Some(false), Some(true), Some(false)]
    );
    assert!(!has_node(&graph, "a"));
    assert!(has_node(&graph, "b"));
    expect_counts(&graph, 2, 0, "after the deleting batch");
}

#[test]
fn binding_cascade_removes_an_edge_between_two_cascaded_nodes_once() {
    run_database_modes(exercise_binding_cascade);
}

/// The cascade delete policy reads each bound node's own adjacency; an edge
/// between two of them is found from both ends and removed — and counted —
/// once, while their edges to other nodes go too.
fn exercise_binding_cascade(mut database: Database) {
    let target = GraphBindingTarget::new(GraphBindingPrimitive::Kv, None, space("docs"), "doc-1")
        .expect("valid binding target");
    let bound = GraphNodeData::new(None, Some(GraphEntityBinding::new(target.clone())));
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &["other"], &[]);
    for id in ["a", "b"] {
        graph
            .upsert_node(&name(), node(id), bound.clone())
            .expect("bound node written");
    }
    for (src, dst) in [("a", "b"), ("b", "a"), ("a", "other"), ("other", "b")] {
        graph
            .upsert_edge(
                &name(),
                node(src),
                kind(),
                node(dst),
                GraphEdgeData::default(),
            )
            .expect("edge written");
    }
    expect_counts(&graph, 3, 4, "seeded");

    let outcome = graph
        .apply_binding_delete_policy(&target, GraphDeletePolicy::Cascade)
        .expect("cascade applies");
    assert_eq!(outcome.nodes_affected(), 2);
    assert!(outcome.commit().is_some());
    assert!(has_node(&graph, "other"));
    assert!(!has_node(&graph, "a"));
    assert!(!has_node(&graph, "b"));
    assert_eq!(
        neighbor_ids(&graph, "other", GraphDirection::Both),
        Vec::<String>::new()
    );
    expect_counts(&graph, 1, 0, "after the cascade");
}

#[test]
fn single_delete_node_cascades_every_stored_incident_edge_once() {
    run_database_modes(exercise_single_delete_cascade);
}

/// The single-node delete reads the same adjacency the batch does: incoming,
/// outgoing and self-loop go, the self-loop once, and other nodes' edges stay.
fn exercise_single_delete_cascade(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(
        &mut graph,
        &["p", "x", "q", "r"],
        &[("p", "x"), ("x", "q"), ("x", "x"), ("p", "r")],
    );

    let outcome = graph
        .delete_node(&name(), &node("x"))
        .expect("delete succeeds");
    assert!(outcome.deleted());

    assert!(!has_edge(&graph, "p", "x"), "incoming edge cascaded");
    assert!(!has_edge(&graph, "x", "q"), "outgoing edge cascaded");
    assert!(!has_edge(&graph, "x", "x"), "self-loop cascaded");
    assert!(has_edge(&graph, "p", "r"), "an edge of another node stays");
    assert_eq!(neighbor_ids(&graph, "p", GraphDirection::Outgoing), ["r"]);
    expect_counts(&graph, 3, 1, "after the cascade");
}

#[test]
fn batch_deleting_both_endpoints_removes_a_shared_edge_once() {
    run_database_modes(exercise_both_endpoints_deleted);
}

/// Two `DeleteNode` ops in one batch that share edges: the first marks each
/// cascaded edge absent in the overlay, so the second — which reads the same
/// stored rows from its own adjacency prefixes — does not remove or count them
/// again. A double count would drive the edge delta past the live edge count
/// and fail the commit with `data_loss.engine.graph_metadata`, so a clean
/// commit at the right counts is the proof the overlay dedupes across ops.
fn exercise_both_endpoints_deleted(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(
        &mut graph,
        &["p", "x", "y", "r"],
        // (x,y) and (y,x) are each incident to both deleted nodes; (x,x) is a
        // self-loop on a deleted node; (p,x) enters and (y,r) leaves the
        // deleted pair; (p,r) is untouched.
        &[
            ("x", "y"),
            ("y", "x"),
            ("x", "x"),
            ("p", "x"),
            ("y", "r"),
            ("p", "r"),
        ],
    );
    expect_counts(&graph, 4, 6, "seeded");

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![delete_node("x"), delete_node("y")]),
        )
        .expect("deleting both endpoints of shared edges must not double-count");
    assert_eq!(deleted_flags(&outcome), vec![Some(true), Some(true)]);

    assert!(!has_node(&graph, "x"));
    assert!(!has_node(&graph, "y"));
    for (src, dst) in [("x", "y"), ("y", "x"), ("x", "x"), ("p", "x"), ("y", "r")] {
        assert!(!has_edge(&graph, src, dst), "{src}->{dst} cascaded");
    }
    assert!(has_edge(&graph, "p", "r"), "the survivors' edge stays");
    assert_eq!(neighbor_ids(&graph, "p", GraphDirection::Outgoing), ["r"]);
    expect_counts(&graph, 2, 1, "after deleting both endpoints");
}

/// A node bound to `target`.
fn bound_node(id: &str, target: &GraphBindingTarget) -> GraphBatchOperation {
    GraphBatchOperation::UpsertNode {
        node_id: node(id),
        data: GraphNodeData::new(None, Some(GraphEntityBinding::new(target.clone()))),
    }
}

/// A `Kv` binding target in the `docs` space, keyed by `key`.
fn kv_target(key: &str) -> GraphBindingTarget {
    GraphBindingTarget::new(GraphBindingPrimitive::Kv, None, space("docs"), key)
        .expect("valid binding target")
}

/// The node ids bound to `target`, in id order.
fn bound_ids(graph: &GraphService<'_>, target: &GraphBindingTarget) -> Vec<String> {
    let mut ids: Vec<String> = graph
        .bindings_for_entity(target, None, 64)
        .expect("bindings read")
        .bindings()
        .iter()
        .map(|binding| binding.node_id().as_str().to_owned())
        .collect();
    ids.sort();
    ids
}

#[test]
fn batch_rebinding_a_node_within_the_batch_rewrites_the_binding_index() {
    run_database_modes(exercise_within_batch_rebind);
}

/// Re-binding a node later in the same batch reads the batch-local record the
/// earlier op wrote, so the old target's index row is dropped and only the new
/// target's remains; a later batch that deletes the bound node clears that too.
fn exercise_within_batch_rebind(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &[], &[]);
    let first = kv_target("doc-1");
    let second = kv_target("doc-2");

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![bound_node("a", &first), bound_node("a", &second)]),
        )
        .expect("rebind batch succeeds");
    // The node is created once — the second upsert replaces the batch-local
    // record rather than creating a second node.
    assert_eq!(created_flags(&outcome), vec![Some(true), Some(false)]);
    assert_eq!(
        bound_ids(&graph, &first),
        Vec::<String>::new(),
        "the first target's index row was rewritten away"
    );
    assert_eq!(
        bound_ids(&graph, &second),
        ["a"],
        "the second target's index row is the live one"
    );
    expect_counts(&graph, 1, 0, "after the rebind");

    graph
        .batch_write(&name(), &GraphBatchWrite::new(vec![delete_node("a")]))
        .expect("delete batch succeeds");
    assert_eq!(
        bound_ids(&graph, &second),
        Vec::<String>::new(),
        "deleting the bound node cleared its binding index row"
    );
    expect_counts(&graph, 0, 0, "after deleting the bound node");
}

/// A node declaring `object_type`.
fn typed_node(id: &str, object_type: &str) -> GraphBatchOperation {
    GraphBatchOperation::UpsertNode {
        node_id: node(id),
        data: GraphNodeData::new(None, None)
            .with_object_type(GraphTypeName::new(object_type).expect("type name")),
    }
}

/// The node ids declaring `object_type`, in id order.
fn typed_ids(graph: &GraphService<'_>, object_type: &str) -> Vec<String> {
    let mut ids: Vec<String> = graph
        .nodes_by_type(
            &name(),
            &GraphTypeName::new(object_type).expect("type name"),
            None,
            64,
        )
        .expect("type index read")
        .nodes()
        .iter()
        .map(|node| node.node_id().as_str().to_owned())
        .collect();
    ids.sort();
    ids
}

#[test]
fn batch_retyping_a_node_within_the_batch_rewrites_the_type_index() {
    run_database_modes(exercise_within_batch_retype);
}

/// Re-typing a node later in the same batch reads the batch-local record the
/// earlier op wrote, so the old type's index row is dropped and only the new
/// type's remains; a later batch that deletes the node clears that too.
fn exercise_within_batch_retype(mut database: Database) {
    let mut graph = graph_service(&mut database);
    seed(&mut graph, &[], &[]);

    let outcome = graph
        .batch_write(
            &name(),
            &GraphBatchWrite::new(vec![typed_node("a", "Author"), typed_node("a", "Editor")]),
        )
        .expect("retype batch succeeds");
    assert_eq!(created_flags(&outcome), vec![Some(true), Some(false)]);
    assert_eq!(
        typed_ids(&graph, "Author"),
        Vec::<String>::new(),
        "the first type's index row was rewritten away"
    );
    assert_eq!(
        typed_ids(&graph, "Editor"),
        ["a"],
        "the second type's index row is the live one"
    );
    expect_counts(&graph, 1, 0, "after the retype");

    graph
        .batch_write(&name(), &GraphBatchWrite::new(vec![delete_node("a")]))
        .expect("delete batch succeeds");
    assert_eq!(
        typed_ids(&graph, "Editor"),
        Vec::<String>::new(),
        "deleting the typed node cleared its type index row"
    );
    expect_counts(&graph, 0, 0, "after deleting the typed node");
}
