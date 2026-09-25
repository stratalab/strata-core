//! GI3 executor behavior: bulk ingest through the command surface —
//! counts, chunked commits, and refusal codes.

#![allow(clippy::result_large_err)]

use strata_executor::{Command, Executor, GraphBulkEdge, GraphBulkNode, GraphDirection, Output};
use tempfile::TempDir;

fn run_modes(mut exercise: impl FnMut(&mut Executor)) {
    let mut cache = Executor::open_cache().expect("cache executor opens");
    exercise(&mut cache);

    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");
    let mut durable = Executor::open_durable_local(&path).expect("durable executor opens");
    exercise(&mut durable);
}

fn bulk_node(id: &str) -> GraphBulkNode {
    GraphBulkNode::new(id.to_owned(), None, None, None)
}

fn graph_meta(executor: &mut Executor, graph: &str) -> Option<strata_executor::GraphInfoData> {
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

fn bulk_edge(src: &str, kind: &str, dst: &str, weight: f64) -> GraphBulkEdge {
    GraphBulkEdge::new(
        src.to_owned(),
        kind.to_owned(),
        dst.to_owned(),
        Some(weight),
        None,
    )
}

#[test]
fn bulk_insert_command_ingests_in_cache_and_durable_modes() {
    run_modes(exercise_bulk_command);
}

fn exercise_bulk_command(executor: &mut Executor) {
    executor.graph_create("bulk").expect("graph created");

    let output = executor
        .graph_bulk_insert(
            "bulk",
            vec![bulk_node("a"), bulk_node("b"), bulk_node("c")],
            vec![bulk_edge("a", "e", "b", 1.0), bulk_edge("b", "e", "c", 2.0)],
        )
        .expect("bulk ingest");
    let Output::GraphBulkInsertResult {
        graph,
        nodes_inserted,
        edges_inserted,
        commits,
        commit,
        ..
    } = output
    else {
        panic!("unexpected bulk output");
    };
    assert_eq!(graph, "bulk");
    assert_eq!(nodes_inserted, 3);
    assert_eq!(edges_inserted, 2);
    assert_eq!(commits, 2, "one node chunk, one edge chunk");
    assert!(commit.is_some());

    // The ingested rows serve every read surface.
    let output = executor
        .graph_neighbors("bulk", "a", GraphDirection::Outgoing, None, None, None)
        .expect("neighbors read");
    let Output::GraphNeighborPage { items, .. } = output else {
        panic!("unexpected neighbors output");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].node_id(), "b");

    let Output::GraphWccResult(wcc) = executor.graph_wcc("bulk").expect("wcc runs") else {
        panic!("unexpected wcc output");
    };
    assert_eq!(wcc.component_count(), 1);

    // A completed import leaves no watermark: graph meta reports it not
    // pending (#3464). This drives the engine info through `graph_info_data`
    // and reads the wire DTO's `import_pending`, so the not-pending value is
    // covered under default features.
    let meta = graph_meta(executor, "bulk").expect("graph exists");
    assert!(
        !meta.import_pending(),
        "a completed bulk import leaves no pending watermark"
    );

    // A dangling endpoint refuses by code, with an explicit chunk size
    // on the wire.
    let error = executor
        .execute(Command::GraphBulkInsert {
            branch: None,
            space: None,
            graph: "bulk".to_owned(),
            nodes: Vec::new(),
            edges: vec![bulk_edge("a", "e", "ghost", 1.0)],
            chunk_size: Some(10),
        })
        .expect_err("dangling endpoint");
    assert_eq!(error.code(), "invalid_argument.engine.graph_edge_endpoint");
}

/// #3464: an interrupted multi-commit import surfaces through the wire DTO as
/// `import_pending`. Reaching a pending state needs the engine interruption
/// seam (`bulk_insert_interrupted_for_test`, `testkit`-gated), so this test —
/// like the executor mutation lane — runs under `testkit`. It plants the cut
/// on a raw engine handle, then reads it back through `graph_info_data` and the
/// DTO accessor, covering the pending value neither the corpus nor the
/// completed-import test can produce through the public command surface.
#[cfg(feature = "testkit")]
#[test]
fn graph_meta_reports_import_pending_after_an_interrupted_bulk_import() {
    use strata_engine::{
        CacheOpenOptions, Database, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData,
        GraphNodeId, ProductSpace,
    };

    let database = Database::open_cache(CacheOpenOptions::new())
        .expect("cache opens")
        .into_database();
    // Plant on the branch/space the executor's `GraphGetMeta { branch: None,
    // space: None }` resolves to: the handle's default branch and DEFAULT_SPACE.
    let branch = database.default_branch().clone();
    let city = GraphName::new("city").expect("graph name");
    {
        let mut graph = database
            .graph(branch, ProductSpace::new("default").expect("space"))
            .expect("graph service");
        graph.create_graph(city.clone()).expect("graph created");
        let street = GraphEdgeType::new("street").expect("edge type");
        let node = |index: usize| GraphNodeId::new(format!("n:{index}")).expect("node id");
        let nodes: Vec<_> = (0..20)
            .map(|i| (node(i), GraphNodeData::default()))
            .collect();
        let edges: Vec<_> = (0..20)
            .map(|i| {
                (
                    node(i),
                    street.clone(),
                    node((i + 1) % 20),
                    GraphEdgeData::default(),
                )
            })
            .collect();
        // Six chunks at size 8; stop after four, mid-edges — the graph is left
        // pending, exactly as a crash there would leave it.
        graph
            .bulk_insert_interrupted_for_test(&city, &nodes, &edges, Some(8), 4)
            .expect("interrupted import commits four chunks");
    }

    let mut executor = Executor::from_database(database);
    let meta = graph_meta(&mut executor, "city").expect("graph exists");
    assert!(
        meta.import_pending(),
        "an interrupted import surfaces through the wire DTO as pending"
    );
}

#[test]
fn graph_bulk_insert_rejects_unknown_node_and_edge_keys_instead_of_dropping_data() {
    // A misspelled optional key on a bulk node/edge must be rejected, not
    // silently dropped: dropping it ingests the item without the intended
    // properties/binding/weight, with no error.
    let node_error = serde_json::from_str::<Command>(
        r#"{"type":"graph_bulk_insert","graph":"g","nodes":[{"node_id":"a","propertes":{}}]}"#,
    )
    .expect_err("an unknown node key is rejected");
    assert!(
        node_error.to_string().contains("propertes"),
        "the rejection names the unknown node key: {node_error}"
    );

    let edge_error = serde_json::from_str::<Command>(
        r#"{"type":"graph_bulk_insert","graph":"g","edges":[{"src":"a","edge_type":"e","dst":"b","weightt":2.0}]}"#,
    )
    .expect_err("an unknown edge key is rejected");
    assert!(
        edge_error.to_string().contains("weightt"),
        "the rejection names the unknown edge key: {edge_error}"
    );

    // Correctly-spelled bulk items still deserialize.
    serde_json::from_str::<Command>(
        r#"{"type":"graph_bulk_insert","graph":"g","nodes":[{"node_id":"a","properties":{"k":1}}],"edges":[{"src":"a","edge_type":"e","dst":"b","weight":2.0}]}"#,
    )
    .expect("valid bulk items deserialize");
}
