//! Graph ontology conformance tests (GO1): draft/freeze lifecycle,
//! freeze-time validation, frozen immutability, temporal reads, and
//! space isolation with durable reopen.

mod common;

use serde_json::json;
use strata_engine::{
    Database, GraphBatchOperation, GraphBatchWrite, GraphEdgeData, GraphEdgeType, GraphLinkTypeDef,
    GraphName, GraphNodeData, GraphNodeId, GraphObjectTypeDef, GraphOntologyStatus,
    GraphProperties, GraphPropertyDef, GraphTypeName,
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

fn type_name(value: &str) -> GraphTypeName {
    GraphTypeName::new(value).expect("valid type name")
}

fn object_type(name: &str) -> GraphObjectTypeDef {
    GraphObjectTypeDef::new(
        type_name(name),
        [(
            "title".to_owned(),
            GraphPropertyDef::new(Some("string".to_owned()), true).expect("property"),
        )],
    )
    .expect("object type")
}

fn link_type(name: &str, source: &str, target: &str) -> GraphLinkTypeDef {
    GraphLinkTypeDef::new(
        type_name(name),
        type_name(source),
        type_name(target),
        Some("one-to-many".to_owned()),
        [],
    )
    .expect("link type")
}

fn node_id(value: &str) -> GraphNodeId {
    GraphNodeId::new(value).expect("valid node id")
}

fn edge_type(value: &str) -> GraphEdgeType {
    GraphEdgeType::new(value).expect("valid edge type")
}

fn typed_node(object_type_name: &str, title: Option<&str>) -> GraphNodeData {
    let properties =
        title.map(|title| GraphProperties::new(json!({ "title": title })).expect("properties"));
    GraphNodeData::new(properties, None).with_object_type(type_name(object_type_name))
}

/// A frozen Author/Document/wrote ontology on graph `name`.
fn freeze_author_document(graph: &mut strata_engine::GraphService<'_>, name: &GraphName) {
    graph
        .define_object_type(name, object_type("Author"))
        .expect("author type");
    graph
        .define_object_type(name, object_type("Document"))
        .expect("document type");
    graph
        .define_link_type(name, link_type("wrote", "Author", "Document"))
        .expect("link type");
    graph.freeze_ontology(name).expect("freeze succeeds");
}

#[test]
fn graph_ontology_lifecycle_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_ontology_lifecycle);
}

fn exercise_ontology_lifecycle(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    graph.create_graph(name.clone()).expect("graph created");

    assert!(
        graph.ontology(&name).expect("read succeeds").is_none(),
        "no ontology before any definition"
    );

    // Definitions are upserts while Draft.
    let created = graph
        .define_object_type(&name, object_type("Document"))
        .expect("define succeeds");
    assert!(created.created(), "first definition is new");
    let redefined = graph
        .define_object_type(&name, object_type("Document"))
        .expect("redefine succeeds while draft");
    assert!(!redefined.created(), "redefinition is not new");
    graph
        .define_object_type(&name, object_type("Author"))
        .expect("second type");
    graph
        .define_link_type(&name, link_type("wrote", "Author", "Document"))
        .expect("link type");

    let ontology = graph
        .ontology(&name)
        .expect("read succeeds")
        .expect("ontology visible");
    assert_eq!(ontology.status(), GraphOntologyStatus::Draft);
    let object_names: Vec<&str> = ontology
        .object_types()
        .iter()
        .map(|def| def.name().as_str())
        .collect();
    assert_eq!(object_names, vec!["Author", "Document"], "sorted by name");
    assert_eq!(ontology.link_types().len(), 1);
    let link = &ontology.link_types()[0];
    assert_eq!(link.source().as_str(), "Author");
    assert_eq!(link.target().as_str(), "Document");
    assert_eq!(link.cardinality(), Some("one-to-many"));

    // Draft deletes work and report absence honestly.
    let deleted = graph
        .delete_object_type(&name, &type_name("Author"))
        .expect("delete succeeds");
    assert!(deleted.deleted());
    let missing = graph
        .delete_object_type(&name, &type_name("Author"))
        .expect("second delete succeeds");
    assert!(!missing.deleted(), "absent type reports deleted=false");

    // Restore and freeze.
    graph
        .define_object_type(&name, object_type("Author"))
        .expect("restore type");
    let frozen = graph.freeze_ontology(&name).expect("freeze succeeds");
    assert_eq!(frozen.object_types(), 2);
    assert_eq!(frozen.link_types(), 1);
    let ontology = graph
        .ontology(&name)
        .expect("read succeeds")
        .expect("ontology visible");
    assert_eq!(ontology.status(), GraphOntologyStatus::Frozen);

    // Frozen ontologies are immutable — every mutation refuses.
    let error = graph
        .define_object_type(&name, object_type("Reviewer"))
        .expect_err("define after freeze refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_frozen"
    );
    let error = graph
        .delete_link_type(&name, &type_name("wrote"))
        .expect_err("delete after freeze refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_frozen"
    );
    let error = graph
        .freeze_ontology(&name)
        .expect_err("second freeze refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_frozen"
    );
}

#[test]
fn graph_ontology_freeze_validation_and_refusals_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_freeze_validation_and_refusals);
}

fn exercise_freeze_validation_and_refusals(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");

    // Every ontology call requires the graph to exist.
    let error = graph.ontology(&name).expect_err("missing graph refuses");
    assert_eq!(error.code(), "not_found.engine.graph");
    let error = graph
        .define_object_type(&name, object_type("Document"))
        .expect_err("missing graph refuses");
    assert_eq!(error.code(), "not_found.engine.graph");

    graph.create_graph(name.clone()).expect("graph created");

    // Freezing nothing refuses.
    let error = graph
        .freeze_ontology(&name)
        .expect_err("empty ontology cannot freeze");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_freeze"
    );

    // A link endpoint must reference a declared object type.
    graph
        .define_link_type(&name, link_type("wrote", "Author", "Document"))
        .expect("link type in draft");
    let error = graph
        .freeze_ontology(&name)
        .expect_err("dangling endpoint cannot freeze");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_freeze"
    );

    // Declaring the endpoints repairs the freeze.
    graph
        .define_object_type(&name, object_type("Author"))
        .expect("author type");
    graph
        .define_object_type(&name, object_type("Document"))
        .expect("document type");
    assert!(graph.freeze_ontology(&name).is_ok(), "freeze repaired");
}

#[test]
fn graph_ontology_temporal_reads_track_the_freeze_in_cache_and_durable_modes() {
    run_database_modes(exercise_temporal_reads);
}

fn exercise_temporal_reads(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    graph.create_graph(name.clone()).expect("graph created");

    let defined = graph
        .define_object_type(&name, object_type("Document"))
        .expect("define succeeds");
    let draft_version = defined.commit().version();
    let frozen = graph.freeze_ontology(&name).expect("freeze succeeds");
    let frozen_version = frozen.commit().version();

    let at_draft = graph
        .ontology_at_version(&name, draft_version)
        .expect("read succeeds")
        .expect("ontology visible at draft version");
    assert_eq!(at_draft.status(), GraphOntologyStatus::Draft);
    assert_eq!(at_draft.version(), draft_version);

    let at_freeze = graph
        .ontology_at_version(&name, frozen_version)
        .expect("read succeeds")
        .expect("ontology visible at freeze version");
    assert_eq!(at_freeze.status(), GraphOntologyStatus::Frozen);

    let latest = graph
        .ontology(&name)
        .expect("read succeeds")
        .expect("ontology visible");
    assert_eq!(latest.status(), GraphOntologyStatus::Frozen);
    assert_eq!(latest.version(), frozen_version);
}

#[test]
fn graph_ontology_enforces_writes_once_frozen_in_cache_and_durable_modes() {
    run_database_modes(exercise_write_enforcement);
}

#[allow(clippy::too_many_lines)]
fn exercise_write_enforcement(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    graph.create_graph(name.clone()).expect("graph created");

    // Before any ontology, everything is accepted — including typed nodes
    // naming types that do not exist yet.
    graph
        .upsert_node(&name, node_id("free"), GraphNodeData::default())
        .expect("untyped node before ontology");
    graph
        .upsert_node(&name, node_id("early"), typed_node("Anything", None))
        .expect("typed node before ontology is unvalidated");

    freeze_author_document(&mut graph, &name);

    // Untyped nodes still pass after the freeze (light enforcement).
    graph
        .upsert_node(&name, node_id("free2"), GraphNodeData::default())
        .expect("untyped node after freeze");

    // Typed writes validate.
    graph
        .upsert_node(&name, node_id("d1"), typed_node("Document", Some("Spec")))
        .expect("declared type with required property");
    let error = graph
        .upsert_node(&name, node_id("x1"), typed_node("Reviewer", Some("T")))
        .expect_err("undeclared object type refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_node_type"
    );
    let error = graph
        .upsert_node(&name, node_id("d2"), typed_node("Document", None))
        .expect_err("missing required property refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_required_property"
    );

    // The stored node round-trips its object type.
    let node = graph
        .get_node(&name, &node_id("d1"))
        .expect("read succeeds")
        .expect("node visible");
    assert_eq!(
        node.data().object_type().map(GraphTypeName::as_str),
        Some("Document")
    );

    // Edge validation: declared link type, endpoint types must match.
    graph
        .upsert_node(&name, node_id("a1"), typed_node("Author", Some("Ada")))
        .expect("author node");
    graph
        .upsert_edge(
            &name,
            node_id("a1"),
            edge_type("wrote"),
            node_id("d1"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect("declared link with matching endpoints");
    let error = graph
        .upsert_edge(
            &name,
            node_id("a1"),
            edge_type("cites"),
            node_id("d1"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect_err("undeclared edge type refuses");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_edge_type"
    );
    let error = graph
        .upsert_edge(
            &name,
            node_id("d1"),
            edge_type("wrote"),
            node_id("a1"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect_err("mismatched endpoint types refuse");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_endpoint_type"
    );

    // Untyped endpoints skip the endpoint check.
    graph
        .upsert_edge(
            &name,
            node_id("free"),
            edge_type("wrote"),
            node_id("d1"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect("untyped source endpoint skips type matching");
}

#[test]
fn graph_ontology_batch_writes_validate_against_pending_state() {
    run_database_modes(exercise_batch_enforcement);
}

fn exercise_batch_enforcement(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    graph.create_graph(name.clone()).expect("graph created");
    freeze_author_document(&mut graph, &name);

    // An edge is validated against nodes typed earlier in the same batch.
    let batch = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node_id("a1"),
            data: typed_node("Author", Some("Ada")),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node_id("d1"),
            data: typed_node("Document", Some("Spec")),
        },
        GraphBatchOperation::UpsertEdge {
            src: node_id("a1"),
            edge_type: edge_type("wrote"),
            dst: node_id("d1"),
            data: GraphEdgeData::new(1.0, None).expect("edge data"),
        },
    ]);
    let outcome = graph.batch_write(&name, &batch).expect("batch succeeds");
    assert_eq!(outcome.results().len(), 3);

    // A refused op refuses the whole batch: nothing lands (atomicity).
    let batch = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node_id("d9"),
            data: typed_node("Document", Some("Nine")),
        },
        GraphBatchOperation::UpsertEdge {
            src: node_id("d9"),
            edge_type: edge_type("cites"),
            dst: node_id("d9"),
            data: GraphEdgeData::new(1.0, None).expect("edge data"),
        },
    ]);
    let error = graph
        .batch_write(&name, &batch)
        .expect_err("undeclared edge type refuses the batch");
    assert_eq!(
        error.code(),
        "failed_precondition.engine.graph_ontology_edge_type"
    );
    assert!(
        graph
            .get_node(&name, &node_id("d9"))
            .expect("read succeeds")
            .is_none(),
        "refused batch landed nothing"
    );

    // A Draft ontology does not validate: same shapes pass on a fresh
    // graph whose ontology was never frozen.
    let draft = graph_name("draft");
    graph.create_graph(draft.clone()).expect("graph created");
    graph
        .define_object_type(&draft, object_type("Author"))
        .expect("draft type");
    graph
        .upsert_node(&draft, node_id("n1"), typed_node("Unheard", None))
        .expect("draft ontology does not validate node types");
    graph
        .upsert_node(&draft, node_id("n2"), GraphNodeData::default())
        .expect("untyped node");
    graph
        .upsert_edge(
            &draft,
            node_id("n1"),
            edge_type("whatever"),
            node_id("n2"),
            GraphEdgeData::new(1.0, None).expect("edge data"),
        )
        .expect("draft ontology does not validate edge types");
}

#[test]
fn graph_type_index_tracks_node_types_in_cache_and_durable_modes() {
    run_database_modes(exercise_type_index);
}

#[allow(clippy::too_many_lines)]
fn exercise_type_index(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");
    graph.create_graph(name.clone()).expect("graph created");

    // The index is ontology-status-agnostic: no ontology exists here.
    for id in ["d1", "d2", "d3"] {
        graph
            .upsert_node(&name, node_id(id), typed_node("Document", Some(id)))
            .expect("typed node");
    }
    let initial = graph
        .upsert_node(&name, node_id("a1"), typed_node("Author", Some("Ada")))
        .expect("author node");
    let initial_version = initial.commit().version();
    graph
        .upsert_node(&name, node_id("free"), GraphNodeData::default())
        .expect("untyped node");

    // Typed upserts count one authored row: the derived index row is
    // engine-maintained and excluded.
    assert_eq!(initial.commit().put_count(), 1);

    let page = graph
        .nodes_by_type(&name, &type_name("Document"), None, 10)
        .expect("nodes by type");
    let ids: Vec<&str> = page
        .nodes()
        .iter()
        .map(|node| node.node_id().as_str())
        .collect();
    assert_eq!(ids, vec!["d1", "d2", "d3"], "node-id ordered");
    assert!(page
        .nodes()
        .iter()
        .all(|node| node.data().object_type().map(GraphTypeName::as_str) == Some("Document")));

    // Pagination through the index.
    let first = graph
        .nodes_by_type(&name, &type_name("Document"), None, 2)
        .expect("first page");
    assert_eq!(first.nodes().len(), 2);
    assert!(first.has_more());
    let second = graph
        .nodes_by_type(&name, &type_name("Document"), first.cursor(), 2)
        .expect("second page");
    assert_eq!(second.nodes().len(), 1);
    assert!(!second.has_more());

    // Retype, untype, delete: the index follows.
    graph
        .upsert_node(&name, node_id("d2"), typed_node("Author", Some("d2")))
        .expect("retype");
    graph
        .upsert_node(&name, node_id("d3"), GraphNodeData::default())
        .expect("untype");
    graph.delete_node(&name, &node_id("d1")).expect("delete");
    let documents = graph
        .nodes_by_type(&name, &type_name("Document"), None, 10)
        .expect("documents");
    assert!(documents.nodes().is_empty(), "all documents left the type");
    let author_page = graph
        .nodes_by_type(&name, &type_name("Author"), None, 10)
        .expect("authors");
    let authors: Vec<&str> = author_page
        .nodes()
        .iter()
        .map(|node| node.node_id().as_str())
        .collect();
    assert_eq!(authors, vec!["a1", "d2"]);

    // Temporal: the index answers as of the initial state.
    let at_initial: Vec<String> = graph
        .nodes_by_type_at_version(&name, &type_name("Document"), None, 10, initial_version)
        .expect("temporal read")
        .nodes()
        .iter()
        .map(|node| node.node_id().as_str().to_owned())
        .collect();
    assert_eq!(at_initial, vec!["d1", "d2", "d3"]);

    // Batch maintenance is batch-local: an op sequence lands consistently.
    let batch = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node_id("x"),
            data: typed_node("Document", Some("X")),
        },
        GraphBatchOperation::UpsertNode {
            node_id: node_id("y"),
            data: typed_node("Document", Some("Y")),
        },
        GraphBatchOperation::DeleteNode {
            node_id: node_id("x"),
        },
    ]);
    graph.batch_write(&name, &batch).expect("batch succeeds");
    let document_page = graph
        .nodes_by_type(&name, &type_name("Document"), None, 10)
        .expect("documents");
    let documents: Vec<&str> = document_page
        .nodes()
        .iter()
        .map(|node| node.node_id().as_str())
        .collect();
    assert_eq!(documents, vec!["y"], "batch-local index maintenance");
}

#[test]
fn graph_ontology_summary_reports_type_usage_in_cache_and_durable_modes() {
    run_database_modes(exercise_ontology_summary);
}

fn exercise_ontology_summary(mut database: Database) {
    let mut graph = graph_service(&mut database, "default", "default");
    let name = graph_name("deps");

    let error = graph
        .ontology_summary(&name)
        .expect_err("missing graph refuses");
    assert_eq!(error.code(), "not_found.engine.graph");

    graph.create_graph(name.clone()).expect("graph created");
    assert!(
        graph
            .ontology_summary(&name)
            .expect("read succeeds")
            .is_none(),
        "no summary before any definition"
    );

    freeze_author_document(&mut graph, &name);
    let freeze_version = graph
        .ontology(&name)
        .expect("read succeeds")
        .expect("ontology visible")
        .version();

    let empty = graph
        .ontology_summary(&name)
        .expect("read succeeds")
        .expect("summary visible");
    assert_eq!(empty.status(), GraphOntologyStatus::Frozen);
    assert!(empty
        .object_types()
        .iter()
        .all(|summary| summary.node_count() == 0));
    assert!(empty
        .link_types()
        .iter()
        .all(|summary| summary.edge_count() == 0));

    // Populate: 1 Author, 2 Documents, 2 `wrote` edges, 1 untyped node.
    graph
        .upsert_node(&name, node_id("a1"), typed_node("Author", Some("Ada")))
        .expect("author");
    for id in ["d1", "d2"] {
        graph
            .upsert_node(&name, node_id(id), typed_node("Document", Some(id)))
            .expect("document");
    }
    graph
        .upsert_node(&name, node_id("free"), GraphNodeData::default())
        .expect("untyped");
    for dst in ["d1", "d2"] {
        graph
            .upsert_edge(
                &name,
                node_id("a1"),
                edge_type("wrote"),
                node_id(dst),
                GraphEdgeData::new(1.0, None).expect("edge data"),
            )
            .expect("edge");
    }

    let summary = graph
        .ontology_summary(&name)
        .expect("read succeeds")
        .expect("summary visible");
    let object_counts: Vec<(&str, u64)> = summary
        .object_types()
        .iter()
        .map(|entry| (entry.def().name().as_str(), entry.node_count()))
        .collect();
    assert_eq!(object_counts, vec![("Author", 1), ("Document", 2)]);
    assert_eq!(summary.link_types().len(), 1);
    assert_eq!(summary.link_types()[0].def().name().as_str(), "wrote");
    assert_eq!(summary.link_types()[0].edge_count(), 2);

    // Temporal: counts as of the freeze are zero.
    let at_freeze = graph
        .ontology_summary_at_version(&name, freeze_version)
        .expect("read succeeds")
        .expect("summary visible");
    assert!(at_freeze
        .object_types()
        .iter()
        .all(|entry| entry.node_count() == 0));

    // Deletes are reflected exactly.
    graph
        .delete_edge(&name, &node_id("a1"), &edge_type("wrote"), &node_id("d2"))
        .expect("delete edge");
    let summary = graph
        .ontology_summary(&name)
        .expect("read succeeds")
        .expect("summary visible");
    assert_eq!(summary.link_types()[0].edge_count(), 1);
}

#[test]
fn graph_ontology_space_isolation_reopen_and_delete_cleanup_on_durable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let name = graph_name("deps");
    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        let mut graph = graph_service(&mut database, "default", "docs");
        graph.create_graph(name.clone()).expect("graph created");
        graph
            .define_object_type(&name, object_type("Document"))
            .expect("define succeeds");
        graph.freeze_ontology(&name).expect("freeze succeeds");
        graph
            .upsert_node(&name, node_id("d1"), typed_node("Document", Some("Spec")))
            .expect("typed node");

        // The same graph name in another space has no ontology.
        let mut other = graph_service(&mut database, "default", "notes");
        other.create_graph(name.clone()).expect("graph created");
        assert!(
            other.ontology(&name).expect("read succeeds").is_none(),
            "ontology is space-isolated"
        );
        database.close().expect("close succeeds");
    }

    // Reopen: the frozen ontology survives durably.
    let mut database = open_durable_database(tempdir.path()).expect("durable reopen succeeds");
    let mut graph = graph_service(&mut database, "default", "docs");
    let ontology = graph
        .ontology(&name)
        .expect("read succeeds")
        .expect("ontology survives reopen");
    assert_eq!(ontology.status(), GraphOntologyStatus::Frozen);
    assert_eq!(ontology.object_types().len(), 1);
    let documents = graph
        .nodes_by_type(&name, &type_name("Document"), None, 10)
        .expect("nodes by type after reopen");
    assert_eq!(documents.nodes().len(), 1, "type index survives reopen");

    // Deleting the graph removes its ontology: a recreated graph starts
    // with no ontology and a mutable draft.
    graph.delete_graph(&name, true).expect("delete succeeds");
    graph.create_graph(name.clone()).expect("recreate succeeds");
    assert!(
        graph.ontology(&name).expect("read succeeds").is_none(),
        "recreated graph has no stale ontology"
    );
    graph
        .define_object_type(&name, object_type("Fresh"))
        .expect("recreated graph accepts definitions");
}
