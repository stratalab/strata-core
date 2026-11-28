//! Graph operations on one graph `g`: typed nodes, edges, a bound node and
//! a draft ontology.

use serde_json::{json, Value};

use super::harness::{answer, Answer, Catalog, Op};
use crate::{
    BranchName, Database, GraphAnalyticsBudget, GraphBatchOperation, GraphBatchWrite,
    GraphBindingPrimitive, GraphBindingTarget, GraphDeletePolicy, GraphEdgeData, GraphEdgeType,
    GraphEntityBinding, GraphLinkTypeDef, GraphName, GraphNodeData, GraphNodeId,
    GraphObjectTypeDef, GraphService, GraphTypeName, ProductSpace,
};
use crate::{GraphDeleteOutcome, GraphEdgeWriteOutcome, GraphWriteOutcome};

fn graphs(database: &Database) -> GraphService<'_> {
    database
        .graph(
            BranchName::new("default").expect("valid branch"),
            ProductSpace::new("default").expect("valid space"),
        )
        .expect("graph service opens")
}

fn g() -> GraphName {
    GraphName::new("g").expect("valid graph name")
}

/// A graph with no nodes, so a delete without force is not refused.
fn empty() -> GraphName {
    GraphName::new("e").expect("valid graph name")
}

fn node(id: &str) -> GraphNodeId {
    GraphNodeId::new(id).expect("valid node id")
}

fn edge_type(name: &str) -> GraphEdgeType {
    GraphEdgeType::new(name).expect("valid edge type")
}

fn type_name(name: &str) -> GraphTypeName {
    GraphTypeName::new(name).expect("valid type name")
}

fn object_type(name: &str) -> GraphObjectTypeDef {
    GraphObjectTypeDef::new(type_name(name), []).expect("valid object type")
}

fn link_type(name: &str) -> GraphLinkTypeDef {
    GraphLinkTypeDef::new(type_name(name), type_name("T"), type_name("T"), None, [])
        .expect("valid link type")
}

fn typed(name: &str) -> GraphNodeData {
    GraphNodeData::new(None, None).with_object_type(type_name(name))
}

fn doc_target() -> GraphBindingTarget {
    GraphBindingTarget::new(
        GraphBindingPrimitive::Kv,
        None,
        ProductSpace::new("docs").expect("valid space"),
        "doc-1",
    )
    .expect("valid binding target")
}

fn seed(database: &mut Database) {
    let mut graph = graphs(database);
    graph.create_graph(g()).expect("seed graph");
    graph.create_graph(empty()).expect("seed empty graph");
    for name in ["T", "V"] {
        graph
            .define_object_type(&g(), object_type(name))
            .expect("seed object type");
    }
    graph
        .define_link_type(&g(), link_type("L"))
        .expect("seed link type");
    graph
        .upsert_node(&g(), node("a"), typed("T"))
        .expect("seed a");
    graph
        .upsert_node(&g(), node("b"), typed("T"))
        .expect("seed b");
    graph
        .upsert_node(&g(), node("c"), GraphNodeData::default())
        .expect("seed c");
    graph
        .upsert_node(
            &g(),
            node("bound"),
            GraphNodeData::new(None, Some(GraphEntityBinding::new(doc_target()))),
        )
        .expect("seed bound");
    graph
        .upsert_edge(
            &g(),
            node("a"),
            edge_type("L"),
            node("b"),
            GraphEdgeData::default(),
        )
        .expect("seed a->b");
    graph
        .upsert_edge(
            &g(),
            node("b"),
            edge_type("uses"),
            node("c"),
            GraphEdgeData::default(),
        )
        .expect("seed b->c");
}

fn created<T>(result: Result<T, crate::EngineError>, flag: impl FnOnce(&T) -> bool) -> Answer {
    answer(result, |outcome| json!({ "created": flag(&outcome) }))
}

fn deleted<T>(result: Result<T, crate::EngineError>, flag: impl FnOnce(&T) -> bool) -> Answer {
    answer(result, |outcome| json!({ "deleted": flag(&outcome) }))
}

fn add_n(database: &Database) -> Answer {
    created(
        graphs(database).upsert_node(&g(), node("n"), GraphNodeData::default()),
        GraphWriteOutcome::created,
    )
}

fn retype_a(database: &Database) -> Answer {
    created(
        graphs(database).upsert_node(&g(), node("a"), typed("V")),
        GraphWriteOutcome::created,
    )
}

fn remove_b(database: &Database) -> Answer {
    deleted(graphs(database).delete_node(&g(), &node("b")), |o| {
        o.deleted()
    })
}

fn remove_c(database: &Database) -> Answer {
    deleted(graphs(database).delete_node(&g(), &node("c")), |o| {
        o.deleted()
    })
}

fn add_a_c(database: &Database) -> Answer {
    created(
        graphs(database).upsert_edge(
            &g(),
            node("a"),
            edge_type("uses"),
            node("c"),
            GraphEdgeData::default(),
        ),
        GraphEdgeWriteOutcome::created,
    )
}

fn add_c_a(database: &Database) -> Answer {
    created(
        graphs(database).upsert_edge(
            &g(),
            node("c"),
            edge_type("uses"),
            node("a"),
            GraphEdgeData::default(),
        ),
        GraphEdgeWriteOutcome::created,
    )
}

fn remove_a_b(database: &Database) -> Answer {
    deleted(
        graphs(database).delete_edge(&g(), &node("a"), &edge_type("L"), &node("b")),
        GraphDeleteOutcome::deleted,
    )
}

fn batch_m(database: &Database) -> Answer {
    let batch = GraphBatchWrite::new(vec![
        GraphBatchOperation::UpsertNode {
            node_id: node("m"),
            data: GraphNodeData::default(),
        },
        GraphBatchOperation::UpsertEdge {
            src: node("m"),
            edge_type: edge_type("uses"),
            dst: node("a"),
            data: GraphEdgeData::default(),
        },
    ]);
    answer(graphs(database).batch_write(&g(), &batch), |outcome| {
        json!(outcome
            .results()
            .iter()
            .map(|item| json!([item.created_flag(), item.deleted_flag()]))
            .collect::<Vec<_>>())
    })
}

fn bulk_p(database: &Database) -> Answer {
    answer(
        graphs(database).bulk_insert(
            &g(),
            &[(node("p"), GraphNodeData::default())],
            &[(
                node("p"),
                edge_type("uses"),
                node("c"),
                GraphEdgeData::default(),
            )],
            None,
        ),
        |outcome| json!({ "nodes": outcome.nodes_inserted(), "edges": outcome.edges_inserted() }),
    )
}

fn delete_graph_guarded(database: &Database) -> Answer {
    deleted(
        graphs(database).delete_graph(&g(), false),
        GraphDeleteOutcome::deleted,
    )
}

fn delete_graph_forced(database: &Database) -> Answer {
    deleted(
        graphs(database).delete_graph(&g(), true),
        GraphDeleteOutcome::deleted,
    )
}

fn create_g(database: &Database) -> Answer {
    answer(graphs(database).create_graph(g()), |_| json!("created"))
}

fn define_u(database: &Database) -> Answer {
    answer(
        graphs(database).define_object_type(&g(), object_type("U")),
        |_| json!("defined"),
    )
}

fn define_m(database: &Database) -> Answer {
    answer(
        graphs(database).define_link_type(&g(), link_type("M")),
        |_| json!("defined"),
    )
}

fn delete_v(database: &Database) -> Answer {
    deleted(
        graphs(database).delete_object_type(&g(), &type_name("V")),
        GraphDeleteOutcome::deleted,
    )
}

fn delete_l(database: &Database) -> Answer {
    deleted(
        graphs(database).delete_link_type(&g(), &type_name("L")),
        GraphDeleteOutcome::deleted,
    )
}

fn freeze(database: &Database) -> Answer {
    answer(
        graphs(database).freeze_ontology(&g()),
        |outcome| json!({ "object_types": outcome.object_types(), "link_types": outcome.link_types() }),
    )
}

fn cascade_doc(database: &Database) -> Answer {
    answer(
        graphs(database).apply_binding_delete_policy(&doc_target(), GraphDeletePolicy::Cascade),
        |outcome| json!({ "nodes_affected": outcome.nodes_affected() }),
    )
}

fn delete_empty_guarded(database: &Database) -> Answer {
    deleted(graphs(database).delete_graph(&empty(), false), |o| {
        o.deleted()
    })
}

fn add_to_empty(database: &Database) -> Answer {
    created(
        graphs(database).upsert_node(&empty(), node("x"), GraphNodeData::default()),
        GraphWriteOutcome::created,
    )
}

fn digest(database: &mut Database) -> Answer {
    let graph = graphs(database);
    let info = answer(graph.graph_info(&g()), |info| {
        json!(info.map(|info| json!([info.node_count(), info.edge_count(), info.import_pending()])))
    });
    let names = answer(graph.list_graphs(None, 100), |page| {
        json!(page
            .graphs()
            .iter()
            .map(|name| name.as_str().to_owned())
            .collect::<Vec<_>>())
    });
    let nodes = answer(graph.list_nodes(&g(), None, None, 100), |page| {
        json!(page
            .nodes()
            .iter()
            .map(|node| {
                json!([
                    node.node_id().as_str(),
                    node.data().object_type().map(GraphTypeName::as_str),
                    node.data().binding().is_some(),
                ])
            })
            .collect::<Vec<_>>())
    });
    let edges = answer(graph.list_edges(&g(), None, 100), |page| {
        json!(page
            .edges()
            .iter()
            .map(|edge| json!([
                edge.src().as_str(),
                edge.edge_type().as_str(),
                edge.dst().as_str()
            ]))
            .collect::<Vec<_>>())
    });
    let ontology = answer(graph.ontology(&g()), |ontology| {
        json!(ontology.map(|ontology| {
            json!({
                "status": format!("{:?}", ontology.status()),
                "object_types": ontology.object_types().iter().map(|t| t.name().as_str().to_owned()).collect::<Vec<_>>(),
                "link_types": ontology.link_types().iter().map(|t| t.name().as_str().to_owned()).collect::<Vec<_>>(),
            })
        }))
    });
    let adjacency = answer(
        graph.adjacency_index(&g(), &GraphAnalyticsBudget::default()),
        |index| json!({ "nodes": index.node_count() }),
    );
    let by_type = ["T", "V"]
        .iter()
        .map(|name| {
            let ids = answer(
                graph.nodes_by_type(&g(), &type_name(name), None, 100),
                |page| {
                    json!(page
                        .nodes()
                        .iter()
                        .map(|node| node.node_id().as_str().to_owned())
                        .collect::<Vec<String>>())
                },
            );
            ((*name).to_owned(), ids)
        })
        .collect::<serde_json::Map<String, Value>>();
    let empty_graph = answer(graph.list_nodes(&empty(), None, None, 100), |page| {
        json!(page
            .nodes()
            .iter()
            .map(|node| node.node_id().as_str().to_owned())
            .collect::<Vec<String>>())
    });
    json!({
        "empty_graph": empty_graph,
        "info": info,
        "graphs": names,
        "nodes": nodes,
        "edges": edges,
        "ontology": ontology,
        "adjacency": adjacency,
        "nodes_by_type": by_type,
    })
}

fn neighbors_a(database: &Database) -> Answer {
    answer(
        graphs(database).neighbors(
            &g(),
            &node("a"),
            crate::GraphDirection::Both,
            None,
            None,
            100,
        ),
        |page| {
            let mut found = page
                .neighbors()
                .iter()
                .map(|neighbor| {
                    format!(
                        "{} {} {:?}",
                        neighbor.node().node_id().as_str(),
                        neighbor.edge().edge_type().as_str(),
                        neighbor.direction()
                    )
                })
                .collect::<Vec<_>>();
            found.sort();
            json!(found)
        },
    )
}

fn nodes_by_type_t(database: &Database) -> Answer {
    answer(
        graphs(database).nodes_by_type(&g(), &type_name("T"), None, 100),
        |page| {
            json!(page
                .nodes()
                .iter()
                .map(|node| node.node_id().as_str().to_owned())
                .collect::<Vec<String>>())
        },
    )
}

fn get_b(database: &Database) -> Answer {
    answer(graphs(database).get_node(&g(), &node("b")), |found| {
        json!(found.is_some())
    })
}

fn ontology_summary(database: &Database) -> Answer {
    answer(graphs(database).ontology_summary(&g()), |summary| {
        json!(format!("{summary:?}"))
    })
}

fn adjacency(database: &Database) -> Answer {
    answer(
        graphs(database).adjacency_index(&g(), &GraphAnalyticsBudget::default()),
        |index| json!({ "nodes": index.node_count() }),
    )
}

fn info(database: &Database) -> Answer {
    answer(graphs(database).graph_info(&g()), |info| {
        json!(info.map(|info| json!([info.node_count(), info.edge_count()])))
    })
}

pub(super) const CATALOG: Catalog = Catalog {
    family: "graph",
    seed,
    ops: &[
        Op {
            label: "graph.node.add(n)",
            command: "graph.node.add",
            run: add_n,
        },
        Op {
            label: "graph.node.add(a as V)",
            command: "graph.node.add",
            run: retype_a,
        },
        Op {
            label: "graph.node.remove(b)",
            command: "graph.node.remove",
            run: remove_b,
        },
        Op {
            label: "graph.node.remove(c)",
            command: "graph.node.remove",
            run: remove_c,
        },
        Op {
            label: "graph.edge.add(a->c)",
            command: "graph.edge.add",
            run: add_a_c,
        },
        Op {
            label: "graph.edge.add(c->a)",
            command: "graph.edge.add",
            run: add_c_a,
        },
        Op {
            label: "graph.edge.remove(a->b)",
            command: "graph.edge.remove",
            run: remove_a_b,
        },
        Op {
            label: "graph.batch_write(m, m->a)",
            command: "graph.batch_write",
            run: batch_m,
        },
        Op {
            label: "graph.bulk_insert(p, p->c)",
            command: "graph.bulk_insert",
            run: bulk_p,
        },
        Op {
            label: "graph.delete(g)",
            command: "graph.delete",
            run: delete_graph_guarded,
        },
        Op {
            label: "graph.delete(g, force)",
            command: "graph.delete",
            run: delete_graph_forced,
        },
        Op {
            label: "graph.create(g)",
            command: "graph.create",
            run: create_g,
        },
        Op {
            label: "graph.ontology.define_object_type(U)",
            command: "graph.ontology.define_object_type",
            run: define_u,
        },
        Op {
            label: "graph.ontology.define_link_type(M)",
            command: "graph.ontology.define_link_type",
            run: define_m,
        },
        Op {
            label: "graph.ontology.delete_object_type(V)",
            command: "graph.ontology.delete_object_type",
            run: delete_v,
        },
        Op {
            label: "graph.ontology.delete_link_type(L)",
            command: "graph.ontology.delete_link_type",
            run: delete_l,
        },
        Op {
            label: "graph.ontology.freeze",
            command: "graph.ontology.freeze",
            run: freeze,
        },
        Op {
            label: "graph.apply_delete_policy(doc-1, cascade)",
            command: "graph.apply_delete_policy",
            run: cascade_doc,
        },
        Op {
            label: "read graph.neighbors(a)",
            command: "graph.neighbors",
            run: neighbors_a,
        },
        Op {
            label: "read graph.nodes_by_type(T)",
            command: "graph.nodes_by_type",
            run: nodes_by_type_t,
        },
        Op {
            label: "read graph.node.get(b)",
            command: "graph.node.get",
            run: get_b,
        },
        Op {
            label: "read graph.ontology.summary",
            command: "graph.ontology.summary",
            run: ontology_summary,
        },
        Op {
            label: "read graph.adjacency",
            command: "graph.analytics.wcc",
            run: adjacency,
        },
        Op {
            label: "read graph.meta",
            command: "graph.meta",
            run: info,
        },
        Op {
            label: "graph.delete(e)",
            command: "graph.delete",
            run: delete_empty_guarded,
        },
        Op {
            label: "graph.node.add(e: x)",
            command: "graph.node.add",
            run: add_to_empty,
        },
    ],
    digest,
};
