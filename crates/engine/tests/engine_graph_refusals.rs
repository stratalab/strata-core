//! Graph validation-refusal code coverage (TCP3.5b).
//!
//! The engine's graph value constructors reject malformed input with stable
//! `<class>.engine.<detail>` codes. The existing graph suite asserts these
//! refusals by *class* (`EngineErrorClass::InvalidInput`); this pins each one
//! by its literal *code*, which is what the workspace error-code guard tracks
//! and what an SDK error handler matches on. Every case here is reachable
//! through a public constructor with no database setup — pure input validation.

use serde_json::json;
use strata_engine::{
    GraphBindingPrimitive, GraphBindingTarget, GraphEdgeType, GraphName, GraphNodeId,
    GraphObjectTypeDef, GraphProperties, GraphPropertyDef, GraphTypeName, ProductSpace,
};

/// Assert a constructor result carries the expected stable refusal code.
#[track_caller]
fn assert_code<T: std::fmt::Debug>(result: Result<T, strata_engine::EngineError>, expected: &str) {
    let error = result.expect_err("constructor must reject invalid input");
    assert_eq!(error.code(), expected, "refusal code mismatch");
}

#[test]
fn graph_name_refusals_carry_stable_codes() {
    assert_code(GraphName::new(""), "invalid_argument.engine.graph_name");
    // Leading underscore is reserved for engine internals.
    assert_code(
        GraphName::new("_internal"),
        "invalid_argument.engine.graph_name_reserved",
    );
}

#[test]
fn graph_node_id_empty_is_rejected() {
    assert_code(
        GraphNodeId::new(""),
        "invalid_argument.engine.graph_node_id",
    );
}

#[test]
fn graph_edge_type_refusals_carry_stable_codes() {
    assert_code(
        GraphEdgeType::new(""),
        "invalid_argument.engine.graph_edge_type",
    );
    assert_code(
        GraphEdgeType::new("_reserved"),
        "invalid_argument.engine.graph_edge_type_reserved",
    );
}

#[test]
fn graph_binding_empty_key_is_rejected() {
    let space = ProductSpace::new("docs").expect("valid space");
    assert_code(
        GraphBindingTarget::new(GraphBindingPrimitive::Json, None, space, ""),
        "invalid_argument.engine.graph_binding",
    );
}

#[test]
fn graph_properties_too_large_is_rejected() {
    // A JSON object whose encoding exceeds the 16 MiB properties ceiling.
    let oversized = json!({ "blob": "x".repeat(17 * 1024 * 1024) });
    assert_code(
        GraphProperties::new(oversized),
        "invalid_argument.engine.graph_properties_too_large",
    );
}

#[test]
fn graph_type_hint_empty_is_rejected() {
    // The value-type hint on a property definition is validated when present.
    assert_code(
        GraphPropertyDef::new(Some(String::new()), false),
        "invalid_argument.engine.graph_type_hint",
    );
}

#[test]
fn graph_property_name_empty_is_rejected() {
    let type_name = GraphTypeName::new("Document").expect("valid type name");
    let good_def = GraphPropertyDef::new(None, false).expect("valid property def");
    assert_code(
        GraphObjectTypeDef::new(type_name, [(String::new(), good_def)]),
        "invalid_argument.engine.graph_property_name",
    );
}

// --- published limits (#3214) ------------------------------------------
//
// Each pins one number the graph commands publish in their
// `# Guaranteed semantics` block, proven from both sides — the largest
// accepted input and the smallest refused one with its code — so a published
// limit is never merely a number nobody checked against the engine.

/// A node id is bounded at 1,024 bytes.
#[test]
fn graph_node_id_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 1024;
    GraphNodeId::new("a".repeat(LIMIT)).expect("the longest accepted node id");
    assert_code(
        GraphNodeId::new("a".repeat(LIMIT + 1)),
        "invalid_argument.engine.graph_node_id",
    );
}

/// A type name is bounded at 256 bytes.
#[test]
fn graph_type_name_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 256;
    GraphTypeName::new("a".repeat(LIMIT)).expect("the longest accepted type name");
    assert_code(
        GraphTypeName::new("a".repeat(LIMIT + 1)),
        "invalid_argument.engine.graph_type_name",
    );
}

/// A property name is bounded at 256 bytes.
#[test]
fn graph_property_name_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 256;
    let type_name = GraphTypeName::new("Document").expect("valid type name");
    GraphObjectTypeDef::new(
        type_name.clone(),
        [(
            "a".repeat(LIMIT),
            GraphPropertyDef::new(None, false).expect("valid property def"),
        )],
    )
    .expect("the longest accepted property name");
    assert_code(
        GraphObjectTypeDef::new(
            type_name,
            [(
                "a".repeat(LIMIT + 1),
                GraphPropertyDef::new(None, false).expect("valid property def"),
            )],
        ),
        "invalid_argument.engine.graph_property_name",
    );
}
