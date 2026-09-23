//! Corruption injection at the persistence read boundary (TCP3.15a).
//!
//! Companion to the storage-fault seam. Where `StorageFaultKind` fails an
//! operation outright, `RowCorruption` lets the read succeed and mangles the
//! rows it returns — modelling on-disk bit-rot the storage layer hands back
//! intact but whose *content* the engine's `data_loss.*` decoders must reject.
//! TCP3.15a built the harness and closed the two simplest content-corruption
//! codes (KV value, JSON index definition). TCP3.15b adds the four graph record
//! and index-key codes; TCP3.15c/d reuse it for vector and control-plane
//! records.

#![cfg(feature = "testkit")]

mod common;

use common::{assert_status, branch, key, open_cache_database, space, value};
use strata_engine::testkit::RowCorruption;
use strata_engine::{
    EngineErrorClass, GraphDirection, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData,
    GraphNodeId, GraphTypeName, JsonIndexName, JsonIndexType, JsonPath,
};

/// A malformed value whose leading byte is not any decoder's format version.
const BAD_RECORD_BYTES: [u8; 2] = [0xFF, 0x00];

#[test]
fn a_scanned_kv_row_with_no_value_is_rejected_as_corruption() {
    let mut db = open_cache_database().expect("cache database opens");
    {
        let mut kv = db
            .kv(branch("default"), space("default"))
            .expect("kv opens");
        kv.put(key(b"k"), value(b"v")).expect("seed write");
    }

    // The next scan returns the row with its value cell stripped — as if the
    // stored value rotted away. The KV scan decoder must reject it rather than
    // fabricate an empty value.
    db.inject_scan_corruption_for_test(RowCorruption::DropValue);
    let mut kv = db
        .kv(branch("default"), space("default"))
        .expect("kv opens");
    let error = kv
        .scan(None, None)
        .expect_err("a valueless KV row must be rejected");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.kv_value",
        false,
    );
}

#[test]
fn a_json_index_definition_with_a_bad_version_byte_is_rejected_as_corruption() {
    let mut db = open_cache_database().expect("cache database opens");
    {
        let mut json = db
            .json(branch("default"), space("default"))
            .expect("json opens");
        json.create_index(
            JsonIndexName::new("by_name").expect("index name"),
            "name".parse::<JsonPath>().expect("json path"),
            JsonIndexType::Tag,
        )
        .expect("index create");
    }

    // The next scan returns the index-definition row with malformed bytes whose
    // leading format-version byte is wrong; `decode_index_definition` must
    // reject it.
    db.inject_scan_corruption_for_test(RowCorruption::SetValue(vec![0xFF, 0x00]));
    let mut json = db
        .json(branch("default"), space("default"))
        .expect("json opens");
    let error = json
        .list_indexes()
        .expect_err("a malformed index definition must be rejected");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.json_index",
        false,
    );
}

// --- graph (TCP3.15b) -----------------------------------------------------

fn graph_name() -> GraphName {
    GraphName::new("g").expect("graph name")
}

/// Opens the graph service, seeds it via `seed`, then returns the db ready for
/// an armed corruption on the next scan.
fn graph_db_with(seed: impl FnOnce(&mut strata_engine::GraphService)) -> strata_engine::Database {
    let db = open_cache_database().expect("cache database opens");
    {
        let mut graph = db
            .graph(branch("default"), space("default"))
            .expect("graph opens");
        graph.create_graph(graph_name()).expect("graph create");
        seed(&mut graph);
    }
    db
}

#[test]
fn a_graph_node_record_with_a_bad_version_byte_is_rejected_as_corruption() {
    let mut db = graph_db_with(|graph| {
        graph
            .upsert_node(
                &graph_name(),
                GraphNodeId::new("n1").expect("id"),
                GraphNodeData::new(None, None),
            )
            .expect("node upsert");
    });

    db.inject_scan_corruption_for_test(RowCorruption::SetValue(BAD_RECORD_BYTES.to_vec()));
    let graph = db
        .graph(branch("default"), space("default"))
        .expect("graph opens");
    let error = graph
        .list_nodes(&graph_name(), None, None, 10)
        .expect_err("a malformed node record must be rejected");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.graph_node_record",
        false,
    );
}

#[test]
fn a_graph_edge_record_with_a_bad_version_byte_is_rejected_as_corruption() {
    let mut db = graph_db_with(|graph| {
        for id in ["n1", "n2"] {
            graph
                .upsert_node(
                    &graph_name(),
                    GraphNodeId::new(id).expect("id"),
                    GraphNodeData::new(None, None),
                )
                .expect("node upsert");
        }
        graph
            .upsert_edge(
                &graph_name(),
                GraphNodeId::new("n1").expect("id"),
                GraphEdgeType::new("links").expect("edge type"),
                GraphNodeId::new("n2").expect("id"),
                GraphEdgeData::new(1.0, None).expect("edge data"),
            )
            .expect("edge upsert");
    });

    db.inject_scan_corruption_for_test(RowCorruption::SetValue(BAD_RECORD_BYTES.to_vec()));
    let graph = db
        .graph(branch("default"), space("default"))
        .expect("graph opens");
    let error = graph
        .neighbors(
            &graph_name(),
            &GraphNodeId::new("n1").expect("id"),
            GraphDirection::Outgoing,
            None,
            None,
            10,
        )
        .expect_err("a malformed edge record must be rejected");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.graph_edge_record",
        false,
    );
}

/// Seeds a graph with one typed node so the type index has an entry to scan.
fn typed_node_db() -> strata_engine::Database {
    graph_db_with(|graph| {
        graph
            .upsert_node(
                &graph_name(),
                GraphNodeId::new("n1").expect("id"),
                GraphNodeData::new(None, None)
                    .with_object_type(GraphTypeName::new("Doc").expect("type name")),
            )
            .expect("typed node upsert");
    })
}

#[test]
fn a_graph_type_index_record_with_a_bad_version_byte_is_rejected_as_corruption() {
    let mut db = typed_node_db();

    // The type-index row's key decodes fine; its value is malformed.
    db.inject_scan_corruption_for_test(RowCorruption::SetValue(BAD_RECORD_BYTES.to_vec()));
    let graph = db
        .graph(branch("default"), space("default"))
        .expect("graph opens");
    let error = graph
        .nodes_by_type(
            &graph_name(),
            &GraphTypeName::new("Doc").expect("type name"),
            None,
            10,
        )
        .expect_err("a malformed type-index record must be rejected");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.graph_type_index_record",
        false,
    );
}

#[test]
fn a_graph_type_index_key_that_is_malformed_is_rejected_as_corruption() {
    let mut db = typed_node_db();

    // Corrupt the row *key*: the type-index key decoder rejects it before the
    // value is ever read.
    db.inject_scan_corruption_for_test(RowCorruption::SetKey(vec![0xFF, 0x00, 0x01]));
    let graph = db
        .graph(branch("default"), space("default"))
        .expect("graph opens");
    let error = graph
        .nodes_by_type(
            &graph_name(),
            &GraphTypeName::new("Doc").expect("type name"),
            None,
            10,
        )
        .expect_err("a malformed type-index key must be rejected");
    assert_status(
        &error,
        EngineErrorClass::Corruption,
        "data_loss.engine.graph_type_index_key",
        false,
    );
}
