//! Graph capability branch adapters — comparison only.
//!
//! Graph is multi-row-class: metadata, nodes, edges, and the ontology are each an
//! authored [`RowClass`], one KV-shaped MVCC row per entity (space prefix + a
//! length-prefixed identity tuple → version byte + JSON). Each is compared by its
//! own thin adapter — a graph diff reports metadata, node, edge, and ontology
//! changes separately, which is what a reader wants; the metadata row makes an
//! empty graph's creation or deletion visible on its own. The derived rows
//! (reverse-edge maps, binding index, type index) are rebuildable and excluded.
//!
//! Graph is compared but never promoted:
//! [`CapabilityBranchAdapter::supports_promotion`] is `false` on every graph
//! adapter. A structural merge (preserving edge referential integrity, ontology
//! consistency, and branch-relative bindings, and rebuilding the derived rows)
//! needs machinery beyond the generic three-way and lands in a later slice.

use crate::branch::adapter::{
    CapabilityBranchAdapter, ComparableEntity, DerivedDisposition, EntitySummary,
};
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{
    decode_graph_edge_key, decode_graph_metadata_key, decode_graph_node_key,
    decode_graph_ontology_key, encode_graph_edge_space_prefix, encode_graph_metadata_prefix,
    encode_graph_node_space_prefix, encode_graph_ontology_space_prefix, PersistenceReadRow,
    RowClass,
};

/// The space-relative identity of a graph row (its key with the space prefix
/// stripped) and the value summary, sharing the tombstone/no-value handling
/// across the three graph adapters.
fn interpret(
    row: &PersistenceReadRow,
    prefix: &[u8],
    key_code: &'static str,
    record_code: &'static str,
) -> Result<ComparableEntity, EngineError> {
    let identity = row
        .key()
        .strip_prefix(prefix)
        .ok_or_else(|| {
            EngineError::corruption(
                key_code,
                "stored graph row key is outside the requested space",
            )
        })?
        .to_vec();
    let summary = if row.is_tombstone() {
        EntitySummary::Absent
    } else {
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                record_code,
                "stored graph row is present but carries no value",
            )
        })?;
        EntitySummary::Present(value.to_vec())
    };
    Ok(ComparableEntity::new(
        identity,
        summary,
        row.commit_version(),
    ))
}

/// The graph metadata capability's branch adapter (comparison only). One row per
/// graph records its existence, so creating or dropping a graph diffs as a single
/// added or removed entity — an empty graph is not an empty diff.
pub(crate) struct GraphMetadataBranchAdapter;

impl CapabilityBranchAdapter for GraphMetadataBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::GraphMetadata
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn supports_promotion(&self) -> bool {
        false
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_graph_metadata_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        decode_graph_metadata_key(space, row.key())?;
        interpret(
            row,
            &encode_graph_metadata_prefix(space),
            "data_loss.engine.graph_key",
            "data_loss.engine.graph_metadata",
        )
    }
}

/// The graph node capability's branch adapter (comparison only).
pub(crate) struct GraphNodeBranchAdapter;

impl CapabilityBranchAdapter for GraphNodeBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::GraphNode
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn supports_promotion(&self) -> bool {
        false
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_graph_node_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        decode_graph_node_key(space, row.key())?;
        interpret(
            row,
            &encode_graph_node_space_prefix(space),
            "data_loss.engine.graph_node_key",
            "data_loss.engine.graph_node_record",
        )
    }
}

/// The graph edge capability's branch adapter (comparison only).
pub(crate) struct GraphEdgeBranchAdapter;

impl CapabilityBranchAdapter for GraphEdgeBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::GraphEdge
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn supports_promotion(&self) -> bool {
        false
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_graph_edge_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        decode_graph_edge_key(space, row.key())?;
        interpret(
            row,
            &encode_graph_edge_space_prefix(space),
            "data_loss.engine.graph_edge_key",
            "data_loss.engine.graph_edge_record",
        )
    }
}

/// The graph ontology capability's branch adapter (comparison only). One row per
/// graph holds the whole type system, so any type or freeze-state change diffs
/// as a single modified entity.
pub(crate) struct GraphOntologyBranchAdapter;

impl CapabilityBranchAdapter for GraphOntologyBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::GraphOntology
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn supports_promotion(&self) -> bool {
        false
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_graph_ontology_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        // Validate the full length-prefixed ontology key structure — not just the
        // space prefix — so a malformed suffix under the right prefix surfaces as
        // structured corruption instead of an opaque identity.
        decode_graph_ontology_key(space, row.key())?;
        interpret(
            row,
            &encode_graph_ontology_space_prefix(space),
            "data_loss.engine.graph_key",
            "data_loss.engine.graph_ontology_record",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GraphEdgeBranchAdapter, GraphMetadataBranchAdapter, GraphNodeBranchAdapter,
        GraphOntologyBranchAdapter,
    };

    use crate::branch::adapter::{CapabilityBranchAdapter, DerivedDisposition, EntitySummary};
    use crate::data::graph::{GraphEdgeType, GraphName, GraphNodeId};
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{
        encode_graph_edge_key, encode_graph_edge_space_prefix, encode_graph_metadata_key,
        encode_graph_metadata_prefix, encode_graph_node_key, encode_graph_node_space_prefix,
        encode_graph_ontology_key, encode_graph_ontology_space_prefix, PersistenceReadRow,
        RowClass,
    };

    fn space() -> ProductSpace {
        ProductSpace::new("default").expect("default is a valid space")
    }

    fn metadata_row(space: &ProductSpace, graph: &str, value: Option<&[u8]>) -> PersistenceReadRow {
        let encoded = encode_graph_metadata_key(space, &GraphName::new(graph).expect("graph"));
        PersistenceReadRow::for_test(encoded, value.map(<[u8]>::to_vec), false)
    }

    #[test]
    fn metadata_adapter_decodes_a_present_row_and_reports_facets() {
        let entity = GraphMetadataBranchAdapter
            .interpret_row(&space(), &metadata_row(&space(), "deps", Some(b"meta")))
            .expect("decodes a present metadata row");
        assert_eq!(entity.summary(), &EntitySummary::Present(b"meta".to_vec()));
        assert_eq!(
            GraphMetadataBranchAdapter.row_class(),
            RowClass::GraphMetadata
        );
        assert_eq!(
            GraphMetadataBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
        assert!(!GraphMetadataBranchAdapter.supports_promotion());
        assert_eq!(
            GraphMetadataBranchAdapter.space_prefix(&space()),
            encode_graph_metadata_prefix(&space())
        );
    }

    fn node_row(
        space: &ProductSpace,
        graph: &str,
        node: &str,
        value: Option<&[u8]>,
    ) -> PersistenceReadRow {
        let encoded = encode_graph_node_key(
            space,
            &GraphName::new(graph).expect("graph"),
            &GraphNodeId::new(node).expect("node"),
        );
        PersistenceReadRow::for_test(encoded, value.map(<[u8]>::to_vec), false)
    }

    #[test]
    fn node_adapter_decodes_a_present_row_and_reports_facets() {
        let entity = GraphNodeBranchAdapter
            .interpret_row(
                &space(),
                &node_row(&space(), "deps", "chunk", Some(b"node")),
            )
            .expect("decodes a present node row");
        assert_eq!(entity.summary(), &EntitySummary::Present(b"node".to_vec()));
        assert_eq!(GraphNodeBranchAdapter.row_class(), RowClass::GraphNode);
        assert_eq!(
            GraphNodeBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
        assert!(!GraphNodeBranchAdapter.supports_promotion());
        assert_eq!(
            GraphNodeBranchAdapter.space_prefix(&space()),
            encode_graph_node_space_prefix(&space())
        );
    }

    #[test]
    fn node_identity_distinguishes_graphs_and_ids() {
        let a = GraphNodeBranchAdapter
            .interpret_row(&space(), &node_row(&space(), "one", "k", Some(b"x")))
            .expect("decodes");
        let b = GraphNodeBranchAdapter
            .interpret_row(&space(), &node_row(&space(), "two", "k", Some(b"x")))
            .expect("decodes");
        assert_ne!(
            a.identity(),
            b.identity(),
            "the graph is part of the identity"
        );
    }

    #[test]
    fn node_adapter_rejects_a_key_from_another_space() {
        let other = ProductSpace::new("other").expect("other is a valid space");
        let error = GraphNodeBranchAdapter
            .interpret_row(&space(), &node_row(&other, "deps", "chunk", Some(b"x")))
            .expect_err("a key encoded for another space is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_node_key");
    }

    #[test]
    fn node_adapter_rejects_a_present_row_without_a_value() {
        let error = GraphNodeBranchAdapter
            .interpret_row(&space(), &node_row(&space(), "deps", "chunk", None))
            .expect_err("a present row without a value is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_node_record");
    }

    #[test]
    fn edge_adapter_decodes_and_is_not_promotable() {
        let encoded = encode_graph_edge_key(
            &space(),
            &GraphName::new("deps").expect("graph"),
            &GraphNodeId::new("doc").expect("src"),
            &GraphEdgeType::new("contains").expect("type"),
            &GraphNodeId::new("chunk").expect("dst"),
        );
        let row = PersistenceReadRow::for_test(encoded, Some(b"edge".to_vec()), false);
        let entity = GraphEdgeBranchAdapter
            .interpret_row(&space(), &row)
            .expect("decodes a present edge row");
        assert_eq!(entity.summary(), &EntitySummary::Present(b"edge".to_vec()));
        assert_eq!(GraphEdgeBranchAdapter.row_class(), RowClass::GraphEdge);
        assert!(!GraphEdgeBranchAdapter.supports_promotion());
        assert_eq!(
            GraphEdgeBranchAdapter.space_prefix(&space()),
            encode_graph_edge_space_prefix(&space())
        );
    }

    #[test]
    fn ontology_adapter_is_authored_and_not_promotable() {
        assert_eq!(
            GraphOntologyBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
        assert!(!GraphOntologyBranchAdapter.supports_promotion());
        assert_eq!(
            GraphOntologyBranchAdapter.space_prefix(&space()),
            encode_graph_ontology_space_prefix(&space())
        );
        // The ontology prefix is a distinct family from the node's.
        assert_ne!(
            GraphOntologyBranchAdapter.space_prefix(&space()),
            encode_graph_node_space_prefix(&space())
        );
    }

    #[test]
    fn ontology_adapter_rejects_a_malformed_key() {
        // A row under the ontology prefix whose suffix is not a valid
        // length-prefixed graph name is structured corruption, not an opaque
        // identity — the adapter must reject it.
        let mut key = encode_graph_ontology_space_prefix(&space());
        key.push(0xff); // a length prefix promising bytes that do not follow
        let row = PersistenceReadRow::for_test(key, Some(b"onto".to_vec()), false);
        let error = GraphOntologyBranchAdapter
            .interpret_row(&space(), &row)
            .expect_err("a malformed ontology key must be rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_key");
    }

    #[test]
    fn ontology_adapter_rejects_trailing_bytes_after_the_graph_name() {
        // A complete graph name followed by extra bytes is corruption, not a
        // longer identity — the trailing-bytes check must reject it.
        let mut key = encode_graph_ontology_key(&space(), &GraphName::new("deps").expect("graph"));
        key.push(0xff);
        let row = PersistenceReadRow::for_test(key, Some(b"onto".to_vec()), false);
        let error = GraphOntologyBranchAdapter
            .interpret_row(&space(), &row)
            .expect_err("trailing bytes after the graph name must be rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.graph_key");
    }
}
