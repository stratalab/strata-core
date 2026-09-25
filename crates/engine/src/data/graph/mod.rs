//! Graph core capability.

mod adapter;
mod adjacency;
mod analytics;
mod iterative;
mod ontology;
mod outcome;
mod record;
mod service;
mod traversal;
mod types;

pub(crate) use adapter::{
    GraphEdgeBranchAdapter, GraphMetadataBranchAdapter, GraphNodeBranchAdapter,
    GraphOntologyBranchAdapter,
};
pub(crate) use adjacency::GraphAdjacencyIndexBuilder;
pub use adjacency::{GraphAdjacencyEdge, GraphAdjacencyIndex, GraphAnalyticsBudget};
pub use analytics::{GraphLccResult, GraphSsspOptions, GraphSsspResult, GraphWccResult};
pub use iterative::{GraphCdlpOptions, GraphCdlpResult, GraphPageRankOptions, GraphPageRankResult};
pub use ontology::{
    GraphLinkTypeDef, GraphLinkTypeSummary, GraphObjectTypeDef, GraphObjectTypeSummary,
    GraphOntology, GraphOntologyFreezeOutcome, GraphOntologyStatus, GraphOntologySummary,
    GraphOntologyWriteOutcome, GraphPropertyDef, GraphTypeName,
};
pub use outcome::{
    GraphBatchOpOutcome, GraphBatchWriteOutcome, GraphBinding, GraphBindingPage,
    GraphBulkInsertOutcome, GraphDeleteOutcome, GraphDeletePolicyOutcome, GraphEdge, GraphEdgePage,
    GraphEdgeWriteOutcome, GraphInfo, GraphNamePage, GraphNeighbor, GraphNeighborPage, GraphNode,
    GraphNodePage, GraphTargetStatus, GraphWriteOutcome,
};
pub use service::GraphService;
pub use traversal::{GraphBfsOptions, GraphBfsResult, GraphSubgraphResult, GraphTraversalEdge};
pub use types::{
    GraphBatchOperation, GraphBatchWrite, GraphBindingPrimitive, GraphBindingTarget,
    GraphDeletePolicy, GraphDirection, GraphEdgeData, GraphEdgeType, GraphEntityBinding, GraphName,
    GraphNodeData, GraphNodeId, GraphProperties,
};

pub(crate) use ontology::{
    decode_graph_ontology_record, encode_graph_ontology_record, GraphOntologyRecord,
};
pub(crate) use record::{
    decode_graph_binding_record, decode_graph_edge_record, decode_graph_metadata_record,
    decode_graph_node_record, decode_graph_type_index_record, encode_graph_binding_record,
    encode_graph_edge_record, encode_graph_metadata_record, encode_graph_node_record,
    encode_graph_type_index_record, GraphBindingRecord, GraphCommitPoint, GraphCounts,
    GraphEdgeRecord, GraphMetadataRecord, GraphNodeRecord, GraphTypeIndexRecord,
};
