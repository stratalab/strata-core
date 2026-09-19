//! Vector capability.

mod adapter;
mod artifact;
mod distance;
mod index;
mod manifest;
mod outcome;
mod record;
mod service;
mod types;

pub use index::{VectorArtifactSourceDiagnostic, VectorIndexDiagnostics};
pub use outcome::{
    VectorBatchDeleteOutcome, VectorBatchGetOutcome, VectorBatchUpsertOutcome,
    VectorBulkDeleteOutcome, VectorCollectionInfo, VectorDeleteOutcome,
    VectorEmbeddingUpdateOutcome, VectorEntry, VectorHistory, VectorHistoryRow, VectorKeyPage,
    VectorMetadataUpdateOutcome, VectorSearchMatch, VectorSearchResult, VectorVersionedEntry,
    VectorWriteOutcome,
};
pub use service::VectorService;
pub use types::{
    EmbeddingModelId, VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding,
    VectorFilter, VectorFilterCondition, VectorFilterOp, VectorKey, VectorMetadata,
    VectorMetadataPatch, VectorScalar, VectorUpsertEntry,
};

pub(crate) use adapter::{
    plan_collection_promotion, VectorBranchAdapter, VectorCollectionBranchAdapter,
};
pub(crate) use artifact::{
    default_flat_artifact_build_budget_bytes, default_flat_artifact_load_budget_bytes,
    default_hnsw_artifact_build_budget_bytes, default_hnsw_artifact_load_budget_bytes,
    FlatVectorArtifact, HnswArtifactConfig, HnswVectorArtifact, VectorArtifactId,
    VectorArtifactStore, VectorFlatArtifactIdentity, VectorFlatArtifactLoad,
    VectorHnswArtifactLoad, VectorSourceId,
};
pub(crate) use distance::vector_score;
#[cfg(any(test, feature = "testkit"))]
pub(crate) use index::query_vector_exact;
pub(crate) use index::{
    query_vector_sources_with_index_artifacts, VectorFlatArtifactSourceInput,
    VectorHnswArtifactSourceInput, VectorIndexManifestLookup, VectorIndexPolicy, VectorTombstone,
};
pub(crate) use manifest::{
    decode_vector_index_manifest, encode_vector_index_manifest, VectorArtifactKind,
    VectorArtifactRef, VectorIndexManifest,
};
pub(crate) use record::{
    decode_collection_config, decode_vector_record, encode_collection_config, encode_vector_record,
    VectorRecord,
};

#[cfg(any(test, feature = "testkit"))]
pub use index::VectorIndexDiagnostics as VectorIndexDiagnosticsForTest;
#[cfg(any(test, feature = "testkit"))]
pub use index::VectorIndexPolicy as VectorIndexPolicyForTest;
