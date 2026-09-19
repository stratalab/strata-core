//! Vector API type re-exports.

pub use crate::data::vector::{
    EmbeddingModelId, VectorArtifactSourceDiagnostic, VectorBatchDeleteOutcome,
    VectorBatchGetOutcome, VectorBatchUpsertOutcome, VectorBulkDeleteOutcome, VectorCollectionInfo,
    VectorCollectionName, VectorConfig, VectorDeleteOutcome, VectorDistanceMetric, VectorEmbedding,
    VectorEmbeddingUpdateOutcome, VectorEntry, VectorFilter, VectorFilterCondition, VectorFilterOp,
    VectorHistory, VectorHistoryRow, VectorIndexDiagnostics, VectorKey, VectorKeyPage,
    VectorMetadata, VectorMetadataPatch, VectorMetadataUpdateOutcome, VectorScalar,
    VectorSearchMatch, VectorSearchResult, VectorService, VectorUpsertEntry, VectorVersionedEntry,
    VectorWriteOutcome,
};
