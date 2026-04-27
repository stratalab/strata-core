//! Vector semantic types and configuration helpers used by engine-facing code.

pub use strata_core::primitives::vector::{
    CollectionId, CollectionInfo, DistanceMetric, FilterCondition, FilterOp, JsonScalar,
    MetadataFilter, StorageDtype, VectorConfig, VectorEntry, VectorId, VectorMatch,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_helpers_behave_through_engine_surface() {
        assert_eq!(DistanceMetric::Cosine.name(), "cosine");
        assert_eq!(
            DistanceMetric::parse("inner_product"),
            Some(DistanceMetric::DotProduct)
        );
        assert_eq!(
            DistanceMetric::from_byte(1),
            Some(DistanceMetric::Euclidean)
        );
        assert_eq!(StorageDtype::from_byte(3), Some(StorageDtype::Binary));
        assert_eq!(StorageDtype::Int8.element_size(), 1);

        let config = VectorConfig::new(128, DistanceMetric::Cosine).unwrap();
        assert_eq!(config.dimension, 128);
        assert_eq!(config.metric, DistanceMetric::Cosine);
        assert_eq!(config.storage_dtype, StorageDtype::F32);

        let quantized =
            VectorConfig::new_with_dtype(256, DistanceMetric::DotProduct, StorageDtype::Int8)
                .unwrap();
        assert_eq!(quantized.dimension, 256);
        assert_eq!(quantized.metric, DistanceMetric::DotProduct);
        assert_eq!(quantized.storage_dtype, StorageDtype::Int8);

        assert_eq!(VectorConfig::for_openai_ada().dimension, 1536);
        assert_eq!(VectorConfig::for_openai_large().dimension, 3072);
        assert_eq!(VectorConfig::for_minilm().dimension, 384);
        assert_eq!(VectorConfig::for_mpnet().dimension, 768);
        assert_eq!(VectorConfig::for_embedding(42).dimension, 42);
    }

    #[test]
    fn distance_metric_bytes_roundtrip() {
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ] {
            assert_eq!(DistanceMetric::from_byte(metric.to_byte()), Some(metric));
        }
    }
}
