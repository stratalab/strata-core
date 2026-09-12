//! Vector storage envelopes.

use serde::{Deserialize, Serialize};

use crate::diagnostics::EngineError;

use super::EmbeddingModelId;
use super::{
    VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey,
    VectorMetadata,
};

const COLLECTION_FORMAT_VERSION: u8 = 1;
const VECTOR_RECORD_FORMAT_VERSION: u8 = 1;

/// Stored vector record.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VectorRecord {
    collection: VectorCollectionName,
    key: VectorKey,
    embedding: VectorEmbedding,
    metadata: Option<VectorMetadata>,
    vector_revision: u64,
}

impl VectorRecord {
    pub(crate) const fn new(
        collection: VectorCollectionName,
        key: VectorKey,
        embedding: VectorEmbedding,
        metadata: Option<VectorMetadata>,
        vector_revision: u64,
    ) -> Self {
        Self {
            collection,
            key,
            embedding,
            metadata,
            vector_revision,
        }
    }

    pub(crate) const fn collection(&self) -> &VectorCollectionName {
        &self.collection
    }

    pub(crate) const fn key(&self) -> &VectorKey {
        &self.key
    }

    pub(crate) const fn embedding(&self) -> &VectorEmbedding {
        &self.embedding
    }

    pub(crate) const fn metadata(&self) -> Option<&VectorMetadata> {
        self.metadata.as_ref()
    }

    pub(crate) const fn vector_revision(&self) -> u64 {
        self.vector_revision
    }
}

#[derive(Serialize, Deserialize)]
struct StoredCollectionConfig {
    collection: String,
    dimension: usize,
    metric: VectorDistanceMetric,
    // D9. `default` + `skip_serializing_if` is what makes this additive: rows
    // written before provenance existed decode with no model, and rows written
    // without one are byte-identical to what the previous version produced. No
    // format version bump, so the golden vectors still hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    embedding_model: Option<EmbeddingModelId>,
}

#[derive(Serialize, Deserialize)]
struct StoredVectorRecord {
    collection: String,
    key: String,
    vector_revision: u64,
    embedding: Vec<f32>,
    metadata: Option<serde_json::Value>,
}

pub(crate) fn encode_collection_config(
    collection: &VectorCollectionName,
    config: &VectorConfig,
) -> Result<Vec<u8>, EngineError> {
    let stored = StoredCollectionConfig {
        collection: collection.as_str().to_owned(),
        dimension: config.dimension(),
        metric: config.metric(),
        embedding_model: config.embedding_model().cloned(),
    };
    let mut bytes = vec![COLLECTION_FORMAT_VERSION];
    bytes.extend(serde_json::to_vec(&stored).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.vector_collection",
            format!("vector collection config cannot be encoded: {error}"),
        )
    })?);
    Ok(bytes)
}

pub(crate) fn decode_collection_config(
    expected_collection: &VectorCollectionName,
    bytes: &[u8],
) -> Result<VectorConfig, EngineError> {
    if bytes.first().copied() != Some(COLLECTION_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_collection",
            "stored vector collection config has an unknown format version",
        ));
    }
    let stored =
        serde_json::from_slice::<StoredCollectionConfig>(&bytes[1..]).map_err(|error| {
            EngineError::corruption(
                "data_loss.engine.vector_collection",
                format!("stored vector collection config cannot be decoded: {error}"),
            )
        })?;
    let collection = VectorCollectionName::new(stored.collection).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_collection",
            "stored vector collection name is invalid",
        )
    })?;
    if &collection != expected_collection {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_collection",
            "stored vector collection name does not match its row key",
        ));
    }
    let config = VectorConfig::new(stored.dimension, stored.metric).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_collection",
            "stored vector collection config violates engine limits",
        )
    })?;
    Ok(match stored.embedding_model {
        Some(model) => config.with_embedding_model(model),
        None => config,
    })
}

pub(crate) fn encode_vector_record(record: &VectorRecord) -> Result<Vec<u8>, EngineError> {
    let stored = StoredVectorRecord {
        collection: record.collection().as_str().to_owned(),
        key: record.key().as_str().to_owned(),
        vector_revision: record.vector_revision(),
        embedding: record.embedding().as_slice().to_vec(),
        metadata: record.metadata().map(VectorMetadata::clone_inner),
    };
    let mut bytes = vec![VECTOR_RECORD_FORMAT_VERSION];
    bytes.extend(serde_json::to_vec(&stored).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.vector_record",
            format!("vector record cannot be encoded: {error}"),
        )
    })?);
    Ok(bytes)
}

pub(crate) fn decode_vector_record(
    expected_collection: &VectorCollectionName,
    expected_key: &VectorKey,
    bytes: &[u8],
) -> Result<VectorRecord, EngineError> {
    if bytes.first().copied() != Some(VECTOR_RECORD_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_record",
            "stored vector record has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredVectorRecord>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.vector_record",
            format!("stored vector record cannot be decoded: {error}"),
        )
    })?;
    let collection = VectorCollectionName::new(stored.collection).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_record",
            "stored vector record collection name is invalid",
        )
    })?;
    let key = VectorKey::new(stored.key).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_record",
            "stored vector record key is invalid",
        )
    })?;
    if &collection != expected_collection || &key != expected_key {
        return Err(EngineError::corruption(
            "data_loss.engine.vector_record",
            "stored vector record identity does not match its row key",
        ));
    }
    let embedding = VectorEmbedding::from_stored(stored.embedding).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.vector_record",
            "stored vector embedding violates engine limits",
        )
    })?;
    let metadata = stored
        .metadata
        .map(VectorMetadata::new)
        .transpose()
        .map_err(|_| {
            EngineError::corruption(
                "data_loss.engine.vector_record",
                "stored vector metadata violates engine limits",
            )
        })?;
    Ok(VectorRecord::new(
        collection,
        key,
        embedding,
        metadata,
        stored.vector_revision,
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        decode_collection_config, decode_vector_record, encode_collection_config,
        encode_vector_record, VectorRecord,
    };
    use crate::data::vector::{
        VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey,
        VectorMetadata,
    };
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn vector_record_envelopes_round_trip_and_validate_identity() {
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        let config = VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config");
        let encoded = encode_collection_config(&collection, &config).expect("encoded config");
        assert_eq!(
            decode_collection_config(&collection, &encoded).expect("decoded config"),
            config
        );

        let key = VectorKey::new("doc/1").expect("valid key");
        let record = VectorRecord::new(
            collection.clone(),
            key.clone(),
            VectorEmbedding::new([1.0, 0.0]).expect("valid embedding"),
            Some(VectorMetadata::new(json!({"kind": "doc"})).expect("valid metadata")),
            7,
        );
        let encoded = encode_vector_record(&record).expect("encoded record");
        assert_eq!(
            decode_vector_record(&collection, &key, &encoded).expect("decoded record"),
            record
        );
    }

    #[test]
    fn vector_record_envelopes_have_deterministic_json_fixtures() {
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        let config = VectorConfig::new(3, VectorDistanceMetric::DotProduct).expect("valid config");
        assert_eq!(
            encode_collection_config(&collection, &config).expect("encoded config"),
            b"\x01{\"collection\":\"docs\",\"dimension\":3,\"metric\":\"dot_product\"}".to_vec()
        );

        let key = VectorKey::new("doc/1").expect("valid key");
        let record = VectorRecord::new(
            collection.clone(),
            key.clone(),
            VectorEmbedding::new([1.0, 0.0, -1.5]).expect("valid embedding"),
            Some(VectorMetadata::new(json!({"kind": "doc"})).expect("valid metadata")),
            9,
        );
        assert_eq!(
            encode_vector_record(&record).expect("encoded record"),
            b"\x01{\"collection\":\"docs\",\"key\":\"doc/1\",\"vector_revision\":9,\"embedding\":[1.0,0.0,-1.5],\"metadata\":{\"kind\":\"doc\"}}".to_vec()
        );
    }

    #[test]
    fn vector_collection_config_decode_rejects_corrupt_payloads() {
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        for (case, bytes) in [
            ("unknown-version", b"\x02{}".as_slice().to_vec()),
            ("truncated-json", b"\x01{\"collection\"".as_slice().to_vec()),
            (
                "unknown-metric",
                b"\x01{\"collection\":\"docs\",\"dimension\":2,\"metric\":\"bad\"}"
                    .as_slice()
                    .to_vec(),
            ),
            (
                "zero-dimension",
                b"\x01{\"collection\":\"docs\",\"dimension\":0,\"metric\":\"cosine\"}"
                    .as_slice()
                    .to_vec(),
            ),
            (
                "mismatched-collection",
                b"\x01{\"collection\":\"other\",\"dimension\":2,\"metric\":\"cosine\"}"
                    .as_slice()
                    .to_vec(),
            ),
            (
                "invalid-collection",
                b"\x01{\"collection\":\"_internal\",\"dimension\":2,\"metric\":\"cosine\"}"
                    .as_slice()
                    .to_vec(),
            ),
        ] {
            let error = decode_collection_config(&collection, &bytes).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.vector_collection");
        }
    }

    #[test]
    fn vector_record_decode_rejects_corrupt_payloads() {
        let collection = VectorCollectionName::new("docs").expect("valid collection");
        let key = VectorKey::new("doc/1").expect("valid key");
        for (case, bytes) in [
            ("unknown-version", b"\x02{}".as_slice().to_vec()),
            ("truncated-json", b"\x01{\"collection\"".as_slice().to_vec()),
            (
                "mismatched-collection",
                b"\x01{\"collection\":\"other\",\"key\":\"doc/1\",\"vector_revision\":1,\"embedding\":[1.0],\"metadata\":null}"
                    .as_slice()
                    .to_vec(),
            ),
            (
                "mismatched-key",
                b"\x01{\"collection\":\"docs\",\"key\":\"other\",\"vector_revision\":1,\"embedding\":[1.0],\"metadata\":null}"
                    .as_slice()
                    .to_vec(),
            ),
            (
                "invalid-key",
                "\x01{\"collection\":\"docs\",\"key\":\"bad\\u0000key\",\"vector_revision\":1,\"embedding\":[1.0],\"metadata\":null}"
                    .as_bytes()
                    .to_vec(),
            ),
            (
                "empty-embedding",
                b"\x01{\"collection\":\"docs\",\"key\":\"doc/1\",\"vector_revision\":1,\"embedding\":[],\"metadata\":null}"
                    .as_slice()
                    .to_vec(),
            ),
            (
                "non-numeric-embedding",
                b"\x01{\"collection\":\"docs\",\"key\":\"doc/1\",\"vector_revision\":1,\"embedding\":[null],\"metadata\":null}"
                    .as_slice()
                    .to_vec(),
            ),
            ("wrong-value-type", b"\x01[]".as_slice().to_vec()),
        ] {
            let error = decode_vector_record(&collection, &key, &bytes).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.vector_record");
        }
    }
}
