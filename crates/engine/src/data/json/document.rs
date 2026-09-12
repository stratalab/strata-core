//! JSON storage envelope and index value helpers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;

use super::{
    get_at_path, JsonDocumentId, JsonIndexDefinition, JsonIndexName, JsonIndexType, JsonPath,
    JsonValue,
};

const DOCUMENT_FORMAT_VERSION: u8 = 1;
const INDEX_DEFINITION_FORMAT_VERSION: u8 = 1;
const INDEX_ENTRY_VALUE: &[u8] = b"\x01";

/// Stored JSON document envelope.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JsonDocument {
    id: JsonDocumentId,
    value: JsonValue,
    document_version: u64,
    updated_at_micros: u64,
}

impl JsonDocument {
    pub(crate) fn new(id: JsonDocumentId, value: JsonValue) -> Self {
        Self {
            id,
            value,
            document_version: 1,
            updated_at_micros: unix_micros(),
        }
    }

    pub(crate) fn from_existing(
        id: JsonDocumentId,
        value: JsonValue,
        document_version: u64,
        updated_at_micros: u64,
    ) -> Self {
        Self {
            id,
            value,
            document_version,
            updated_at_micros,
        }
    }

    pub(crate) const fn id(&self) -> &JsonDocumentId {
        &self.id
    }

    pub(crate) const fn value(&self) -> &JsonValue {
        &self.value
    }

    pub(crate) fn value_mut(&mut self) -> &mut JsonValue {
        &mut self.value
    }

    pub(crate) const fn document_version(&self) -> u64 {
        self.document_version
    }

    pub(crate) fn touch(&mut self) {
        self.document_version = self.document_version.saturating_add(1);
        self.updated_at_micros = unix_micros();
    }
}

#[derive(Serialize, Deserialize)]
struct StoredJsonDocument {
    id: String,
    document_version: u64,
    updated_at_micros: u64,
    value: Value,
}

#[derive(Serialize, Deserialize)]
struct StoredIndexDefinition {
    name: String,
    space: String,
    field_path: String,
    index_type: JsonIndexType,
    created_version: u64,
    created_timestamp: u64,
}

pub(crate) fn encode_stored_document(document: &JsonDocument) -> Result<Vec<u8>, EngineError> {
    let stored = StoredJsonDocument {
        id: document.id().as_str().to_owned(),
        document_version: document.document_version(),
        updated_at_micros: document.updated_at_micros,
        value: document.value().as_inner().clone(),
    };
    let mut bytes = Vec::new();
    bytes.push(DOCUMENT_FORMAT_VERSION);
    bytes.extend(serde_json::to_vec(&stored).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.json_document",
            format!("JSON document cannot be encoded: {error}"),
        )
    })?);
    Ok(bytes)
}

pub(crate) fn decode_stored_document(
    expected_id: &JsonDocumentId,
    bytes: &[u8],
) -> Result<JsonDocument, EngineError> {
    if bytes.first().copied() != Some(DOCUMENT_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.json_document",
            "stored JSON document has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredJsonDocument>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.json_document",
            format!("stored JSON document cannot be decoded: {error}"),
        )
    })?;
    let id = JsonDocumentId::new(stored.id).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_document",
            "stored JSON document id is invalid",
        )
    })?;
    if &id != expected_id {
        return Err(EngineError::corruption(
            "data_loss.engine.json_document",
            "stored JSON document id does not match its row key",
        ));
    }
    let value = JsonValue::new(stored.value).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_document",
            "stored JSON document value violates engine limits",
        )
    })?;
    Ok(JsonDocument::from_existing(
        id,
        value,
        stored.document_version,
        stored.updated_at_micros,
    ))
}

pub(crate) fn encode_index_definition(
    definition: &JsonIndexDefinition,
) -> Result<Vec<u8>, EngineError> {
    let stored = StoredIndexDefinition {
        name: definition.name().as_str().to_owned(),
        space: definition.space().as_str().to_owned(),
        field_path: definition.field_path().to_string(),
        index_type: definition.index_type(),
        created_version: definition.created_version(),
        created_timestamp: definition.created_timestamp(),
    };
    let mut bytes = Vec::new();
    bytes.push(INDEX_DEFINITION_FORMAT_VERSION);
    bytes.extend(serde_json::to_vec(&stored).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.json_index",
            format!("JSON index definition cannot be encoded: {error}"),
        )
    })?);
    Ok(bytes)
}

pub(crate) fn decode_index_definition(bytes: &[u8]) -> Result<JsonIndexDefinition, EngineError> {
    if bytes.first().copied() != Some(INDEX_DEFINITION_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.json_index",
            "stored JSON index definition has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredIndexDefinition>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.json_index",
            format!("stored JSON index definition cannot be decoded: {error}"),
        )
    })?;
    let name = JsonIndexName::new(stored.name).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_index",
            "stored JSON index name is invalid",
        )
    })?;
    let space = ProductSpace::new(stored.space).map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_index",
            "stored JSON index space is invalid",
        )
    })?;
    let field_path = stored.field_path.parse::<JsonPath>().map_err(|_| {
        EngineError::corruption(
            "data_loss.engine.json_index",
            "stored JSON index field path is invalid",
        )
    })?;
    Ok(JsonIndexDefinition::new(
        name,
        space,
        field_path,
        stored.index_type,
        stored.created_version,
        stored.created_timestamp,
    ))
}

pub(crate) fn extract_index_value(
    document: &JsonValue,
    definition: &JsonIndexDefinition,
) -> Option<Vec<u8>> {
    let value = get_at_path(document, definition.field_path())?;
    match definition.index_type() {
        JsonIndexType::Numeric => {
            let number = value.as_inner().as_f64()?;
            Some(encode_f64(number).to_vec())
        }
        JsonIndexType::Tag => {
            let text = value.as_inner().as_str()?;
            Some(text.to_lowercase().into_bytes())
        }
        JsonIndexType::Text => {
            let text = value.as_inner().as_str()?;
            Some(truncate_text(&text.to_lowercase(), 128).as_bytes().to_vec())
        }
    }
}

pub(crate) fn index_entry_value_bytes() -> Vec<u8> {
    INDEX_ENTRY_VALUE.to_vec()
}

fn encode_f64(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let encoded = if bits & (1_u64 << 63) == 0 {
        bits ^ (1_u64 << 63)
    } else {
        !bits
    };
    encoded.to_be_bytes()
}

fn truncate_text(value: &str, limit: usize) -> &str {
    if value.len() <= limit {
        return value;
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn unix_micros() -> u64 {
    crate::time_compat::SystemTime::now()
        .duration_since(crate::time_compat::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{decode_stored_document, encode_stored_document, JsonDocument};
    use crate::data::json::{JsonDocumentId, JsonValue};
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn document_envelope_encodes_deterministic_fixture() {
        let id = JsonDocumentId::new("doc").expect("valid id");
        let document = JsonDocument::from_existing(
            id.clone(),
            JsonValue::new(json!(["stable"])).expect("valid"),
            7,
            123,
        );
        let encoded = encode_stored_document(&document).expect("encoded");
        assert_eq!(
            encoded,
            b"\x01{\"id\":\"doc\",\"document_version\":7,\"updated_at_micros\":123,\"value\":[\"stable\"]}".to_vec()
        );

        let decoded = decode_stored_document(&id, &encoded).expect("decoded");
        assert_eq!(decoded.id(), &id);
        assert_eq!(decoded.document_version(), 7);
        assert_eq!(
            encode_stored_document(&decoded).expect("re-encoded"),
            encoded
        );
    }

    #[test]
    fn document_envelope_round_trips_and_checks_id() {
        let id = JsonDocumentId::new("doc").expect("valid id");
        let document =
            JsonDocument::new(id.clone(), JsonValue::new(json!({"a": 1})).expect("valid"));
        let encoded = encode_stored_document(&document).expect("encoded");
        let decoded = decode_stored_document(&id, &encoded).expect("decoded");
        assert_eq!(decoded.id(), &id);
        assert_eq!(decoded.document_version(), 1);

        let other = JsonDocumentId::new("other").expect("valid id");
        let error = decode_stored_document(&other, &encoded).expect_err("id mismatch");
        assert_eq!(error.code(), "data_loss.engine.json_document");
    }

    #[test]
    fn document_envelope_rejects_corrupt_payloads() {
        let id = JsonDocumentId::new("doc").expect("valid id");
        for (case, bytes) in [
            ("unknown-version", b"\x02{}".to_vec()),
            ("truncated-payload", b"\x01{".to_vec()),
            ("not-json", b"\x01not-json".to_vec()),
            (
                "invalid-id",
                b"\x01{\"id\":\"\",\"document_version\":1,\"updated_at_micros\":1,\"value\":null}"
                    .to_vec(),
            ),
        ] {
            let error = decode_stored_document(&id, &bytes).expect_err(case);
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.json_document");
        }
    }
}
