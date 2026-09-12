use super::{
    BranchName, Bytes, EngineJsonIndexType, EngineJsonValue, ExecutorError, JsonDocumentId,
    JsonGetEntry, JsonIndexName, JsonIndexType, JsonPath, KvKey, KvValue, ProductSpace,
};

pub(super) fn branch_name(
    branch: Option<&str>,
    default: &str,
) -> Result<BranchName, ExecutorError> {
    BranchName::new(branch.unwrap_or(default)).map_err(ExecutorError::from)
}

pub(super) fn product_space(
    space: Option<&str>,
    default: &str,
) -> Result<ProductSpace, ExecutorError> {
    ProductSpace::new(space.unwrap_or(default)).map_err(ExecutorError::from)
}

pub(super) fn kv_key(key: Bytes) -> Result<KvKey, ExecutorError> {
    KvKey::new(key.into_vec()).map_err(ExecutorError::from)
}

pub(super) fn optional_key(key: Option<Bytes>) -> Result<Option<KvKey>, ExecutorError> {
    key.map(kv_key).transpose()
}

pub(super) fn kv_value(value: Bytes) -> KvValue {
    KvValue::new(value.into_vec())
}

pub(super) fn json_document_id(key: impl Into<String>) -> Result<JsonDocumentId, ExecutorError> {
    JsonDocumentId::new(key).map_err(ExecutorError::from)
}

pub(super) fn optional_json_document_id(
    key: Option<String>,
) -> Result<Option<JsonDocumentId>, ExecutorError> {
    key.map(json_document_id).transpose()
}

pub(super) fn optional_json_prefix(
    key: Option<String>,
) -> Result<Option<JsonDocumentId>, ExecutorError> {
    match key {
        Some(key) if key.is_empty() => Ok(None),
        Some(key) => json_document_id(key).map(Some),
        None => Ok(None),
    }
}

pub(super) fn json_path(path: &str) -> Result<JsonPath, ExecutorError> {
    path.parse().map_err(ExecutorError::from)
}

pub(super) fn json_value(value: serde_json::Value) -> Result<EngineJsonValue, ExecutorError> {
    EngineJsonValue::new(value).map_err(ExecutorError::from)
}

pub(super) fn json_index_name(name: String) -> Result<JsonIndexName, ExecutorError> {
    JsonIndexName::new(name).map_err(ExecutorError::from)
}

pub(super) fn json_get_entry(key: String, path: &str) -> Result<JsonGetEntry, ExecutorError> {
    Ok(JsonGetEntry::new(json_document_id(key)?, json_path(path)?))
}

pub(super) const fn engine_json_index_type(index_type: JsonIndexType) -> EngineJsonIndexType {
    match index_type {
        JsonIndexType::Numeric => EngineJsonIndexType::Numeric,
        JsonIndexType::Tag => EngineJsonIndexType::Tag,
        JsonIndexType::Text => EngineJsonIndexType::Text,
    }
}

pub(super) const fn output_json_index_type(index_type: EngineJsonIndexType) -> JsonIndexType {
    match index_type {
        EngineJsonIndexType::Numeric => JsonIndexType::Numeric,
        EngineJsonIndexType::Tag => JsonIndexType::Tag,
        EngineJsonIndexType::Text => JsonIndexType::Text,
    }
}

pub(super) fn optional_limit(limit: Option<u64>) -> Result<Option<usize>, ExecutorError> {
    limit
        .map(|limit| {
            usize::try_from(limit).map_err(|_| {
                ExecutorError::new(
                    "invalid_argument.executor.limit",
                    "limit does not fit this platform",
                )
            })
        })
        .transpose()
}

pub(super) fn required_usize(
    value: u64,
    code: &'static str,
    message: &'static str,
) -> Result<usize, ExecutorError> {
    usize::try_from(value).map_err(|_| ExecutorError::new(code, message))
}
