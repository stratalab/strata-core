//! JSON document capability.

mod adapter;
#[cfg(test)]
mod batch_snapshot_tests;
mod document;
mod outcome;
mod service;
mod types;

pub(crate) use adapter::JsonBranchAdapter;
pub use outcome::{
    JsonBatchDeleteOutcome, JsonBatchSetItemOutcome, JsonBatchSetOutcome, JsonDeleteOutcome,
    JsonHistory, JsonHistoryRow, JsonListPage, JsonSample, JsonSampleRow, JsonVersionedValue,
    JsonWriteOutcome,
};
pub use service::JsonService;
pub use types::{
    JsonDocumentId, JsonGetEntry, JsonIndexDefinition, JsonIndexName, JsonIndexType, JsonPath,
    JsonPathSegment, JsonSetEntry, JsonValue,
};

pub(crate) use document::{
    decode_index_definition, decode_stored_document, encode_index_definition,
    encode_stored_document, extract_index_value, index_entry_value_bytes, JsonDocument,
};
pub(crate) use types::{delete_at_path, get_at_path, set_at_path};
