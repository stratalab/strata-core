//! JSON and vector validation-refusal code coverage (TCP3.5c).
//!
//! Pins the reachable JSON/vector refusal codes by their literal
//! `<class>.engine.<detail>` string (the existing suites assert by class).
//! Only the genuinely user-reachable refusals are here: investigation found
//! most of the deep-dive's "15 vector/json codes" are unreachable defensive
//! `serde_json::to_vec` encode arms, short-circuited empty-batch invariants,
//! or reopen-time layout/IO faults (#2651, TCP3.15) — recorded in the
//! workspace error-code guard's allowlist with per-code reasons.

mod common;

use serde_json::json;
use strata_engine::{
    JsonDocumentId, JsonIndexName, VectorCollectionName, VectorConfig, VectorDistanceMetric,
    VectorKey, VectorMetadata,
};

use common::{branch, open_cache_database, space};

/// The JSON index-name validator rejects an empty name with a stable code.
#[test]
fn json_index_name_empty_is_rejected() {
    let error = JsonIndexName::new("").expect_err("empty index name must reject");
    assert_eq!(error.code(), "invalid_argument.engine.json_index_name");
}

/// Vector metadata exceeding the 16 MiB encoded ceiling is rejected.
#[test]
fn vector_metadata_too_large_is_rejected() {
    let oversized = json!({ "blob": "x".repeat(17 * 1024 * 1024) });
    let error = VectorMetadata::new(oversized).expect_err("oversized metadata must reject");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.vector_metadata_too_large"
    );
}

/// Vector metadata that is not a JSON object (list, scalar, bool, null) is
/// rejected. Filters match on object fields, so a non-object row would be
/// stored verbatim and then silently unfilterable.
#[test]
fn vector_metadata_must_be_a_json_object() {
    for value in [
        json!([1, 2, 3]),
        json!("scalar"),
        json!(7),
        json!(true),
        json!(null),
    ] {
        let error =
            VectorMetadata::new(value.clone()).expect_err("non-object metadata must reject");
        assert_eq!(
            error.code(),
            "invalid_argument.engine.vector_metadata",
            "value {value} must reject as non-object"
        );
    }

    // An object — including an empty one — is accepted.
    VectorMetadata::new(json!({})).expect("empty object metadata is valid");
    VectorMetadata::new(json!({ "kind": "doc" })).expect("object metadata is valid");
}

/// A JSON batch-delete carrying the same document id twice is rejected — the
/// duplicate-id refusal reached through the public `batch_delete` API.
#[test]
fn json_batch_delete_rejects_duplicate_document_ids() {
    let database = open_cache_database().expect("cache open");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("json service");

    let id = JsonDocumentId::new("dup").expect("doc id");
    let error = json
        .batch_delete([id.clone(), id])
        .expect_err("duplicate ids in one batch must reject");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.json_batch_duplicate_document"
    );
}

// --- published limits (#3214) ------------------------------------------
//
// Each test below pins one number the JSON commands publish in their
// `# Guaranteed semantics` block. The pair matters: a published limit that is
// only tested from the refusing side says nothing about where refusal starts,
// and an off-by-one would put a wrong number in the reference. So every limit
// is proven from both sides — the largest accepted input, and the smallest
// refused one, with its code.

/// A document id is bounded at 65,535 bytes.
#[test]
fn json_document_id_limit_is_the_refusal_boundary() {
    const LIMIT: usize = u16::MAX as usize;
    JsonDocumentId::new("a".repeat(LIMIT)).expect("the largest accepted id");
    let error = JsonDocumentId::new("a".repeat(LIMIT + 1)).expect_err("one byte past");
    assert_eq!(error.code(), "invalid_argument.engine.json_document_id");
}

/// An index name is bounded at 256 bytes.
#[test]
fn json_index_name_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 256;
    JsonIndexName::new("a".repeat(LIMIT)).expect("the largest accepted name");
    let error = JsonIndexName::new("a".repeat(LIMIT + 1)).expect_err("one byte past");
    assert_eq!(error.code(), "invalid_argument.engine.json_index_name");
}

/// A document is bounded at 16 MiB of serialized JSON.
#[test]
fn json_document_size_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 16 * 1024 * 1024;
    // `{"v":"<pad>"}` is the payload plus six bytes of framing and two quotes.
    let framing = json!({ "v": "" }).to_string().len();
    let at = json!({ "v": "a".repeat(LIMIT - framing) });
    assert_eq!(
        at.to_string().len(),
        LIMIT,
        "the fixture is exactly at the limit"
    );
    strata_engine::JsonValue::new(at).expect("a document exactly at the limit");

    let over = json!({ "v": "a".repeat(LIMIT - framing + 1) });
    let error = strata_engine::JsonValue::new(over).expect_err("one byte past");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.json_document_too_large"
    );
}

/// Nesting is bounded at 100 levels.
#[test]
fn json_nesting_depth_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 100;
    let nest = |depth: usize| {
        let mut value = json!(1);
        for _ in 0..depth {
            value = json!([value]);
        }
        value
    };
    strata_engine::JsonValue::new(nest(LIMIT)).expect("the deepest accepted document");
    let error = strata_engine::JsonValue::new(nest(LIMIT + 1)).expect_err("one level past");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.json_document_too_deep"
    );
}

/// A single array is bounded at 1,000,000 elements.
#[test]
fn json_array_size_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 1_000_000;
    let array = |len: usize| serde_json::Value::Array(vec![json!(0); len]);
    strata_engine::JsonValue::new(array(LIMIT)).expect("the largest accepted array");
    let error = strata_engine::JsonValue::new(array(LIMIT + 1)).expect_err("one element past");
    assert_eq!(error.code(), "invalid_argument.engine.json_array_too_large");
}

/// A path is bounded at 256 segments.
#[test]
fn json_path_segment_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 256;
    let path = |count: usize| {
        (0..count)
            .map(|index| strata_engine::JsonPathSegment::Key(format!("s{index}")))
            .collect::<Vec<_>>()
    };
    strata_engine::JsonPath::from_segments(path(LIMIT)).expect("the longest accepted path");
    let error =
        strata_engine::JsonPath::from_segments(path(LIMIT + 1)).expect_err("one segment past");
    assert_eq!(error.code(), "invalid_argument.engine.json_path_too_long");
}

/// A collection dimension is bounded at 32,768.
#[test]
fn vector_dimension_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 32_768;
    VectorConfig::new(LIMIT, VectorDistanceMetric::Cosine).expect("the widest accepted dimension");
    let error =
        VectorConfig::new(LIMIT + 1, VectorDistanceMetric::Cosine).expect_err("one dimension past");
    assert_eq!(error.code(), "invalid_argument.engine.vector_dimension");
}

/// A collection name is bounded at 256 bytes.
#[test]
fn vector_collection_name_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 256;
    VectorCollectionName::new("a".repeat(LIMIT)).expect("the longest accepted name");
    let error = VectorCollectionName::new("a".repeat(LIMIT + 1)).expect_err("one byte past");
    assert_eq!(error.code(), "invalid_argument.engine.vector_collection");
}

/// A vector key is bounded at 1,024 bytes.
#[test]
fn vector_key_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 1024;
    VectorKey::new("a".repeat(LIMIT)).expect("the longest accepted key");
    let error = VectorKey::new("a".repeat(LIMIT + 1)).expect_err("one byte past");
    assert_eq!(error.code(), "invalid_argument.engine.vector_key");
}

/// Metadata is bounded at 16 MiB of encoded JSON.
#[test]
fn vector_metadata_size_limit_is_the_refusal_boundary() {
    const LIMIT: usize = 16 * 1024 * 1024;
    // `{"v":"<pad>"}` is the payload plus six bytes of framing and two quotes.
    let framing = json!({ "v": "" }).to_string().len();
    let at = json!({ "v": "a".repeat(LIMIT - framing) });
    assert_eq!(
        serde_json::to_vec(&at).expect("encode").len(),
        LIMIT,
        "the fixture is exactly at the limit"
    );
    VectorMetadata::new(at).expect("metadata exactly at the limit");

    let over = json!({ "v": "a".repeat(LIMIT - framing + 1) });
    let error = VectorMetadata::new(over).expect_err("one byte past");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.vector_metadata_too_large"
    );
}
