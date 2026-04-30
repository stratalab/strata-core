//! Engine-owned JSON transaction semantics.
//!
//! This module models pending JSON document mutations independently from the
//! lower transaction runtime. It owns:
//! - document snapshot versions used for JSON document OCC
//! - buffered JSON patches
//! - write-write path conflict detection
//! - full-document materialization of JSON patches

use std::collections::HashMap;

use strata_core::id::CommitVersion;
use strata_core::Value;
use strata_storage::Key;

use crate::primitives::json::{JsonDoc, JsonStore};
use crate::{apply_patches, JsonPatch, JsonPath, JsonValue, StrataError, StrataResult};

/// Buffered JSON mutation for a single document key.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingJsonWrite {
    /// JSON document storage key.
    pub key: Key,
    /// Patch to apply to the document.
    pub patch: JsonPatch,
}

impl PendingJsonWrite {
    /// Construct a new buffered JSON mutation.
    pub fn new(key: Key, patch: JsonPatch) -> Self {
        Self { key, patch }
    }
}

/// Conflict between two overlapping JSON writes to the same document.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonWriteConflict {
    /// JSON document storage key.
    pub key: Key,
    /// Path from the first conflicting write.
    pub path1: JsonPath,
    /// Path from the second conflicting write.
    pub path2: JsonPath,
}

/// Materialized JSON mutation ready to merge into generic write/delete sets.
#[derive(Debug, Clone, PartialEq)]
pub enum MaterializedJsonWrite {
    /// Replace or create the document with the serialized payload.
    Put(Value),
    /// Delete the entire document.
    Delete,
}

/// Engine-owned JSON transaction state.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JsonTxnState {
    snapshot_versions: HashMap<Key, CommitVersion>,
    writes: Vec<PendingJsonWrite>,
}

impl JsonTxnState {
    /// Create empty JSON transaction state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `true` if any JSON snapshot versions or writes are recorded.
    pub fn has_ops(&self) -> bool {
        !self.snapshot_versions.is_empty() || !self.writes.is_empty()
    }

    /// Return `true` if any JSON writes are buffered.
    pub fn has_writes(&self) -> bool {
        !self.writes.is_empty()
    }

    /// Immutable snapshot-version view.
    pub fn snapshot_versions(&self) -> &HashMap<Key, CommitVersion> {
        &self.snapshot_versions
    }

    /// Immutable buffered-write view.
    pub fn writes(&self) -> &[PendingJsonWrite] {
        &self.writes
    }

    /// Return `true` if this state has any pending writes for the given key.
    pub fn has_writes_for_key(&self, key: &Key) -> bool {
        self.writes.iter().any(|write| write.key == *key)
    }

    /// Clear all buffered JSON state.
    pub fn clear(&mut self) {
        self.snapshot_versions.clear();
        self.writes.clear();
    }

    /// Remember the version first seen for a JSON document.
    ///
    /// The first seen version wins so repeated reads in the same transaction do
    /// not weaken OCC by overwriting the original snapshot version.
    pub fn record_snapshot_version(&mut self, key: Key, version: CommitVersion) {
        self.snapshot_versions.entry(key).or_insert(version);
    }

    /// Buffer a JSON set patch.
    pub fn record_set(&mut self, key: Key, path: JsonPath, value: JsonValue) {
        self.writes
            .push(PendingJsonWrite::new(key, JsonPatch::set_at(path, value)));
    }

    /// Buffer a JSON delete patch.
    pub fn record_delete(&mut self, key: Key, path: JsonPath) {
        self.writes
            .push(PendingJsonWrite::new(key, JsonPatch::delete_at(path)));
    }

    /// Detect overlapping write-write path conflicts.
    pub fn write_conflicts(&self) -> Vec<JsonWriteConflict> {
        detect_write_conflicts(&self.writes)
    }

    /// Materialize buffered JSON patches into generic put/delete mutations.
    ///
    /// The caller provides the base document lookup.
    ///
    /// Correctness note:
    /// the JSON OCC path depends on this loader participating in the lower
    /// transaction read-set. For commit-time materialization that means using
    /// `TransactionContext::get`, not a direct storage lookup. The lower OCC
    /// validator runs under the branch commit lock and relies on the read-set
    /// entries populated by this loader to catch concurrent writers that race
    /// after engine-side JSON preparation begins.
    ///
    /// A direct `SegmentedStore` read here would preserve materialization but
    /// silently drop the final under-lock conflict check for JSON-only writes.
    pub fn materialize<F>(&self, load_base: F) -> StrataResult<Vec<(Key, MaterializedJsonWrite)>>
    where
        F: FnMut(&Key) -> StrataResult<Option<Value>>,
    {
        materialize_pending_writes(&self.writes, load_base)
    }

    /// Preview the current document view for one key.
    ///
    /// This applies any buffered JSON patches on top of the caller-provided
    /// base value and returns the resulting document, if any.
    pub fn preview_document<F>(&self, key: &Key, mut load_base: F) -> StrataResult<Option<JsonDoc>>
    where
        F: FnMut(&Key) -> StrataResult<Option<Value>>,
    {
        let patches = self
            .writes
            .iter()
            .filter(|write| write.key == *key)
            .map(|write| write.patch.clone())
            .collect::<Vec<_>>();

        if patches.is_empty() {
            return load_base(key)?
                .map(|value| {
                    let doc_id = key.user_key_string().ok_or_else(|| {
                        StrataError::internal("JSON preview requires a string document id")
                    })?;
                    JsonStore::deserialize_doc_with_fallback_id(&value, &doc_id)
                })
                .transpose();
        }

        let materialized = materialize_key(key, patches, load_base(key)?)?;
        match materialized {
            None | Some(MaterializedJsonWrite::Delete) => Ok(None),
            Some(MaterializedJsonWrite::Put(value)) => {
                let doc_id = key.user_key_string().ok_or_else(|| {
                    StrataError::internal("JSON preview requires a string document id")
                })?;
                Ok(Some(JsonStore::deserialize_doc_with_fallback_id(
                    &value, &doc_id,
                )?))
            }
        }
    }
}

/// Detect overlapping JSON writes to the same document.
pub fn detect_write_conflicts(writes: &[PendingJsonWrite]) -> Vec<JsonWriteConflict> {
    let mut conflicts = Vec::new();

    for (i, w1) in writes.iter().enumerate() {
        for w2 in writes.iter().skip(i + 1) {
            if w1.key != w2.key {
                continue;
            }

            if w1.patch.path().overlaps(w2.patch.path()) {
                conflicts.push(JsonWriteConflict {
                    key: w1.key.clone(),
                    path1: w1.patch.path().clone(),
                    path2: w2.patch.path().clone(),
                });
            }
        }
    }

    conflicts
}

/// Materialize JSON patches into document puts/deletes.
///
/// The `load_base` callback is part of the OCC contract, not just a storage
/// convenience. When used during commit preparation it must populate the lower
/// transaction read-set for every touched document key so the generic under-lock
/// validator can detect concurrent JSON writers.
pub fn materialize_pending_writes<F>(
    writes: &[PendingJsonWrite],
    mut load_base: F,
) -> StrataResult<Vec<(Key, MaterializedJsonWrite)>>
where
    F: FnMut(&Key) -> StrataResult<Option<Value>>,
{
    if writes.is_empty() {
        return Ok(Vec::new());
    }

    let mut patches_by_key: Vec<(Key, Vec<JsonPatch>)> = Vec::new();
    for write in writes {
        if let Some((_key, patches)) = patches_by_key.iter_mut().find(|(key, _)| *key == write.key)
        {
            patches.push(write.patch.clone());
        } else {
            patches_by_key.push((write.key.clone(), vec![write.patch.clone()]));
        }
    }

    let mut materialized = Vec::with_capacity(patches_by_key.len());

    for (key, patches) in patches_by_key {
        if let Some(write) = materialize_key(&key, patches, load_base(&key)?)? {
            materialized.push((key, write));
        }
    }

    Ok(materialized)
}

fn materialize_key(
    key: &Key,
    patches: Vec<JsonPatch>,
    base_value: Option<Value>,
) -> StrataResult<Option<MaterializedJsonWrite>> {
    let doc_id = key.user_key_string().ok_or_else(|| {
        StrataError::internal("JSON materialization requires a string document id")
    })?;

    let mut base_doc = base_value
        .map(|value| JsonStore::deserialize_doc_with_fallback_id(&value, &doc_id))
        .transpose()?;

    let has_set_patch = patches
        .iter()
        .any(|patch| matches!(patch, JsonPatch::Set { .. }));
    let delete_document = patches.iter().fold(false, |deleted, patch| match patch {
        JsonPatch::Delete { path } if path.is_root() => true,
        JsonPatch::Set { path, .. } if path.is_root() => false,
        _ => deleted,
    });

    if delete_document {
        return Ok(Some(MaterializedJsonWrite::Delete));
    }

    if base_doc.is_none() && !has_set_patch {
        return Ok(None);
    }

    let existing = base_doc.is_some();
    let mut doc = base_doc
        .take()
        .unwrap_or_else(|| JsonDoc::new(&doc_id, JsonValue::object()));

    apply_patches(&mut doc.value, &patches)
        .map_err(|e| StrataError::invalid_input(format!("Path error: {}", e)))?;
    doc.value
        .validate()
        .map_err(|e| StrataError::invalid_input(format!("JSON document exceeds limits: {}", e)))?;

    let version_bumps = if existing {
        patches
            .iter()
            .filter(|patch| !matches!(patch, JsonPatch::Delete { path } if path.is_root()))
            .count()
    } else {
        patches
            .iter()
            .filter(|patch| !matches!(patch, JsonPatch::Delete { path } if path.is_root()))
            .count()
            .saturating_sub(1)
    };
    for _ in 0..version_bumps {
        doc.touch();
    }

    Ok(Some(MaterializedJsonWrite::Put(JsonStore::serialize_doc(
        &doc,
    )?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use strata_core::BranchId;
    use strata_storage::Namespace;

    fn test_key(doc_id: &str) -> Key {
        Key::new_json(Arc::new(Namespace::for_branch(BranchId::new())), doc_id)
    }

    fn as_json_doc(value: &Value, doc_id: &str) -> JsonDoc {
        JsonStore::deserialize_doc_with_fallback_id(value, doc_id).unwrap()
    }

    fn materialize_with_bases(
        state: &JsonTxnState,
        bases: &HashMap<Key, Value>,
    ) -> Vec<(Key, MaterializedJsonWrite)> {
        state
            .materialize(|key| Ok(bases.get(key).cloned()))
            .unwrap()
    }

    #[test]
    fn snapshot_version_records_first_seen_version() {
        let key = test_key("doc");
        let mut state = JsonTxnState::new();

        state.record_snapshot_version(key.clone(), CommitVersion(3));
        state.record_snapshot_version(key.clone(), CommitVersion(9));

        assert_eq!(
            state.snapshot_versions().get(&key),
            Some(&CommitVersion(3)),
            "first seen version should remain authoritative"
        );
    }

    #[test]
    fn detects_ancestor_descendant_write_conflicts() {
        let key = test_key("doc");
        let mut state = JsonTxnState::new();
        state.record_set(key.clone(), "foo".parse().unwrap(), JsonValue::from(1i64));
        state.record_set(
            key.clone(),
            "foo.bar".parse().unwrap(),
            JsonValue::from(2i64),
        );

        let conflicts = state.write_conflicts();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].key, key);
        assert_eq!(conflicts[0].path1.to_string(), "foo");
        assert_eq!(conflicts[0].path2.to_string(), "foo.bar");
    }

    #[test]
    fn ignores_disjoint_writes_and_different_documents() {
        let key1 = test_key("doc-a");
        let key2 = test_key("doc-b");
        let mut state = JsonTxnState::new();
        state.record_set(key1.clone(), "foo".parse().unwrap(), JsonValue::from(1i64));
        state.record_set(key1, "bar".parse().unwrap(), JsonValue::from(2i64));
        state.record_set(key2, "foo".parse().unwrap(), JsonValue::from(3i64));

        assert!(state.write_conflicts().is_empty());
    }

    #[test]
    fn materialize_root_set_creates_versioned_document() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_set(key.clone(), JsonPath::root(), JsonValue::from("hello"));

        let materialized = materialize_with_bases(&state, &HashMap::new());
        assert_eq!(materialized.len(), 1);

        match &materialized[0].1 {
            MaterializedJsonWrite::Put(value) => {
                let doc = as_json_doc(value, "doc1");
                assert_eq!(doc.id, "doc1");
                assert_eq!(doc.value, JsonValue::from("hello"));
                assert_eq!(doc.version, 1);
            }
            other => panic!("expected put, got {:?}", other),
        }
    }

    #[test]
    fn materialize_nested_set_on_missing_doc_creates_object() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_set(
            key.clone(),
            "profile.name".parse().unwrap(),
            JsonValue::from("Ada"),
        );

        let materialized = materialize_with_bases(&state, &HashMap::new());
        assert_eq!(materialized.len(), 1);

        match &materialized[0].1 {
            MaterializedJsonWrite::Put(value) => {
                let doc = as_json_doc(value, "doc1");
                assert_eq!(doc.version, 1);
                let mut expected = BTreeMap::new();
                expected.insert("profile".to_string(), serde_json::json!({ "name": "Ada" }));
                assert_eq!(
                    doc.value,
                    JsonValue::from_value(serde_json::Value::Object(
                        expected.into_iter().collect()
                    ))
                );
            }
            other => panic!("expected put, got {:?}", other),
        }
    }

    #[test]
    fn materialize_existing_doc_applies_patches_in_order_and_bumps_version() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_set(
            key.clone(),
            "profile.name".parse().unwrap(),
            JsonValue::from("Ada"),
        );
        state.record_set(
            key.clone(),
            "profile.age".parse().unwrap(),
            JsonValue::from(42i64),
        );

        let mut base_doc = JsonDoc::new(
            "doc1",
            JsonValue::from_value(serde_json::json!({ "profile": { "country": "UK" } })),
        );
        base_doc.touch();
        let base_value = JsonStore::serialize_doc(&base_doc).unwrap();
        let bases = HashMap::from([(key.clone(), base_value)]);

        let materialized = materialize_with_bases(&state, &bases);
        assert_eq!(materialized.len(), 1);

        match &materialized[0].1 {
            MaterializedJsonWrite::Put(value) => {
                let doc = as_json_doc(value, "doc1");
                assert_eq!(doc.version, 4, "existing doc should bump once per patch");
                assert_eq!(
                    doc.value,
                    JsonValue::from_value(serde_json::json!({
                        "profile": {
                            "country": "UK",
                            "name": "Ada",
                            "age": 42
                        }
                    }))
                );
            }
            other => panic!("expected put, got {:?}", other),
        }
    }

    #[test]
    fn materialize_root_delete_becomes_delete() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_delete(key.clone(), JsonPath::root());

        let mut base_doc =
            JsonDoc::new("doc1", JsonValue::from_value(serde_json::json!({ "x": 1 })));
        base_doc.touch();
        let base_value = JsonStore::serialize_doc(&base_doc).unwrap();
        let bases = HashMap::from([(key.clone(), base_value)]);

        let materialized = materialize_with_bases(&state, &bases);
        assert_eq!(materialized, vec![(key, MaterializedJsonWrite::Delete)]);
    }

    #[test]
    fn materialize_root_set_after_root_delete_uses_last_root_operation() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_delete(key.clone(), JsonPath::root());
        state.record_set(
            key.clone(),
            JsonPath::root(),
            JsonValue::from_value(serde_json::json!({
                "status": "restored"
            })),
        );

        let materialized = materialize_with_bases(&state, &HashMap::new());
        assert_eq!(materialized.len(), 1);

        match &materialized[0].1 {
            MaterializedJsonWrite::Put(value) => {
                let doc = as_json_doc(value, "doc1");
                assert_eq!(
                    doc.value,
                    JsonValue::from_value(serde_json::json!({ "status": "restored" }))
                );
            }
            other => panic!("expected put, got {:?}", other),
        }
    }

    #[test]
    fn materialize_legacy_raw_json_value_uses_fallback_decoder() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_set(
            key.clone(),
            JsonPath::root(),
            JsonValue::from_value(serde_json::json!({ "extra": true })),
        );

        let raw = Value::Bytes(rmp_serde::to_vec(&JsonValue::from(2i64)).unwrap());
        let bases = HashMap::from([(key.clone(), raw)]);

        let materialized = materialize_with_bases(&state, &bases);
        assert_eq!(materialized.len(), 1);

        match &materialized[0].1 {
            MaterializedJsonWrite::Put(value) => {
                let doc = as_json_doc(value, "doc1");
                assert_eq!(doc.version, 2);
                assert_eq!(
                    doc.value,
                    JsonValue::from_value(serde_json::json!({
                        "extra": true
                    }))
                );
            }
            other => panic!("expected put, got {:?}", other),
        }
    }

    #[test]
    fn preview_document_applies_pending_writes() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_set(
            key.clone(),
            "profile.name".parse().unwrap(),
            JsonValue::from("Ada"),
        );

        let doc = state
            .preview_document(&key, |_| Ok(None))
            .unwrap()
            .expect("preview document should exist");
        assert_eq!(
            doc.value,
            JsonValue::from_value(serde_json::json!({
                "profile": { "name": "Ada" }
            }))
        );
    }

    #[test]
    fn materialize_path_type_mismatch_is_invalid_input() {
        let key = test_key("doc1");
        let mut state = JsonTxnState::new();
        state.record_set(
            key.clone(),
            "extra.flag".parse().unwrap(),
            JsonValue::from(true),
        );

        let base = JsonStore::serialize_doc(&JsonDoc::new("doc1", JsonValue::from(2i64))).unwrap();
        let err = state
            .materialize(|_| Ok(Some(base.clone())))
            .expect_err("non-object path write should fail");

        match err {
            StrataError::InvalidInput { message, .. } => {
                assert!(
                    message.contains("Path error"),
                    "expected path error, got: {message}"
                );
            }
            other => panic!("expected invalid input, got {:?}", other),
        }
    }
}
