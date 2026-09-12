//! JSON document conformance tests.

mod common;

use serde_json::{json, Value};
use strata_core::{CommitVersion, Timestamp};
use strata_engine::{
    Database, EngineErrorClass, JsonDocumentId, JsonGetEntry, JsonIndexName, JsonIndexType,
    JsonPath, JsonService, JsonSetEntry, JsonValue, ProductSpace,
};

use common::{branch, open_cache_database, open_durable_database, space};

/// #3112 S4: JSON history rows carry their own commit's wall-clock instant.
/// In the ENGINE crate because the join is engine code and the mutation gate
/// runs each mutated crate's own tests.
#[test]
fn json_history_rows_carry_their_commits_wall_clock_instant() {
    let database = open_cache_database().expect("cache open succeeds");
    let doc = JsonDocumentId::new("doc").expect("valid document id");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");

    json.create(doc.clone(), json_value(json!({})))
        .expect("document created");

    let first = json
        .set(doc.clone(), &path("field"), json_value(json!("one")))
        .expect("first set succeeds")
        .commit()
        .committed_at()
        .expect("write ack carries an instant");
    std::thread::sleep(std::time::Duration::from_millis(2));
    let second = json
        .set(doc.clone(), &path("field"), json_value(json!("two")))
        .expect("second set succeeds")
        .commit()
        .committed_at()
        .expect("write ack carries an instant");
    assert!(second > first, "commits must land at distinct instants");

    let history = json
        .get_versions(&doc)
        .expect("history succeeds")
        .expect("history exists");
    // Three rows newest-first: the two sets, plus the create that made the
    // document.
    let rows = history.rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].committed_at(),
        Some(second),
        "the newest row must carry the second set's instant"
    );
    assert_eq!(
        rows[1].committed_at(),
        Some(first),
        "the middle row must carry the first set's instant"
    );
    assert!(
        rows[2]
            .committed_at()
            .is_some_and(|created| created < first),
        "the create is dated too, and precedes both sets"
    );
}

#[test]
fn json_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_json_contract);
}

#[test]
fn json_branch_isolation_matches_kv_shape() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let doc = doc_id("profile");
    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        json.set_or_create(
            doc.clone(),
            &root(),
            json_value(json!({"name": "base", "visits": 1})),
        )
        .expect("base write succeeds");
    }

    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch create succeeds");

    {
        let mut feature = database
            .json(branch("feature"), space("default"))
            .expect("feature JSON opens");
        feature
            .set(doc.clone(), &path("name"), json_value(json!("feature")))
            .expect("feature update succeeds");
    }

    let mut default_json = database
        .json(branch("default"), space("default"))
        .expect("default JSON opens");
    assert_eq!(
        default_json
            .get(&doc, &path("name"))
            .expect("read succeeds")
            .expect("value")
            .as_inner(),
        &json!("base")
    );
    drop(default_json);

    let mut feature_json = database
        .json(branch("feature"), space("default"))
        .expect("feature JSON opens");
    assert_eq!(
        feature_json
            .get(&doc, &path("name"))
            .expect("read succeeds")
            .expect("value")
            .as_inner(),
        &json!("feature")
    );
    assert_eq!(
        feature_json
            .list(Some(&doc_id("pro")), None, 10)
            .expect("feature list succeeds")
            .document_ids(),
        std::slice::from_ref(&doc)
    );
    assert_eq!(feature_json.count(None).expect("feature count succeeds"), 1);
    assert_eq!(
        feature_json
            .sample(None, 1)
            .expect("feature sample succeeds")
            .rows()
            .len(),
        1
    );
    assert_eq!(
        feature_json
            .get_versions(&doc)
            .expect("feature history succeeds")
            .expect("history exists")
            .rows()[0]
            .value()
            .expect("latest value")
            .as_inner(),
        &json!({"name": "feature", "visits": 1})
    );
    drop(feature_json);

    let mut default_json = database
        .json(branch("default"), space("default"))
        .expect("default JSON opens");
    default_json
        .delete_document(doc.clone())
        .expect("default delete succeeds");
    assert!(!default_json.exists(&doc).expect("exists succeeds"));
    assert_eq!(default_json.count(None).expect("default count succeeds"), 0);
    drop(default_json);

    let mut feature_json = database
        .json(branch("feature"), space("default"))
        .expect("feature JSON opens");
    assert_eq!(
        feature_json
            .get(&doc, &path("name"))
            .expect("read succeeds")
            .expect("value")
            .as_inner(),
        &json!("feature")
    );
}

#[test]
fn json_invalid_inputs_are_engine_errors() {
    let error = JsonDocumentId::new("").expect_err("empty id rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.json_document_id");

    let error = "user..name"
        .parse::<JsonPath>()
        .expect_err("invalid path rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.json_path");

    let error = JsonIndexName::new("_internal").expect_err("reserved index rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.json_index_name_reserved"
    );
}

#[test]
fn special_character_keys_are_addressable_by_bracket_notation() {
    // The issue's repro: keys that dot notation cannot express (emoji, an
    // embedded dot) are stored and then read back by bracket notation in either
    // quote style, while dot notation reports a typed, guidance-carrying error.
    let database = open_cache_database().expect("cache open");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("json opens");
    let doc = doc_id("special");
    json.set_or_create(
        doc.clone(),
        &root(),
        json_value(json!({ "k\u{1f389}": "V", "a.b": "W" })),
    )
    .expect("write with special-character keys succeeds");

    for path_str in ["$['k\u{1f389}']", "$[\"k\u{1f389}\"]"] {
        assert_eq!(
            json.get(&doc, &path(path_str))
                .expect("read succeeds")
                .expect("value")
                .as_inner(),
            &json!("V"),
            "{path_str}"
        );
    }
    // A key containing a dot is reachable only through bracket notation.
    assert_eq!(
        json.get(&doc, &path("$['a.b']"))
            .expect("read succeeds")
            .expect("value")
            .as_inner(),
        &json!("W")
    );

    // Dot notation cannot express the emoji key; it reports the typed path error.
    let error = "$.k\u{1f389}"
        .parse::<JsonPath>()
        .expect_err("dot notation rejects the emoji key");
    assert_eq!(error.code(), "invalid_argument.engine.json_path");
}

#[test]
fn json_serde_construction_preserves_validation() {
    let error = serde_json::from_value::<JsonDocumentId>(json!(""))
        .expect_err("empty document id rejected through serde");
    assert!(error.to_string().contains("JSON document id"));

    let error = serde_json::from_value::<JsonIndexName>(json!("_internal"))
        .expect_err("reserved index name rejected through serde");
    assert!(error.to_string().contains("reserved"));

    let error = serde_json::from_value::<JsonPath>(json!("user..name"))
        .expect_err("invalid string path rejected through serde");
    assert!(error.to_string().contains("empty key"));

    let error = serde_json::from_value::<JsonPath>(json!([{ "key": "" }]))
        .expect_err("invalid segment path rejected through serde");
    assert!(error.to_string().contains("JSON path key"));

    let mut deep = json!(null);
    for _ in 0..101 {
        deep = json!({ "nested": deep });
    }
    let error =
        serde_json::from_value::<JsonValue>(deep).expect_err("deep value rejected through serde");
    assert!(error.to_string().contains("maximum nesting depth"));
}

#[test]
fn json_durable_reopen_preserves_documents_versions_and_indexes() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let doc = doc_id("profile:1");
    let by_name = JsonIndexName::new("by_name").expect("valid index");
    let update_version;

    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        {
            let mut json = database
                .json(branch("default"), space("default"))
                .expect("JSON service opens");
            json.create(
                doc.clone(),
                json_value(json!({"name": "alice", "visits": 1})),
            )
            .expect("create succeeds");
            update_version = json
                .set(doc.clone(), &path("visits"), json_value(json!(2)))
                .expect("set succeeds")
                .document_version();
            json.create_index(by_name.clone(), path("name"), JsonIndexType::Tag)
                .expect("index create succeeds");
        }
        database.close().expect("close succeeds");
    }

    let reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut json = reopened
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    let value = json
        .get_versioned(&doc, &root())
        .expect("read succeeds")
        .expect("document exists");
    assert_eq!(
        value.value().as_inner(),
        &json!({"name": "alice", "visits": 2})
    );
    assert_eq!(value.document_version(), update_version);

    let indexes = json.list_indexes().expect("list indexes succeeds");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name(), &by_name);
    assert!(indexes[0].created_version() > 0);
    assert!(indexes[0].created_timestamp() > 0);
}

#[test]
fn json_list_pages_live_rows_after_deletes() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    for id in ["page:1", "page:2", "page:3", "page:4"] {
        json.create(doc_id(id), json_value(json!({ "id": id })))
            .expect("create succeeds");
    }
    json.delete_document(doc_id("page:2"))
        .expect("delete succeeds");

    let first = json
        .list(Some(&doc_id("page:")), None, 2)
        .expect("first page succeeds");
    assert_eq!(first.document_ids(), &[doc_id("page:1"), doc_id("page:3")]);
    assert!(first.has_more());

    let second = json
        .list(Some(&doc_id("page:")), first.cursor(), 2)
        .expect("second page succeeds");
    assert_eq!(second.document_ids(), &[doc_id("page:4")]);
    assert!(!second.has_more());
}

#[test]
fn json_set_and_batch_contracts_are_explicit() {
    let database = open_cache_database().expect("cache open succeeds");
    let doc = doc_id("contract:1");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");

    let missing_document = json
        .set(
            doc.clone(),
            &path("profile.name"),
            json_value(json!("alice")),
        )
        .expect_err("set requires an existing document");
    assert_eq!(missing_document.class(), EngineErrorClass::NotFound);

    json.create(doc.clone(), json_value(json!({})))
        .expect("create succeeds");
    json.set(
        doc.clone(),
        &path("profile.name"),
        json_value(json!("alice")),
    )
    .expect("set creates missing object fields inside existing documents");
    assert_eq!(
        json.get(&doc, &path("profile"))
            .expect("read succeeds")
            .expect("profile exists")
            .as_inner(),
        &json!({"name": "alice"})
    );

    let batch = json
        .batch_set_or_create([
            JsonSetEntry::new(doc.clone(), path("profile.age"), json_value(json!(37))),
            JsonSetEntry::new(
                doc.clone(),
                path("profile.city"),
                json_value(json!("Paris")),
            ),
        ])
        .expect("batch applies same-document entries in order");
    assert_eq!(batch.results()[0].document_version(), 3);
    assert_eq!(batch.results()[1].document_version(), 4);
    assert!(batch.commit().is_some());
    assert_eq!(
        json.get(&doc, &path("profile"))
            .expect("read succeeds")
            .expect("profile exists")
            .as_inner(),
        &json!({"age": 37, "city": "Paris", "name": "alice"})
    );

    let empty_set = json
        .batch_set_or_create([])
        .expect("empty batch set succeeds");
    assert!(empty_set.results().is_empty());
    assert!(empty_set.commit().is_none());

    let empty_delete = json.batch_delete([]).expect("empty batch delete succeeds");
    assert!(empty_delete.deleted().is_empty());
    assert!(empty_delete.commit().is_none());
}

#[test]
fn json_space_isolation_covers_reads_lists_and_indexes() {
    let database = open_cache_database().expect("cache open succeeds");
    let doc = doc_id("shared");
    {
        let mut default_json = database
            .json(branch("default"), space("default"))
            .expect("default JSON opens");
        default_json
            .create(doc.clone(), json_value(json!({"space": "default"})))
            .expect("default create succeeds");
        default_json
            .create_index(
                JsonIndexName::new("by_space").expect("valid index"),
                path("space"),
                JsonIndexType::Tag,
            )
            .expect("default index succeeds");
    }

    {
        let mut other_json = database
            .json(branch("default"), space("other"))
            .expect("other JSON opens");
        other_json
            .create(doc.clone(), json_value(json!({"space": "other"})))
            .expect("other create succeeds");
        assert!(other_json.list_indexes().expect("other indexes").is_empty());
    }

    let mut default_json = database
        .json(branch("default"), space("default"))
        .expect("default JSON opens");
    assert_eq!(
        default_json
            .get(&doc, &path("space"))
            .expect("read succeeds")
            .expect("value")
            .as_inner(),
        &json!("default")
    );
    assert_eq!(
        default_json
            .list(None, None, 10)
            .expect("list succeeds")
            .document_ids(),
        std::slice::from_ref(&doc)
    );
    assert_eq!(default_json.count(None).expect("count succeeds"), 1);
    assert_eq!(
        default_json
            .sample(None, 10)
            .expect("sample succeeds")
            .rows()
            .len(),
        1
    );
    assert_eq!(default_json.list_indexes().expect("list indexes").len(), 1);
    drop(default_json);

    let mut other_json = database
        .json(branch("default"), space("other"))
        .expect("other JSON opens");
    assert_eq!(
        other_json
            .get(&doc, &path("space"))
            .expect("read succeeds")
            .expect("value")
            .as_inner(),
        &json!("other")
    );
    assert_eq!(other_json.count(None).expect("count succeeds"), 1);
    assert!(other_json.list_indexes().expect("other indexes").is_empty());
}

#[test]
fn json_list_count_and_sample_cover_boundaries() {
    run_database_modes(|database| {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        for id in ["alpha:1", "alpha:2", "beta:1"] {
            json.create(doc_id(id), json_value(json!({ "id": id })))
                .expect("create succeeds");
        }
        json.create_index(
            JsonIndexName::new("by_id").expect("valid index"),
            path("id"),
            JsonIndexType::Tag,
        )
        .expect("index create succeeds");
        json.delete_document(doc_id("alpha:2"))
            .expect("delete succeeds");

        assert!(json
            .list(None, None, 0)
            .expect("zero limit succeeds")
            .document_ids()
            .is_empty());
        assert_eq!(
            json.list(None, None, 10)
                .expect("list succeeds")
                .document_ids(),
            &[doc_id("alpha:1"), doc_id("beta:1")]
        );
        assert_eq!(
            json.list(Some(&doc_id("alpha:")), None, 10)
                .expect("prefix list succeeds")
                .document_ids(),
            &[doc_id("alpha:1")]
        );
        assert_eq!(
            json.list(None, Some(&doc_id("alpha:1")), 10)
                .expect("cursor list succeeds")
                .document_ids(),
            &[doc_id("beta:1")]
        );
        assert_eq!(json.count(None).expect("count succeeds"), 2);
        assert_eq!(
            json.count(Some(&doc_id("alpha:"))).expect("prefix count"),
            1
        );

        let empty_sample = json.sample(None, 0).expect("empty sample succeeds");
        assert_eq!(empty_sample.total_count(), 2);
        assert!(empty_sample.rows().is_empty());

        let all = json.sample(None, 10).expect("all sample succeeds");
        assert_eq!(all.total_count(), 2);
        assert_eq!(all.rows().len(), 2);

        let one = json
            .sample(Some(&doc_id("alpha:")), 1)
            .expect("sample succeeds");
        assert_eq!(one.total_count(), 1);
        assert_eq!(one.rows()[0].document_id(), &doc_id("alpha:1"));
    });
}

#[test]
fn json_null_documents_remain_live_for_list_count_and_timestamp_reads() {
    run_database_modes(|database| {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");

        let null_outcome = json
            .create(doc_id("doc-null"), json_value(json!(null)))
            .expect("create null document succeeds");
        json.create(doc_id("doc-object"), json_value(json!({"name": "Ada"})))
            .expect("create object document succeeds");

        assert_eq!(
            json.list(Some(&doc_id("doc-")), None, 10)
                .expect("prefix list succeeds")
                .document_ids(),
            &[doc_id("doc-null"), doc_id("doc-object")]
        );
        assert_eq!(
            json.count(Some(&doc_id("doc-")))
                .expect("prefix count succeeds"),
            2
        );
        assert_eq!(
            json.get(&doc_id("doc-null"), &root())
                .expect("null document get succeeds")
                .expect("null document is present")
                .as_inner(),
            &json!(null)
        );
        assert_eq!(
            json.list_at(
                Some(&doc_id("doc-")),
                None,
                10,
                null_outcome.commit().timestamp()
            )
            .expect("historical prefix list succeeds")
            .document_ids(),
            &[doc_id("doc-null")]
        );
        assert!(json
            .get(&doc_id("doc-missing"), &root())
            .expect("missing get succeeds")
            .is_none());
    });
}

#[test]
fn json_history_version_timestamp_and_list_at_are_stable() {
    let database = open_cache_database().expect("cache open succeeds");
    let doc = doc_id("history:1");
    let other = doc_id("history:2");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");

    // Reads before the retained window are out-of-range diagnostics, not
    // ordinary absence (F8): a version below the retained floor and a timestamp
    // before retained history are distinguishable from "never existed".
    assert_eq!(
        json.get_at_version(&doc, &root(), CommitVersion::ZERO)
            .expect_err("pre-history version read is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );
    assert_eq!(
        json.get_at(&doc, &root(), Timestamp::EPOCH)
            .expect_err("pre-history timestamp read is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );

    let created = json
        .create(
            doc.clone(),
            json_value(json!({"count": 1, "tags": ["a", "b"]})),
        )
        .expect("create succeeds");
    let other_created = json
        .create(other.clone(), json_value(json!({"count": 9})))
        .expect("other create succeeds");
    let updated = json
        .set(doc.clone(), &path("count"), json_value(json!(2)))
        .expect("update succeeds");
    let path_deleted = json
        .delete(doc.clone(), &path("tags[0]"))
        .expect("path delete succeeds")
        .commit()
        .expect("path delete committed");
    let root_deleted = json
        .delete_document(doc.clone())
        .expect("root delete succeeds")
        .commit()
        .expect("root delete committed");

    assert_json_version_timestamp_reads(
        &mut json,
        &doc,
        JsonHistoryReadFacts {
            created_version: created.commit().version(),
            updated_version: updated.commit().version(),
            path_deleted_version: path_deleted.version(),
            root_deleted_version: root_deleted.version(),
            created_timestamp: created.commit().timestamp(),
            updated_timestamp: updated.commit().timestamp(),
            root_deleted_timestamp: root_deleted.timestamp(),
        },
    );
    assert_json_list_at_reads(
        &mut json,
        &doc,
        &other,
        created.commit().timestamp(),
        other_created.commit().timestamp(),
        root_deleted.timestamp(),
    );
    assert_json_history_rows(&mut json, &doc);
}

#[derive(Clone, Copy)]
struct JsonHistoryReadFacts {
    created_version: CommitVersion,
    updated_version: CommitVersion,
    path_deleted_version: CommitVersion,
    root_deleted_version: CommitVersion,
    created_timestamp: Timestamp,
    updated_timestamp: Timestamp,
    root_deleted_timestamp: Timestamp,
}

fn assert_json_version_timestamp_reads(
    json: &mut JsonService<'_>,
    doc: &JsonDocumentId,
    facts: JsonHistoryReadFacts,
) {
    assert_eq!(
        json.get_at_version(doc, &path("count"), facts.created_version)
            .expect("version read succeeds")
            .expect("created value")
            .as_inner(),
        &json!(1)
    );
    assert_eq!(
        json.get_at_version(doc, &path("count"), facts.updated_version)
            .expect("version read succeeds")
            .expect("updated value")
            .as_inner(),
        &json!(2)
    );
    assert_eq!(
        json.get_at_version(doc, &path("tags[0]"), facts.path_deleted_version)
            .expect("version read succeeds")
            .expect("shifted array value")
            .as_inner(),
        &json!("b")
    );
    assert!(json
        .get_at_version(doc, &root(), facts.root_deleted_version)
        .expect("root delete read succeeds")
        .is_none());
    assert_eq!(
        json.get_at(doc, &path("count"), facts.updated_timestamp)
            .expect("timestamp read succeeds")
            .expect("updated value")
            .as_inner(),
        &json!(2)
    );
    assert!(json
        .get_at(doc, &root(), facts.root_deleted_timestamp)
        .expect("root delete timestamp read succeeds")
        .is_none());
    assert_json_versioned_at_reads(json, doc, facts);
}

/// #3334: a read as of a timestamp answers with the commit that was visible
/// then and says which one that was, so an as-of read carries the same record
/// a live read does — the contract `KvService::get_versioned_at` already held.
fn assert_json_versioned_at_reads(
    json: &mut JsonService<'_>,
    doc: &JsonDocumentId,
    facts: JsonHistoryReadFacts,
) {
    let updated = json
        .get_versioned_at(doc, &path("count"), facts.updated_timestamp)
        .expect("versioned timestamp read succeeds")
        .expect("updated value");
    assert_eq!(updated.value().as_inner(), &json!(2));
    assert_eq!(updated.version(), facts.updated_version);
    assert_eq!(updated.timestamp(), facts.updated_timestamp);
    // The earlier commit is still reachable, and reports its own metadata
    // rather than the instant that was asked for.
    let created = json
        .get_versioned_at(doc, &path("count"), facts.created_timestamp)
        .expect("versioned timestamp read succeeds")
        .expect("created value");
    assert_eq!(created.value().as_inner(), &json!(1));
    assert_eq!(created.version(), facts.created_version);
    assert_ne!(created.version(), updated.version());
    assert!(json
        .get_versioned_at(doc, &root(), facts.root_deleted_timestamp)
        .expect("versioned root delete read succeeds")
        .is_none());
}

fn assert_json_list_at_reads(
    json: &mut JsonService<'_>,
    doc: &JsonDocumentId,
    other: &JsonDocumentId,
    created_timestamp: Timestamp,
    other_created_timestamp: Timestamp,
    root_deleted_timestamp: Timestamp,
) {
    let after_doc_create = json
        .list_at(None, None, 10, created_timestamp)
        .expect("list at create succeeds");
    assert_eq!(after_doc_create.document_ids(), std::slice::from_ref(doc));

    let after_other_create = json
        .list_at(None, Some(doc), 10, other_created_timestamp)
        .expect("list at other create succeeds");
    assert_eq!(
        after_other_create.document_ids(),
        std::slice::from_ref(other)
    );

    let after_delete = json
        .list_at(Some(&doc_id("history:")), None, 10, root_deleted_timestamp)
        .expect("list at delete succeeds");
    assert_eq!(after_delete.document_ids(), std::slice::from_ref(other));
}

fn assert_json_history_rows(json: &mut JsonService<'_>, doc: &JsonDocumentId) {
    let history = json
        .get_versions(doc)
        .expect("history succeeds")
        .expect("history exists");
    assert_eq!(history.rows().len(), 4);
    assert!(history.rows()[0].is_tombstone());
    assert_eq!(history.rows()[1].document_version(), Some(3));
    assert_eq!(history.rows()[2].document_version(), Some(2));
    assert_eq!(history.rows()[3].document_version(), Some(1));
    assert!(json
        .get_versions(&doc_id("missing"))
        .expect("missing history succeeds")
        .is_none());
}

#[test]
fn json_index_metadata_covers_supported_kinds_and_errors() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    json.create(
        doc_id("indexed"),
        json_value(json!({"age": 42, "name": "Alice", "bio": "Hello world"})),
    )
    .expect("create succeeds");

    let numeric = json
        .create_index(
            JsonIndexName::new("by_age").expect("valid index"),
            path("age"),
            JsonIndexType::Numeric,
        )
        .expect("numeric index succeeds");
    let tag = json
        .create_index(
            JsonIndexName::new("by_name").expect("valid index"),
            path("name"),
            JsonIndexType::Tag,
        )
        .expect("tag index succeeds");
    let text = json
        .create_index(
            JsonIndexName::new("by_bio").expect("valid index"),
            path("bio"),
            JsonIndexType::Text,
        )
        .expect("text index succeeds");
    let indexes = json.list_indexes().expect("list indexes succeeds");
    assert_eq!(indexes, vec![numeric.clone(), text.clone(), tag.clone()]);
    assert!(indexes.iter().all(|index| index.created_version() > 0));
    assert!(indexes.iter().all(|index| index.created_timestamp() > 0));

    let duplicate = json
        .create_index(
            JsonIndexName::new("by_name").expect("valid index"),
            path("name"),
            JsonIndexType::Tag,
        )
        .expect_err("duplicate index rejected");
    assert_eq!(duplicate.class(), EngineErrorClass::Conflict);

    let invalid_name = JsonIndexName::new("_reserved").expect_err("reserved rejected");
    assert_eq!(invalid_name.class(), EngineErrorClass::InvalidInput);

    let invalid_path = "bad..path".parse::<JsonPath>().expect_err("invalid path");
    assert_eq!(invalid_path.class(), EngineErrorClass::InvalidInput);

    assert!(json
        .drop_index(&JsonIndexName::new("by_name").expect("valid index"))
        .expect("drop succeeds"));
    assert!(!json
        .drop_index(&JsonIndexName::new("missing").expect("valid index"))
        .expect("missing drop succeeds"));
}

#[test]
fn json_durable_reopen_preserves_full_surface() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let keep = doc_id("keep");
    let deleted = doc_id("deleted");
    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        {
            let mut default_json = database
                .json(branch("default"), space("default"))
                .expect("JSON service opens");
            default_json
                .create(
                    keep.clone(),
                    json_value(json!({"name": "keep", "visits": 1})),
                )
                .expect("keep create succeeds");
            default_json
                .set(keep.clone(), &path("visits"), json_value(json!(2)))
                .expect("keep update succeeds");
            default_json
                .create(deleted.clone(), json_value(json!({"name": "deleted"})))
                .expect("deleted create succeeds");
            default_json
                .create_index(
                    JsonIndexName::new("by_name").expect("valid index"),
                    path("name"),
                    JsonIndexType::Tag,
                )
                .expect("index succeeds");
            default_json
                .delete_document(deleted.clone())
                .expect("delete succeeds");
        }
        database
            .json(branch("default"), space("other"))
            .expect("other JSON opens")
            .create(keep.clone(), json_value(json!({"space": "other"})))
            .expect("other create succeeds");
        database.close().expect("close succeeds");
    }

    let reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut default_json = reopened
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    assert_eq!(
        default_json
            .get(&keep, &root())
            .expect("read succeeds")
            .expect("keep exists")
            .as_inner(),
        &json!({"name": "keep", "visits": 2})
    );
    assert!(default_json
        .get(&deleted, &root())
        .expect("deleted read")
        .is_none());
    assert_eq!(
        default_json
            .list(None, None, 10)
            .expect("list succeeds")
            .document_ids(),
        std::slice::from_ref(&keep)
    );
    assert_eq!(default_json.count(None).expect("count succeeds"), 1);
    assert_eq!(
        default_json
            .sample(None, 10)
            .expect("sample succeeds")
            .rows()
            .len(),
        1
    );
    assert_eq!(
        default_json
            .get_versions(&keep)
            .expect("history succeeds")
            .expect("history exists")
            .rows()
            .len(),
        2
    );
    assert_eq!(default_json.list_indexes().expect("indexes").len(), 1);
    drop(default_json);

    let mut other_json = reopened
        .json(branch("default"), space("other"))
        .expect("other JSON opens");
    assert_eq!(
        other_json
            .get(&keep, &path("space"))
            .expect("read succeeds")
            .expect("other value exists")
            .as_inner(),
        &json!("other")
    );
}

#[test]
fn json_error_surfaces_are_engine_owned() {
    let database = open_cache_database().expect("cache open succeeds");
    let missing_branch = database
        .json(branch("missing"), space("default"))
        .map(|_| ())
        .expect_err("missing branch rejected at service construction");
    assert_eq!(missing_branch.class(), EngineErrorClass::NotFound);
    assert_eq!(missing_branch.code(), "not_found.engine.branch");
    assert_no_storage_detail(&missing_branch);

    let invalid_space = ProductSpace::new("_system_").expect_err("reserved space rejected");
    assert_eq!(invalid_space.class(), EngineErrorClass::InvalidInput);
    assert_no_storage_detail(&invalid_space);

    let invalid_value = JsonValue::new(Value::Array(vec![Value::Null; 1_000_001]))
        .expect_err("oversized JSON array rejected");
    assert_eq!(invalid_value.class(), EngineErrorClass::InvalidInput);
    assert_no_storage_detail(&invalid_value);

    let mut closed = open_cache_database().expect("cache open succeeds");
    closed.close().expect("close succeeds");
    let Err(error) = closed.json(branch("default"), space("default")) else {
        panic!("closed database accepted JSON service");
    };
    assert_eq!(error.class(), EngineErrorClass::ClosedRuntime);
    assert_no_storage_detail(&error);
}

#[test]
#[cfg(feature = "testkit")]
fn json_index_entries_track_document_mutations() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let by_name = JsonIndexName::new("by_name").expect("valid index");
    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        json.create(doc_id("indexed:1"), json_value(json!({"name": "alice"})))
            .expect("create indexed document");
        json.create(doc_id("indexed:2"), json_value(json!({"name": "bob"})))
            .expect("create indexed document");
        json.create(doc_id("indexed:3"), json_value(json!({"age": 42})))
            .expect("create unindexed document");
        json.create_index(by_name.clone(), path("name"), JsonIndexType::Tag)
            .expect("index create succeeds");
    }
    assert_eq!(
        database
            .json_index_entry_count_for_test(&branch("default"), &space("default"), &by_name)
            .expect("entry count succeeds"),
        2
    );

    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        json.set(
            doc_id("indexed:3"),
            &path("name"),
            json_value(json!("carol")),
        )
        .expect("set adds entry");
    }
    assert_eq!(
        database
            .json_index_entry_count_for_test(&branch("default"), &space("default"), &by_name)
            .expect("entry count succeeds"),
        3
    );

    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        json.delete(doc_id("indexed:1"), &path("name"))
            .expect("path delete removes entry");
    }
    assert_eq!(
        database
            .json_index_entry_count_for_test(&branch("default"), &space("default"), &by_name)
            .expect("entry count succeeds"),
        2
    );

    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        json.delete_document(doc_id("indexed:2"))
            .expect("root delete removes entry");
    }
    assert_eq!(
        database
            .json_index_entry_count_for_test(&branch("default"), &space("default"), &by_name)
            .expect("entry count succeeds"),
        1
    );

    {
        let mut json = database
            .json(branch("default"), space("default"))
            .expect("JSON service opens");
        json.drop_index(&by_name).expect("drop succeeds");
    }
    assert_eq!(
        database
            .json_index_entry_count_for_test(&branch("default"), &space("default"), &by_name)
            .expect("entry count succeeds"),
        0
    );
}

fn exercise_json_contract(database: &mut Database) {
    let doc = doc_id("user:1");
    let other = doc_id("user:2");
    let archived = doc_id("archive:1");

    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    exercise_json_single_document_contract(&mut json, &doc);
    exercise_json_batch_list_index_contract(&mut json, &doc, &other, &archived);
}

fn exercise_json_single_document_contract(json: &mut JsonService<'_>, doc: &JsonDocumentId) {
    let created = json
        .create(
            doc.clone(),
            json_value(json!({"name": "alice", "count": 1, "tags": ["a", "b"]})),
        )
        .expect("create succeeds");
    assert_eq!(created.document_version(), 1);

    let duplicate = json
        .create(doc.clone(), json_value(json!({})))
        .expect_err("duplicate create rejected");
    assert_eq!(duplicate.class(), EngineErrorClass::Conflict);

    let name = json
        .get(doc, &path("name"))
        .expect("path read succeeds")
        .expect("name exists");
    assert_eq!(name.as_inner(), &json!("alice"));

    let updated = json
        .set(doc.clone(), &path("count"), json_value(json!(2)))
        .expect("path set succeeds");
    assert_eq!(updated.document_version(), 2);

    let versioned = json
        .get_versioned(doc, &path("count"))
        .expect("versioned read succeeds")
        .expect("value exists");
    assert_eq!(versioned.value().as_inner(), &json!(2));
    assert_eq!(versioned.document_version(), 2);

    let history = json
        .get_versions(doc)
        .expect("history succeeds")
        .expect("history exists");
    assert_eq!(history.rows().len(), 2);
    assert_eq!(history.rows()[0].document_version(), Some(2));

    let deleted_path = json
        .delete(doc.clone(), &path("tags[0]"))
        .expect("path delete succeeds");
    assert!(deleted_path.deleted());
    assert_eq!(
        json.get(doc, &path("tags[0]"))
            .expect("array read succeeds")
            .expect("first array element")
            .as_inner(),
        &json!("b")
    );

    let missing_delete = json
        .delete(doc_id("missing"), &root())
        .expect("missing root delete succeeds");
    assert!(!missing_delete.deleted());
}

fn exercise_json_batch_list_index_contract(
    json: &mut JsonService<'_>,
    doc: &JsonDocumentId,
    other: &JsonDocumentId,
    archived: &JsonDocumentId,
) {
    let batch = json
        .batch_set_or_create([
            JsonSetEntry::new(other.clone(), root(), json_value(json!({"name": "bob"}))),
            JsonSetEntry::new(archived.clone(), root(), json_value(json!({"name": "old"}))),
        ])
        .expect("batch set succeeds");
    assert_eq!(batch.results().len(), 2);

    let batch_get = json
        .batch_get(&[
            JsonGetEntry::new(other.clone(), path("name")),
            JsonGetEntry::new(doc_id("missing"), root()),
        ])
        .expect("batch get succeeds");
    assert_eq!(
        batch_get[0].as_ref().expect("value").value().as_inner(),
        &json!("bob")
    );
    assert!(batch_get[1].is_none());

    let page = json
        .list(Some(&doc_id("user:")), None, 1)
        .expect("list succeeds");
    assert_eq!(page.document_ids(), std::slice::from_ref(doc));
    assert!(page.has_more());
    assert_eq!(page.cursor(), Some(doc));

    assert_eq!(
        json.count(Some(&doc_id("user:"))).expect("count succeeds"),
        2
    );
    let sample = json
        .sample(Some(&doc_id("user:")), 1)
        .expect("sample succeeds");
    assert_eq!(sample.total_count(), 2);
    assert_eq!(sample.rows().len(), 1);

    let index = json
        .create_index(
            JsonIndexName::new("by_name").expect("valid index"),
            path("name"),
            JsonIndexType::Tag,
        )
        .expect("index create succeeds");
    assert_eq!(index.name().as_str(), "by_name");
    assert_eq!(json.list_indexes().expect("list indexes").len(), 1);
    assert_eq!(json.count(None).expect("count excludes index rows"), 3);
    assert!(json
        .drop_index(&JsonIndexName::new("by_name").expect("valid index"))
        .expect("drop succeeds"));
    assert!(json.list_indexes().expect("list indexes").is_empty());

    let deleted = json
        .batch_delete([other.clone(), doc_id("missing")])
        .expect("batch delete succeeds");
    assert_eq!(deleted.deleted(), &[true, false]);
}

#[test]
fn json_batch_delete_entries_removes_paths_positionally() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    json.create(
        doc_id("doc"),
        json_value(json!({"a": 1, "b": {"c": 2, "d": 3}, "tags": ["x", "y"]})),
    )
    .expect("create doc");
    json.create(doc_id("other"), json_value(json!({"k": "v"})))
        .expect("create other");

    let outcome = json
        .batch_delete_entries([
            JsonGetEntry::new(doc_id("doc"), path("a")),
            JsonGetEntry::new(doc_id("doc"), path("b.c")),
            JsonGetEntry::new(doc_id("missing"), path("x")),
            JsonGetEntry::new(doc_id("doc"), path("tags[0]")),
            JsonGetEntry::new(doc_id("other"), root()),
        ])
        .expect("batch delete succeeds");

    // Positional flags: path delete, path delete, missing document, array
    // element delete, and a root-path entry that deletes the whole document.
    assert_eq!(outcome.deleted(), &[true, true, false, true, true]);
    assert!(outcome.commit().is_some());

    assert!(json
        .get(&doc_id("doc"), &path("a"))
        .expect("read a")
        .is_none());
    assert!(json
        .get(&doc_id("doc"), &path("b.c"))
        .expect("read b.c")
        .is_none());
    assert_eq!(
        json.get(&doc_id("doc"), &path("b.d"))
            .expect("read b.d")
            .expect("b.d present")
            .as_inner(),
        &json!(3)
    );
    assert_eq!(
        json.get(&doc_id("doc"), &path("tags[0]"))
            .expect("read tags[0]")
            .expect("tags[0] present")
            .as_inner(),
        &json!("y")
    );
    // The root-path entry deleted the whole `other` document.
    assert!(!json.exists(&doc_id("other")).expect("exists other"));
}

#[test]
fn json_batch_delete_entries_empty_is_a_noop() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    let outcome = json
        .batch_delete_entries(Vec::new())
        .expect("empty batch succeeds");
    assert!(outcome.deleted().is_empty());
    assert!(outcome.commit().is_none());
}

#[test]
fn json_set_path_type_and_not_found_errors_are_engine_owned() {
    let database = open_cache_database().expect("cache open succeeds");
    let mut json = database
        .json(branch("default"), space("default"))
        .expect("JSON service opens");
    json.create(doc_id("doc"), json_value(json!({"a": 5, "tags": ["x"]})))
        .expect("create doc");

    // Descending a key into a scalar value is a path type mismatch.
    let type_error = json
        .set_or_create(doc_id("doc"), &path("a.b"), json_value(json!(1)))
        .expect_err("type mismatch rejected");
    assert_eq!(type_error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(type_error.code(), "invalid_argument.engine.json_path_type");

    // An out-of-bounds array index is not found — array slots are not created.
    let oob_error = json
        .set_or_create(doc_id("doc"), &path("tags[5]"), json_value(json!(1)))
        .expect_err("out-of-bounds index rejected");
    assert_eq!(
        oob_error.code(),
        "invalid_argument.engine.json_path_not_found"
    );

    // `set` on a missing document reports a missing document, not a path error.
    let no_document = json
        .set(doc_id("ghost"), &root(), json_value(json!(1)))
        .expect_err("missing document rejected");
    assert_eq!(no_document.class(), EngineErrorClass::NotFound);
    assert_eq!(no_document.code(), "not_found.engine.json_document");

    // The set/set_or_create distinction is document-level only: `set` on an
    // existing document still auto-creates missing path segments.
    json.set(doc_id("doc"), &path("nested.key"), json_value(json!(7)))
        .expect("set creates missing path segments on an existing document");
    assert_eq!(
        json.get(&doc_id("doc"), &path("nested.key"))
            .expect("read nested.key")
            .expect("nested.key present")
            .as_inner(),
        &json!(7)
    );
}

fn run_database_modes(exercise: fn(&mut Database)) {
    let mut cache = open_cache_database().expect("cache open succeeds");
    exercise(&mut cache);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open succeeds");
    exercise(&mut durable);
}

fn doc_id(value: &str) -> JsonDocumentId {
    JsonDocumentId::new(value).expect("valid document id")
}

fn path(value: &str) -> JsonPath {
    value.parse().expect("valid JSON path")
}

fn root() -> JsonPath {
    JsonPath::root()
}

fn json_value(value: serde_json::Value) -> JsonValue {
    JsonValue::new(value).expect("valid JSON value")
}

fn assert_no_storage_detail(error: &strata_engine::EngineError) {
    let text = error.to_string();
    for forbidden in [
        "StorageRuntime",
        "CommitBatch",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "BranchRequest",
        "storage_api",
    ] {
        assert!(
            !text.contains(forbidden),
            "engine error exposed storage detail: {text}"
        );
    }
}
