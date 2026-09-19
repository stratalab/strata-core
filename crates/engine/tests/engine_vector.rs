//! Vector conformance tests.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]

mod common;

#[cfg(feature = "testkit")]
use std::collections::BTreeSet;

use serde_json::{json, Map, Value};
#[cfg(feature = "testkit")]
use sha2::{Digest, Sha256};
#[cfg(feature = "testkit")]
use strata_core::BranchId;
#[cfg(feature = "testkit")]
use strata_engine::testkit::{VectorIndexDiagnostics, VectorIndexPolicy};
use strata_engine::{
    Database, EngineErrorClass, VectorCollectionName, VectorConfig, VectorDistanceMetric,
    VectorEmbedding, VectorFilter, VectorFilterCondition, VectorKey, VectorMetadata,
    VectorMetadataPatch, VectorScalar, VectorUpsertEntry,
};

use common::{branch, open_cache_database, open_durable_database, space};

#[path = "engine_vector/core.rs"]
mod vector_core;

#[cfg(feature = "testkit")]
#[path = "engine_vector/index_artifacts.rs"]
mod vector_index_artifacts;
#[cfg(feature = "testkit")]
#[path = "engine_vector/index_fixtures.rs"]
mod vector_index_fixtures;
#[cfg(feature = "testkit")]
#[path = "engine_vector/index_hnsw.rs"]
mod vector_index_hnsw;
#[cfg(feature = "testkit")]
#[path = "engine_vector/index_manifest.rs"]
mod vector_index_manifest;
#[cfg(feature = "testkit")]
#[path = "engine_vector/index_planner.rs"]
mod vector_index_planner;
#[cfg(feature = "testkit")]
#[path = "engine_vector/index_policy.rs"]
mod vector_index_policy;
#[cfg(feature = "testkit")]
#[path = "engine_vector/index_sources.rs"]
mod vector_index_sources;

fn exercise_vector_collection_lifecycle(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = create_collection_lifecycle_fixture(&mut vectors);
    assert_collection_count_updates(&mut vectors, &docs);
    assert_collection_delete_hides_rows(&mut vectors, &docs);
}

fn create_collection_lifecycle_fixture(
    vectors: &mut strata_engine::VectorService<'_>,
) -> VectorCollectionName {
    assert!(vectors
        .list_collections()
        .expect("empty collection list succeeds")
        .is_empty());
    let docs = collection("docs");
    let alpha = collection("alpha");
    let zeta = collection("zeta");
    let info = vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    assert_eq!(info.name(), &docs);
    assert_eq!(info.count(), 0);
    assert_eq!(info.config().dimension(), 2);
    assert!(info.created_version().as_u64() > 0);
    assert!(vectors
        .create_collection(docs.clone(), config(3, VectorDistanceMetric::DotProduct))
        .expect_err("duplicate collection rejected")
        .code()
        .contains("vector_collection"));

    vectors
        .create_collection(alpha.clone(), config(2, VectorDistanceMetric::Euclidean))
        .expect("alpha create succeeds");
    vectors
        .create_collection(zeta.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("zeta create succeeds");
    assert_eq!(
        vectors
            .list_collections()
            .expect("collection list succeeds")
            .iter()
            .map(|info| info.name().as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "docs", "zeta"]
    );
    docs
}

fn assert_collection_count_updates(
    vectors: &mut strata_engine::VectorService<'_>,
    docs: &VectorCollectionName,
) {
    vectors
        .batch_upsert(
            docs,
            &[
                upsert("a", [1.0, 0.0], json!({})),
                upsert("b", [0.0, 1.0], json!({})),
            ],
        )
        .expect("batch upsert succeeds");
    assert_eq!(vectors.count(docs).expect("count succeeds"), 2);
    vectors
        .upsert(
            docs.clone(),
            vector_key("a"),
            embedding([0.5, 0.5]),
            Some(metadata(json!({}))),
        )
        .expect("overwrite succeeds");
    assert_eq!(vectors.count(docs).expect("count succeeds"), 2);

    assert!(vectors
        .delete(docs, vector_key("a"))
        .expect("delete succeeds")
        .deleted());
    assert!(!vectors
        .delete(docs, vector_key("a"))
        .expect("second delete succeeds")
        .deleted());
    assert_eq!(vectors.count(docs).expect("count succeeds"), 1);
    assert_eq!(
        key_strings(
            vectors
                .list_keys(docs, None, None, 10)
                .expect("key list succeeds")
                .keys()
        ),
        vec!["b"]
    );
}

fn assert_collection_delete_hides_rows(
    vectors: &mut strata_engine::VectorService<'_>,
    docs: &VectorCollectionName,
) {
    assert!(vectors
        .delete_collection(docs)
        .expect("collection delete succeeds"));
    assert!(vectors
        .collection_info(docs)
        .expect("info succeeds")
        .is_none());
    assert_eq!(
        vectors
            .count(docs)
            .expect_err("deleted collection count rejected")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .upsert(docs.clone(), vector_key("c"), embedding([1.0, 0.0]), None)
            .expect_err("upsert into deleted collection rejected")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .list_collections()
            .expect("collection list succeeds")
            .iter()
            .map(|info| info.name().as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "zeta"]
    );
}

fn exercise_vector_metadata_patch_contract(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("metadata");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("empty"),
            embedding([1.0, 0.0]),
            None,
        )
        .expect("empty metadata upsert succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("object"),
            embedding([0.0, 1.0]),
            Some(metadata(json!({"kind": "doc", "rank": 1}))),
        )
        .expect("object metadata upsert succeeds");

    let empty_patch = vectors
        .update_metadata(&docs, vector_key("empty"), &patch(json!({"added": true})))
        .expect("metadata patch succeeds");
    assert!(empty_patch.updated());
    assert_eq!(empty_patch.vector_revision(), Some(2));
    assert!(empty_patch.commit().is_some());
    assert_eq!(
        vectors
            .get(&docs, &vector_key("empty"))
            .expect("read succeeds")
            .expect("entry exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"added": true})
    );

    vectors
        .update_metadata(
            &docs,
            vector_key("object"),
            &patch(json!({"rank": 2, "nullable": null})),
        )
        .expect("object patch succeeds");
    let patched = vectors
        .get(&docs, &vector_key("object"))
        .expect("read succeeds")
        .expect("entry exists");
    assert_eq!(patched.embedding().as_slice(), &[0.0, 1.0]);
    assert_eq!(
        patched.metadata().expect("metadata").as_inner(),
        &json!({"kind": "doc", "rank": 2, "nullable": null})
    );

    let missing = vectors
        .update_metadata(&docs, vector_key("missing"), &patch(json!({"x": 1})))
        .expect("missing metadata patch succeeds");
    assert!(!missing.updated());
    assert_eq!(missing.vector_revision(), None);
    assert_eq!(missing.commit(), None);
    assert_eq!(
        vectors
            .update_metadata(
                &collection("missing"),
                vector_key("missing"),
                &patch(json!({"x": 1}))
            )
            .expect_err("missing collection rejected")
            .code(),
        "not_found.engine.vector_collection"
    );
}

fn exercise_vector_batch_contracts(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let collection = collection("batch");
    vectors
        .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");

    let empty_upsert = vectors
        .batch_upsert(&collection, &[])
        .expect("empty batch upsert succeeds");
    assert!(empty_upsert.vector_revisions().is_empty());
    assert_eq!(empty_upsert.commit(), None);
    assert!(vectors
        .batch_get(&collection, &[])
        .expect("empty batch get succeeds")
        .entries()
        .is_empty());
    let empty_delete = vectors
        .batch_delete(&collection, &[])
        .expect("empty batch delete succeeds");
    assert!(empty_delete.deleted().is_empty());
    assert_eq!(empty_delete.commit(), None);

    let upserted = vectors
        .batch_upsert(
            &collection,
            &[
                upsert("a", [1.0, 0.0], json!({"round": 1})),
                upsert("b", [0.0, 1.0], json!({"round": 1})),
                upsert("a", [0.5, 0.5], json!({"round": 2})),
            ],
        )
        .expect("batch upsert succeeds");
    assert_eq!(upserted.vector_revisions(), &[1, 1, 2]);
    assert!(upserted.commit().is_some());
    assert_eq!(vectors.count(&collection).expect("count succeeds"), 2);
    assert_eq!(
        vectors
            .get(&collection, &vector_key("a"))
            .expect("read succeeds")
            .expect("entry exists")
            .embedding()
            .as_slice(),
        &[0.5, 0.5]
    );

    let batch_get = vectors
        .batch_get(
            &collection,
            &[vector_key("a"), vector_key("missing"), vector_key("a")],
        )
        .expect("batch get succeeds");
    assert!(batch_get.entries()[0].is_some());
    assert!(batch_get.entries()[1].is_none());
    assert!(batch_get.entries()[2].is_some());
    assert_eq!(
        batch_get.entries()[0]
            .as_ref()
            .expect("entry")
            .vector_revision(),
        2
    );

    let error = vectors
        .batch_upsert(
            &collection,
            &[
                upsert("c", [1.0, 0.0], json!({})),
                upsert("bad", [1.0, 0.0, 0.0], json!({})),
            ],
        )
        .expect_err("dimension mismatch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.vector_dimension");
    assert!(!vectors
        .exists(&collection, &vector_key("c"))
        .expect("exists succeeds"));

    let deleted = vectors
        .batch_delete(
            &collection,
            &[vector_key("a"), vector_key("a"), vector_key("missing")],
        )
        .expect("batch delete succeeds");
    assert_eq!(deleted.deleted(), &[true, false, false]);
    assert!(deleted.commit().is_some());
    assert!(vectors
        .get(&collection, &vector_key("a"))
        .expect("read succeeds")
        .is_none());
    assert_eq!(
        vectors
            .query(&collection, &embedding([0.5, 0.5]), 10, None)
            .expect("search succeeds")
            .matches()
            .iter()
            .map(|row| row.entry().key().as_str())
            .collect::<Vec<_>>(),
        vec!["b"]
    );
}

fn exercise_vector_bulk_delete_contracts(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("docs");
    let other = collection("other");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("docs create succeeds");
    vectors
        .create_collection(other.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("other create succeeds");
    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("a", [1.0, 0.0], json!({"kind": "doc", "active": true})),
                upsert("b", [0.0, 1.0], json!({"kind": "doc", "active": false})),
                upsert("c", [0.5, 0.5], json!({"kind": "note", "active": true})),
                VectorUpsertEntry::new(vector_key("d"), embedding([0.25, 0.75]), None),
            ],
        )
        .expect("docs batch upsert succeeds");
    vectors
        .upsert(
            other.clone(),
            vector_key("o"),
            embedding([1.0, 0.0]),
            Some(metadata(json!({"kind": "doc"}))),
        )
        .expect("other upsert succeeds");

    let deleted = vectors
        .delete_by_filter(&docs, &filter_and([("kind", "doc"), ("active", "true")]))
        .expect("filtered delete succeeds");
    assert_eq!(deleted.deleted_count(), 0);
    assert_eq!(deleted.commit(), None);
    let deleted = vectors
        .delete_by_filter(
            &docs,
            &filter_eq("kind", "doc").eq("active", true).expect("filter"),
        )
        .expect("filtered delete succeeds");
    assert_eq!(deleted.deleted_count(), 1);
    assert!(deleted.commit().is_some());
    assert!(vectors
        .get(&docs, &vector_key("a"))
        .expect("read succeeds")
        .is_none());
    assert_eq!(vectors.count(&docs).expect("count succeeds"), 3);
    assert_eq!(
        key_strings(
            vectors
                .list_keys(&docs, None, None, 10)
                .expect("key list succeeds")
                .keys()
        ),
        vec!["b", "c", "d"]
    );
    assert!(vectors
        .history(&docs, &vector_key("a"))
        .expect("history succeeds")
        .expect("history exists")
        .rows()[0]
        .is_tombstone());
    let rerun = vectors
        .delete_by_filter(
            &docs,
            &filter_eq("kind", "doc").eq("active", true).expect("filter"),
        )
        .expect("filtered delete rerun succeeds");
    assert_eq!(rerun.deleted_count(), 0);
    assert_eq!(rerun.commit(), None);

    let all_deleted = vectors.delete_all(&docs).expect("delete all succeeds");
    assert_eq!(all_deleted.deleted_count(), 3);
    assert!(all_deleted.commit().is_some());
    assert_eq!(vectors.count(&docs).expect("count succeeds"), 0);
    assert!(vectors
        .collection_info(&docs)
        .expect("info succeeds")
        .is_some());
    assert!(vectors
        .list_keys(&docs, None, None, 10)
        .expect("key list succeeds")
        .keys()
        .is_empty());
    assert!(vectors
        .query(&docs, &embedding([1.0, 0.0]), 10, None)
        .expect("search succeeds")
        .matches()
        .is_empty());
    let idempotent = vectors
        .delete_all(&docs)
        .expect("delete all rerun succeeds");
    assert_eq!(idempotent.deleted_count(), 0);
    assert_eq!(idempotent.commit(), None);
    assert_eq!(vectors.count(&other).expect("other count succeeds"), 1);
}

fn exercise_vector_exact_search_metrics(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    assert_cosine_search_fixture(&mut vectors);
    assert_euclidean_search_fixture(&mut vectors);
    assert_dot_product_search_fixture(&mut vectors);
}

fn assert_cosine_search_fixture(vectors: &mut strata_engine::VectorService<'_>) {
    let cosine = collection("cosine");
    vectors
        .create_collection(cosine.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("cosine create succeeds");
    vectors
        .batch_upsert(
            &cosine,
            &[
                upsert("same", [1.0, 0.0], json!({})),
                upsert("orthogonal", [0.0, 1.0], json!({})),
                upsert("zero", [0.0, 0.0], json!({})),
                upsert("opposite", [-1.0, 0.0], json!({})),
            ],
        )
        .expect("cosine upsert succeeds");
    let cosine_scores = score_pairs(
        vectors
            .query(&cosine, &embedding([1.0, 0.0]), 10, None)
            .expect("cosine query succeeds")
            .matches(),
    );
    assert_eq!(
        cosine_scores
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec!["same", "orthogonal", "zero", "opposite"]
    );
    assert_close(cosine_scores[0].1, 1.0);
    assert_close(cosine_scores[1].1, 0.0);
    assert_close(cosine_scores[2].1, 0.0);
    assert_close(cosine_scores[3].1, -1.0);
}

fn assert_euclidean_search_fixture(vectors: &mut strata_engine::VectorService<'_>) {
    let euclidean = collection("euclidean");
    vectors
        .create_collection(
            euclidean.clone(),
            config(2, VectorDistanceMetric::Euclidean),
        )
        .expect("euclidean create succeeds");
    vectors
        .batch_upsert(
            &euclidean,
            &[
                upsert("same", [0.0, 0.0], json!({})),
                upsert("near", [0.5, 0.0], json!({})),
                upsert("far", [2.0, 0.0], json!({})),
            ],
        )
        .expect("euclidean upsert succeeds");
    let euclidean_scores = score_pairs(
        vectors
            .query(&euclidean, &embedding([0.0, 0.0]), 10, None)
            .expect("euclidean query succeeds")
            .matches(),
    );
    assert_eq!(
        euclidean_scores
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec!["same", "near", "far"]
    );
    assert_close(euclidean_scores[0].1, 1.0);
    assert_close(euclidean_scores[1].1, 1.0 / 1.5);
    assert_close(euclidean_scores[2].1, 1.0 / 3.0);
}

fn assert_dot_product_search_fixture(vectors: &mut strata_engine::VectorService<'_>) {
    let dot = collection("dot");
    vectors
        .create_collection(dot.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("dot create succeeds");
    vectors
        .batch_upsert(
            &dot,
            &[
                upsert("high", [3.0, 0.0], json!({})),
                upsert("tie-a", [2.0, 0.0], json!({})),
                upsert("tie-b", [2.0, 0.0], json!({})),
                upsert("negative", [-1.0, 0.0], json!({})),
            ],
        )
        .expect("dot upsert succeeds");
    assert_eq!(
        vectors
            .query(&dot, &embedding([1.0, 0.0]), 10, None)
            .expect("dot query succeeds")
            .matches()
            .iter()
            .map(|row| row.entry().key().as_str())
            .collect::<Vec<_>>(),
        vec!["high", "tie-a", "tie-b", "negative"]
    );
    assert!(vectors
        .query(&dot, &embedding([1.0, 0.0]), 0, None)
        .expect("zero limit query succeeds")
        .matches()
        .is_empty());
    assert_eq!(
        vectors
            .query(&dot, &embedding([1.0, 0.0, 0.0]), 1, None)
            .expect_err("dimension mismatch rejected")
            .code(),
        "invalid_argument.engine.vector_dimension"
    );
}

/// Every input of a time-travel read is resolved at the selector, existence
/// included.
///
/// `count_at` and `list_keys_at` validated the collection at **head** and then
/// read rows at the snapshot (#3229), so the two halves of one answer came from
/// two different moments. `query_at` and `get_at` in the same file already read
/// the config at the selector, which is what made the disagreement visible.
fn exercise_vector_snapshot_existence_contract(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");

    // A commit that predates the collection under test, so its timestamp names
    // a moment when that collection did not yet exist.
    let anchor = collection("snapshot-anchor");
    vectors
        .create_collection(anchor.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("anchor collection create succeeds");
    let before = vectors
        .upsert(anchor.clone(), vector_key("x"), embedding([1.0, 0.0]), None)
        .expect("anchor upsert succeeds");
    let before = before.commit().timestamp();

    // (1) Created after T. Reading at T must refuse, not answer "empty" — an
    // empty page is indistinguishable from a collection that existed and held
    // nothing, which is a different fact.
    let late = collection("snapshot-late");
    vectors
        .create_collection(late.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("late collection create succeeds");
    vectors
        .upsert(late.clone(), vector_key("a"), embedding([1.0, 0.0]), None)
        .expect("late upsert succeeds");
    assert_eq!(
        vectors
            .count_at(&late, before)
            .expect_err("count before the collection existed is not-found")
            .code(),
        "not_found.engine.vector_collection",
        "count_at answered for a collection that did not exist at that snapshot"
    );
    assert_eq!(
        vectors
            .list_keys_at(&late, None, None, 10, before)
            .expect_err("keys before the collection existed is not-found")
            .code(),
        "not_found.engine.vector_collection",
        "list_keys_at answered for a collection that did not exist at that snapshot"
    );
    // The same reads at head still answer, so the refusal is about the snapshot
    // and not about the collection.
    assert_eq!(vectors.count(&late).expect("count at head succeeds"), 1);

    // (2) Dropped after T. Reading at T must still answer, because the
    // collection existed then and its rows are still reachable — this is the
    // half `query_at`/`get_at` already got right.
    let dropped = collection("snapshot-dropped");
    vectors
        .create_collection(dropped.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("dropped collection create succeeds");
    let alive = vectors
        .upsert(
            dropped.clone(),
            vector_key("a"),
            embedding([1.0, 0.0]),
            None,
        )
        .expect("upsert into the soon-dropped collection succeeds");
    let alive = alive.commit().timestamp();
    assert!(vectors
        .delete_collection(&dropped)
        .expect("collection delete succeeds"));

    assert_eq!(
        vectors
            .count_at(&dropped, alive)
            .expect("count at a moment the collection was alive succeeds"),
        1,
        "count_at refused a collection that existed at that snapshot"
    );
    assert_eq!(
        key_strings(
            vectors
                .list_keys_at(&dropped, None, None, 10, alive)
                .expect("keys at a moment the collection was alive succeeds")
                .keys()
        ),
        vec!["a"],
        "list_keys_at refused a collection that existed at that snapshot"
    );
    // Direction control: at head the collection is gone, and both still refuse.
    assert_eq!(
        vectors
            .count(&dropped)
            .expect_err("count at head is not-found")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .list_keys(&dropped, None, None, 10)
            .expect_err("keys at head is not-found")
            .code(),
        "not_found.engine.vector_collection"
    );
}

fn exercise_vector_timestamp_reads(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let collection = collection("history");
    let key = vector_key("doc");
    // Querying at a timestamp before any retained commit is an out-of-range
    // diagnostic (F8), surfaced at the collection-config read before the
    // collection-existence check.
    assert_eq!(
        vectors
            .query_at(
                &collection,
                &embedding([1.0, 0.0]),
                10,
                None,
                strata_core::Timestamp::EPOCH
            )
            .expect_err("pre-history query is a diagnostic")
            .code(),
        "history_unavailable.engine.persistence_history"
    );
    vectors
        .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    let first = vectors
        .upsert(
            collection.clone(),
            key.clone(),
            embedding([1.0, 0.0]),
            Some(metadata(json!({"kind": "doc"}))),
        )
        .expect("first upsert succeeds");
    let second = vectors
        .upsert(
            collection.clone(),
            key.clone(),
            embedding([0.0, 1.0]),
            Some(metadata(json!({"kind": "note"}))),
        )
        .expect("second upsert succeeds");
    let deleted = vectors
        .delete(&collection, key.clone())
        .expect("delete succeeds");

    assert_eq!(
        vectors
            .get_at(&collection, &key, first.commit().timestamp())
            .expect("first timestamp read succeeds")
            .expect("first value exists")
            .embedding()
            .as_slice(),
        &[1.0, 0.0]
    );
    assert_eq!(
        vectors
            .query_at(
                &collection,
                &embedding([1.0, 0.0]),
                10,
                Some(&filter_eq("kind", "doc")),
                first.commit().timestamp()
            )
            .expect("first timestamp query succeeds")
            .matches()
            .len(),
        1
    );
    assert_eq!(
        vectors
            .get_at(&collection, &key, second.commit().timestamp())
            .expect("second timestamp read succeeds")
            .expect("second value exists")
            .embedding()
            .as_slice(),
        &[0.0, 1.0]
    );
    assert!(vectors
        .query_at(
            &collection,
            &embedding([1.0, 0.0]),
            10,
            Some(&filter_eq("kind", "doc")),
            second.commit().timestamp()
        )
        .expect("second timestamp query succeeds")
        .matches()
        .is_empty());
    assert!(vectors
        .get_at(
            &collection,
            &key,
            deleted.commit().expect("delete commit").timestamp()
        )
        .expect("delete timestamp read succeeds")
        .is_none());
    let history = vectors
        .history(&collection, &key)
        .expect("history succeeds")
        .expect("history exists");
    assert_eq!(history.rows().len(), 3);
    assert!(history.rows()[0].is_tombstone());
    assert_eq!(history.rows()[1].vector_revision(), Some(2));
    assert_eq!(history.rows()[2].vector_revision(), Some(1));
}

fn exercise_vector_contract(database: &mut Database) {
    let collection = collection("docs");
    let key_a = vector_key("doc/a");
    let key_b = vector_key("doc/b");
    let key_c = vector_key("note/c");
    let mut vectors = vector_service(database, "default", "default");

    let first = create_contract_vectors(&mut vectors, &collection, &key_a, &key_b, &key_c);
    assert_contract_reads(
        &mut vectors,
        &collection,
        &key_a,
        &key_b,
        first.commit().version(),
    );
    assert_contract_patch_and_search(&mut vectors, &collection, &key_a);
    assert_contract_batches_and_deletes(&mut vectors, &collection, &key_a, &key_b);
}

fn create_contract_vectors(
    vectors: &mut strata_engine::VectorService<'_>,
    collection: &VectorCollectionName,
    key_a: &VectorKey,
    key_b: &VectorKey,
    key_c: &VectorKey,
) -> strata_engine::VectorWriteOutcome {
    let info = vectors
        .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    assert_eq!(info.name(), collection);
    assert_eq!(info.count(), 0);

    let duplicate = vectors
        .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect_err("duplicate collection rejected");
    assert_eq!(duplicate.class(), EngineErrorClass::Conflict);
    assert_eq!(duplicate.code(), "already_exists.engine.vector_collection");

    let first = vectors
        .upsert(
            collection.clone(),
            key_a.clone(),
            embedding([1.0, 0.0]),
            Some(metadata(json!({"kind": "doc", "rank": 1}))),
        )
        .expect("first upsert succeeds");
    assert_eq!(first.vector_revision(), 1);
    let second = vectors
        .upsert(
            collection.clone(),
            key_a.clone(),
            embedding([0.5, 0.5]),
            Some(metadata(json!({"kind": "doc", "rank": 2}))),
        )
        .expect("second upsert succeeds");
    assert_eq!(second.vector_revision(), 2);

    let batch = vectors
        .batch_upsert(
            collection,
            &[
                VectorUpsertEntry::new(
                    key_b.clone(),
                    embedding([0.0, 1.0]),
                    Some(metadata(json!({"kind": "doc", "rank": 3}))),
                ),
                VectorUpsertEntry::new(
                    key_c.clone(),
                    embedding([0.25, 0.25]),
                    Some(metadata(json!({"kind": "note", "rank": 4}))),
                ),
                VectorUpsertEntry::new(
                    key_b.clone(),
                    embedding([1.0, 0.0]),
                    Some(metadata(json!({"kind": "doc", "rank": 5}))),
                ),
            ],
        )
        .expect("batch upsert succeeds");
    assert_eq!(batch.vector_revisions(), &[1, 1, 2]);
    assert!(batch.commit().is_some());
    first
}

fn assert_contract_reads(
    vectors: &mut strata_engine::VectorService<'_>,
    collection: &VectorCollectionName,
    key_a: &VectorKey,
    key_b: &VectorKey,
    first_version: strata_core::CommitVersion,
) {
    assert_eq!(vectors.count(collection).expect("count succeeds"), 3);
    assert!(vectors.exists(collection, key_a).expect("exists succeeds"));
    assert_eq!(
        vectors
            .get_versioned(collection, key_a)
            .expect("read succeeds")
            .expect("entry exists")
            .vector_revision(),
        2
    );
    assert!(vectors
        .get_at_version(collection, key_a, first_version)
        .expect("version read succeeds")
        .is_some());

    let page = vectors
        .list_keys(collection, Some(&vector_key("doc/")), None, 1)
        .expect("first key page succeeds");
    assert_eq!(page.keys(), std::slice::from_ref(key_a));
    assert!(page.has_more());
    let next = vectors
        .list_keys(collection, Some(&vector_key("doc/")), page.cursor(), 10)
        .expect("second key page succeeds");
    assert_eq!(next.keys(), std::slice::from_ref(key_b));
    assert!(!next.has_more());
}

fn assert_contract_patch_and_search(
    vectors: &mut strata_engine::VectorService<'_>,
    collection: &VectorCollectionName,
    key_a: &VectorKey,
) {
    let mut patch = Map::new();
    patch.insert("patched".to_owned(), json!(true));
    let patched = vectors
        .update_metadata(
            collection,
            key_a.clone(),
            &VectorMetadataPatch::from_map(patch).expect("valid patch"),
        )
        .expect("metadata patch succeeds");
    assert!(patched.updated());
    assert_eq!(patched.vector_revision(), Some(3));
    assert_eq!(
        vectors
            .get(collection, key_a)
            .expect("read succeeds")
            .expect("entry exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"kind": "doc", "rank": 2, "patched": true})
    );

    let search = vectors
        .query(
            collection,
            &embedding([1.0, 0.0]),
            10,
            Some(&filter_eq("kind", "doc")),
        )
        .expect("query succeeds");
    assert_eq!(
        search
            .matches()
            .iter()
            .map(|row| row.entry().key().as_str())
            .collect::<Vec<_>>(),
        vec!["doc/b", "doc/a"]
    );
}

fn assert_contract_batches_and_deletes(
    vectors: &mut strata_engine::VectorService<'_>,
    collection: &VectorCollectionName,
    key_a: &VectorKey,
    key_b: &VectorKey,
) {
    let batch_get = vectors
        .batch_get(
            collection,
            &[key_a.clone(), vector_key("missing"), key_b.clone()],
        )
        .expect("batch get succeeds");
    assert!(batch_get.entries()[0].is_some());
    assert!(batch_get.entries()[1].is_none());
    assert!(batch_get.entries()[2].is_some());

    let batch_delete = vectors
        .batch_delete(
            collection,
            &[key_b.clone(), key_b.clone(), vector_key("missing")],
        )
        .expect("batch delete succeeds");
    assert_eq!(batch_delete.deleted(), &[true, false, false]);
    assert_eq!(vectors.count(collection).expect("count succeeds"), 2);

    let deleted = vectors
        .delete_by_filter(collection, &filter_eq("kind", "note"))
        .expect("filtered delete succeeds");
    assert_eq!(deleted.deleted_count(), 1);
    assert_eq!(vectors.count(collection).expect("count succeeds"), 1);

    let history = vectors
        .history(collection, key_b)
        .expect("history succeeds")
        .expect("history exists");
    assert!(history.rows()[0].is_tombstone());
    assert_eq!(history.rows()[1].vector_revision(), Some(2));

    let all_deleted = vectors.delete_all(collection).expect("delete all succeeds");
    assert_eq!(all_deleted.deleted_count(), 1);
    assert_eq!(vectors.count(collection).expect("count succeeds"), 0);
}

fn run_database_modes(exercise: fn(&mut Database)) {
    let mut cache = open_cache_database().expect("cache open succeeds");
    exercise(&mut cache);

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open succeeds");
    exercise(&mut durable);
    durable.close().expect("durable close succeeds");
}

#[cfg(feature = "testkit")]
fn durable_flat_artifact_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("engine-artifacts").join("vector").join("flat")
}

#[cfg(feature = "testkit")]
fn durable_hnsw_artifact_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("engine-artifacts").join("vector").join("hnsw")
}

#[cfg(feature = "testkit")]
fn durable_flat_artifact_file(root: &std::path::Path, artifact_id: &str) -> std::path::PathBuf {
    durable_flat_artifact_dir(root).join(durable_artifact_file_name(artifact_id))
}

#[cfg(feature = "testkit")]
fn durable_hnsw_artifact_file(root: &std::path::Path, artifact_id: &str) -> std::path::PathBuf {
    durable_hnsw_artifact_dir(root).join(durable_artifact_file_name(artifact_id))
}

#[cfg(feature = "testkit")]
fn durable_artifact_file_name(artifact_id: &str) -> String {
    let digest = Sha256::digest(artifact_id.as_bytes());
    let mut name = String::with_capacity(64 + ".bin".len());
    for byte in digest {
        name.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        name.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0f)]));
    }
    name.push_str(".bin");
    name
}

fn vector_service<'a>(
    database: &'a mut Database,
    branch_name: &str,
    space_name: &str,
) -> strata_engine::VectorService<'a> {
    database
        .vector(branch(branch_name), space(space_name))
        .expect("vector service opens")
}

fn collection(name: &str) -> VectorCollectionName {
    VectorCollectionName::new(name).expect("valid collection")
}

fn vector_key(key: &str) -> VectorKey {
    VectorKey::new(key).expect("valid vector key")
}

fn embedding<const N: usize>(values: [f32; N]) -> VectorEmbedding {
    VectorEmbedding::new(values).expect("valid embedding")
}

fn metadata(value: Value) -> VectorMetadata {
    VectorMetadata::new(value).expect("valid metadata")
}

fn patch(value: Value) -> VectorMetadataPatch {
    VectorMetadataPatch::new(value).expect("valid metadata patch")
}

fn filter_eq(field: &str, value: impl Into<VectorScalar>) -> VectorFilter {
    VectorFilter::new()
        .eq(field, value)
        .expect("valid vector filter")
}

fn filter_and<const N: usize>(conditions: [(&str, &str); N]) -> VectorFilter {
    conditions
        .into_iter()
        .fold(VectorFilter::new(), |filter, (field, value)| {
            filter.eq(field, value).expect("valid vector filter")
        })
}

fn key_strings(keys: &[VectorKey]) -> Vec<&str> {
    keys.iter().map(VectorKey::as_str).collect()
}

#[cfg(feature = "testkit")]
fn match_key_strings(result: &strata_engine::VectorSearchResult) -> Vec<&str> {
    result
        .matches()
        .iter()
        .map(|row| row.entry().key().as_str())
        .collect()
}

#[cfg(feature = "testkit")]
fn assert_search_results_match(
    actual: &strata_engine::VectorSearchResult,
    expected: &strata_engine::VectorSearchResult,
) {
    assert_eq!(
        result_bytes(actual),
        result_bytes(expected),
        "search result mismatch"
    );
}

#[cfg(feature = "testkit")]
fn assert_flat_policy_matches_exact(
    vectors: &mut strata_engine::VectorService<'_>,
    docs: &VectorCollectionName,
    query: &VectorEmbedding,
    k: usize,
    filter: Option<&VectorFilter>,
) -> (strata_engine::VectorSearchResult, VectorIndexDiagnostics) {
    let (exact, _) = vectors
        .query_with_index_policy_for_test(
            docs,
            query,
            k,
            filter,
            VectorIndexPolicy::exact_only_for_test(),
        )
        .expect("exact policy query succeeds");
    let (flat, diagnostics) = vectors
        .query_with_index_policy_for_test(
            docs,
            query,
            k,
            filter,
            VectorIndexPolicy::flat_only_for_test(),
        )
        .expect("flat policy query succeeds");
    assert_search_results_match(&flat, &exact);
    (flat, diagnostics)
}

#[cfg(feature = "testkit")]
#[derive(Clone, Copy)]
enum HnswRecallDataset {
    Random,
    Clustered,
}

#[cfg(feature = "testkit")]
struct HnswRecallFixture {
    collection: VectorCollectionName,
    dataset: HnswRecallDataset,
    count: usize,
    dimension: usize,
    seed: u64,
}

#[cfg(feature = "testkit")]
impl HnswRecallFixture {
    fn random(name: &str, count: usize, dimension: usize, seed: u64) -> Self {
        Self {
            collection: collection(name),
            dataset: HnswRecallDataset::Random,
            count,
            dimension,
            seed,
        }
    }

    fn clustered(name: &str, count: usize, dimension: usize, seed: u64) -> Self {
        Self {
            collection: collection(name),
            dataset: HnswRecallDataset::Clustered,
            count,
            dimension,
            seed,
        }
    }

    const fn dimension(&self) -> usize {
        self.dimension
    }

    const fn collection(&self) -> &VectorCollectionName {
        &self.collection
    }

    fn seed_collection(&self, vectors: &mut strata_engine::VectorService<'_>) {
        vectors
            .create_collection(
                self.collection.clone(),
                config(self.dimension, VectorDistanceMetric::Cosine),
            )
            .expect("collection create succeeds");
        for chunk_start in (0..self.count).step_by(128) {
            let chunk_len = 128.min(self.count - chunk_start);
            let entries = (0..chunk_len)
                .map(|offset| {
                    let index = chunk_start + offset;
                    VectorUpsertEntry::new(
                        vector_key(&format!("vec-{index:04}")),
                        self.embedding(index),
                        Some(metadata(json!({
                            "fixture": self.collection.as_str(),
                            "bucket": index % 16,
                        }))),
                    )
                })
                .collect::<Vec<_>>();
            vectors
                .batch_upsert(&self.collection, &entries)
                .expect("batch upsert succeeds");
        }
    }

    fn queries(&self) -> Vec<VectorEmbedding> {
        (0_usize..16)
            .map(|sample| match self.dataset {
                HnswRecallDataset::Random => {
                    let base_index = sample.saturating_mul(17) % self.count;
                    perturb_embedding(
                        self.embedding(base_index),
                        self.seed ^ 0xA5A5_5A5A ^ sample as u64,
                        0.005,
                    )
                }
                HnswRecallDataset::Clustered => {
                    let base_index = sample.saturating_mul(19) % self.count;
                    perturb_embedding(
                        self.embedding(base_index),
                        self.seed ^ 0x5A5A_A5A5 ^ sample as u64,
                        0.005,
                    )
                }
            })
            .collect()
    }

    fn embedding(&self, index: usize) -> VectorEmbedding {
        match self.dataset {
            HnswRecallDataset::Random => hnsw_recall_embedding(index, self.dimension, self.seed),
            HnswRecallDataset::Clustered => {
                hnsw_clustered_embedding(index, self.dimension, self.seed)
            }
        }
    }
}

#[cfg(feature = "testkit")]
#[derive(Debug)]
struct HnswRecallStats {
    total_queries: usize,
    hnsw_queries: usize,
    recall_at_1: f64,
    recall_at_5: f64,
    recall_at_10: f64,
}

#[cfg(feature = "testkit")]
fn assert_hnsw_recall_gates(
    vectors: &mut strata_engine::VectorService<'_>,
    branch_name: &str,
    docs: &VectorCollectionName,
    queries: &[VectorEmbedding],
) -> HnswRecallStats {
    let stats = hnsw_recall_stats(vectors, docs, queries);
    assert!(
        stats.recall_at_1 >= 0.90,
        "{branch_name}/{} recall@1 below gate: {:?}",
        docs.as_str(),
        stats
    );
    assert!(
        stats.recall_at_5 >= 0.92,
        "{branch_name}/{} recall@5 below gate: {:?}",
        docs.as_str(),
        stats
    );
    assert!(
        stats.recall_at_10 >= 0.95,
        "{branch_name}/{} recall@10 below gate: {:?}",
        docs.as_str(),
        stats
    );
    stats
}

#[cfg(feature = "testkit")]
fn hnsw_recall_stats(
    vectors: &mut strata_engine::VectorService<'_>,
    docs: &VectorCollectionName,
    queries: &[VectorEmbedding],
) -> HnswRecallStats {
    let mut hits_at_1 = 0usize;
    let mut expected_at_1 = 0usize;
    let mut hits_at_5 = 0usize;
    let mut expected_at_5 = 0usize;
    let mut hits_at_10 = 0usize;
    let mut expected_at_10 = 0usize;
    let mut hnsw_queries = 0usize;

    for query in queries {
        let exact = vectors
            .query_exact_for_test(docs, query, 10, None)
            .expect("exact query succeeds");
        let (hnsw, diagnostics) = vectors
            .query_with_index_policy_for_test(
                docs,
                query,
                10,
                None,
                VectorIndexPolicy::hnsw_only_for_test(),
            )
            .expect("HNSW query succeeds");
        assert_eq!(diagnostics.manifest_status(), "loaded");
        assert_eq!(diagnostics.hnsw_source_count(), 1);
        assert!(diagnostics.last_query_used_index());
        assert_eq!(diagnostics.exact_source_count(), 0);
        assert_eq!(diagnostics.last_query_fallback_reason(), None);
        hnsw_queries = hnsw_queries.saturating_add(1);

        let exact_keys = owned_match_key_strings(&exact);
        let hnsw_keys = owned_match_key_strings(&hnsw);
        let (hits, expected) = recall_counts_at_k(&exact_keys, &hnsw_keys, 1);
        hits_at_1 = hits_at_1.saturating_add(hits);
        expected_at_1 = expected_at_1.saturating_add(expected);
        let (hits, expected) = recall_counts_at_k(&exact_keys, &hnsw_keys, 5);
        hits_at_5 = hits_at_5.saturating_add(hits);
        expected_at_5 = expected_at_5.saturating_add(expected);
        let (hits, expected) = recall_counts_at_k(&exact_keys, &hnsw_keys, 10);
        hits_at_10 = hits_at_10.saturating_add(hits);
        expected_at_10 = expected_at_10.saturating_add(expected);
    }

    HnswRecallStats {
        total_queries: queries.len(),
        hnsw_queries,
        recall_at_1: recall_ratio(hits_at_1, expected_at_1),
        recall_at_5: recall_ratio(hits_at_5, expected_at_5),
        recall_at_10: recall_ratio(hits_at_10, expected_at_10),
    }
}

#[cfg(feature = "testkit")]
fn recall_counts_at_k(exact: &[String], actual: &[String], k: usize) -> (usize, usize) {
    let expected = exact.iter().take(k).collect::<BTreeSet<_>>();
    if expected.is_empty() {
        return (0, 0);
    }
    let actual = actual.iter().take(k).collect::<BTreeSet<_>>();
    let hits = expected.iter().filter(|key| actual.contains(*key)).count();
    (hits, expected.len())
}

#[cfg(feature = "testkit")]
fn recall_ratio(hits: usize, expected: usize) -> f64 {
    if expected == 0 {
        return 1.0;
    }
    hits as f64 / expected as f64
}

#[cfg(feature = "testkit")]
fn owned_match_key_strings(result: &strata_engine::VectorSearchResult) -> Vec<String> {
    result
        .matches()
        .iter()
        .map(|row| row.entry().key().as_str().to_owned())
        .collect()
}

#[cfg(feature = "testkit")]
fn hnsw_recall_embedding(index: usize, dimension: usize, seed: u64) -> VectorEmbedding {
    VectorEmbedding::new(normalized_hnsw_values(index, dimension, seed))
        .expect("valid recall embedding")
}

#[cfg(feature = "testkit")]
fn hnsw_clustered_embedding(index: usize, dimension: usize, seed: u64) -> VectorEmbedding {
    let cluster = index % 8;
    let center = normalized_hnsw_values(cluster, dimension, seed ^ 0xC1C5_7EAD);
    let noise = normalized_hnsw_values(index, dimension, seed ^ 0xDEC0_DED0);
    let values = center
        .into_iter()
        .zip(noise)
        .map(|(center, noise)| center + 0.35 * noise)
        .collect::<Vec<_>>();
    VectorEmbedding::new(normalize_values(values)).expect("valid clustered recall embedding")
}

#[cfg(feature = "testkit")]
fn perturb_embedding(embedding: VectorEmbedding, seed: u64, amplitude: f32) -> VectorEmbedding {
    let noise = normalized_hnsw_values(seed as usize, embedding.dimension(), seed ^ 0x9E37_79B9);
    let values = embedding
        .as_slice()
        .iter()
        .copied()
        .zip(noise)
        .map(|(value, noise)| value + amplitude * noise)
        .collect::<Vec<_>>();
    VectorEmbedding::new(normalize_values(values)).expect("valid perturbed recall embedding")
}

#[cfg(feature = "testkit")]
fn normalized_hnsw_values(index: usize, dimension: usize, seed: u64) -> Vec<f32> {
    let mut state = seed
        .wrapping_add((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    let mut values = Vec::with_capacity(dimension);
    for lane in 0..dimension {
        state ^= (lane as u64).wrapping_mul(0xA24B_AED4_963E_E407);
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let sample = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        let unit = ((sample >> 40) as f32) / ((1u64 << 24) as f32);
        values.push(unit.mul_add(2.0, -1.0));
    }
    normalize_values(values)
}

#[cfg(feature = "testkit")]
fn normalize_values(mut values: Vec<f32>) -> Vec<f32> {
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut values {
            *value /= norm;
        }
    }
    values
}

#[cfg(feature = "testkit")]
fn assert_default_exact_policy_diagnostics(
    diagnostics: &VectorIndexDiagnostics,
    collection_name: &str,
    source_candidate_limit: usize,
) {
    assert_default_exact_policy_core(diagnostics, collection_name, source_candidate_limit);
    assert_eq!(diagnostics.manifest_status(), "missing");
    assert_eq!(diagnostics.manifest_generation(), None);
    assert_eq!(diagnostics.manifest_ref_count(), 0);
    assert_eq!(diagnostics.manifest_inherited_ref_count(), 0);
    assert_eq!(diagnostics.manifest_owned_ref_count(), 0);
    assert_eq!(diagnostics.manifest_active_delta_count(), 0);
}

#[cfg(feature = "testkit")]
fn assert_default_exact_policy_core(
    diagnostics: &VectorIndexDiagnostics,
    collection_name: &str,
    source_candidate_limit: usize,
) {
    assert_eq!(diagnostics.collection(), collection_name);
    assert_eq!(diagnostics.policy_mode(), "auto");
    assert_eq!(diagnostics.collection_exact_threshold(), 64);
    assert_eq!(diagnostics.source_flat_threshold(), 64);
    assert_eq!(diagnostics.active_delta_seal_threshold(), 16);
    assert_eq!(diagnostics.overfetch_factor(), 4);
    assert!(diagnostics.filtered_underfill_fallback());
    assert_eq!(diagnostics.source_candidate_limit(), source_candidate_limit);
    assert_eq!(diagnostics.resolved_index_kind_summary(), "exact");
    assert_eq!(diagnostics.exact_fallback_count(), 0);
    assert_eq!(diagnostics.indexed_source_count(), 0);
    assert_eq!(diagnostics.exact_source_count(), 1);
    assert_eq!(diagnostics.flat_source_count(), 0);
    assert_eq!(diagnostics.hnsw_source_count(), 0);
    assert_eq!(diagnostics.active_delta_source_count(), 0);
    assert_eq!(diagnostics.indexed_vector_count(), 0);
    assert_eq!(diagnostics.derived_bytes(), 0);
    assert!(!diagnostics.last_query_used_index());
    assert_eq!(
        diagnostics.last_query_fallback_reason(),
        Some("collection_below_exact_threshold")
    );
}

#[cfg(feature = "testkit")]
fn storage_branch_id(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; BranchId::BYTE_LEN])
}

fn score_pairs(matches: &[strata_engine::VectorSearchMatch]) -> Vec<(String, f32)> {
    matches
        .iter()
        .map(|row| (row.entry().key().as_str().to_owned(), row.score()))
        .collect()
}

#[cfg(feature = "testkit")]
fn result_bytes(result: &strata_engine::VectorSearchResult) -> Vec<u8> {
    let mut bytes = Vec::new();
    push_len(&mut bytes, result.matches().len());
    for row in result.matches() {
        push_text(&mut bytes, row.entry().key().as_str());
        push_len(&mut bytes, row.entry().embedding().as_slice().len());
        for value in row.entry().embedding().as_slice() {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        match row.entry().metadata() {
            Some(metadata) => {
                bytes.push(1);
                let metadata_bytes =
                    serde_json::to_vec(metadata.as_inner()).expect("metadata encodes");
                push_len(&mut bytes, metadata_bytes.len());
                bytes.extend_from_slice(&metadata_bytes);
            }
            None => bytes.push(0),
        }
        bytes.extend_from_slice(&row.entry().vector_revision().to_le_bytes());
        bytes.extend_from_slice(&row.score().to_bits().to_le_bytes());
        bytes.extend_from_slice(&row.version().as_u64().to_le_bytes());
        bytes.extend_from_slice(&row.timestamp().as_micros().to_le_bytes());
    }
    bytes
}

#[cfg(feature = "testkit")]
fn push_text(bytes: &mut Vec<u8>, text: &str) {
    push_len(bytes, text.len());
    bytes.extend_from_slice(text.as_bytes());
}

#[cfg(feature = "testkit")]
fn push_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&u64::try_from(len).expect("length fits").to_le_bytes());
}

fn assert_close(actual: f32, expected: f32) {
    assert!(
        (actual - expected).abs() <= 0.000_001,
        "expected {expected}, got {actual}"
    );
}

fn config(dimension: usize, metric: VectorDistanceMetric) -> VectorConfig {
    VectorConfig::new(dimension, metric).expect("valid config")
}

fn upsert<const N: usize>(key: &str, values: [f32; N], metadata_value: Value) -> VectorUpsertEntry {
    VectorUpsertEntry::new(
        vector_key(key),
        embedding(values),
        Some(metadata(metadata_value)),
    )
}

#[cfg(feature = "testkit")]
fn descending_fixture_upsert(index: u16, metadata_value: Value) -> VectorUpsertEntry {
    let offset = f32::from(index);
    upsert(
        &format!("doc-{index:03}"),
        [100.0 - offset, offset],
        metadata_value,
    )
}

/// A write-time IO failure on the durable artifact directory surfaces the one
/// vector corruption code that genuinely propagates: `unavailable.engine.
/// vector_artifacts` (TCP3.15c). The other five vector corruption codes are
/// swallowed into a "corrupt" status by design (source rows stay authoritative,
/// rule 26) and are asserted by decoder unit tests in `manifest.rs`/`artifact.rs`.
#[cfg(feature = "testkit")]
#[test]
fn a_write_failure_on_the_artifact_directory_reports_the_unavailable_code() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let flat_dir = tempdir
        .path()
        .join("engine-artifacts")
        .join("vector")
        .join("flat");

    let mut database = open_durable_database(tempdir.path()).expect("durable open");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("io-fault");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create");
    vectors
        .upsert(docs.clone(), vector_key("a"), embedding([1.0, 0.0]), None)
        .expect("upsert");
    // Materialize a flat artifact on disk (durable mode writes the `.bin`).
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("seed flat artifact");
    assert!(
        flat_dir.is_dir(),
        "seeding must create the flat artifact directory"
    );

    // Replace the artifact directory with a regular file: the next artifact
    // write's `create_dir_all` fails with a not-a-directory error regardless of
    // the process uid (root-proof, unlike a chmod).
    std::fs::remove_dir_all(&flat_dir).expect("remove flat dir");
    std::fs::write(&flat_dir, b"blocker").expect("place blocker file");

    let error = vectors
        .corrupt_flat_artifact_for_test(&docs, "source-a")
        .expect_err("a write into a non-directory path must fail");
    assert_eq!(error.code(), "unavailable.engine.vector_artifacts");
    assert_eq!(error.class(), EngineErrorClass::Unavailable);
}
