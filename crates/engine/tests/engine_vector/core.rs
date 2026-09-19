use super::*;

#[test]
fn vector_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_contract);
}

#[test]
fn vector_collection_lifecycle_and_counts_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_collection_lifecycle);
}

#[test]
fn vector_metadata_patch_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_metadata_patch_contract);
}

#[test]
fn vector_embedding_update_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_embedding_update_contract);
}

#[test]
fn vector_batch_contracts_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_batch_contracts);
}

#[test]
fn vector_bulk_delete_contracts_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_bulk_delete_contracts);
}

#[test]
fn vector_exact_search_metrics_run_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_exact_search_metrics);
}

#[test]
fn vector_timestamp_reads_track_overwrite_filter_and_delete() {
    run_database_modes(exercise_vector_timestamp_reads);
}

#[test]
fn vector_snapshot_existence_contract_runs_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_snapshot_existence_contract);
}

#[test]
fn vector_branch_and_space_isolation_match_other_primitives() {
    let mut database = open_cache_database().expect("cache open succeeds");
    {
        let mut vectors = vector_service(&mut database, "default", "default");
        vectors
            .create_collection(collection("docs"), config(2, VectorDistanceMetric::Cosine))
            .expect("collection create succeeds");
        vectors
            .upsert(
                collection("docs"),
                vector_key("shared"),
                embedding([1.0, 0.0]),
                Some(metadata(json!({"branch": "default"}))),
            )
            .expect("base upsert succeeds");
    }

    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch fork succeeds");

    {
        let mut feature = vector_service(&mut database, "feature", "default");
        let inherited = feature
            .get(&collection("docs"), &vector_key("shared"))
            .expect("read inherited vector succeeds")
            .expect("inherited vector exists");
        assert_eq!(
            inherited.metadata().expect("metadata").as_inner(),
            &json!({"branch": "default"})
        );
        feature
            .upsert(
                collection("docs"),
                vector_key("shared"),
                embedding([0.0, 1.0]),
                Some(metadata(json!({"branch": "feature"}))),
            )
            .expect("feature upsert succeeds");
    }

    let mut default_vectors = vector_service(&mut database, "default", "default");
    assert_eq!(
        default_vectors
            .get(&collection("docs"), &vector_key("shared"))
            .expect("default read succeeds")
            .expect("default vector exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"branch": "default"})
    );
    drop(default_vectors);

    let mut feature_vectors = vector_service(&mut database, "feature", "default");
    assert_eq!(
        feature_vectors
            .get(&collection("docs"), &vector_key("shared"))
            .expect("feature read succeeds")
            .expect("feature vector exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"branch": "feature"})
    );
    drop(feature_vectors);

    let mut other_space = vector_service(&mut database, "default", "other");
    other_space
        .create_collection(
            collection("docs"),
            config(2, VectorDistanceMetric::DotProduct),
        )
        .expect("other-space collection create succeeds");
    assert_eq!(
        other_space
            .collection_info(&collection("docs"))
            .expect("info succeeds")
            .expect("info exists")
            .config()
            .metric(),
        VectorDistanceMetric::DotProduct
    );
}

#[test]
fn vector_branch_destructive_operations_stay_isolated() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let collection = collection("docs");
    {
        let mut vectors = vector_service(&mut database, "default", "default");
        vectors
            .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
            .expect("collection create succeeds");
        vectors
            .batch_upsert(
                &collection,
                &[
                    upsert("shared", [1.0, 0.0], json!({"kind": "keep"})),
                    upsert("delete", [0.0, 1.0], json!({"kind": "keep"})),
                    upsert("filter", [0.5, 0.5], json!({"kind": "remove"})),
                    upsert("all", [0.25, 0.75], json!({"kind": "keep"})),
                ],
            )
            .expect("batch upsert succeeds");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch fork succeeds");

    {
        let mut feature = vector_service(&mut database, "feature", "default");
        feature
            .update_metadata(
                &collection,
                vector_key("shared"),
                &patch(json!({"patched": true})),
            )
            .expect("metadata patch succeeds");
        assert!(feature
            .delete(&collection, vector_key("delete"))
            .expect("delete succeeds")
            .deleted());
        assert_eq!(
            feature
                .delete_by_filter(&collection, &filter_eq("kind", "remove"))
                .expect("filtered delete succeeds")
                .deleted_count(),
            1
        );
        assert_eq!(feature.count(&collection).expect("count succeeds"), 2);
        assert_eq!(
            feature
                .delete_all(&collection)
                .expect("delete all succeeds")
                .deleted_count(),
            2
        );
        assert_eq!(feature.count(&collection).expect("count succeeds"), 0);
        assert!(feature
            .delete_collection(&collection)
            .expect("collection delete succeeds"));
        assert!(feature
            .collection_info(&collection)
            .expect("info succeeds")
            .is_none());
    }

    let mut parent = vector_service(&mut database, "default", "default");
    assert_eq!(parent.count(&collection).expect("parent count succeeds"), 4);
    assert_eq!(
        parent
            .get(&collection, &vector_key("shared"))
            .expect("parent read succeeds")
            .expect("parent value exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"kind": "keep"})
    );
    assert!(parent
        .get(&collection, &vector_key("delete"))
        .expect("parent read succeeds")
        .is_some());
    assert!(parent
        .get(&collection, &vector_key("filter"))
        .expect("parent read succeeds")
        .is_some());
}

#[test]
fn vector_space_destructive_operations_stay_isolated() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let collection = collection("docs");
    {
        let mut default_space = vector_service(&mut database, "default", "default");
        default_space
            .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
            .expect("collection create succeeds");
        default_space
            .batch_upsert(
                &collection,
                &[
                    upsert("shared", [1.0, 0.0], json!({"space": "default"})),
                    upsert("keep", [0.0, 1.0], json!({"kind": "keep"})),
                ],
            )
            .expect("batch upsert succeeds");
    }
    {
        let mut other_space = vector_service(&mut database, "default", "other");
        other_space
            .create_collection(
                collection.clone(),
                config(2, VectorDistanceMetric::DotProduct),
            )
            .expect("collection create succeeds");
        other_space
            .batch_upsert(
                &collection,
                &[
                    upsert("shared", [0.0, 1.0], json!({"space": "other"})),
                    upsert("remove", [1.0, 0.0], json!({"kind": "remove"})),
                ],
            )
            .expect("batch upsert succeeds");
        assert_eq!(
            other_space
                .delete_by_filter(&collection, &filter_eq("kind", "remove"))
                .expect("filtered delete succeeds")
                .deleted_count(),
            1
        );
        assert!(other_space
            .delete_collection(&collection)
            .expect("collection delete succeeds"));
    }

    let mut default_space = vector_service(&mut database, "default", "default");
    assert_eq!(default_space.count(&collection).expect("count succeeds"), 2);
    assert_eq!(
        default_space
            .collection_info(&collection)
            .expect("info succeeds")
            .expect("collection exists")
            .config()
            .metric(),
        VectorDistanceMetric::Cosine
    );
    assert_eq!(
        default_space
            .get(&collection, &vector_key("shared"))
            .expect("read succeeds")
            .expect("value exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"space": "default"})
    );
}

#[test]
fn vector_durable_reopen_preserves_collections_entries_and_history() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let collection = collection("durable");
    let key = vector_key("doc-1");
    let delete_timestamp;

    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        let mut vectors = database
            .vector(branch("default"), space("default"))
            .expect("vector service opens");
        vectors
            .create_collection(
                collection.clone(),
                config(2, VectorDistanceMetric::Euclidean),
            )
            .expect("collection create succeeds");
        vectors
            .upsert(
                collection.clone(),
                key.clone(),
                embedding([1.0, 0.0]),
                Some(metadata(json!({"stage": "created"}))),
            )
            .expect("first upsert succeeds");
        let updated = vectors
            .upsert(
                collection.clone(),
                key.clone(),
                embedding([0.0, 1.0]),
                Some(metadata(json!({"stage": "updated"}))),
            )
            .expect("second upsert succeeds");
        assert_eq!(updated.vector_revision(), 2);
        delete_timestamp = vectors
            .delete(&collection, vector_key("missing"))
            .expect("missing delete succeeds")
            .commit()
            .unwrap_or_else(|| updated.commit())
            .timestamp();
        drop(vectors);
        database.close().expect("close succeeds");
    }

    let mut reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut vectors = reopened
        .vector(branch("default"), space("default"))
        .expect("vector service opens");
    let info = vectors
        .collection_info(&collection)
        .expect("info succeeds")
        .expect("collection exists");
    assert_eq!(info.config().metric(), VectorDistanceMetric::Euclidean);
    assert_eq!(info.count(), 1);
    assert_eq!(
        vectors
            .get(&collection, &key)
            .expect("read succeeds")
            .expect("entry exists")
            .embedding()
            .as_slice(),
        &[0.0, 1.0]
    );
    let history = vectors
        .history(&collection, &key)
        .expect("history succeeds")
        .expect("history exists");
    assert_eq!(history.rows().len(), 2);
    assert_eq!(history.rows()[0].vector_revision(), Some(2));
    assert_eq!(history.rows()[1].vector_revision(), Some(1));
    // #3112 S4: every history row carries its commit's wall-clock instant,
    // newest-first like the rows themselves. Asserted here, in the engine
    // crate, because that is where the join lives and where its mutants are.
    let instants: Vec<_> = history
        .rows()
        .iter()
        .map(strata_engine::VectorHistoryRow::committed_at)
        .collect();
    assert!(
        instants.iter().all(Option::is_some),
        "live commits record instants: {instants:?}"
    );
    assert!(
        instants[0] > instants[1],
        "the newer row must carry the later instant: {instants:?}"
    );
    assert_eq!(
        vectors
            .query_at(
                &collection,
                &embedding([0.0, 1.0]),
                10,
                None,
                delete_timestamp
            )
            .expect("timestamp query succeeds")
            .matches()
            .len(),
        1
    );
}

#[test]
fn vector_durable_reopen_preserves_collection_delete() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let collection = collection("deleted");
    let key = vector_key("doc-1");

    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        let mut vectors = vector_service(&mut database, "default", "default");
        vectors
            .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
            .expect("collection create succeeds");
        vectors
            .upsert(
                collection.clone(),
                key.clone(),
                embedding([1.0, 0.0]),
                Some(metadata(json!({"stage": "before-delete"}))),
            )
            .expect("upsert succeeds");
        assert!(vectors
            .delete_collection(&collection)
            .expect("collection delete succeeds"));
        assert!(vectors
            .collection_info(&collection)
            .expect("latest info succeeds")
            .is_none());
        drop(vectors);
        database.close().expect("close succeeds");
    }

    let mut reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut vectors = vector_service(&mut reopened, "default", "default");
    assert!(vectors
        .collection_info(&collection)
        .expect("info succeeds")
        .is_none());
    assert_eq!(
        vectors
            .count(&collection)
            .expect_err("deleted collection count rejected")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .query(&collection, &embedding([1.0, 0.0]), 1, None)
            .expect_err("deleted collection query rejected")
            .code(),
        "not_found.engine.vector_collection"
    );
    let history = vectors
        .history(&collection, &key)
        .expect("history succeeds")
        .expect("history exists");
    assert!(history.rows()[0].is_tombstone());
    assert_eq!(history.rows()[1].vector_revision(), Some(1));
    drop(vectors);
    reopened.close().expect("close succeeds");
}

#[test]
fn vector_durable_reopen_preserves_branch_and_space_isolation() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let collection = collection("docs");
    {
        let mut database = open_durable_database(tempdir.path()).expect("durable open succeeds");
        {
            let mut vectors = vector_service(&mut database, "default", "default");
            vectors
                .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
                .expect("collection create succeeds");
            vectors
                .upsert(
                    collection.clone(),
                    vector_key("shared"),
                    embedding([1.0, 0.0]),
                    Some(metadata(json!({"branch": "default"}))),
                )
                .expect("upsert succeeds");
        }
        database
            .branches()
            .expect("branch service opens")
            .fork_current(&branch("default"), branch("feature"))
            .expect("branch fork succeeds");
        {
            let mut feature = vector_service(&mut database, "feature", "default");
            feature
                .upsert(
                    collection.clone(),
                    vector_key("feature-only"),
                    embedding([0.0, 1.0]),
                    Some(metadata(json!({"branch": "feature"}))),
                )
                .expect("feature upsert succeeds");
        }
        {
            let mut other_space = vector_service(&mut database, "default", "other");
            other_space
                .create_collection(
                    collection.clone(),
                    config(2, VectorDistanceMetric::Euclidean),
                )
                .expect("space collection create succeeds");
            other_space
                .upsert(
                    collection.clone(),
                    vector_key("shared"),
                    embedding([0.0, 1.0]),
                    Some(metadata(json!({"space": "other"}))),
                )
                .expect("space upsert succeeds");
        }
        database.close().expect("close succeeds");
    }

    let mut reopened = open_durable_database(tempdir.path()).expect("reopen succeeds");
    let mut default_vectors = vector_service(&mut reopened, "default", "default");
    assert_eq!(
        default_vectors
            .get(&collection, &vector_key("shared"))
            .expect("default read succeeds")
            .expect("default vector exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"branch": "default"})
    );
    assert!(default_vectors
        .get(&collection, &vector_key("feature-only"))
        .expect("default read succeeds")
        .is_none());
    drop(default_vectors);

    let mut feature_vectors = vector_service(&mut reopened, "feature", "default");
    assert!(feature_vectors
        .get(&collection, &vector_key("shared"))
        .expect("feature inherited read succeeds")
        .is_some());
    assert!(feature_vectors
        .get(&collection, &vector_key("feature-only"))
        .expect("feature read succeeds")
        .is_some());
    drop(feature_vectors);

    let mut other_space = vector_service(&mut reopened, "default", "other");
    assert_eq!(
        other_space
            .collection_info(&collection)
            .expect("space info succeeds")
            .expect("space collection exists")
            .config()
            .metric(),
        VectorDistanceMetric::Euclidean
    );
    assert_eq!(
        other_space
            .get(&collection, &vector_key("shared"))
            .expect("space read succeeds")
            .expect("space value exists")
            .metadata()
            .expect("metadata")
            .as_inner(),
        &json!({"space": "other"})
    );
}

#[test]
fn vector_invalid_inputs_are_engine_errors() {
    let error = VectorCollectionName::new("_internal").expect_err("reserved collection rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.vector_collection_reserved"
    );

    let error =
        VectorEmbedding::new([1.0, f32::INFINITY]).expect_err("non-finite embedding rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.vector_embedding");

    let error = VectorMetadataPatch::new(json!("not-object")).expect_err("patch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.vector_metadata_patch"
    );

    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    vectors
        .create_collection(collection("docs"), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    let error = vectors
        .upsert(
            collection("docs"),
            vector_key("wrong-dim"),
            embedding([1.0, 2.0, 3.0]),
            None,
        )
        .expect_err("dimension mismatch rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.vector_dimension");

    let error = vectors
        .delete_by_filter(&collection("docs"), &VectorFilter::new())
        .expect_err("empty filtered delete rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.vector_filter");
}

#[test]
fn vector_missing_branch_errors_are_engine_owned() {
    let mut database = open_cache_database().expect("cache open succeeds");

    // Branch existence is validated at service construction, so a missing
    // branch fails fast before any op — including reads and empty batches,
    // which previously relied on each op path to re-check the branch.
    let error = database
        .vector(branch("missing"), space("default"))
        .map(|_| ())
        .expect_err("missing branch rejected at service construction");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.branch");
}

#[test]
fn vector_noop_operations_validate_collection_and_query_shape() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("docs");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");

    let empty_keys = vectors
        .list_keys(&docs, None, None, 0)
        .expect("zero-limit key list succeeds");
    assert!(empty_keys.keys().is_empty());
    assert!(!empty_keys.has_more());
    assert_eq!(
        vectors
            .batch_upsert(&docs, &[])
            .expect("empty batch upsert succeeds")
            .commit(),
        None
    );
    assert_eq!(
        vectors
            .batch_delete(&docs, &[])
            .expect("empty batch delete succeeds")
            .commit(),
        None
    );
    assert!(vectors
        .query(&docs, &embedding([1.0, 0.0]), 0, None)
        .expect("zero-limit query succeeds")
        .matches()
        .is_empty());

    let missing = collection("missing");
    assert_eq!(
        vectors
            .list_keys(&missing, None, None, 0)
            .expect_err("zero-limit key list validates collection")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .batch_upsert(&missing, &[])
            .expect_err("empty batch upsert validates collection")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .batch_delete(&missing, &[])
            .expect_err("empty batch delete validates collection")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .query(&missing, &embedding([1.0, 0.0]), 0, None)
            .expect_err("zero-limit query validates collection")
            .code(),
        "not_found.engine.vector_collection"
    );
    assert_eq!(
        vectors
            .query(&docs, &embedding([1.0, 0.0, 0.0]), 0, None)
            .expect_err("zero-limit query validates dimension")
            .code(),
        "invalid_argument.engine.vector_dimension"
    );
}

#[test]
fn vector_exact_metric_ordering_is_deterministic() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    vectors
        .create_collection(
            collection("dots"),
            config(2, VectorDistanceMetric::DotProduct),
        )
        .expect("collection create succeeds");
    vectors
        .batch_upsert(
            &collection("dots"),
            &[
                upsert("b", [1.0, 0.0], json!({"keep": true})),
                upsert("a", [1.0, 0.0], json!({"keep": true})),
                upsert("c", [0.0, 1.0], json!({"keep": false})),
            ],
        )
        .expect("batch upsert succeeds");

    let matches = vectors
        .query(
            &collection("dots"),
            &embedding([1.0, 0.0]),
            3,
            Some(&filter_eq("keep", true)),
        )
        .expect("query succeeds");
    assert_eq!(
        matches
            .matches()
            .iter()
            .map(|row| row.entry().key().as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!((matches.matches()[0].score() - 1.0).abs() < f32::EPSILON);
    assert!((matches.matches()[1].score() - 1.0).abs() < f32::EPSILON);
}

#[test]
fn vector_collection_delete_tombstones_visible_rows() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    vectors
        .create_collection(
            collection("scratch"),
            config(2, VectorDistanceMetric::Cosine),
        )
        .expect("collection create succeeds");
    vectors
        .batch_upsert(
            &collection("scratch"),
            &[
                upsert("a", [1.0, 0.0], json!({})),
                upsert("b", [0.0, 1.0], json!({})),
            ],
        )
        .expect("batch upsert succeeds");
    assert_eq!(
        vectors
            .count(&collection("scratch"))
            .expect("count succeeds"),
        2
    );
    assert!(vectors
        .delete_collection(&collection("scratch"))
        .expect("collection delete succeeds"));
    assert!(vectors
        .collection_info(&collection("scratch"))
        .expect("info succeeds")
        .is_none());
    assert!(!vectors
        .delete_collection(&collection("scratch"))
        .expect("missing collection delete succeeds"));
    let error = vectors
        .query(&collection("scratch"), &embedding([1.0, 0.0]), 1, None)
        .expect_err("query missing collection rejected");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

#[test]
fn vector_historical_reads_use_historical_collection_config_after_delete() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let collection = collection("historical");
    let key = vector_key("doc");
    vectors
        .create_collection(
            collection.clone(),
            config(2, VectorDistanceMetric::Euclidean),
        )
        .expect("collection create succeeds");
    let written = vectors
        .upsert(
            collection.clone(),
            key.clone(),
            embedding([1.0, 0.0]),
            Some(metadata(json!({"kind": "doc"}))),
        )
        .expect("upsert succeeds");
    let version = written.commit().version();
    let timestamp = written.commit().timestamp();

    assert!(vectors
        .delete_collection(&collection)
        .expect("collection delete succeeds"));
    assert!(vectors
        .collection_info(&collection)
        .expect("latest info succeeds")
        .is_none());

    assert_eq!(
        vectors
            .get_at_version(&collection, &key, version)
            .expect("historical version read succeeds")
            .expect("historical vector exists")
            .entry()
            .embedding()
            .as_slice(),
        &[1.0, 0.0]
    );
    assert_eq!(
        vectors
            .get_at(&collection, &key, timestamp)
            .expect("historical timestamp read succeeds")
            .expect("historical vector exists")
            .entry()
            .embedding()
            .as_slice(),
        &[1.0, 0.0]
    );
    assert_eq!(
        vectors
            .query_at(&collection, &embedding([1.0, 0.0]), 10, None, timestamp)
            .expect("historical query succeeds")
            .matches()
            .iter()
            .map(|row| row.entry().key().as_str())
            .collect::<Vec<_>>(),
        vec!["doc"]
    );
    let history = vectors
        .history(&collection, &key)
        .expect("history succeeds")
        .expect("history exists");
    assert_eq!(history.rows().len(), 2);
    assert!(history.rows()[0].is_tombstone());
    assert_eq!(history.rows()[1].vector_revision(), Some(1));
}

#[test]
fn vector_list_keys_pages_in_public_key_order() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let collection = collection("paged");
    vectors
        .create_collection(collection.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    vectors
        .batch_upsert(
            &collection,
            &[
                upsert("b", [1.0, 0.0], json!({})),
                upsert("aa", [1.0, 0.0], json!({})),
                upsert("ba", [1.0, 0.0], json!({})),
                upsert("a", [1.0, 0.0], json!({})),
                upsert("ab", [1.0, 0.0], json!({})),
            ],
        )
        .expect("batch upsert succeeds");

    let first = vectors
        .list_keys(&collection, None, None, 2)
        .expect("first page succeeds");
    assert_eq!(key_strings(first.keys()), vec!["a", "aa"]);
    assert!(first.has_more());

    let second = vectors
        .list_keys(&collection, None, first.cursor(), 2)
        .expect("second page succeeds");
    assert_eq!(key_strings(second.keys()), vec!["ab", "b"]);
    assert!(second.has_more());

    let third = vectors
        .list_keys(&collection, None, second.cursor(), 2)
        .expect("third page succeeds");
    assert_eq!(key_strings(third.keys()), vec!["ba"]);
    assert!(!third.has_more());
}

#[test]
fn vector_serde_and_filter_builder_reject_invalid_inputs() {
    assert!(serde_json::from_value::<VectorEmbedding>(json!([])).is_err());
    assert!(serde_json::from_value::<VectorConfig>(json!({
        "dimension": 0,
        "metric": "cosine"
    }))
    .is_err());
    assert!(serde_json::from_value::<VectorFilterCondition>(json!({
        "field": "nested/path",
        "op": "eq",
        "value": {
            "type": "bool",
            "value": true
        }
    }))
    .is_err());

    let error = VectorFilter::new()
        .eq("nested/path", true)
        .expect_err("invalid filter field rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(
        error.code(),
        "invalid_argument.engine.vector_metadata_field"
    );
}
