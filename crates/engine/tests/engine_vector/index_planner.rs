use super::*;

#[cfg(feature = "testkit")]
#[test]
fn vector_index_planner_reports_exact_and_flat_sources() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("index-docs");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("a", [1.0, 0.0], json!({"kind": "doc"})),
                upsert("b", [0.8, 0.2], json!({"kind": "doc"})),
                upsert("c", [0.2, 0.8], json!({"kind": "doc"})),
                upsert("d", [0.0, 1.0], json!({"kind": "note"})),
            ],
        )
        .expect("batch upsert succeeds");

    let (exact, exact_diagnostics) = vectors
        .query_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            3,
            Some(&filter_eq("kind", "doc")),
            VectorIndexPolicy::exact_only_for_test(),
        )
        .expect("exact policy query succeeds");
    assert_eq!(exact_diagnostics.collection(), "index-docs");
    assert_eq!(exact_diagnostics.policy_mode(), "exact_only");
    assert_eq!(exact_diagnostics.resolved_index_kind_summary(), "exact");
    assert_eq!(exact_diagnostics.exact_source_count(), 1);
    assert_eq!(exact_diagnostics.flat_source_count(), 0);
    assert_eq!(exact_diagnostics.exact_fallback_count(), 0);
    assert!(!exact_diagnostics.last_query_used_index());
    assert_eq!(
        exact_diagnostics.last_query_fallback_reason(),
        Some("policy_exact_only")
    );
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");

    let (flat, flat_diagnostics) = vectors
        .query_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            3,
            Some(&filter_eq("kind", "doc")),
            VectorIndexPolicy::flat_only_for_test(),
        )
        .expect("flat policy query succeeds");
    assert_eq!(match_key_strings(&flat), match_key_strings(&exact));
    assert_eq!(flat_diagnostics.policy_mode(), "flat_only");
    assert_eq!(flat_diagnostics.resolved_index_kind_summary(), "flat");
    assert_eq!(flat_diagnostics.indexed_source_count(), 1);
    assert_eq!(flat_diagnostics.flat_source_count(), 1);
    assert_eq!(flat_diagnostics.active_delta_source_count(), 1);
    assert_eq!(flat_diagnostics.exact_source_count(), 0);
    assert_eq!(flat_diagnostics.indexed_vector_count(), 4);
    assert!(flat_diagnostics.derived_bytes() > 0);
    assert!(flat_diagnostics.last_query_used_index());
    assert_eq!(flat_diagnostics.last_query_fallback_reason(), None);
    assert_eq!(flat_diagnostics.artifact_sources()[0].status(), "loaded");
    assert!(flat_diagnostics.artifact_sources()[0].searched());
}

#[cfg(feature = "testkit")]
#[test]
fn vector_flat_planner_matches_exact_after_update_delete_and_filter() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("freshness");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("a", [1.0, 0.0], json!({"keep": true})),
                upsert("b", [0.9, 0.1], json!({"keep": true})),
                upsert("c", [0.8, 0.2], json!({"keep": false})),
                upsert("d", [0.7, 0.3], json!({"keep": true})),
            ],
        )
        .expect("initial batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("d"),
            embedding([0.0, 1.0]),
            Some(metadata(json!({"keep": true}))),
        )
        .expect("update succeeds");
    vectors
        .delete(&docs, vector_key("b"))
        .expect("delete succeeds");

    let filter = filter_eq("keep", true);
    let (exact, _) = vectors
        .query_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            10,
            Some(&filter),
            VectorIndexPolicy::exact_only_for_test(),
        )
        .expect("exact policy query succeeds");
    let (flat, diagnostics) = vectors
        .query_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            10,
            Some(&filter),
            VectorIndexPolicy::flat_only_for_test(),
        )
        .expect("flat policy query succeeds");

    assert_eq!(match_key_strings(&flat), match_key_strings(&exact));
    assert_eq!(match_key_strings(&flat), vec!["a", "d"]);
    assert_eq!(diagnostics.indexed_vector_count(), 4);
    assert_eq!(diagnostics.flat_source_count(), 1);
    assert_eq!(diagnostics.active_delta_source_count(), 1);
    assert_eq!(diagnostics.exact_source_count(), 1);
    assert_eq!(diagnostics.exact_fallback_count(), 1);
    assert!(!diagnostics.last_query_used_index());
    assert_eq!(
        diagnostics.last_query_fallback_reason(),
        Some("indexed_underfill")
    );
}

#[cfg(feature = "testkit")]
#[test]
fn vector_flat_planner_falls_back_when_tombstones_underfill_filtered_candidates() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("flat-underfill");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    let entries = (0_u16..30)
        .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&docs, &entries)
        .expect("initial batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    for index in 0_u16..20 {
        vectors
            .delete(&docs, vector_key(&format!("doc-{index:03}")))
            .expect("delete succeeds");
    }

    let filter = filter_eq("keep", true);
    let (exact, _) = vectors
        .query_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            5,
            Some(&filter),
            VectorIndexPolicy::exact_only_for_test(),
        )
        .expect("exact policy query succeeds");
    let (flat, diagnostics) = vectors
        .query_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            5,
            Some(&filter),
            VectorIndexPolicy::flat_only_for_test(),
        )
        .expect("flat policy query succeeds");

    assert_search_results_match(&flat, &exact);
    assert_eq!(
        match_key_strings(&flat),
        vec!["doc-020", "doc-021", "doc-022", "doc-023", "doc-024"]
    );
    assert_eq!(diagnostics.flat_source_count(), 1);
    assert_eq!(diagnostics.active_delta_source_count(), 1);
    assert_eq!(diagnostics.exact_source_count(), 1);
    assert_eq!(diagnostics.exact_fallback_count(), 1);
    assert!(!diagnostics.last_query_used_index());
    assert_eq!(
        diagnostics.last_query_fallback_reason(),
        Some("indexed_underfill")
    );
    assert_eq!(diagnostics.artifact_sources()[0].status(), "loaded");
    assert!(diagnostics.artifact_sources()[0].searched());
}

#[cfg(feature = "testkit")]
#[test]
fn vector_active_delta_filtered_update_suppresses_older_artifact_candidate() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("active-filter-suppression");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    let entries = (0_u16..72)
        .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&docs, &entries)
        .expect("initial batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("doc-000"),
            embedding([200.0, 0.0]),
            Some(metadata(json!({"keep": false}))),
        )
        .expect("filtered update succeeds");

    let filter = filter_eq("keep", true);
    let exact = vectors
        .query_exact_for_test(&docs, &embedding([1.0, 0.0]), 5, Some(&filter))
        .expect("exact query succeeds");
    let (indexed, diagnostics) = vectors
        .query_with_index_diagnostics_for_test(&docs, &embedding([1.0, 0.0]), 5, Some(&filter))
        .expect("indexed query succeeds");

    assert_search_results_match(&indexed, &exact);
    assert_eq!(
        match_key_strings(&indexed),
        vec!["doc-001", "doc-002", "doc-003", "doc-004", "doc-005"]
    );
    assert_eq!(diagnostics.flat_source_count(), 1);
    assert_eq!(diagnostics.active_delta_source_count(), 1);
    assert_eq!(diagnostics.exact_source_count(), 0);
}

#[cfg(feature = "testkit")]
#[test]
fn vector_flat_planner_matches_exact_for_timestamp_reads() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = vector_service(&mut database, "default", "default");
    let docs = collection("indexed-history");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::Cosine))
        .expect("collection create succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("a"),
            embedding([1.0, 0.0]),
            Some(metadata(json!({"keep": true}))),
        )
        .expect("first upsert succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("b"),
            embedding([0.8, 0.2]),
            Some(metadata(json!({"keep": true}))),
        )
        .expect("second upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    let snapshot = vectors
        .upsert(
            docs.clone(),
            vector_key("filtered-marker"),
            embedding([0.0, 1.0]),
            Some(metadata(json!({"keep": false}))),
        )
        .expect("marker upsert succeeds")
        .commit()
        .timestamp();
    vectors
        .upsert(
            docs.clone(),
            vector_key("a"),
            embedding([0.0, 1.0]),
            Some(metadata(json!({"keep": false}))),
        )
        .expect("update succeeds");
    vectors
        .delete(&docs, vector_key("b"))
        .expect("delete succeeds");

    let filter = filter_eq("keep", true);
    let (exact, _) = vectors
        .query_at_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            10,
            Some(&filter),
            snapshot,
            VectorIndexPolicy::exact_only_for_test(),
        )
        .expect("exact timestamp query succeeds");
    let (flat, diagnostics) = vectors
        .query_at_with_index_policy_for_test(
            &docs,
            &embedding([1.0, 0.0]),
            10,
            Some(&filter),
            snapshot,
            VectorIndexPolicy::flat_only_for_test(),
        )
        .expect("flat timestamp query succeeds");

    assert_eq!(match_key_strings(&flat), match_key_strings(&exact));
    assert_eq!(match_key_strings(&flat), vec!["a", "b"]);
    assert_eq!(diagnostics.indexed_vector_count(), 2);
    assert_eq!(diagnostics.flat_source_count(), 1);
    assert_eq!(diagnostics.active_delta_source_count(), 1);
    assert_eq!(diagnostics.exact_source_count(), 1);
    assert_eq!(diagnostics.exact_fallback_count(), 1);
    assert!(!diagnostics.last_query_used_index());
    assert_eq!(
        diagnostics.last_query_fallback_reason(),
        Some("indexed_underfill")
    );
}

#[cfg(feature = "testkit")]
#[test]
fn vector_flat_planner_edge_cases_match_exact_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_flat_planner_edge_cases_match_exact);
}

#[cfg(feature = "testkit")]
fn exercise_vector_flat_planner_edge_cases_match_exact(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("flat-edge-matrix");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("collection create succeeds");
    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("rank-1", [100.0, 0.0], json!({"kind": "doc", "rank": 1})),
                upsert("rank-2", [90.0, 0.0], json!({"kind": "doc", "rank": 2})),
                upsert("missing-rank", [80.0, 0.0], json!({"kind": "doc"})),
                upsert(
                    "note-rank-2",
                    [70.0, 0.0],
                    json!({"kind": "note", "rank": 2}),
                ),
                VectorUpsertEntry::new(vector_key("no-metadata"), embedding([60.0, 0.0]), None),
                upsert("rank-3", [50.0, 0.0], json!({"kind": "doc", "rank": 3})),
            ],
        )
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");

    let (zero_limit, zero_diagnostics) =
        assert_flat_policy_matches_exact(&mut vectors, &docs, &embedding([1.0, 0.0]), 0, None);
    assert!(zero_limit.matches().is_empty());
    assert_eq!(zero_diagnostics.manifest_status(), "not_checked");
    assert_eq!(zero_diagnostics.flat_source_count(), 0);
    assert!(zero_diagnostics.artifact_sources().is_empty());

    let (all_rows, all_diagnostics) =
        assert_flat_policy_matches_exact(&mut vectors, &docs, &embedding([1.0, 0.0]), 20, None);
    assert_eq!(
        match_key_strings(&all_rows),
        vec![
            "rank-1",
            "rank-2",
            "missing-rank",
            "note-rank-2",
            "no-metadata",
            "rank-3"
        ]
    );
    assert_eq!(all_diagnostics.flat_source_count(), 1);
    assert_eq!(all_diagnostics.exact_source_count(), 1);
    assert_eq!(all_diagnostics.exact_fallback_count(), 1);
    assert_eq!(
        all_diagnostics.last_query_fallback_reason(),
        Some("indexed_underfill")
    );

    let kind_filter = filter_eq("kind", "doc");
    let (kind_matches, kind_diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &docs,
        &embedding([1.0, 0.0]),
        3,
        Some(&kind_filter),
    );
    assert_eq!(
        match_key_strings(&kind_matches),
        vec!["rank-1", "rank-2", "missing-rank"]
    );
    assert_eq!(kind_diagnostics.flat_source_count(), 1);
    assert_eq!(kind_diagnostics.exact_source_count(), 0);
    assert!(kind_diagnostics.last_query_used_index());

    let numeric_filter = filter_eq("rank", 2_i32);
    let (numeric_matches, numeric_diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &docs,
        &embedding([1.0, 0.0]),
        2,
        Some(&numeric_filter),
    );
    assert_eq!(
        match_key_strings(&numeric_matches),
        vec!["rank-2", "note-rank-2"]
    );
    assert_eq!(numeric_diagnostics.flat_source_count(), 1);
    assert_eq!(numeric_diagnostics.exact_source_count(), 0);
    assert!(numeric_diagnostics.last_query_used_index());

    let missing_filter = filter_eq("absent", true);
    let (missing_matches, missing_diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &docs,
        &embedding([1.0, 0.0]),
        4,
        Some(&missing_filter),
    );
    assert!(missing_matches.matches().is_empty());
    assert_eq!(missing_diagnostics.flat_source_count(), 1);
    assert_eq!(missing_diagnostics.exact_source_count(), 1);
    assert_eq!(missing_diagnostics.exact_fallback_count(), 1);
    assert_eq!(
        missing_diagnostics.last_query_fallback_reason(),
        Some("indexed_underfill")
    );
    assert_eq!(missing_diagnostics.artifact_sources()[0].status(), "loaded");
    assert!(missing_diagnostics.artifact_sources()[0].searched());
}

#[cfg(feature = "testkit")]
#[test]
fn vector_flat_active_delta_update_delete_matches_exact_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_flat_active_delta_update_delete_matches_exact);
}

#[cfg(feature = "testkit")]
fn exercise_vector_flat_active_delta_update_delete_matches_exact(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("flat-active-delta-matrix");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("collection create succeeds");
    let entries = (0_u16..72)
        .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&docs, &entries)
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("doc-000"),
            embedding([200.0, 0.0]),
            Some(metadata(json!({"keep": false}))),
        )
        .expect("filtered update succeeds");
    vectors
        .delete(&docs, vector_key("doc-001"))
        .expect("delete succeeds");
    vectors
        .upsert(
            docs.clone(),
            vector_key("fresh"),
            embedding([300.0, 0.0]),
            Some(metadata(json!({"keep": true}))),
        )
        .expect("fresh upsert succeeds");

    let filter = filter_eq("keep", true);
    let (indexed, diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &docs,
        &embedding([1.0, 0.0]),
        5,
        Some(&filter),
    );

    assert_eq!(
        match_key_strings(&indexed),
        vec!["fresh", "doc-002", "doc-003", "doc-004", "doc-005"]
    );
    assert_eq!(diagnostics.flat_source_count(), 1);
    assert_eq!(diagnostics.active_delta_source_count(), 1);
    assert_eq!(diagnostics.exact_source_count(), 0);
    assert!(diagnostics.last_query_used_index());
    assert_eq!(diagnostics.last_query_fallback_reason(), None);
}

#[cfg(feature = "testkit")]
#[test]
fn vector_flat_delete_all_and_recreate_match_exact_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_flat_delete_all_and_recreate_match_exact);
}

#[cfg(feature = "testkit")]
fn exercise_vector_flat_delete_all_and_recreate_match_exact(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let delete_all_docs = collection("flat-delete-all-matrix");
    vectors
        .create_collection(
            delete_all_docs.clone(),
            config(2, VectorDistanceMetric::DotProduct),
        )
        .expect("collection create succeeds");
    let entries = (0_u16..72)
        .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&delete_all_docs, &entries)
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&delete_all_docs, "source-a")
        .expect("flat artifact seed succeeds");
    let deleted = vectors
        .delete_all(&delete_all_docs)
        .expect("delete all succeeds");
    assert_eq!(deleted.deleted_count(), 72);

    let (after_delete_all, delete_diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &delete_all_docs,
        &embedding([1.0, 0.0]),
        5,
        None,
    );
    assert!(after_delete_all.matches().is_empty());
    assert_eq!(delete_diagnostics.flat_source_count(), 1);
    assert_eq!(delete_diagnostics.active_delta_source_count(), 1);
    assert_eq!(delete_diagnostics.exact_source_count(), 1);
    assert_eq!(delete_diagnostics.exact_fallback_count(), 1);
    assert_eq!(
        delete_diagnostics.last_query_fallback_reason(),
        Some("indexed_underfill")
    );

    let recreate_docs = collection("flat-recreate-matrix");
    vectors
        .create_collection(
            recreate_docs.clone(),
            config(2, VectorDistanceMetric::DotProduct),
        )
        .expect("collection create succeeds");
    vectors
        .batch_upsert(&recreate_docs, &entries)
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&recreate_docs, "source-a")
        .expect("flat artifact seed succeeds");
    assert!(vectors
        .delete_collection(&recreate_docs, true)
        .expect("collection delete succeeds"));
    vectors
        .create_collection(
            recreate_docs.clone(),
            config(2, VectorDistanceMetric::DotProduct),
        )
        .expect("collection recreate succeeds");
    vectors
        .batch_upsert(
            &recreate_docs,
            &[
                upsert("new-a", [10.0, 0.0], json!({"phase": "new"})),
                upsert("new-b", [9.0, 0.0], json!({"phase": "new"})),
            ],
        )
        .expect("new batch upsert succeeds");

    let (after_recreate, recreate_diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &recreate_docs,
        &embedding([1.0, 0.0]),
        10,
        None,
    );
    assert_eq!(match_key_strings(&after_recreate), vec!["new-a", "new-b"]);
    assert_eq!(recreate_diagnostics.manifest_status(), "stale");
    assert_eq!(recreate_diagnostics.flat_source_count(), 0);
    assert_eq!(recreate_diagnostics.exact_source_count(), 1);
    assert_eq!(
        recreate_diagnostics.last_query_fallback_reason(),
        Some("stale")
    );
}

#[cfg(feature = "testkit")]
#[test]
fn vector_batch_invalidation_preserves_manifest_refs_and_uses_active_delta_in_cache_and_durable_modes(
) {
    run_database_modes(
        exercise_vector_batch_invalidation_preserves_manifest_refs_and_uses_active_delta,
    );
}

#[cfg(feature = "testkit")]
fn exercise_vector_batch_invalidation_preserves_manifest_refs_and_uses_active_delta(
    database: &mut Database,
) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("flat-batch-invalidation");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("collection create succeeds");
    let entries = (0_u16..72)
        .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&docs, &entries)
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    let original_refs = vectors
        .index_manifest_artifact_ids_for_test(&docs)
        .expect("manifest artifact IDs read");

    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("doc-000", [500.0, 0.0], json!({"keep": false})),
                upsert("fresh-a", [400.0, 0.0], json!({"keep": true})),
                upsert("fresh-b", [350.0, 0.0], json!({"keep": true})),
            ],
        )
        .expect("post-index batch upsert succeeds");
    let deleted = vectors
        .batch_delete(&docs, &[vector_key("doc-001"), vector_key("doc-002")])
        .expect("post-index batch delete succeeds");
    assert_eq!(deleted.deleted(), &[true, true]);
    let refs_after_batch = vectors
        .index_manifest_artifact_ids_for_test(&docs)
        .expect("manifest artifact IDs read after batch");
    assert_eq!(refs_after_batch, original_refs);

    let filter = filter_eq("keep", true);
    let (indexed, diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &docs,
        &embedding([1.0, 0.0]),
        5,
        Some(&filter),
    );

    assert_eq!(
        match_key_strings(&indexed),
        vec!["fresh-a", "fresh-b", "doc-003", "doc-004", "doc-005"]
    );
    assert_eq!(diagnostics.manifest_status(), "loaded");
    assert_eq!(diagnostics.flat_source_count(), 1);
    assert_eq!(diagnostics.active_delta_source_count(), 1);
    assert_eq!(diagnostics.exact_source_count(), 0);
    assert_eq!(diagnostics.last_query_fallback_reason(), None);
    assert!(diagnostics.last_query_used_index());
}

#[cfg(feature = "testkit")]
#[test]
fn vector_branch_local_invalidation_isolated_after_fork_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_branch_local_invalidation_isolated_after_fork);
}

#[cfg(feature = "testkit")]
fn exercise_vector_branch_local_invalidation_isolated_after_fork(database: &mut Database) {
    let docs = collection("flat-branch-invalidation");
    {
        let mut parent = vector_service(database, "default", "default");
        parent
            .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
            .expect("collection create succeeds");
        let entries = (0_u16..72)
            .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
            .collect::<Vec<_>>();
        parent
            .batch_upsert(&docs, &entries)
            .expect("batch upsert succeeds");
        parent
            .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
            .expect("flat artifact seed succeeds");
    }
    database
        .branches()
        .expect("branch service opens")
        .fork_current(&branch("default"), branch("feature"))
        .expect("branch fork succeeds");
    {
        let mut parent = vector_service(database, "default", "default");
        parent
            .upsert(
                docs.clone(),
                vector_key("parent-only"),
                embedding([400.0, 0.0]),
                Some(metadata(json!({"keep": true}))),
            )
            .expect("parent post-fork upsert succeeds");
        parent
            .delete(&docs, vector_key("doc-000"))
            .expect("parent post-fork delete succeeds");
    }
    {
        let mut feature = vector_service(database, "feature", "default");
        feature
            .upsert(
                docs.clone(),
                vector_key("feature-only"),
                embedding([500.0, 0.0]),
                Some(metadata(json!({"keep": true}))),
            )
            .expect("feature post-fork upsert succeeds");
        feature
            .delete(&docs, vector_key("doc-001"))
            .expect("feature post-fork delete succeeds");
    }

    let filter = filter_eq("keep", true);
    {
        let mut parent = vector_service(database, "default", "default");
        let (parent_indexed, parent_diagnostics) = assert_flat_policy_matches_exact(
            &mut parent,
            &docs,
            &embedding([1.0, 0.0]),
            3,
            Some(&filter),
        );
        assert_eq!(
            match_key_strings(&parent_indexed),
            vec!["parent-only", "doc-001", "doc-002"]
        );
        assert_eq!(parent_diagnostics.manifest_inherited_ref_count(), 0);
        assert_eq!(parent_diagnostics.manifest_owned_ref_count(), 1);
        assert_eq!(parent_diagnostics.flat_source_count(), 1);
        assert_eq!(parent_diagnostics.active_delta_source_count(), 1);
        assert_eq!(parent_diagnostics.exact_source_count(), 0);
        assert_eq!(parent_diagnostics.last_query_fallback_reason(), None);
    }
    {
        let mut feature = vector_service(database, "feature", "default");
        let (feature_indexed, feature_diagnostics) = assert_flat_policy_matches_exact(
            &mut feature,
            &docs,
            &embedding([1.0, 0.0]),
            3,
            Some(&filter),
        );
        assert_eq!(
            match_key_strings(&feature_indexed),
            vec!["feature-only", "doc-000", "doc-002"]
        );
        assert_eq!(feature_diagnostics.manifest_inherited_ref_count(), 1);
        assert_eq!(feature_diagnostics.manifest_owned_ref_count(), 0);
        assert_eq!(feature_diagnostics.flat_source_count(), 1);
        assert_eq!(feature_diagnostics.active_delta_source_count(), 1);
        assert_eq!(feature_diagnostics.exact_source_count(), 0);
        assert_eq!(feature_diagnostics.last_query_fallback_reason(), None);
    }
}

#[cfg(feature = "testkit")]
#[test]
fn vector_recreated_collection_rejects_stale_flat_manifest_in_cache_and_durable_modes() {
    run_database_modes(exercise_vector_recreated_collection_rejects_stale_flat_manifest);
}

#[cfg(feature = "testkit")]
fn exercise_vector_recreated_collection_rejects_stale_flat_manifest(database: &mut Database) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("flat-stale-manifest-recreate");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("collection create succeeds");
    let entries = (0_u16..72)
        .map(|index| descending_fixture_upsert(index, json!({"phase": "old"})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&docs, &entries)
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    assert!(vectors
        .delete_collection(&docs, true)
        .expect("collection delete succeeds"));
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("collection recreate succeeds");
    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("new-a", [20.0, 0.0], json!({"phase": "new"})),
                upsert("new-b", [10.0, 0.0], json!({"phase": "new"})),
            ],
        )
        .expect("new batch upsert succeeds");

    let (indexed, diagnostics) =
        assert_flat_policy_matches_exact(&mut vectors, &docs, &embedding([1.0, 0.0]), 10, None);

    assert_eq!(match_key_strings(&indexed), vec!["new-a", "new-b"]);
    assert_eq!(diagnostics.manifest_status(), "stale");
    assert_eq!(diagnostics.flat_source_count(), 0);
    assert_eq!(diagnostics.exact_source_count(), 1);
    assert_eq!(diagnostics.last_query_fallback_reason(), Some("stale"));
    assert!(!diagnostics.last_query_used_index());
}

#[cfg(feature = "testkit")]
#[test]
fn vector_failed_artifact_invalidation_falls_back_to_exact_after_fresh_writes_in_cache_and_durable_modes(
) {
    run_database_modes(
        exercise_vector_failed_artifact_invalidation_falls_back_to_exact_after_fresh_writes,
    );
}

#[cfg(feature = "testkit")]
fn exercise_vector_failed_artifact_invalidation_falls_back_to_exact_after_fresh_writes(
    database: &mut Database,
) {
    let mut vectors = vector_service(database, "default", "default");
    let docs = collection("flat-failed-invalidation");
    vectors
        .create_collection(docs.clone(), config(2, VectorDistanceMetric::DotProduct))
        .expect("collection create succeeds");
    let entries = (0_u16..72)
        .map(|index| descending_fixture_upsert(index, json!({"keep": true})))
        .collect::<Vec<_>>();
    vectors
        .batch_upsert(&docs, &entries)
        .expect("batch upsert succeeds");
    vectors
        .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
        .expect("flat artifact seed succeeds");
    vectors
        .batch_upsert(
            &docs,
            &[
                upsert("fresh-a", [400.0, 0.0], json!({"keep": true})),
                upsert("doc-000", [500.0, 0.0], json!({"keep": false})),
            ],
        )
        .expect("post-index batch upsert succeeds");
    vectors
        .delete(&docs, vector_key("doc-001"))
        .expect("post-index delete succeeds");
    vectors
        .remove_flat_artifact_for_test(&docs, "source-a")
        .expect("artifact remove succeeds");

    let filter = filter_eq("keep", true);
    let (indexed, diagnostics) = assert_flat_policy_matches_exact(
        &mut vectors,
        &docs,
        &embedding([1.0, 0.0]),
        5,
        Some(&filter),
    );

    assert_eq!(
        match_key_strings(&indexed),
        vec!["fresh-a", "doc-002", "doc-003", "doc-004", "doc-005"]
    );
    assert_eq!(diagnostics.manifest_status(), "loaded");
    assert_eq!(diagnostics.flat_source_count(), 0);
    assert_eq!(diagnostics.exact_source_count(), 1);
    assert_eq!(
        diagnostics.last_query_fallback_reason(),
        Some("artifact_unavailable")
    );
    assert_eq!(diagnostics.artifact_sources().len(), 1);
    assert_eq!(diagnostics.artifact_sources()[0].status(), "missing");
    assert!(!diagnostics.artifact_sources()[0].searched());
    assert!(!diagnostics.last_query_used_index());
}
