//! Engine admin and space service behavior tests.

mod common;

use strata_engine::{AdminHealthStatus, DatabaseOpenTarget, EngineErrorClass, ProductSpace};
#[cfg(feature = "testkit")]
use strata_engine::{
    VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey,
};

#[cfg(feature = "localfs")]
use common::open_durable_database;
use common::{branch, key, open_cache_database, space, value};

#[test]
fn admin_reports_sanitized_database_and_primitive_facts() {
    let mut database = open_cache_database().expect("cache database opens");
    database
        .kv(branch("default"), space("default"))
        .expect("KV service opens")
        .put(key(b"alpha"), value(b"one"))
        .expect("kv put succeeds");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("tenant_a"))
        .expect("space create succeeds");

    let mut admin = database.admin().expect("admin service opens");
    let ping = admin.ping();
    assert!(!ping.version.is_empty());

    let info = admin.info(Some(&branch("default"))).expect("info succeeds");
    assert_eq!(info.target, DatabaseOpenTarget::Cache);
    assert!(info.created);
    assert!(!info.durable);
    assert!(info.open);
    assert_eq!(info.default_branch.as_str(), "default");
    assert_eq!(info.branch_count, 1);
    assert_eq!(info.space_count, 2);

    let health = admin.health(Some(&branch("default")));
    assert_eq!(health.status, AdminHealthStatus::Healthy);
    assert_eq!(health.default_branch.as_str(), "default");
    assert_eq!(health.branch_count, 1);

    let metrics = admin
        .metrics(Some(&branch("default")))
        .expect("metrics succeeds");
    assert_eq!(metrics.target, DatabaseOpenTarget::Cache);
    assert_eq!(metrics.control_status, AdminHealthStatus::Healthy);
    assert_eq!(metrics.space_count, 2);

    let describe = admin
        .describe(Some(&branch("default")), None)
        .expect("describe succeeds");
    assert_eq!(describe.branch.as_str(), "default");
    assert_eq!(
        describe
            .spaces
            .iter()
            .map(ProductSpace::as_str)
            .collect::<Vec<_>>(),
        vec!["default", "tenant_a"]
    );
    assert_eq!(describe.primitives.kv_count, 1);
    assert!(describe.capabilities.kv);
    assert!(describe.capabilities.json);
    assert!(describe.capabilities.event);
    assert!(describe.capabilities.vector);
    assert!(describe.capabilities.vector_index);
    assert!(describe.capabilities.graph_core);

    let config = admin.config();
    assert_eq!(config.target, DatabaseOpenTarget::Cache);
    assert_eq!(config.default_branch.as_str(), "default");
    assert_eq!(
        admin
            .config_value("default_branch")
            .expect("config key succeeds"),
        Some("default".to_owned())
    );
    assert_eq!(
        admin
            .config_value("openai_api_key")
            .expect("secret-like unknown key is hidden"),
        None
    );
}

#[test]
fn describe_scopes_primitive_counts_to_the_selected_space() {
    let mut database = open_cache_database().expect("cache database opens");
    database
        .kv(branch("default"), space("default"))
        .expect("default KV service opens")
        .put(key(b"alpha"), value(b"one"))
        .expect("default kv put");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("analytics"))
        .expect("space create");
    {
        let mut analytics = database
            .kv(branch("default"), space("analytics"))
            .expect("analytics KV service opens");
        analytics.put(key(b"z"), value(b"v")).expect("put z");
        analytics.put(key(b"y"), value(b"v")).expect("put y");
        analytics.put(key(b"w"), value(b"v")).expect("put w");
    }

    let mut admin = database.admin().expect("admin service opens");
    // #3385: describe's primitive counts are scoped to the SELECTED space. Before
    // the fix the space was hard-coded to default, so both of these reported the
    // default space's single key regardless of what the caller asked for.
    let default_view = admin
        .describe(Some(&branch("default")), None)
        .expect("describe default");
    assert_eq!(default_view.primitives.kv_count, 1, "default space count");

    let analytics_view = admin
        .describe(Some(&branch("default")), Some(&space("analytics")))
        .expect("describe analytics");
    assert_eq!(
        analytics_view.primitives.kv_count, 3,
        "counts must reflect the selected space, not always default (#3385)"
    );

    // Both views list every registered space regardless of which one is counted.
    assert_eq!(
        analytics_view
            .spaces
            .iter()
            .map(ProductSpace::as_str)
            .collect::<Vec<_>>(),
        vec!["analytics", "default"]
    );
}

#[test]
fn space_delete_force_tombstones_visible_kv_and_catalog_atomically() {
    let mut database = open_cache_database().expect("cache database opens");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(space("tenant_a"))
        .expect("space create succeeds");
    database
        .kv(branch("default"), space("tenant_a"))
        .expect("tenant KV service opens")
        .put(key(b"alpha"), value(b"one"))
        .expect("kv put succeeds");

    let error = database
        .spaces(branch("default"))
        .expect("space service opens")
        .delete(&space("tenant_a"), false)
        .expect_err("non-empty delete without force fails");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(error.code(), "failed_precondition.engine.space_not_empty");
    assert!(database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("tenant_a"))
        .expect("space exists succeeds"));
    assert_eq!(
        database
            .kv(branch("default"), space("tenant_a"))
            .expect("tenant KV service opens")
            .count(None)
            .expect("count succeeds"),
        1
    );

    let outcome = database
        .spaces(branch("default"))
        .expect("space service opens")
        .delete(&space("tenant_a"), true)
        .expect("forced delete succeeds");
    assert!(outcome.deleted());
    assert!(outcome.force());
    assert!(outcome.deleted_rows() >= 1);
    assert!(outcome.version().is_some());
    assert!(outcome.timestamp().is_some());
    assert!(!database
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("tenant_a"))
        .expect("space exists succeeds"));
    assert_eq!(
        database
            .kv(branch("default"), space("tenant_a"))
            .expect("tenant KV service opens")
            .count(None)
            .expect("count succeeds"),
        0
    );
}

#[cfg(feature = "testkit")]
#[test]
fn space_delete_force_removes_vector_index_manifest_control_rows() {
    let mut database = open_cache_database().expect("cache database opens");
    let tenant = space("tenant_a");
    let docs = VectorCollectionName::new("docs").expect("valid collection name");

    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(tenant.clone())
        .expect("space create succeeds");
    {
        let mut vectors = database
            .vector(branch("default"), tenant.clone())
            .expect("vector service opens");
        vectors
            .create_collection(
                docs.clone(),
                VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
            )
            .expect("collection create succeeds");
        for (key, embedding) in [
            ("doc-0", [1.0_f32, 0.0]),
            ("doc-1", [2.0_f32, 0.0]),
            ("doc-2", [3.0_f32, 0.0]),
        ] {
            vectors
                .upsert(
                    docs.clone(),
                    VectorKey::new(key).expect("valid vector key"),
                    VectorEmbedding::new(embedding).expect("valid embedding"),
                    None,
                )
                .expect("vector upsert succeeds");
        }
        vectors
            .seed_flat_index_manifest_from_visible_rows_for_test(&docs, "source-a")
            .expect("manifest seed succeeds");
        assert!(vectors
            .index_manifest_byte_len_for_test(&docs)
            .expect("manifest lookup succeeds")
            .is_some());
    }

    database
        .spaces(branch("default"))
        .expect("space service opens")
        .delete(&tenant, true)
        .expect("forced delete succeeds");
    database
        .spaces(branch("default"))
        .expect("space service opens")
        .create(tenant.clone())
        .expect("space recreate succeeds");

    let mut recreated = database
        .vector(branch("default"), tenant)
        .expect("vector service opens after recreate");
    recreated
        .create_collection(
            docs.clone(),
            VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config"),
        )
        .expect("collection recreate succeeds");
    assert_eq!(
        recreated
            .index_manifest_byte_len_for_test(&docs)
            .expect("manifest lookup succeeds"),
        None
    );
}

#[cfg(feature = "localfs")]
#[test]
fn admin_and_spaces_survive_durable_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("db");

    {
        let mut database = open_durable_database(&path).expect("durable database opens");
        database
            .spaces(branch("default"))
            .expect("space service opens")
            .create(space("tenant_a"))
            .expect("space create succeeds");
        let info = database
            .admin()
            .expect("admin service opens")
            .info(Some(&branch("default")))
            .expect("info succeeds");
        assert_eq!(info.target, DatabaseOpenTarget::DurableLocal);
        assert!(info.created);
        assert!(info.durable);
        assert_eq!(info.space_count, 2);
        database.close().expect("durable close succeeds");
    }

    let mut reopened = open_durable_database(&path).expect("durable database reopens");
    let info = reopened
        .admin()
        .expect("admin service opens")
        .info(Some(&branch("default")))
        .expect("info succeeds");
    assert_eq!(info.target, DatabaseOpenTarget::DurableLocal);
    assert!(!info.created);
    assert!(info.durable);
    assert_eq!(info.space_count, 2);
    assert!(reopened
        .spaces(branch("default"))
        .expect("space service opens")
        .exists(&space("tenant_a"))
        .expect("space exists succeeds"));
}

#[test]
fn admin_config_value_covers_allowlist_and_rejects_empty_key() {
    let mut database = open_cache_database().expect("cache database opens");
    let admin = database.admin().expect("admin service opens");

    assert_eq!(
        admin.config_value("target").expect("target value"),
        Some("cache".to_owned())
    );
    assert_eq!(
        admin.config_value("created").expect("created value"),
        Some("true".to_owned())
    );
    assert_eq!(
        admin.config_value("durable").expect("durable value"),
        Some("false".to_owned())
    );
    // Keys are trimmed and case-insensitive.
    assert_eq!(
        admin.config_value("  TARGET  ").expect("trimmed key"),
        Some("cache".to_owned())
    );

    let error = admin
        .config_value("")
        .expect_err("an empty config key is rejected");
    assert_eq!(error.class(), EngineErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.engine.config_key");
    assert_eq!(
        admin
            .config_value("unknown")
            .expect("unknown key is hidden"),
        None
    );
}

#[test]
fn space_usage_reports_per_capability_counts_and_lists_spaces() {
    let mut database = open_cache_database().expect("cache database opens");
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("kv service opens");
        kv.put(key(b"a"), value(b"1")).expect("put a");
        kv.put(key(b"b"), value(b"2")).expect("put b");
    }

    let mut spaces = database
        .spaces(branch("default"))
        .expect("space service opens");
    let usage = spaces.usage(&space("default")).expect("usage succeeds");
    assert_eq!(usage.kv_count(), 2);
    assert_eq!(usage.json_count(), 0);
    assert_eq!(usage.vector_collection_count(), 0);
    assert_eq!(usage.vector_entry_count(), 0);
    assert_eq!(usage.event_count(), 0);
    assert_eq!(usage.graph_count(), 0);
    assert_eq!(usage.graph_node_count(), 0);
    assert_eq!(usage.graph_edge_count(), 0);

    let listed: Vec<String> = spaces
        .list()
        .expect("list succeeds")
        .iter()
        .map(|entry| entry.as_str().to_owned())
        .collect();
    assert!(listed.contains(&"default".to_owned()));
}
