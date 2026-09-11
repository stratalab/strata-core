//! Executor spaces and admin behavior tests.

use serde_json::json;
use strata_executor::{
    AdminHealthStatus, AdminOpenTarget, AdminPing, Bytes, Command, Executor, ExecutorErrorClass,
    MutationEffect, MutationEffectKind, Output, VectorDistanceMetric, DEFAULT_BRANCH,
};
use tempfile::TempDir;

#[test]
fn space_commands_manage_branch_local_catalogs() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    assert_eq!(space_list(&mut executor, None), vec!["default"]);
    assert!(!space_list(&mut executor, None)
        .iter()
        .any(|space| space == "_system_"));

    let Output::SpaceCreateResult {
        space,
        effect,
        commit,
    } = executor
        .execute(Command::SpaceCreate {
            branch: None,
            space: "tenant_a".to_owned(),
        })
        .expect("space create succeeds")
    else {
        panic!("unexpected space create output");
    };
    assert_eq!(space, "tenant_a");
    assert_eq!(effect.kind(), MutationEffectKind::Created);
    assert_eq!(effect, MutationEffect::created());
    assert!(commit.is_some());
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::version)
        .is_some());
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::timestamp)
        .is_some());
    assert_eq!(space_list(&mut executor, None), vec!["default", "tenant_a"]);

    let Output::SpaceCreateResult { effect, commit, .. } = executor
        .execute(Command::SpaceCreate {
            branch: None,
            space: "tenant_a".to_owned(),
        })
        .expect("idempotent space create succeeds")
    else {
        panic!("unexpected idempotent space create output");
    };
    assert_ne!(effect.kind(), MutationEffectKind::Created);
    assert!(!effect.applied());
    assert!(effect.matched());
    assert_eq!(commit, None);
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::version)
        .is_none());
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::timestamp)
        .is_none());

    assert!(space_exists(&mut executor, None, "tenant_a"));
    assert!(!space_exists(&mut executor, None, "missing"));

    executor
        .execute(Command::BranchForkCurrent {
            source: DEFAULT_BRANCH.to_owned(),
            branch: "feature".to_owned(),
        })
        .expect("branch fork succeeds");
    assert_eq!(
        space_list(&mut executor, Some("feature")),
        vec!["default", "tenant_a"]
    );

    create_space(&mut executor, Some("feature"), "tenant_b");
    assert_eq!(
        space_list(&mut executor, Some("feature")),
        vec!["default", "tenant_a", "tenant_b"]
    );
    assert_eq!(space_list(&mut executor, None), vec!["default", "tenant_a"]);

    create_space(&mut executor, None, "tenant_c");
    assert_eq!(
        space_list(&mut executor, None),
        vec!["default", "tenant_a", "tenant_c"]
    );
    assert_eq!(
        space_list(&mut executor, Some("feature")),
        vec!["default", "tenant_a", "tenant_b"]
    );
}

#[test]
fn space_delete_is_conservative_and_force_removes_visible_data() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_space(&mut executor, None, "tenant_a");
    populate_rebuilt_primitives_in_space(&mut executor, "tenant_a");

    let error = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "tenant_a".to_owned(),
            force: false,
        })
        .expect_err("non-empty delete without force fails");
    assert_eq!(error.class(), ExecutorErrorClass::Conflict);
    assert_eq!(error.code(), "failed_precondition.engine.space_not_empty");
    assert!(space_exists(&mut executor, None, "tenant_a"));
    assert_eq!(kv_count(&mut executor, Some("tenant_a")), 1);

    let Output::SpaceDeleteResult {
        space,
        force,
        deleted_rows,
        effect,
        commit,
    } = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "tenant_a".to_owned(),
            force: true,
        })
        .expect("forced space delete succeeds")
    else {
        panic!("unexpected space delete output");
    };
    assert_eq!(space, "tenant_a");
    assert_eq!(effect.kind(), MutationEffectKind::Deleted);
    assert!(force);
    assert!(deleted_rows >= 5);
    assert_eq!(effect, MutationEffect::deleted());
    assert!(commit.is_some());
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::version)
        .is_some());
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::timestamp)
        .is_some());

    assert!(!space_exists(&mut executor, None, "tenant_a"));
    assert_eq!(space_list(&mut executor, None), vec!["default"]);
    assert_eq!(kv_count(&mut executor, Some("tenant_a")), 0);
    assert_eq!(json_count(&mut executor, Some("tenant_a")), 0);
    assert_eq!(event_count(&mut executor, Some("tenant_a")), 0);
    assert_eq!(
        vector_collections(&mut executor, Some("tenant_a")),
        Vec::<String>::new()
    );
    assert_eq!(
        graph_list(&mut executor, Some("tenant_a")),
        Vec::<String>::new()
    );

    let Output::SpaceDeleteResult { effect, commit, .. } = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "tenant_a".to_owned(),
            force: true,
        })
        .expect("missing space delete is a no-op")
    else {
        panic!("unexpected missing space delete output");
    };
    assert!(!effect.applied());
    assert_eq!(effect, MutationEffect::not_found());
    assert_eq!(commit, None);
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::version)
        .is_none());
    assert!(commit
        .as_ref()
        .map(strata_executor::CommitReceipt::timestamp)
        .is_none());
}

#[test]
fn forced_space_delete_on_child_does_not_delete_parent_inherited_data() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_space(&mut executor, None, "tenant_a");
    executor
        .execute(Command::KvPut {
            branch: None,
            space: Some("tenant_a".to_owned()),
            key: Bytes::from("inherited"),
            value: Bytes::from("parent"),
        })
        .expect("parent tenant write succeeds");

    executor
        .execute(Command::BranchForkCurrent {
            source: DEFAULT_BRANCH.to_owned(),
            branch: "child".to_owned(),
        })
        .expect("child branch fork succeeds");
    assert!(space_exists(&mut executor, Some("child"), "tenant_a"));
    assert_eq!(
        kv_count_in(&mut executor, Some("child"), Some("tenant_a")),
        1
    );

    executor
        .execute(Command::SpaceDelete {
            branch: Some("child".to_owned()),
            space: "tenant_a".to_owned(),
            force: true,
        })
        .expect("child forced delete succeeds");

    assert!(!space_exists(&mut executor, Some("child"), "tenant_a"));
    assert_eq!(
        kv_count_in(&mut executor, Some("child"), Some("tenant_a")),
        0
    );
    assert!(space_exists(&mut executor, None, "tenant_a"));
    assert_eq!(kv_count(&mut executor, Some("tenant_a")), 1);

    executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "tenant_a".to_owned(),
            force: true,
        })
        .expect("parent forced delete succeeds");
    assert!(!space_exists(&mut executor, None, "tenant_a"));
    assert!(!space_exists(&mut executor, Some("child"), "tenant_a"));
    assert_eq!(
        kv_count_in(&mut executor, Some("child"), Some("tenant_a")),
        0
    );
}

#[test]
fn reserved_spaces_and_default_space_are_protected() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let error = executor
        .execute(Command::SpaceCreate {
            branch: None,
            space: "_system_".to_owned(),
        })
        .expect_err("reserved space create fails");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);

    let error = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "_system_".to_owned(),
            force: true,
        })
        .expect_err("reserved space delete fails");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);

    let error = executor
        .execute(Command::SpaceDelete {
            branch: None,
            space: "default".to_owned(),
            force: true,
        })
        .expect_err("default space delete fails");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert!(space_exists(&mut executor, None, "default"));
}

#[test]
fn durable_spaces_persist_across_reopen() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");

    {
        let mut executor = Executor::open_durable_local(&path).expect("durable executor opens");
        create_space(&mut executor, None, "tenant_a");
        executor.close().expect("durable executor closes");
    }

    {
        let mut executor = Executor::open_durable_local(&path).expect("durable executor reopens");
        assert_eq!(space_list(&mut executor, None), vec!["default", "tenant_a"]);
        executor
            .execute(Command::SpaceDelete {
                branch: None,
                space: "tenant_a".to_owned(),
                force: false,
            })
            .expect("empty space delete succeeds");
        executor.close().expect("durable executor closes");
    }

    let mut reopened = Executor::open_durable_local(&path).expect("durable executor reopens");
    assert_eq!(space_list(&mut reopened, None), vec!["default"]);
    assert!(!space_exists(&mut reopened, None, "tenant_a"));
}

#[test]
fn admin_commands_report_sanitized_database_facts() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    populate_rebuilt_primitives_in_space(&mut executor, "default");

    let Output::Pong(AdminPing { version }) =
        executor.execute(Command::Ping {}).expect("ping succeeds")
    else {
        panic!("unexpected ping output");
    };
    assert!(!version.is_empty());

    let Output::DatabaseInfo(info) = executor
        .execute(Command::Info { branch: None })
        .expect("info succeeds")
    else {
        panic!("unexpected info output");
    };
    assert_eq!(info.target, AdminOpenTarget::Cache);
    assert!(info.open);
    assert_eq!(info.default_branch, "default");
    assert_eq!(info.branch_count, 1);
    assert_eq!(info.space_count, 1);

    let Output::Health(health) = executor
        .execute(Command::Health { branch: None })
        .expect("health succeeds")
    else {
        panic!("unexpected health output");
    };
    assert_eq!(health.status, AdminHealthStatus::Healthy);
    assert_eq!(health.default_branch, "default");

    let Output::Metrics(metrics) = executor
        .execute(Command::Metrics { branch: None })
        .expect("metrics succeeds")
    else {
        panic!("unexpected metrics output");
    };
    assert_eq!(metrics.target, AdminOpenTarget::Cache);
    assert_eq!(metrics.control_status, AdminHealthStatus::Healthy);

    let Output::Described(describe) = executor
        .execute(Command::Describe { branch: None })
        .expect("describe succeeds")
    else {
        panic!("unexpected describe output");
    };
    assert_eq!(describe.default_branch, "default");
    assert_eq!(describe.branch, "default");
    assert_eq!(describe.spaces, vec!["default"]);
    assert_eq!(describe.primitives.kv_count, 1);
    assert_eq!(describe.primitives.json_count, 1);
    assert_eq!(describe.primitives.event_count, 1);
    assert_eq!(describe.primitives.vector_collections.len(), 1);
    assert_eq!(describe.primitives.graphs.len(), 1);
    assert!(describe.capabilities.kv);
    assert!(describe.capabilities.json);
    assert!(describe.capabilities.event);
    assert!(describe.capabilities.vector);
    assert!(describe.capabilities.vector_index);
    assert!(describe.capabilities.graph_core);

    let Output::Config(config) = executor
        .execute(Command::ConfigGet {})
        .expect("config get succeeds")
    else {
        panic!("unexpected config output");
    };
    assert_eq!(config.target, AdminOpenTarget::Cache);
    assert!(!config.durable);
    assert_eq!(config.default_branch, "default");

    let Output::ConfigValue(Some(target)) = executor
        .execute(Command::ConfigureGetKey {
            key: "target".to_owned(),
        })
        .expect("config key succeeds")
    else {
        panic!("unexpected config value output");
    };
    assert_eq!(target, "cache");

    let Output::ConfigValue(None) = executor
        .execute(Command::ConfigureGetKey {
            key: "openai_api_key".to_owned(),
        })
        .expect("secret-like unknown key is not returned")
    else {
        panic!("unexpected unknown config value output");
    };

    let serialized =
        serde_json::to_string(&describe).expect("describe output serializes without secrets");
    assert!(!serialized.contains("api_key"));
    assert!(!serialized.contains("openai"));
    assert!(!serialized.contains("anthropic"));
    assert!(!serialized.contains("google"));
}

#[test]
fn admin_read_commands_do_not_mutate_catalog_state() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    create_space(&mut executor, None, "tenant_a");
    let branches_before = branch_names(&mut executor);
    let spaces_before = space_list(&mut executor, None);

    for command in [
        Command::Ping {},
        Command::Info { branch: None },
        Command::Health { branch: None },
        Command::Metrics { branch: None },
        Command::Describe { branch: None },
        Command::ConfigGet {},
        Command::ConfigureGetKey {
            key: "target".to_owned(),
        },
        Command::SpaceList { branch: None },
        Command::SpaceExists {
            branch: None,
            space: "tenant_a".to_owned(),
        },
    ] {
        executor.execute(command).expect("read command succeeds");
    }

    assert_eq!(branch_names(&mut executor), branches_before);
    assert_eq!(space_list(&mut executor, None), spaces_before);
}

fn create_space(executor: &mut Executor, branch: Option<&str>, space: &str) {
    executor
        .execute(Command::SpaceCreate {
            branch: branch.map(str::to_owned),
            space: space.to_owned(),
        })
        .expect("space create succeeds");
}

fn space_list(executor: &mut Executor, branch: Option<&str>) -> Vec<String> {
    let Output::SpaceList { items: spaces, .. } = executor
        .execute(Command::SpaceList {
            branch: branch.map(str::to_owned),
        })
        .expect("space list succeeds")
    else {
        panic!("unexpected space list output");
    };
    spaces
}

fn space_exists(executor: &mut Executor, branch: Option<&str>, space: &str) -> bool {
    let Output::Bool(exists) = executor
        .execute(Command::SpaceExists {
            branch: branch.map(str::to_owned),
            space: space.to_owned(),
        })
        .expect("space exists succeeds")
    else {
        panic!("unexpected space exists output");
    };
    exists
}

fn branch_names(executor: &mut Executor) -> Vec<String> {
    let Output::Branches {
        items: branches, ..
    } = executor
        .execute(Command::BranchList {})
        .expect("branch list succeeds")
    else {
        panic!("unexpected branch list output");
    };
    branches
        .iter()
        .map(|branch| branch.name().to_owned())
        .collect()
}

fn populate_rebuilt_primitives_in_space(executor: &mut Executor, space: &str) {
    executor
        .execute(Command::KvPut {
            branch: None,
            space: Some(space.to_owned()),
            key: Bytes::from("alpha"),
            value: Bytes::from("one"),
        })
        .expect("kv put succeeds");

    executor
        .execute(Command::JsonSet {
            branch: None,
            space: Some(space.to_owned()),
            key: "doc-a".to_owned(),
            path: "$".to_owned(),
            value: json!({"kind": "doc"}),
        })
        .expect("json set succeeds");

    executor
        .execute(Command::EventAppend {
            branch: None,
            space: Some(space.to_owned()),
            event_type: "audit.created".to_owned(),
            payload: json!({"id": 1}),
        })
        .expect("event append succeeds");

    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: Some(space.to_owned()),
            collection: "docs".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: None,
        })
        .expect("vector collection create succeeds");
    executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: Some(space.to_owned()),
            collection: "docs".to_owned(),
            key: "doc-a".to_owned(),
            vector: vec![1.0, 0.0],
            text: None,
            metadata: Some(json!({"kind": "doc"})),
        })
        .expect("vector upsert succeeds");

    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: Some(space.to_owned()),
            graph: "deps".to_owned(),
        })
        .expect("graph create succeeds");
    executor
        .execute(Command::GraphAddNode {
            object_type: None,
            branch: None,
            space: Some(space.to_owned()),
            graph: "deps".to_owned(),
            node_id: "node-a".to_owned(),
            properties: Some(json!({"kind": "root"})),
            binding: None,
        })
        .expect("graph node create succeeds");
}

fn kv_count(executor: &mut Executor, space: Option<&str>) -> u64 {
    kv_count_in(executor, None, space)
}

fn kv_count_in(executor: &mut Executor, branch: Option<&str>, space: Option<&str>) -> u64 {
    let Output::Uint(count) = executor
        .execute(Command::KvCount {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            prefix: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("kv count succeeds")
    else {
        panic!("unexpected kv count output");
    };
    count
}

fn json_count(executor: &mut Executor, space: Option<&str>) -> u64 {
    let Output::Uint(count) = executor
        .execute(Command::JsonCount {
            branch: None,
            space: space.map(str::to_owned),
            prefix: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("json count succeeds")
    else {
        panic!("unexpected json count output");
    };
    count
}

fn event_count(executor: &mut Executor, space: Option<&str>) -> u64 {
    let Output::EventCount { count } = executor
        .execute(Command::EventCount {
            branch: None,
            space: space.map(str::to_owned),
            as_of: None,
            as_of_time: None,
        })
        .expect("event len succeeds")
    else {
        panic!("unexpected event len output");
    };
    count
}

fn vector_collections(executor: &mut Executor, space: Option<&str>) -> Vec<String> {
    let Output::VectorCollectionList {
        items: collections, ..
    } = executor
        .execute(Command::VectorListCollections {
            branch: None,
            space: space.map(str::to_owned),
        })
        .expect("vector list collections succeeds")
    else {
        panic!("unexpected vector collection list output");
    };
    collections
        .iter()
        .map(|collection| collection.name().to_owned())
        .collect()
}

fn graph_list(executor: &mut Executor, space: Option<&str>) -> Vec<String> {
    let Output::GraphNamePage { items: graphs, .. } = executor
        .execute(Command::GraphList {
            branch: None,
            space: space.map(str::to_owned),
            cursor: None,
            limit: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("graph list succeeds")
    else {
        panic!("unexpected graph list output");
    };
    graphs
}
