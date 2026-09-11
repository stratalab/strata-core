use crate::support::*;

pub(super) fn admin_space_outputs() -> Vec<Output> {
    let mut outputs = admin_outputs();
    outputs.extend(space_outputs());
    outputs
}

pub(super) fn admin_outputs() -> Vec<Output> {
    vec![
        Output::Pong(strata_executor::AdminPing {
            version: "1.0.0".to_owned(),
        }),
        Output::DatabaseInfo(AdminDatabaseInfo {
            version: "1.0.0".to_owned(),
            target: AdminOpenTarget::Cache,
            created: true,
            durable: false,
            default_branch: "default".to_owned(),
            branch_count: 1,
            space_count: 1,
            memory_budget: strata_executor::AdminMemoryBudget {
                total_bytes: 536_870_912,
                source: strata_executor::AdminMemoryBudgetSource::DerivedFromHost,
                usable_host_bytes: Some(2_147_483_648),
            },
            open: true,
        }),
        Output::Health(AdminHealth {
            status: AdminHealthStatus::Healthy,
            identity: AdminControlStatus::Healthy,
            registry: AdminControlStatus::Healthy,
            branch_catalog: AdminControlStatus::Healthy,
            space_catalog: Some(AdminControlStatus::Healthy),
            default_branch: "default".to_owned(),
            branch_count: 1,
        }),
        Output::Metrics(AdminMetrics {
            target: AdminOpenTarget::DurableLocal,
            durable: true,
            open: true,
            branch_count: 2,
            space_count: 3,
            control_status: AdminHealthStatus::Healthy,
        }),
        Output::Described(AdminDescribe {
            version: "1.0.0".to_owned(),
            target: AdminOpenTarget::Cache,
            default_branch: "default".to_owned(),
            branch: "default".to_owned(),
            branches: vec!["default".to_owned(), "feature".to_owned()],
            spaces: vec!["default".to_owned(), "tenant_a".to_owned()],
            primitives: AdminPrimitives {
                kv_count: 1,
                json_count: 2,
                event_count: 3,
                vector_collections: vec![AdminVectorCollection {
                    name: "docs".to_owned(),
                    dimension: 3,
                    metric: VectorDistanceMetric::Cosine,
                    count: 4,
                }],
                graphs: vec![AdminGraph {
                    name: "deps".to_owned(),
                    node_count: 5,
                    edge_count: 6,
                }],
            },
            config: AdminConfig {
                target: AdminOpenTarget::Cache,
                created: true,
                durable: false,
                default_branch: "default".to_owned(),
            },
            capabilities: AdminCapabilities {
                kv: true,
                json: true,
                event: true,
                vector: true,
                vector_index: true,
                graph_core: true,
                arrow: false,
                inference: false,
            },
        }),
        Output::Config(AdminConfig {
            target: AdminOpenTarget::Cache,
            created: true,
            durable: false,
            default_branch: "default".to_owned(),
        }),
        Output::ConfigValue(Some("cache".to_owned())),
        Output::ConfigValue(None),
    ]
}

pub(super) fn space_outputs() -> Vec<Output> {
    vec![
        Output::SpaceList {
            items: vec!["default".to_owned(), "tenant_a".to_owned()],
            page: PageInfo::terminal(),
        },
        Output::SpaceCreateResult {
            space: "tenant_a".to_owned(),
            effect: MutationEffect::created(),
            commit: Some(commit_receipt(2, 20, 1, 0)),
        },
        Output::SpaceCreateResult {
            space: "tenant_a".to_owned(),
            effect: unchanged_effect(),
            commit: None,
        },
        Output::SpaceDeleteResult {
            space: "tenant_a".to_owned(),
            force: true,
            deleted_rows: 7,
            effect: MutationEffect::deleted(),
            commit: Some(commit_receipt(3, 30, 0, 7)),
        },
        Output::SpaceDeleteResult {
            space: "missing".to_owned(),
            force: false,
            deleted_rows: 0,
            effect: MutationEffect::not_found(),
            commit: None,
        },
    ]
}

pub(super) fn branch_outputs() -> Vec<Output> {
    vec![
        Output::Branch(branch_item("main")),
        Output::Branches {
            items: vec![branch_item("default"), branch_item("main")],
            page: PageInfo::terminal(),
        },
        Output::BranchDeleteResult {
            deleted: true,
            effect: MutationEffect::deleted(),
            branch: branch_item("scratch"),
            generation_before: Some(1),
            generation_after: Some(1),
            cleanup: Some(BranchCleanupItem::new(0, 0, 0)),
        },
    ]
}
