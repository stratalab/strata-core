use std::sync::Arc;

use tracing::warn;

use crate::bridge::{
    from_engine_metric, is_internal_collection, require_branch_exists, to_core_branch_id,
    Primitives,
};
use crate::convert::convert_result;
use crate::types::{
    BranchId, CapabilitySummary, ConfigSummary, CountSummary, DescribeResult, GraphSummary,
    GraphSummaryEntry, PrimitiveSummary, VectorCollectionSummary, VectorSummary,
};
use crate::{Output, Result};

pub(crate) fn describe(primitives: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    require_branch_exists(primitives, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    let version = env!("CARGO_PKG_VERSION").to_string();
    let path = primitives.db.data_dir().to_string_lossy().to_string();
    let follower = primitives.db.is_follower();

    let mut branches = primitives.db.branches().list().unwrap_or_else(|error| {
        warn!("describe: failed to list branches: {error}");
        Vec::new()
    });
    let default_branch = primitives
        .db
        .default_branch_name()
        .unwrap_or_else(|| "default".to_string());
    if !branches.contains(&default_branch) {
        branches.insert(0, default_branch);
    }

    let spaces = primitives.space.list(branch_id).unwrap_or_else(|error| {
        warn!("describe: failed to list spaces: {error}");
        Vec::new()
    });

    let default_space = "default";

    let kv_count = convert_result(primitives.kv.list(&branch_id, default_space, None))
        .map(|keys| keys.len() as u64)
        .unwrap_or_else(|error| {
            warn!("describe: failed to list kv keys: {error}");
            0
        });

    let json_count = {
        let mut total = 0u64;
        let mut cursor: Option<String> = None;
        loop {
            match convert_result(primitives.json.list(
                &branch_id,
                default_space,
                None,
                cursor.as_deref(),
                1000,
            )) {
                Ok(result) => {
                    total += result.doc_ids.len() as u64;
                    match result.next_cursor {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                Err(error) => {
                    warn!("describe: failed to list json docs: {error}");
                    total = 0;
                    break;
                }
            }
        }
        total
    };

    let event_count = convert_result(primitives.event.len(&branch_id, default_space))
        .unwrap_or_else(|error| {
            warn!("describe: failed to read event length: {error}");
            0
        });

    let vector_collections = primitives
        .vector
        .list_collections(branch_id, default_space)
        .map(|collections| {
            collections
                .into_iter()
                .filter(|info| !is_internal_collection(&info.name))
                .map(|info| VectorCollectionSummary {
                    name: info.name,
                    dimension: info.config.dimension,
                    metric: from_engine_metric(info.config.metric),
                    count: info.count as u64,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|error| {
            warn!("describe: failed to list vector collections: {error}");
            Vec::new()
        });

    let graph_names = primitives
        .graph
        .list_graphs(branch_id, default_space)
        .unwrap_or_else(|error| {
            warn!("describe: failed to list graphs: {error}");
            Vec::new()
        });

    let graphs = graph_names
        .into_iter()
        .map(|name| {
            let stats = primitives
                .graph
                .snapshot_stats(branch_id, default_space, &name)
                .unwrap_or_else(|error| {
                    warn!("describe: failed to read graph stats for '{name}': {error}");
                    strata_graph::types::GraphStats {
                        node_count: 0,
                        edge_count: 0,
                    }
                });
            let object_types = primitives
                .graph
                .list_object_types(branch_id, default_space, &name)
                .unwrap_or_default();
            let link_types = primitives
                .graph
                .list_link_types(branch_id, default_space, &name)
                .unwrap_or_default();
            GraphSummaryEntry {
                name,
                nodes: stats.node_count as u64,
                edges: stats.edge_count as u64,
                object_types,
                link_types,
            }
        })
        .collect::<Vec<_>>();

    let config = {
        let cfg = primitives.db.config();
        ConfigSummary {
            provider: cfg.provider.clone(),
            default_model: cfg.default_model.clone(),
            auto_embed: cfg.auto_embed,
            embed_model: cfg.embed_model.clone(),
            durability: cfg.durability.clone(),
        }
    };

    let capabilities = CapabilitySummary {
        search: primitives
            .db
            .extension::<strata_engine::search::InvertedIndex>()
            .map(|index| index.is_enabled())
            .unwrap_or(false),
        vector_query: !vector_collections.is_empty(),
        generation: config.default_model.is_some(),
        auto_embed: config.auto_embed,
    };

    Ok(Output::Described(DescribeResult {
        version,
        path,
        branch: branch.to_string(),
        branches,
        spaces,
        follower,
        primitives: PrimitiveSummary {
            kv: CountSummary { count: kv_count },
            json: CountSummary { count: json_count },
            events: CountSummary { count: event_count },
            vector: VectorSummary {
                collections: vector_collections,
            },
            graph: GraphSummary { graphs },
        },
        config,
        capabilities,
    }))
}
