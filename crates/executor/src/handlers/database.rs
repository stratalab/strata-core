//! Database-level command handlers.

use std::sync::Arc;

use tracing::warn;

use crate::bridge::{from_engine_metric, is_internal_collection, to_core_branch_id, Primitives};
use crate::convert::convert_result;
use crate::types::{
    BranchId, CapabilitySummary, ConfigSummary, CountSummary, DescribeResult, GraphSummary,
    GraphSummaryEntry, PrimitiveSummary, StateSummary, VectorCollectionSummary, VectorSummary,
};
use crate::{Output, Result};

/// Build a structured snapshot of the database for agent introspection.
///
/// All data collection is best-effort: if a primitive fails, use zero/empty
/// defaults so that one failure never blocks the entire describe.
pub fn describe(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    let branch_id = to_core_branch_id(&branch)?;

    // -- Version, path, follower --
    let version = env!("CARGO_PKG_VERSION").to_string();
    let path = p.db.data_dir().to_string_lossy().to_string();
    let follower = p.db.is_follower();

    // -- Branches --
    // list_branches() only returns explicitly-created branches; the implicit
    // "default" branch is always present, so ensure it appears in the list.
    let mut branches = p.branch.list_branches().unwrap_or_else(|e| {
        warn!("describe: list_branches failed: {}", e);
        Vec::new()
    });
    if !branches.contains(&"default".to_string()) {
        branches.insert(0, "default".to_string());
    }

    // -- Spaces --
    let spaces = p.space.list(branch_id).unwrap_or_else(|e| {
        warn!("describe: list spaces failed: {}", e);
        Vec::new()
    });

    let default_space = "default";

    // -- KV count --
    let kv_count = convert_result(p.kv.list(&branch_id, default_space, None))
        .map(|keys| keys.len() as u64)
        .unwrap_or_else(|e| {
            warn!("describe: kv list failed: {}", e);
            0
        });

    // -- JSON count --
    let json_count =
        convert_result(
            p.json
                .list(&branch_id, default_space, None, None, u32::MAX as usize),
        )
        .map(|result| result.doc_ids.len() as u64)
        .unwrap_or_else(|e| {
            warn!("describe: json list failed: {}", e);
            0
        });

    // -- Event count --
    let event_count = convert_result(p.event.len(&branch_id, default_space)).unwrap_or_else(|e| {
        warn!("describe: event len failed: {}", e);
        0
    });

    // -- State cells --
    let state_cells =
        convert_result(p.state.list(&branch_id, default_space, None)).unwrap_or_else(|e| {
            warn!("describe: state list failed: {}", e);
            Vec::new()
        });

    // -- Vector collections --
    let vector_collections = p
        .vector
        .list_collections(branch_id, default_space)
        .map(|colls| {
            colls
                .into_iter()
                .filter(|c| !is_internal_collection(&c.name))
                .map(|c| VectorCollectionSummary {
                    name: c.name,
                    dimension: c.config.dimension,
                    metric: from_engine_metric(c.config.metric),
                    count: c.count as u64,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|e| {
            warn!("describe: vector list_collections failed: {}", e);
            Vec::new()
        });

    // -- Graphs --
    let graph_names = p.graph.list_graphs(branch_id).unwrap_or_else(|e| {
        warn!("describe: list_graphs failed: {}", e);
        Vec::new()
    });

    let graphs: Vec<GraphSummaryEntry> = graph_names
        .into_iter()
        .map(|name| {
            let stats = p
                .graph
                .snapshot_stats(branch_id, &name)
                .unwrap_or_else(|e| {
                    warn!("describe: snapshot_stats for '{}' failed: {}", name, e);
                    strata_engine::graph::types::GraphStats {
                        node_count: 0,
                        edge_count: 0,
                    }
                });
            let object_types = p
                .graph
                .list_object_types(branch_id, &name)
                .unwrap_or_default();
            let link_types = p
                .graph
                .list_link_types(branch_id, &name)
                .unwrap_or_default();
            GraphSummaryEntry {
                name,
                nodes: stats.node_count as u64,
                edges: stats.edge_count as u64,
                object_types,
                link_types,
            }
        })
        .collect();

    // -- Config --
    let cfg = p.db.config();
    let config = ConfigSummary {
        provider: cfg.provider.clone(),
        default_model: cfg.default_model.clone(),
        auto_embed: cfg.auto_embed,
        embed_model: cfg.embed_model.clone(),
        durability: cfg.durability.clone(),
    };

    // -- Capabilities --
    let search =
        p.db.extension::<strata_engine::search::InvertedIndex>()
            .map(|idx| idx.is_enabled())
            .unwrap_or(false);

    let has_vector_collections = !vector_collections.is_empty();

    let generation = cfg.default_model.is_some();

    let capabilities = CapabilitySummary {
        search,
        vector_search: has_vector_collections,
        generation,
        auto_embed: cfg.auto_embed,
    };

    Ok(Output::Described(DescribeResult {
        version,
        path,
        branch: branch.0,
        branches,
        spaces,
        follower,
        primitives: PrimitiveSummary {
            kv: CountSummary { count: kv_count },
            json: CountSummary { count: json_count },
            events: CountSummary { count: event_count },
            state: StateSummary {
                count: state_cells.len() as u64,
                cells: state_cells,
            },
            vector: VectorSummary {
                collections: vector_collections,
            },
            graph: GraphSummary { graphs },
        },
        config,
        capabilities,
    }))
}
