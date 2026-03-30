//! Search command handler.
//!
//! Routes all search through the retrieval substrate. The intelligence layer
//! (query embedding) wraps the substrate transparently.

use std::sync::Arc;

use chrono::DateTime;
use strata_engine::search::recipe::{RetrieveConfig, TransformConfig, VectorRetrieveConfig};
use strata_engine::search::{builtin_defaults, PrimitiveType, Recipe};
use strata_search::substrate::{self, RetrievalRequest};

use crate::bridge::{to_core_branch_id, Primitives};
use crate::types::{BranchId, SearchQuery, SearchResultHit, SearchStatsOutput, TimeRangeInput};
use crate::{Error, Output, Result};

/// Parse an ISO 8601 datetime string to microseconds since epoch.
fn parse_iso8601_to_micros(s: &str) -> Result<u64> {
    let dt = DateTime::parse_from_rfc3339(s).map_err(|e| Error::InvalidInput {
        reason: format!("Invalid ISO 8601 datetime '{}': {}", s, e),
        hint: None,
    })?;
    let micros = dt.timestamp_micros();
    if micros < 0 {
        return Err(Error::InvalidInput {
            reason: format!("Datetime '{}' is before Unix epoch", s),
            hint: None,
        });
    }
    Ok(micros as u64)
}

/// Parse a TimeRangeInput into (start_micros, end_micros).
fn parse_time_range(input: &TimeRangeInput) -> Result<(u64, u64)> {
    let start = parse_iso8601_to_micros(&input.start)?;
    let end = parse_iso8601_to_micros(&input.end)?;
    if start > end {
        return Err(Error::InvalidInput {
            reason: "time_range.start must be <= time_range.end".into(),
            hint: None,
        });
    }
    Ok((start, end))
}

/// Handle Search command via the retrieval substrate.
pub fn search(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    sq: SearchQuery,
) -> Result<Output> {
    let core_branch_id = to_core_branch_id(&branch)?;

    // Parse time_range
    let time_range = sq.time_range.as_ref().map(parse_time_range).transpose()?;

    // Build primitive filter from string names
    let primitive_filter = sq.primitives.as_ref().map(|names| {
        names
            .iter()
            .filter_map(|name| match name.to_lowercase().as_str() {
                "kv" => Some(PrimitiveType::Kv),
                "json" => Some(PrimitiveType::Json),
                "event" => Some(PrimitiveType::Event),
                "branch" => Some(PrimitiveType::Branch),
                "vector" => Some(PrimitiveType::Vector),
                "graph" => Some(PrimitiveType::Graph),
                _ => None,
            })
            .collect::<Vec<_>>()
    });

    // ---- Build recipe from SearchQuery ----
    let mut recipe = builtin_defaults();

    // Map mode to recipe retrieve section
    let mode_str = sq.mode.as_deref().unwrap_or("hybrid");
    match mode_str {
        "keyword" => {
            // BM25 only — remove vector section
            if let Some(ref mut r) = recipe.retrieve {
                r.vector = None;
            }
        }
        _ => {
            // hybrid/vector — enable vector retrieval
            if let Some(ref mut r) = recipe.retrieve {
                r.vector = Some(VectorRetrieveConfig::default());
            } else {
                recipe.retrieve = Some(RetrieveConfig {
                    vector: Some(VectorRetrieveConfig::default()),
                    ..Default::default()
                });
            }
        }
    }

    // Map k to transform.limit
    if let Some(k) = sq.k {
        recipe.transform = Some(TransformConfig {
            limit: Some(k as usize),
            ..Default::default()
        });
    }

    // ---- Resolve recipe (three-level merge with branch default) ----
    let builtin = builtin_defaults();
    let branch_recipe = strata_engine::recipe_store::get_default_recipe(&p.db, core_branch_id)
        .map_err(|e| Error::Internal {
            reason: format!("Failed to get branch recipe: {}", e),
            hint: None,
        })?;
    let resolved = Recipe::resolve(&builtin, &branch_recipe, Some(&recipe));

    // ---- Embed query if needed ----
    let has_vector = resolved
        .retrieve
        .as_ref()
        .and_then(|r| r.vector.as_ref())
        .is_some();

    // Resolve embed model: recipe.models.embed → db.embed_model() fallback
    let embed_model = resolved
        .models
        .as_ref()
        .and_then(|m| m.embed.clone())
        .unwrap_or_else(|| p.db.embed_model());

    let embedding = if let Some(emb) = sq.precomputed_embedding {
        Some(emb)
    } else if has_vector {
        embed_query_with_model(&p.db, &sq.query, &embed_model)
    } else {
        None
    };

    let mode = if has_vector && embedding.is_some() {
        "hybrid"
    } else {
        "keyword"
    };

    // ---- Call substrate ----
    let request = RetrievalRequest {
        query: sq.query,
        branch_id: core_branch_id,
        space,
        recipe: resolved,
        embedding,
        time_range,
        primitive_filter,
    };

    let response = substrate::retrieve(&p.db, &request).map_err(|e| Error::Internal {
        reason: format!("Retrieval failed: {}", e),
        hint: None,
    })?;

    // ---- Convert to Output ----
    let hits: Vec<SearchResultHit> = response
        .hits
        .iter()
        .map(|hit| {
            let (entity, primitive) = format_entity_ref(&hit.doc_ref);
            SearchResultHit {
                entity,
                primitive,
                score: hit.score,
                rank: hit.rank,
                snippet: hit.snippet.clone(),
            }
        })
        .collect();

    // Surface embedding progress when auto-embed is active
    let embed_status = crate::handlers::embed_hook::embed_status(p);
    let (embedding_pending, embedding_total) =
        if embed_status.auto_embed && embed_status.pending > 0 {
            (
                Some(embed_status.pending as u64),
                Some(embed_status.total_queued),
            )
        } else {
            (None, None)
        };

    let mut candidates_by_primitive = std::collections::HashMap::new();
    for (stage_name, stage_stats) in &response.stats.stages {
        candidates_by_primitive.insert(stage_name.clone(), stage_stats.candidates);
    }

    let stats = SearchStatsOutput {
        elapsed_ms: response.stats.elapsed_ms,
        candidates_considered: candidates_by_primitive.values().sum(),
        candidates_by_primitive,
        index_used: true,
        truncated: response.stats.budget_exhausted,
        mode: mode.to_string(),
        expansion_used: false,
        rerank_used: false,
        expansion_model: None,
        rerank_model: None,
        embedding_pending,
        embedding_total,
    };

    Ok(Output::SearchResults { hits, stats })
}

// ============================================================================
// Intelligence layer: query embedding
// ============================================================================

#[cfg(feature = "embed")]
fn embed_query_with_model(
    db: &Arc<strata_engine::Database>,
    text: &str,
    model_spec: &str,
) -> Option<Vec<f32>> {
    strata_intelligence::embed::embed_query_with_model(db, text, model_spec)
}

#[cfg(not(feature = "embed"))]
fn embed_query_with_model(
    _db: &Arc<strata_engine::Database>,
    _text: &str,
    _model_spec: &str,
) -> Option<Vec<f32>> {
    None
}

/// Format an EntityRef into (entity_string, primitive_string) for display.
pub(crate) fn format_entity_ref(doc_ref: &strata_engine::search::EntityRef) -> (String, String) {
    match doc_ref {
        strata_engine::search::EntityRef::Kv { key, .. } => (key.clone(), "kv".to_string()),
        strata_engine::search::EntityRef::Json { doc_id, .. } => {
            (doc_id.clone(), "json".to_string())
        }
        strata_engine::search::EntityRef::Event { sequence, .. } => {
            (format!("seq:{}", sequence), "event".to_string())
        }
        strata_engine::search::EntityRef::Branch { branch_id } => {
            let uuid = uuid::Uuid::from_bytes(*branch_id.as_bytes());
            (uuid.to_string(), "branch".to_string())
        }
        strata_engine::search::EntityRef::Vector { key, .. } => (key.clone(), "vector".to_string()),
        strata_engine::search::EntityRef::Graph { key, .. } => (key.clone(), "graph".to_string()),
    }
}
