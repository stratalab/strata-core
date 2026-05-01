use std::sync::Arc;

use crate::bridge::Primitives;
use crate::output::EmbedStatusInfo;
use crate::{BranchId, Error, Output, Result};

#[cfg(feature = "embed")]
pub(crate) use strata_intelligence::embed::runtime::{SHADOW_EVENT, SHADOW_JSON, SHADOW_KV};

#[cfg(not(feature = "embed"))]
pub(crate) const SHADOW_KV: &str = strata_engine::database::SHADOW_KV;
#[cfg(not(feature = "embed"))]
pub(crate) const SHADOW_JSON: &str = strata_engine::database::SHADOW_JSON;
#[cfg(not(feature = "embed"))]
pub(crate) const SHADOW_EVENT: &str = strata_engine::database::SHADOW_EVENT;

pub(crate) fn maybe_embed_text(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    shadow_collection: &'static str,
    key: &str,
    text: &str,
    source_ref: strata_core::EntityRef,
) {
    #[cfg(feature = "embed")]
    strata_intelligence::embed::runtime::maybe_embed_text(
        &primitives.db,
        branch_id,
        space,
        shadow_collection,
        key,
        text,
        source_ref,
    );

    #[cfg(not(feature = "embed"))]
    let _ = (
        primitives,
        branch_id,
        space,
        shadow_collection,
        key,
        text,
        source_ref,
    );
}

pub(crate) fn flush_embed_buffer(primitives: &Arc<Primitives>) {
    #[cfg(feature = "embed")]
    strata_intelligence::embed::runtime::flush_embed_buffer(&primitives.db);

    #[cfg(not(feature = "embed"))]
    let _ = primitives;
}

pub(crate) fn embed_status(primitives: &Arc<Primitives>) -> EmbedStatusInfo {
    #[cfg(feature = "embed")]
    {
        let status = strata_intelligence::embed::runtime::embed_status(&primitives.db);
        EmbedStatusInfo {
            auto_embed: status.auto_embed,
            batch_size: status.batch_size,
            pending: status.pending,
            total_queued: status.total_queued,
            total_embedded: status.total_embedded,
            total_failed: status.total_failed,
            scheduler_queue_depth: status.scheduler_queue_depth,
            scheduler_active_tasks: status.scheduler_active_tasks,
        }
    }

    #[cfg(not(feature = "embed"))]
    {
        let stats = primitives.db.scheduler().stats();
        EmbedStatusInfo {
            auto_embed: false,
            batch_size: primitives.db.embed_batch_size(),
            pending: 0,
            total_queued: 0,
            total_embedded: 0,
            total_failed: 0,
            scheduler_queue_depth: stats.queue_depth,
            scheduler_active_tasks: stats.active_tasks,
        }
    }
}

pub(crate) fn maybe_remove_embedding(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    maybe_remove_embedding_impl(&primitives.db, branch_id, space, shadow_collection, key);
}

pub(crate) fn delete_shadow_embeddings_for_space(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::BranchId,
    target_space: &str,
) -> usize {
    delete_shadow_embeddings_for_space_impl(&primitives.db, branch_id, target_space)
}

#[cfg(feature = "embed")]
fn maybe_remove_embedding_impl(
    db: &Arc<strata_engine::Database>,
    branch_id: strata_core::BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    strata_intelligence::embed::runtime::maybe_remove_embedding(
        db,
        branch_id,
        space,
        shadow_collection,
        key,
    );
}

#[cfg(not(feature = "embed"))]
fn maybe_remove_embedding_impl(
    db: &Arc<strata_engine::Database>,
    branch_id: strata_core::BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    strata_intelligence::shadow::maybe_remove_embedding(
        db,
        branch_id,
        space,
        shadow_collection,
        key,
    );
}

#[cfg(feature = "embed")]
fn delete_shadow_embeddings_for_space_impl(
    db: &Arc<strata_engine::Database>,
    branch_id: strata_core::BranchId,
    target_space: &str,
) -> usize {
    strata_intelligence::embed::runtime::delete_shadow_embeddings_for_space(
        db,
        branch_id,
        target_space,
    )
}

#[cfg(not(feature = "embed"))]
fn delete_shadow_embeddings_for_space_impl(
    db: &Arc<strata_engine::Database>,
    branch_id: strata_core::BranchId,
    target_space: &str,
) -> usize {
    strata_intelligence::shadow::delete_shadow_embeddings_for_space(db, branch_id, target_space)
}

pub(crate) fn extract_text(value: &strata_core::Value) -> Option<String> {
    #[cfg(feature = "embed")]
    {
        strata_intelligence::embed::runtime::extract_text(value)
    }

    #[cfg(not(feature = "embed"))]
    {
        let _ = value;
        None
    }
}

pub(crate) fn reindex_embeddings(primitives: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    #[cfg(feature = "embed")]
    {
        let branch_id = crate::bridge::to_core_branch_id(&branch)?;
        let stats = strata_intelligence::embed::runtime::reindex_embeddings(&primitives.db, branch_id)
            .map_err(|error| Error::Internal {
                reason: error.to_string(),
                hint: Some(
                    "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                        .to_string(),
                ),
            })?;
        Ok(Output::ReindexResult {
            kv_queued: stats.kv_queued,
            json_queued: stats.json_queued,
            event_queued: stats.event_queued,
            new_dimension: stats.new_dimension,
        })
    }

    #[cfg(not(feature = "embed"))]
    {
        let _ = (primitives, branch);
        Err(Error::Internal {
            reason: "The 'embed' feature is not enabled. Rebuild with --features embed".into(),
            hint: Some(
                "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                    .to_string(),
            ),
        })
    }
}
