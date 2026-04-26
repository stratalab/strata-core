use std::sync::Arc;

use crate::bridge::Primitives;
use crate::output::EmbedStatusInfo;
use crate::{BranchId, Error, Output, Result};

pub(crate) use strata_engine::database::{SHADOW_EVENT, SHADOW_JSON, SHADOW_KV};

#[cfg(feature = "embed")]
const SHADOW_KEY_SEP: char = '\x1f';

#[cfg(feature = "embed")]
pub(crate) struct AutoEmbedState {
    shadow_collections_created: std::sync::Mutex<std::collections::HashSet<String>>,
}

#[cfg(feature = "embed")]
impl Default for AutoEmbedState {
    fn default() -> Self {
        Self {
            shadow_collections_created: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
}

#[cfg(feature = "embed")]
impl AutoEmbedState {
    fn contains(&self, key: &str) -> bool {
        self.shadow_collections_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(key)
    }

    fn insert(&self, key: String) {
        self.shadow_collections_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key);
    }

    fn remove_branch(&self, branch_prefix: &str) {
        self.shadow_collections_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|key| !key.starts_with(branch_prefix));
    }
}

#[cfg(feature = "embed")]
struct PendingEmbed {
    branch_id: strata_core::types::BranchId,
    space: String,
    shadow_collection: &'static str,
    key: String,
    text: String,
    source_ref: strata_core::EntityRef,
}

#[cfg(feature = "embed")]
pub(crate) struct EmbedBuffer {
    pending: std::sync::Mutex<Vec<PendingEmbed>>,
    flush_lock: std::sync::Mutex<()>,
    total_queued: std::sync::atomic::AtomicU64,
    total_embedded: std::sync::atomic::AtomicU64,
    total_failed: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "embed")]
impl Default for EmbedBuffer {
    fn default() -> Self {
        Self {
            pending: std::sync::Mutex::new(Vec::with_capacity(64)),
            flush_lock: std::sync::Mutex::new(()),
            total_queued: std::sync::atomic::AtomicU64::new(0),
            total_embedded: std::sync::atomic::AtomicU64::new(0),
            total_failed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "embed")]
pub(crate) fn maybe_embed_text(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    shadow_collection: &'static str,
    key: &str,
    text: &str,
    source_ref: strata_core::EntityRef,
) {
    if !primitives.db.auto_embed_enabled() {
        return;
    }
    queue_embed_text(
        primitives,
        branch_id,
        space,
        shadow_collection,
        key,
        text,
        source_ref,
    );
}

#[cfg(not(feature = "embed"))]
pub(crate) fn maybe_embed_text(
    _primitives: &Arc<Primitives>,
    _branch_id: strata_core::types::BranchId,
    _space: &str,
    _shadow_collection: &'static str,
    _key: &str,
    _text: &str,
    _source_ref: strata_core::EntityRef,
) {
}

#[cfg(feature = "embed")]
fn queue_embed_text(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    shadow_collection: &'static str,
    key: &str,
    text: &str,
    source_ref: strata_core::EntityRef,
) {
    let buffer = match primitives.db.extension::<EmbedBuffer>() {
        Ok(buffer) => buffer,
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                error = %error,
                "Failed to acquire embed buffer"
            );
            return;
        }
    };

    let should_flush = {
        let mut pending = buffer.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.push(PendingEmbed {
            branch_id,
            space: space.to_owned(),
            shadow_collection,
            key: key.to_owned(),
            text: text.to_owned(),
            source_ref,
        });
        buffer
            .total_queued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        pending.len() >= primitives.db.embed_batch_size()
    };

    if should_flush {
        let primitives = Arc::clone(primitives);
        let flush_primitives = Arc::clone(&primitives);
        if primitives
            .db
            .scheduler()
            .submit(strata_engine::TaskPriority::Normal, move || {
                flush_embed_buffer(&flush_primitives)
            })
            .is_err()
        {
            flush_embed_buffer(&primitives);
        }
    }
}

#[cfg(feature = "embed")]
pub(crate) fn flush_embed_buffer(primitives: &Arc<Primitives>) {
    use strata_intelligence::embed::EmbedModelState;

    let buffer = match primitives.db.extension::<EmbedBuffer>() {
        Ok(buffer) => buffer,
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                error = %error,
                "Failed to acquire embed buffer for flush"
            );
            return;
        }
    };

    let _flush_guard = buffer.flush_lock.lock().unwrap_or_else(|e| e.into_inner());

    let batch = {
        let mut pending = buffer.pending.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *pending)
    };

    if batch.is_empty() {
        return;
    }

    let batch_len = batch.len() as u64;
    let model_dir = primitives.db.model_dir();
    let embed_state = match primitives.db.extension::<EmbedModelState>() {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                error = %error,
                "Failed to acquire embed model state"
            );
            buffer
                .total_failed
                .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };

    let model_name = primitives.db.embed_model();
    let api_key =
        strata_intelligence::embed::resolve_api_key_for_model(&primitives.db, &model_name);
    let shared = match embed_state.get_or_load(&model_dir, &model_name, api_key.as_deref()) {
        Ok(shared) => shared,
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                error = %error,
                "Failed to load embedding model"
            );
            buffer
                .total_failed
                .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };

    let texts: Vec<&str> = batch.iter().map(|pending| pending.text.as_str()).collect();
    let embeddings = {
        let engine = shared.lock().unwrap_or_else(|e| e.into_inner());
        match engine.embed_batch(&texts) {
            Ok(embeddings) => embeddings,
            Err(error) => {
                tracing::warn!(
                    target: "strata::embed",
                    error = %error,
                    "Batch embedding failed"
                );
                buffer
                    .total_failed
                    .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
    };

    for (pending, embedding) in batch.into_iter().zip(embeddings.iter()) {
        ensure_shadow_collection(primitives, pending.branch_id, pending.shadow_collection);

        let composite_key = format!("{}{}{}", pending.space, SHADOW_KEY_SEP, pending.key);
        let metadata = serde_json::json!({
            "source_space": pending.space,
            "source_key": pending.key,
        });

        let _ = primitives.vector.system_insert_with_source(
            pending.branch_id,
            pending.shadow_collection,
            &composite_key,
            embedding,
            Some(metadata),
            pending.source_ref,
        );
    }

    buffer
        .total_embedded
        .fetch_add(batch_len, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(feature = "embed"))]
pub(crate) fn flush_embed_buffer(_primitives: &Arc<Primitives>) {}

#[cfg(feature = "embed")]
pub(crate) fn embed_status(primitives: &Arc<Primitives>) -> EmbedStatusInfo {
    let (pending, total_queued, total_embedded, total_failed) =
        match primitives.db.extension::<EmbedBuffer>() {
            Ok(buffer) => {
                let pending = buffer
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len();
                let total_queued = buffer
                    .total_queued
                    .load(std::sync::atomic::Ordering::Relaxed);
                let total_embedded = buffer
                    .total_embedded
                    .load(std::sync::atomic::Ordering::Relaxed);
                let total_failed = buffer
                    .total_failed
                    .load(std::sync::atomic::Ordering::Relaxed);
                (pending, total_queued, total_embedded, total_failed)
            }
            Err(_) => (0, 0, 0, 0),
        };

    let stats = primitives.db.scheduler().stats();
    EmbedStatusInfo {
        auto_embed: primitives.db.auto_embed_enabled(),
        batch_size: primitives.db.embed_batch_size(),
        pending,
        total_queued,
        total_embedded,
        total_failed,
        scheduler_queue_depth: stats.queue_depth,
        scheduler_active_tasks: stats.active_tasks,
    }
}

#[cfg(not(feature = "embed"))]
pub(crate) fn embed_status(primitives: &Arc<Primitives>) -> EmbedStatusInfo {
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

#[cfg(feature = "embed")]
pub(crate) fn maybe_remove_embedding(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    if let Ok(buffer) = primitives.db.extension::<EmbedBuffer>() {
        let mut pending = buffer.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|entry| {
            !(entry.branch_id == branch_id
                && entry.shadow_collection == shadow_collection
                && entry.space == space
                && entry.key == key)
        });
    }

    let composite_key = format!("{}{}{}", space, SHADOW_KEY_SEP, key);
    if let Err(error) =
        primitives
            .vector
            .system_delete(branch_id, shadow_collection, &composite_key)
    {
        if !error.is_not_found() {
            tracing::warn!(
                target: "strata::embed",
                collection = shadow_collection,
                key = composite_key,
                error = %error,
                "Failed to remove shadow embedding"
            );
        }
    }
}

#[cfg(not(feature = "embed"))]
pub(crate) fn maybe_remove_embedding(
    _primitives: &Arc<Primitives>,
    _branch_id: strata_core::types::BranchId,
    _space: &str,
    _shadow_collection: &str,
    _key: &str,
) {
}

pub(crate) fn delete_shadow_embeddings_for_space(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    target_space: &str,
) -> usize {
    #[cfg(feature = "embed")]
    if let Ok(buffer) = primitives.db.extension::<EmbedBuffer>() {
        let mut pending = buffer.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|entry| !(entry.branch_id == branch_id && entry.space == target_space));
    }

    let prefix = format!("{target_space}\x1f");
    let mut total = 0usize;
    for shadow in [SHADOW_KV, SHADOW_JSON, SHADOW_EVENT] {
        let keys = match primitives.vector.list_keys(
            branch_id,
            strata_engine::system_space::SYSTEM_SPACE,
            shadow,
        ) {
            Ok(keys) => keys,
            Err(error) if error.is_not_found() => continue,
            Err(error) => {
                tracing::warn!(
                    target: "strata::embed",
                    shadow = shadow,
                    error = %error,
                    "Failed to list shadow keys during space cleanup"
                );
                continue;
            }
        };

        for key in keys {
            if !key.starts_with(&prefix) {
                continue;
            }
            match primitives.vector.system_delete(branch_id, shadow, &key) {
                Ok(_) => total += 1,
                Err(error) if error.is_not_found() => {}
                Err(error) => {
                    tracing::warn!(
                        target: "strata::embed",
                        shadow = shadow,
                        key = key.as_str(),
                        error = %error,
                        "Failed to delete shadow embedding during space cleanup"
                    );
                }
            }
        }
    }

    total
}

#[cfg(feature = "embed")]
pub(crate) fn extract_text(value: &strata_core::Value) -> Option<String> {
    strata_intelligence::embed::extract::extract_text(value)
}

#[cfg(not(feature = "embed"))]
pub(crate) fn extract_text(_value: &strata_core::Value) -> Option<String> {
    None
}

#[cfg(feature = "embed")]
pub(crate) fn reindex_embeddings(primitives: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    use crate::bridge::{json_to_value, to_core_branch_id};
    use strata_core::primitives::json::JsonPath;
    use strata_intelligence::embed::EmbedModelState;

    let branch_id = to_core_branch_id(&branch)?;
    let model_state = primitives
        .db
        .extension::<EmbedModelState>()
        .map_err(|error| Error::Internal {
            reason: format!("Failed to get embed model state: {error}"),
            hint: Some(
                "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                    .to_string(),
            ),
        })?;
    let dim = model_state.embedding_dim().ok_or_else(|| Error::Internal {
        reason: "Embedding model not loaded — cannot determine dimension".into(),
        hint: Some(
            "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                .to_string(),
        ),
    })?;

    flush_embed_buffer(primitives);

    for name in [SHADOW_KV, SHADOW_JSON, SHADOW_EVENT] {
        if let Err(error) = primitives.vector.delete_system_collection(branch_id, name) {
            tracing::warn!(
                target: "strata::embed",
                collection = name,
                error = %error,
                "Failed to delete shadow collection during reindex"
            );
        }
    }

    if let Ok(state) = primitives.db.extension::<AutoEmbedState>() {
        let branch_prefix = format!("{:?}{}", branch_id.as_bytes(), SHADOW_KEY_SEP);
        state.remove_branch(&branch_prefix);
    }

    let spaces = primitives
        .space
        .list(branch_id)
        .unwrap_or_else(|_| vec!["default".to_string()]);

    let mut kv_queued = 0u64;
    let mut json_queued = 0u64;
    let mut event_queued = 0u64;

    for space in &spaces {
        if let Ok(keys) = primitives.kv.list(&branch_id, space, None) {
            for key in &keys {
                if let Ok(Some(value)) = primitives.kv.get(&branch_id, space, key) {
                    if let Some(text) = extract_text(&value) {
                        queue_embed_text(
                            primitives,
                            branch_id,
                            space,
                            SHADOW_KV,
                            key,
                            &text,
                            strata_core::EntityRef::kv(branch_id, space, key),
                        );
                        kv_queued += 1;
                    }
                }
            }
        }

        let mut cursor: Option<String> = None;
        loop {
            let result =
                match primitives
                    .json
                    .list(&branch_id, space, None, cursor.as_deref(), 1000)
                {
                    Ok(result) => result,
                    Err(_) => break,
                };
            for doc_id in &result.doc_ids {
                if let Ok(Some(json_value)) =
                    primitives
                        .json
                        .get(&branch_id, space, doc_id, &JsonPath::root())
                {
                    if let Ok(value) = json_to_value(json_value) {
                        if let Some(text) = extract_text(&value) {
                            queue_embed_text(
                                primitives,
                                branch_id,
                                space,
                                SHADOW_JSON,
                                doc_id,
                                &text,
                                strata_core::EntityRef::json(branch_id, space, doc_id),
                            );
                            json_queued += 1;
                        }
                    }
                }
            }
            match result.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        if let Ok(len) = primitives.event.len(&branch_id, space) {
            for seq in 0..len {
                if let Ok(Some(versioned_event)) = primitives.event.get(&branch_id, space, seq) {
                    let payload = strata_core::Value::String(
                        serde_json::to_string(&versioned_event.value.payload).unwrap_or_default(),
                    );
                    if let Some(text) = extract_text(&payload) {
                        let event_key = seq.to_string();
                        queue_embed_text(
                            primitives,
                            branch_id,
                            space,
                            SHADOW_EVENT,
                            &event_key,
                            &text,
                            strata_core::EntityRef::event(branch_id, space, seq),
                        );
                        event_queued += 1;
                    }
                }
            }
        }
    }

    let primitives = Arc::clone(primitives);
    let flush_primitives = Arc::clone(&primitives);
    let _ = primitives
        .db
        .scheduler()
        .submit(strata_engine::TaskPriority::Normal, move || {
            flush_embed_buffer(&flush_primitives)
        });

    tracing::info!(
        target: "strata::embed",
        kv_queued,
        json_queued,
        event_queued,
        new_dimension = dim,
        "Reindex embeddings started"
    );

    Ok(Output::ReindexResult {
        kv_queued,
        json_queued,
        event_queued,
        new_dimension: dim,
    })
}

#[cfg(not(feature = "embed"))]
pub(crate) fn reindex_embeddings(
    _primitives: &Arc<Primitives>,
    _branch: BranchId,
) -> Result<Output> {
    Err(Error::Internal {
        reason: "The 'embed' feature is not enabled. Rebuild with --features embed".into(),
        hint: Some(
            "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues"
                .to_string(),
        ),
    })
}

#[cfg(feature = "embed")]
fn ensure_shadow_collection(
    primitives: &Arc<Primitives>,
    branch_id: strata_core::types::BranchId,
    name: &str,
) {
    use strata_core::primitives::vector::VectorConfig;

    let cache_key = format!("{:?}{}{}", branch_id.as_bytes(), SHADOW_KEY_SEP, name);
    let state = match primitives.db.extension::<AutoEmbedState>() {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                error = %error,
                "Failed to acquire auto-embed state"
            );
            return;
        }
    };

    if state.contains(&cache_key) {
        return;
    }

    let dim = primitives
        .db
        .extension::<strata_intelligence::embed::EmbedModelState>()
        .ok()
        .and_then(|state| state.embedding_dim())
        .unwrap_or(384);
    let config = VectorConfig::for_embedding(dim);

    match primitives
        .vector
        .create_system_collection(branch_id, name, config)
    {
        Ok(_) => state.insert(cache_key),
        Err(strata_vector::VectorError::CollectionAlreadyExists { .. }) => {
            match primitives
                .vector
                .system_collection_dimension(branch_id, name)
            {
                Ok(Some(existing_dim)) if existing_dim != dim => {
                    tracing::error!(
                        target: "strata::embed",
                        collection = name,
                        existing_dim,
                        model_dim = dim,
                        "Shadow collection dimension does not match the configured embedding model"
                    );
                }
                _ => state.insert(cache_key),
            }
        }
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                collection = name,
                error = %error,
                "Failed to create shadow collection"
            );
        }
    }
}

#[cfg(all(test, feature = "embed"))]
mod tests {
    use super::*;
    use strata_engine::database::OpenSpec;
    use strata_engine::{Database, SearchSubsystem};
    use strata_vector::VectorSubsystem;

    fn test_primitives() -> Arc<Primitives> {
        let db = Database::open_runtime(
            OpenSpec::cache()
                .with_subsystem(strata_graph::GraphSubsystem)
                .with_subsystem(VectorSubsystem)
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        db.set_auto_embed(true);
        Arc::new(Primitives::new(db))
    }

    #[test]
    fn embed_status_reflects_local_buffer_updates() {
        let primitives = test_primitives();
        let branch_id = strata_core::types::BranchId::default();

        maybe_embed_text(
            &primitives,
            branch_id,
            "default",
            SHADOW_KV,
            "queued",
            "hello world",
            strata_core::EntityRef::kv(branch_id, "default", "queued"),
        );

        let status = embed_status(&primitives);
        assert!(status.auto_embed);
        assert_eq!(status.pending, 1);
        assert_eq!(status.total_queued, 1);
    }

    #[test]
    fn removing_embedding_drains_matching_pending_entry() {
        let primitives = test_primitives();
        let branch_id = strata_core::types::BranchId::default();

        maybe_embed_text(
            &primitives,
            branch_id,
            "default",
            SHADOW_KV,
            "queued",
            "hello world",
            strata_core::EntityRef::kv(branch_id, "default", "queued"),
        );

        maybe_remove_embedding(&primitives, branch_id, "default", SHADOW_KV, "queued");

        let status = embed_status(&primitives);
        assert_eq!(status.pending, 0);
        assert_eq!(status.total_queued, 1);
    }
}
