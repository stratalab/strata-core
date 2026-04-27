use std::sync::Arc;

use serde_json::Value as SerdeValue;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_core::{EntityRef, StrataError, StrataResult};
pub use strata_engine::database::{SHADOW_EVENT, SHADOW_JSON, SHADOW_KV};
use strata_engine::{
    Database, EventLog, JsonPath, JsonStore, JsonValue, KVStore, SpaceIndex, TaskPriority,
    VectorConfig,
};
use strata_vector::VectorStore;

use super::{resolve_api_key_for_model, EmbedModelState};

const SHADOW_KEY_SEP: char = '\x1f';
const EMBED_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

pub struct EmbedRuntimeStatus {
    pub auto_embed: bool,
    pub batch_size: usize,
    pub pending: usize,
    pub total_queued: u64,
    pub total_embedded: u64,
    pub total_failed: u64,
    pub scheduler_queue_depth: usize,
    pub scheduler_active_tasks: usize,
}

pub struct ReindexStats {
    pub kv_queued: u64,
    pub json_queued: u64,
    pub event_queued: u64,
    pub new_dimension: usize,
}

pub struct AutoEmbedState {
    shadow_collections_created: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl Default for AutoEmbedState {
    fn default() -> Self {
        Self {
            shadow_collections_created: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
}

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

#[derive(Default)]
struct EmbedRefreshRuntime {
    started: std::sync::atomic::AtomicBool,
}

struct PendingEmbed {
    branch_id: BranchId,
    space: String,
    shadow_collection: &'static str,
    key: String,
    text: String,
    source_ref: EntityRef,
}

pub struct EmbedBuffer {
    pending: std::sync::Mutex<Vec<PendingEmbed>>,
    flush_lock: std::sync::Mutex<()>,
    total_queued: std::sync::atomic::AtomicU64,
    total_embedded: std::sync::atomic::AtomicU64,
    total_failed: std::sync::atomic::AtomicU64,
}

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

pub fn extract_text(value: &Value) -> Option<String> {
    crate::embed::extract::extract_text(value)
}

pub fn maybe_embed_text(
    db: &Arc<Database>,
    branch_id: BranchId,
    space: &str,
    shadow_collection: &'static str,
    key: &str,
    text: &str,
    source_ref: EntityRef,
) {
    if !db.auto_embed_enabled() {
        return;
    }
    queue_embed_text(
        db,
        branch_id,
        space,
        shadow_collection,
        key,
        text,
        source_ref,
    );
}

fn queue_embed_text(
    db: &Arc<Database>,
    branch_id: BranchId,
    space: &str,
    shadow_collection: &'static str,
    key: &str,
    text: &str,
    source_ref: EntityRef,
) {
    ensure_refresh_runtime(db);

    let buffer = match db.extension::<EmbedBuffer>() {
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
        pending.len() >= db.embed_batch_size()
    };

    if should_flush {
        schedule_embed_flush(db, true);
    }
}

pub fn flush_embed_buffer(db: &Arc<Database>) {
    let buffer = match db.extension::<EmbedBuffer>() {
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
    let model_dir = db.model_dir();
    let embed_state = match db.extension::<EmbedModelState>() {
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

    let model_name = db.embed_model();
    let api_key = resolve_api_key_for_model(db, &model_name);
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

    let vector = VectorStore::new(db.clone());
    for (pending, embedding) in batch.into_iter().zip(embeddings.iter()) {
        ensure_shadow_collection(db, pending.branch_id, pending.shadow_collection);

        let composite_key = format!("{}{}{}", pending.space, SHADOW_KEY_SEP, pending.key);
        let metadata = serde_json::json!({
            "source_space": pending.space,
            "source_key": pending.key,
        });

        let _ = vector.system_insert_with_source(
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

fn ensure_refresh_runtime(db: &Arc<Database>) {
    let runtime = match db.extension::<EmbedRefreshRuntime>() {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::warn!(
                target: "strata::embed",
                error = %error,
                "Failed to acquire embed refresh runtime"
            );
            return;
        }
    };

    if runtime
        .started
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .is_err()
    {
        return;
    }

    let db = Arc::downgrade(db);
    if let Err(error) = std::thread::Builder::new()
        .name("strata-embed-refresh".into())
        .spawn(move || embed_refresh_loop(db))
    {
        runtime
            .started
            .store(false, std::sync::atomic::Ordering::Release);
        tracing::warn!(
            target: "strata::embed",
            error = %error,
            "Failed to spawn embed refresh thread"
        );
    }
}

fn embed_refresh_loop(db: std::sync::Weak<Database>) {
    loop {
        std::thread::sleep(EMBED_REFRESH_INTERVAL);

        let Some(db) = db.upgrade() else {
            break;
        };
        if !db.auto_embed_enabled() {
            continue;
        }

        let Ok(buffer) = db.extension::<EmbedBuffer>() else {
            continue;
        };
        let has_pending = !buffer
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty();
        if !has_pending {
            continue;
        }

        schedule_embed_flush(&db, false);
    }
}

fn schedule_embed_flush(db: &Arc<Database>, allow_inline_fallback: bool) {
    let flush_db = Arc::clone(db);
    let submitted = db
        .scheduler()
        .submit(TaskPriority::Normal, move || flush_embed_buffer(&flush_db))
        .is_ok();

    if !submitted && allow_inline_fallback {
        flush_embed_buffer(db);
    }
}

pub fn embed_status(db: &Arc<Database>) -> EmbedRuntimeStatus {
    let (pending, total_queued, total_embedded, total_failed) = match db.extension::<EmbedBuffer>()
    {
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

    let stats = db.scheduler().stats();
    EmbedRuntimeStatus {
        auto_embed: db.auto_embed_enabled(),
        batch_size: db.embed_batch_size(),
        pending,
        total_queued,
        total_embedded,
        total_failed,
        scheduler_queue_depth: stats.queue_depth,
        scheduler_active_tasks: stats.active_tasks,
    }
}

pub fn maybe_remove_embedding(
    db: &Arc<Database>,
    branch_id: BranchId,
    space: &str,
    shadow_collection: &str,
    key: &str,
) {
    if let Ok(buffer) = db.extension::<EmbedBuffer>() {
        let mut pending = buffer.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|entry| {
            !(entry.branch_id == branch_id
                && entry.shadow_collection == shadow_collection
                && entry.space == space
                && entry.key == key)
        });
    }

    let composite_key = format!("{}{}{}", space, SHADOW_KEY_SEP, key);
    let vector = VectorStore::new(db.clone());
    if let Err(error) = vector.system_delete(branch_id, shadow_collection, &composite_key) {
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

pub fn delete_shadow_embeddings_for_space(
    db: &Arc<Database>,
    branch_id: BranchId,
    target_space: &str,
) -> usize {
    if let Ok(buffer) = db.extension::<EmbedBuffer>() {
        let mut pending = buffer.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|entry| !(entry.branch_id == branch_id && entry.space == target_space));
    }

    let prefix = format!("{target_space}\x1f");
    let vector = VectorStore::new(db.clone());
    let mut total = 0usize;
    for shadow in [SHADOW_KV, SHADOW_JSON, SHADOW_EVENT] {
        let keys =
            match vector.list_keys(branch_id, strata_engine::system_space::SYSTEM_SPACE, shadow) {
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
            match vector.system_delete(branch_id, shadow, &key) {
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

pub fn reindex_embeddings(db: &Arc<Database>, branch_id: BranchId) -> StrataResult<ReindexStats> {
    let model_state = db.extension::<EmbedModelState>().map_err(|error| {
        StrataError::internal(format!("Failed to get embed model state: {error}"))
    })?;
    let dim = model_state.embedding_dim().ok_or_else(|| {
        StrataError::internal("Embedding model not loaded — cannot determine dimension")
    })?;

    flush_embed_buffer(db);

    let vector = VectorStore::new(db.clone());
    for name in [SHADOW_KV, SHADOW_JSON, SHADOW_EVENT] {
        if let Err(error) = vector.delete_system_collection(branch_id, name) {
            tracing::warn!(
                target: "strata::embed",
                collection = name,
                error = %error,
                "Failed to delete shadow collection during reindex"
            );
        }
    }

    if let Ok(state) = db.extension::<AutoEmbedState>() {
        let branch_prefix = format!("{:?}{}", branch_id.as_bytes(), SHADOW_KEY_SEP);
        state.remove_branch(&branch_prefix);
    }

    let spaces = SpaceIndex::new(db.clone())
        .list(branch_id)
        .unwrap_or_else(|_| vec!["default".to_string()]);
    let kv = KVStore::new(db.clone());
    let json = JsonStore::new(db.clone());
    let event = EventLog::new(db.clone());

    let mut kv_queued = 0u64;
    let mut json_queued = 0u64;
    let mut event_queued = 0u64;

    for space in &spaces {
        if let Ok(keys) = kv.list(&branch_id, space, None) {
            for key in &keys {
                if let Ok(Some(value)) = kv.get(&branch_id, space, key) {
                    if let Some(text) = extract_text(&value) {
                        queue_embed_text(
                            db,
                            branch_id,
                            space,
                            SHADOW_KV,
                            key,
                            &text,
                            EntityRef::kv(branch_id, space, key),
                        );
                        kv_queued += 1;
                    }
                }
            }
        }

        let mut cursor: Option<String> = None;
        loop {
            let result = match json.list(&branch_id, space, None, cursor.as_deref(), 1000) {
                Ok(result) => result,
                Err(_) => break,
            };
            for doc_id in &result.doc_ids {
                if let Ok(Some(json_value)) = json.get(&branch_id, space, doc_id, &JsonPath::root())
                {
                    if let Ok(value) = json_to_value(json_value) {
                        if let Some(text) = extract_text(&value) {
                            queue_embed_text(
                                db,
                                branch_id,
                                space,
                                SHADOW_JSON,
                                doc_id,
                                &text,
                                EntityRef::json(branch_id, space, doc_id),
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

        if let Ok(len) = event.len(&branch_id, space) {
            for seq in 0..len {
                if let Ok(Some(versioned_event)) = event.get(&branch_id, space, seq) {
                    let payload = Value::String(
                        serde_json::to_string(&versioned_event.value.payload).unwrap_or_default(),
                    );
                    if let Some(text) = extract_text(&payload) {
                        let event_key = seq.to_string();
                        queue_embed_text(
                            db,
                            branch_id,
                            space,
                            SHADOW_EVENT,
                            &event_key,
                            &text,
                            EntityRef::event(branch_id, space, seq),
                        );
                        event_queued += 1;
                    }
                }
            }
        }
    }

    schedule_embed_flush(db, false);

    tracing::info!(
        target: "strata::embed",
        kv_queued,
        json_queued,
        event_queued,
        new_dimension = dim,
        "Reindex embeddings started"
    );

    Ok(ReindexStats {
        kv_queued,
        json_queued,
        event_queued,
        new_dimension: dim,
    })
}

fn ensure_shadow_collection(db: &Arc<Database>, branch_id: BranchId, name: &str) {
    let cache_key = format!("{:?}{}{}", branch_id.as_bytes(), SHADOW_KEY_SEP, name);
    let state = match db.extension::<AutoEmbedState>() {
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

    let dim = db
        .extension::<EmbedModelState>()
        .ok()
        .and_then(|state| state.embedding_dim())
        .unwrap_or(384);
    let config = VectorConfig::for_embedding(dim);
    let vector = VectorStore::new(db.clone());

    match vector.create_system_collection(branch_id, name, config) {
        Ok(_) => state.insert(cache_key),
        Err(strata_vector::VectorError::CollectionAlreadyExists { .. }) => {
            match vector.system_collection_dimension(branch_id, name) {
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

fn json_to_value(json: JsonValue) -> StrataResult<Value> {
    let serde_value =
        serde_json::to_value(json).map_err(|e| StrataError::serialization(e.to_string()))?;
    serde_json_to_value(serde_value)
}

fn serde_json_to_value(json: SerdeValue) -> StrataResult<Value> {
    match json {
        SerdeValue::Null => Ok(Value::Null),
        SerdeValue::Bool(value) => Ok(Value::Bool(value)),
        SerdeValue::Number(value) => value
            .as_i64()
            .map(Value::Int)
            .or_else(|| value.as_f64().map(Value::Float))
            .ok_or_else(|| StrataError::serialization("unsupported JSON number")),
        SerdeValue::String(value) => Ok(Value::String(value)),
        SerdeValue::Array(values) => values
            .into_iter()
            .map(serde_json_to_value)
            .collect::<StrataResult<Vec<_>>>()
            .map(|values| Value::Array(Box::new(values))),
        SerdeValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, serde_json_to_value(value)?)))
            .collect::<StrataResult<std::collections::HashMap<_, _>>>()
            .map(|values| Value::Object(Box::new(values))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_engine::database::OpenSpec;
    use strata_engine::SearchSubsystem;
    use strata_vector::VectorSubsystem;

    fn test_db() -> Arc<Database> {
        let db = Database::open_runtime(
            OpenSpec::cache()
                .with_subsystem(strata_graph::GraphSubsystem)
                .with_subsystem(VectorSubsystem)
                .with_subsystem(SearchSubsystem),
        )
        .unwrap();
        db.set_auto_embed(true);
        db
    }

    #[test]
    fn embed_status_reflects_buffer_updates() {
        let db = test_db();
        let branch_id = BranchId::default();

        maybe_embed_text(
            &db,
            branch_id,
            "default",
            SHADOW_KV,
            "queued",
            "hello world",
            EntityRef::kv(branch_id, "default", "queued"),
        );

        let status = embed_status(&db);
        assert!(status.auto_embed);
        assert_eq!(status.pending, 1);
        assert_eq!(status.total_queued, 1);
    }

    #[test]
    fn removing_embedding_drains_matching_pending_entry() {
        let db = test_db();
        let branch_id = BranchId::default();

        maybe_embed_text(
            &db,
            branch_id,
            "default",
            SHADOW_KV,
            "queued",
            "hello world",
            EntityRef::kv(branch_id, "default", "queued"),
        );

        maybe_remove_embedding(&db, branch_id, "default", SHADOW_KV, "queued");

        let status = embed_status(&db);
        assert_eq!(status.pending, 0);
        assert_eq!(status.total_queued, 1);
    }
}
